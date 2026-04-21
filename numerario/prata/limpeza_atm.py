"""
Limpeza e promoção de bronze.abastecimento_atm_raw para prata.abastecimento_atm.

Uso:
    python limpeza_atm.py
    python limpeza_atm.py --batch-size 5000
"""

import argparse
import logging
import os
import re

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)


def _get_engine():
    url = (
        f"postgresql+psycopg2://{os.environ['DB_USER']}:{os.environ['DB_PASSWORD']}"
        f"@{os.environ['DB_HOST']}:{os.environ.get('DB_PORT', 5432)}"
        f"/{os.environ['DB_NAME']}"
    )
    return create_engine(url)


def _ler_watermark(conn) -> int:
    row = conn.execute(
        text(
            "SELECT ultimo_id_raw FROM prata.controle_processamento "
            "WHERE tabela_origem = 'abastecimento_atm_raw'"
        )
    ).fetchone()
    return row[0] if row else 0


def _atualizar_watermark(conn, ultimo_id: int) -> None:
    conn.execute(
        text(
            "UPDATE prata.controle_processamento "
            "SET ultimo_id_raw = :uid, atualizado_em = NOW() "
            "WHERE tabela_origem = 'abastecimento_atm_raw'"
        ),
        {"uid": ultimo_id},
    )


def _parse_data(serie: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(serie, dayfirst=True, errors="coerce")
    nulos = parsed.isna().sum()
    if nulos:
        logger.warning("%d datas de abastecimento não parseáveis.", nulos)
    return parsed


def _parse_numero_br(serie: pd.Series) -> pd.Series:
    def _conv(val):
        if pd.isna(val) or str(val).strip() in ("", "None"):
            return None
        s = re.sub(r"[R$\s]", "", str(val).strip())
        if "," in s:
            s = s.replace(".", "").replace(",", ".")
        try:
            return float(s)
        except ValueError:
            return None

    return serie.apply(_conv)


def limpar(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    antes = len(df)
    df = df[df["cod_atm"].notna() & (df["cod_atm"].str.strip() != "")]
    if len(df) < antes:
        logger.info("%d linhas descartadas por cod_atm nulo/vazio.", antes - len(df))

    df["data_abastecimento"] = _parse_data(df["data_abastecimento"])
    df = df[df["data_abastecimento"].notna()]

    df["valor_abastecido"] = _parse_numero_br(df["valor_abastecido"])
    df["saldo_anterior"]   = _parse_numero_br(df["saldo_anterior"])

    df["cod_atm"]     = df["cod_atm"].str.strip().str.upper()
    df["cod_agencia"] = df["cod_agencia"].str.strip() if "cod_agencia" in df.columns else None

    return df


def _upsert(df: pd.DataFrame, conn) -> tuple[int, int]:
    sql = text("""
        INSERT INTO prata.abastecimento_atm
            (data_abastecimento, cod_atm, cod_agencia,
             valor_abastecido, saldo_anterior, tecnico, _arquivo_origem)
        VALUES
            (:data_abastecimento, :cod_atm, :cod_agencia,
             :valor_abastecido, :saldo_anterior, :tecnico, :_arquivo_origem)
        ON CONFLICT ON CONSTRAINT uq_abastecimento_atm DO NOTHING
    """)
    inseridos = ignorados = 0
    for rec in df.to_dict(orient="records"):
        if pd.notna(rec.get("data_abastecimento")):
            rec["data_abastecimento"] = rec["data_abastecimento"].date()
        result = conn.execute(sql, rec)
        if result.rowcount:
            inseridos += 1
        else:
            ignorados += 1
    return inseridos, ignorados


def main():
    parser = argparse.ArgumentParser(
        description="Limpa bronze.abastecimento_atm_raw e promove para prata."
    )
    parser.add_argument("--batch-size", type=int, default=10_000)
    args = parser.parse_args()

    engine = _get_engine()

    with engine.begin() as conn:
        watermark = _ler_watermark(conn)
        logger.info("Watermark atual: id_raw > %d", watermark)

        df_raw = pd.read_sql(
            text(
                "SELECT * FROM bronze.abastecimento_atm_raw "
                "WHERE id_raw > :wm ORDER BY id_raw LIMIT :lim"
            ),
            conn,
            params={"wm": watermark, "lim": args.batch_size},
        )

    if df_raw.empty:
        logger.info("Nenhum registro novo em bronze.abastecimento_atm_raw.")
        return

    logger.info("Lidos %d registros do bronze.", len(df_raw))
    df_limpo = limpar(df_raw)
    logger.info("%d registros após limpeza.", len(df_limpo))

    if df_limpo.empty:
        return

    colunas_prata = [
        "data_abastecimento", "cod_atm", "cod_agencia",
        "valor_abastecido", "saldo_anterior", "tecnico", "_arquivo_origem",
    ]
    df_insert = df_limpo[[c for c in colunas_prata if c in df_limpo.columns]].copy()

    novo_watermark = int(df_raw["id_raw"].max())

    with engine.begin() as conn:
        inseridos, ignorados = _upsert(df_insert, conn)
        _atualizar_watermark(conn, novo_watermark)

    logger.info(
        "Resultado: %d inseridos, %d ignorados. Novo watermark: %d.",
        inseridos, ignorados, novo_watermark,
    )


if __name__ == "__main__":
    main()
