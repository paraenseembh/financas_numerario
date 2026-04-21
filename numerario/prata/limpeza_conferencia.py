"""
Limpeza e promoção de bronze.conferencia_cofre_raw para prata.conferencia_cofre.

Uso:
    python limpeza_conferencia.py
    python limpeza_conferencia.py --batch-size 5000
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
            "WHERE tabela_origem = 'conferencia_cofre_raw'"
        )
    ).fetchone()
    return row[0] if row else 0


def _atualizar_watermark(conn, ultimo_id: int) -> None:
    conn.execute(
        text(
            "UPDATE prata.controle_processamento "
            "SET ultimo_id_raw = :uid, atualizado_em = NOW() "
            "WHERE tabela_origem = 'conferencia_cofre_raw'"
        ),
        {"uid": ultimo_id},
    )


def _parse_data(serie: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(serie, dayfirst=True, errors="coerce")
    nulos = parsed.isna().sum()
    if nulos:
        logger.warning("%d datas de conferência não parseáveis.", nulos)
    return parsed


def _parse_inteiro(serie: pd.Series) -> pd.Series:
    def _conv(val):
        if pd.isna(val) or str(val).strip() in ("", "None"):
            return None
        s = re.sub(r"[^\d\-]", "", str(val))
        return int(s) if s and s != "-" else None

    return serie.apply(_conv).astype("Int64")


def limpar(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    antes = len(df)
    df = df[df["cod_agencia"].notna() & (df["cod_agencia"].str.strip() != "")]
    if len(df) < antes:
        logger.info("%d linhas descartadas por cod_agencia nulo/vazio.", antes - len(df))

    df["data_conferencia"] = _parse_data(df["data_conferencia"])
    df = df[df["data_conferencia"].notna()]

    df["qtd_contada"]  = _parse_inteiro(df["qtd_contada"])
    df["qtd_esperada"] = _parse_inteiro(df["qtd_esperada"])
    df["diferenca"]    = _parse_inteiro(df["diferenca"])

    df["turno"] = df["turno"].fillna("").str.upper().str.strip()

    return df


def _upsert(df: pd.DataFrame, conn) -> tuple[int, int]:
    sql = text("""
        INSERT INTO prata.conferencia_cofre
            (data_conferencia, cod_agencia, turno, denominacao,
             qtd_contada, qtd_esperada, diferenca, conferente, _arquivo_origem)
        VALUES
            (:data_conferencia, :cod_agencia, :turno, :denominacao,
             :qtd_contada, :qtd_esperada, :diferenca, :conferente, :_arquivo_origem)
        ON CONFLICT ON CONSTRAINT uq_conferencia_cofre DO NOTHING
    """)
    inseridos = ignorados = 0
    for rec in df.to_dict(orient="records"):
        if pd.notna(rec.get("data_conferencia")):
            rec["data_conferencia"] = rec["data_conferencia"].date()
        result = conn.execute(sql, rec)
        if result.rowcount:
            inseridos += 1
        else:
            ignorados += 1
    return inseridos, ignorados


def main():
    parser = argparse.ArgumentParser(
        description="Limpa bronze.conferencia_cofre_raw e promove para prata."
    )
    parser.add_argument("--batch-size", type=int, default=10_000)
    args = parser.parse_args()

    engine = _get_engine()

    with engine.begin() as conn:
        watermark = _ler_watermark(conn)
        logger.info("Watermark atual: id_raw > %d", watermark)

        df_raw = pd.read_sql(
            text(
                "SELECT * FROM bronze.conferencia_cofre_raw "
                "WHERE id_raw > :wm ORDER BY id_raw LIMIT :lim"
            ),
            conn,
            params={"wm": watermark, "lim": args.batch_size},
        )

    if df_raw.empty:
        logger.info("Nenhum registro novo em bronze.conferencia_cofre_raw.")
        return

    logger.info("Lidos %d registros do bronze.", len(df_raw))
    df_limpo = limpar(df_raw)
    logger.info("%d registros após limpeza.", len(df_limpo))

    if df_limpo.empty:
        return

    colunas_prata = [
        "data_conferencia", "cod_agencia", "turno", "denominacao",
        "qtd_contada", "qtd_esperada", "diferenca", "conferente", "_arquivo_origem",
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
