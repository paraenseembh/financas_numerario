"""
Limpeza e promoção de bronze.movimentacao_raw para prata.movimentacao.

Uso:
    python limpeza_movimentacao.py
    python limpeza_movimentacao.py --batch-size 5000
"""

import argparse
import logging
import os
import re
import unicodedata

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
        text("SELECT ultimo_id_raw FROM prata.controle_processamento WHERE tabela_origem = 'movimentacao_raw'")
    ).fetchone()
    return row[0] if row else 0


def _atualizar_watermark(conn, ultimo_id: int) -> None:
    conn.execute(
        text(
            "UPDATE prata.controle_processamento "
            "SET ultimo_id_raw = :uid, atualizado_em = NOW() "
            "WHERE tabela_origem = 'movimentacao_raw'"
        ),
        {"uid": ultimo_id},
    )


def _remover_acentos(texto: str) -> str:
    nfkd = unicodedata.normalize("NFKD", texto)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def _parse_data(serie: pd.Series) -> pd.Series:
    """Parseia datas tolerante aos formatos DD/MM/YYYY e YYYY-MM-DD."""
    # Tenta dayfirst=True para cobrir o padrão BR (dd/mm/yyyy)
    parsed = pd.to_datetime(serie, dayfirst=True, errors="coerce")
    nulos_antes = parsed.isna().sum()
    if nulos_antes:
        logger.warning("%d datas não parseáveis descartadas.", nulos_antes)
    return parsed


def _parse_numero_br(serie: pd.Series) -> pd.Series:
    """Converte strings BR (ponto de milhar, vírgula decimal) para float."""
    def _converter(val):
        if pd.isna(val) or str(val).strip() in ("", "None"):
            return None
        s = str(val).strip()
        # Remove símbolo de moeda e espaços
        s = re.sub(r"[R$\s]", "", s)
        # Se tem vírgula, assume padrão BR: 1.234,56
        if "," in s:
            s = s.replace(".", "").replace(",", ".")
        return float(s) if s else None

    return serie.apply(_converter)


def limpar(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Remove linhas sem agência
    antes = len(df)
    df = df[df["cod_agencia"].notna() & (df["cod_agencia"].str.strip() != "")]
    descartados = antes - len(df)
    if descartados:
        logger.info("%d linhas descartadas por cod_agencia nulo/vazio.", descartados)

    # Parseia datas
    df["data_movimento"] = _parse_data(df["data_movimento"])
    df = df[df["data_movimento"].notna()]

    # Converte numéricos
    df["quantidade"]  = _parse_numero_br(df["quantidade"]).astype("Int64")
    df["valor_total"] = _parse_numero_br(df["valor_total"])

    # Normaliza tipo_operacao: uppercase sem acento
    df["tipo_operacao"] = (
        df["tipo_operacao"]
        .fillna("")
        .apply(lambda x: _remover_acentos(x.upper().strip()))
    )

    return df


def _upsert(df: pd.DataFrame, arquivo_origem_col: pd.Series, conn) -> tuple[int, int]:
    sql = text("""
        INSERT INTO prata.movimentacao
            (data_movimento, cod_agencia, tipo_operacao, denominacao,
             quantidade, valor_total, operador, _arquivo_origem)
        VALUES
            (:data_movimento, :cod_agencia, :tipo_operacao, :denominacao,
             :quantidade, :valor_total, :operador, :_arquivo_origem)
        ON CONFLICT ON CONSTRAINT uq_movimentacao DO NOTHING
    """)
    inseridos = 0
    ignorados = 0
    for rec in df.to_dict(orient="records"):
        # Converte Timestamp para date para o psycopg2
        if pd.notna(rec.get("data_movimento")):
            rec["data_movimento"] = rec["data_movimento"].date()
        result = conn.execute(sql, rec)
        if result.rowcount:
            inseridos += 1
        else:
            ignorados += 1
    return inseridos, ignorados


def main():
    parser = argparse.ArgumentParser(
        description="Limpa bronze.movimentacao_raw e promove para prata.movimentacao."
    )
    parser.add_argument("--batch-size", type=int, default=10_000,
                        help="Quantidade de registros por lote (padrão: 10000)")
    args = parser.parse_args()

    engine = _get_engine()

    with engine.begin() as conn:
        watermark = _ler_watermark(conn)
        logger.info("Watermark atual: id_raw > %d", watermark)

        df_raw = pd.read_sql(
            text(
                "SELECT * FROM bronze.movimentacao_raw "
                "WHERE id_raw > :wm ORDER BY id_raw LIMIT :lim"
            ),
            conn,
            params={"wm": watermark, "lim": args.batch_size},
        )

    if df_raw.empty:
        logger.info("Nenhum registro novo em bronze.movimentacao_raw.")
        return

    logger.info("Lidos %d registros do bronze.", len(df_raw))
    df_limpo = limpar(df_raw)
    logger.info("%d registros após limpeza.", len(df_limpo))

    if df_limpo.empty:
        logger.warning("Nenhum registro válido após limpeza.")
        return

    colunas_prata = [
        "data_movimento", "cod_agencia", "tipo_operacao", "denominacao",
        "quantidade", "valor_total", "operador", "_arquivo_origem",
    ]
    df_insert = df_limpo[[c for c in colunas_prata if c in df_limpo.columns]].copy()

    novo_watermark = int(df_raw["id_raw"].max())

    with engine.begin() as conn:
        inseridos, ignorados = _upsert(df_insert, df_raw["_arquivo_origem"], conn)
        _atualizar_watermark(conn, novo_watermark)

    logger.info(
        "Resultado: %d inseridos, %d ignorados (duplicatas). Novo watermark: %d.",
        inseridos, ignorados, novo_watermark,
    )


if __name__ == "__main__":
    main()
