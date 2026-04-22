"""
Extração do sistema core bancário via SQLAlchemy (JDBC-like) para o schema bronze.

Uso:
    python ingestao_core.py --data-referencia 2024-01-15
    python ingestao_core.py --data-referencia 2024-01-15 --tabelas movimentacao custodia
"""

import argparse
import logging
import os
from datetime import date

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Queries de extração do sistema core (adaptar conforme o schema do core bancário)
QUERIES_CORE = {
    "movimentacao": """
        SELECT
            CAST(data_movimento  AS VARCHAR) AS data_movimento,
            CAST(cod_agencia     AS VARCHAR) AS cod_agencia,
            CAST(tipo_operacao   AS VARCHAR) AS tipo_operacao,
            CAST(denominacao     AS VARCHAR) AS denominacao,
            CAST(quantidade      AS VARCHAR) AS quantidade,
            CAST(valor_total     AS VARCHAR) AS valor_total,
            CAST(operador        AS VARCHAR) AS operador
        FROM core.tb_movimentacao_numerario
        WHERE DATE(data_movimento) = :data_referencia
    """,
    "custodia": """
        SELECT
            CAST(data_referencia AS VARCHAR) AS data_referencia,
            CAST(cod_agencia     AS VARCHAR) AS cod_agencia,
            CAST(denominacao     AS VARCHAR) AS denominacao,
            CAST(saldo_fisico    AS VARCHAR) AS saldo_fisico
        FROM core.tb_custodia_diaria
        WHERE data_referencia = :data_referencia
    """,
}

MAPA_TABELA_BRONZE = {
    "movimentacao": "bronze.movimentacao_raw",
    "custodia":     "bronze.custodia_diaria_raw",
}


def _get_engine_destino():
    url = (
        f"postgresql+psycopg2://{os.environ['DB_USER']}:{os.environ['DB_PASSWORD']}"
        f"@{os.environ['DB_HOST']}:{os.environ.get('DB_PORT', 5432)}"
        f"/{os.environ['DB_NAME']}"
    )
    return create_engine(url)


def _get_engine_core():
    """Conexão com o sistema core bancário (configurar via variáveis CORE_*)."""
    url = (
        f"postgresql+psycopg2://{os.environ['CORE_DB_USER']}:{os.environ['CORE_DB_PASSWORD']}"
        f"@{os.environ['CORE_DB_HOST']}:{os.environ.get('CORE_DB_PORT', 5432)}"
        f"/{os.environ['CORE_DB_NAME']}"
    )
    return create_engine(url)


def _inserir_bronze(df: pd.DataFrame, tabela: str, origem: str, engine) -> int:
    df = df.copy()
    df["_arquivo_origem"] = origem
    colunas = list(df.columns)
    placeholders = ", ".join(f":{c}" for c in colunas)
    cols_sql = ", ".join(colunas)
    sql = text(
        f"INSERT INTO {tabela} ({cols_sql}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"
    )
    inseridos = 0
    with engine.begin() as conn:
        for rec in df.to_dict(orient="records"):
            result = conn.execute(sql, rec)
            inseridos += result.rowcount
    return inseridos


def extrair_tabela(nome: str, data_ref: str, engine_core, engine_dest) -> None:
    query = QUERIES_CORE.get(nome)
    tabela_bronze = MAPA_TABELA_BRONZE.get(nome)

    if query is None or tabela_bronze is None:
        logger.warning("Tabela '%s' não configurada. Pulando.", nome)
        return

    logger.info("Extraindo '%s' para %s (data=%s).", nome, tabela_bronze, data_ref)
    try:
        df = pd.read_sql(text(query), engine_core, params={"data_referencia": data_ref})
    except Exception as exc:
        logger.error("Erro ao consultar core para '%s': %s", nome, exc)
        return

    if df.empty:
        logger.info("Nenhum registro encontrado no core para '%s' em %s.", nome, data_ref)
        return

    origem = f"core_{nome}_{data_ref}"
    inseridos = _inserir_bronze(df.astype(str).replace("nan", None), tabela_bronze, origem, engine_dest)
    logger.info("'%s': %d/%d linhas inseridas.", nome, inseridos, len(df))


def main():
    parser = argparse.ArgumentParser(description="Extrai dados do sistema core para o schema bronze.")
    parser.add_argument(
        "--data-referencia",
        default=str(date.today()),
        help="Data de referência (YYYY-MM-DD). Padrão: hoje.",
    )
    parser.add_argument(
        "--tabelas",
        nargs="+",
        default=list(QUERIES_CORE.keys()),
        choices=list(QUERIES_CORE.keys()),
        help="Tabelas a extrair (padrão: todas).",
    )
    args = parser.parse_args()

    engine_core = _get_engine_core()
    engine_dest = _get_engine_destino()

    logger.info("Iniciando extração do core para %d tabela(s).", len(args.tabelas))
    for tabela in args.tabelas:
        extrair_tabela(tabela, args.data_referencia, engine_core, engine_dest)

    logger.info("Extração do core concluída.")


if __name__ == "__main__":
    main()
