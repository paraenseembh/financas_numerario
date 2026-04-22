"""
Ingestão de planilhas Excel para o schema bronze.

Uso:
    python ingestao_planilhas.py --pasta /data/entrada
    python ingestao_planilhas.py --pasta /data/entrada --padrao "*.xlsx"
"""

import argparse
import logging
import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Mapeamento: fragmento do nome do arquivo -> tabela bronze
MAPA_TABELAS = {
    "movimentacao": "bronze.movimentacao_raw",
    "conferencia":  "bronze.conferencia_cofre_raw",
    "atm":          "bronze.abastecimento_atm_raw",
    "custodia":     "bronze.custodia_diaria_raw",
}

# Colunas esperadas por tabela (excluindo metadados)
COLUNAS_TABELA = {
    "bronze.movimentacao_raw": [
        "data_movimento", "cod_agencia", "tipo_operacao",
        "denominacao", "quantidade", "valor_total", "operador",
    ],
    "bronze.conferencia_cofre_raw": [
        "data_conferencia", "cod_agencia", "turno",
        "denominacao", "qtd_contada", "qtd_esperada", "diferenca", "conferente",
    ],
    "bronze.abastecimento_atm_raw": [
        "data_abastecimento", "cod_atm", "cod_agencia",
        "valor_abastecido", "saldo_anterior", "tecnico",
    ],
    "bronze.custodia_diaria_raw": [
        "data_referencia", "cod_agencia", "denominacao", "saldo_fisico",
    ],
}


def _get_engine():
    url = (
        f"postgresql+psycopg2://{os.environ['DB_USER']}:{os.environ['DB_PASSWORD']}"
        f"@{os.environ['DB_HOST']}:{os.environ.get('DB_PORT', 5432)}"
        f"/{os.environ['DB_NAME']}"
    )
    return create_engine(url)


def _detectar_tabela(nome_arquivo: str) -> str | None:
    nome_lower = nome_arquivo.lower()
    for fragmento, tabela in MAPA_TABELAS.items():
        if fragmento in nome_lower:
            return tabela
    return None


def _normalizar_colunas(df: pd.DataFrame, tabela: str) -> pd.DataFrame:
    """Renomeia colunas do DataFrame para corresponder às da tabela bronze."""
    colunas_esperadas = COLUNAS_TABELA[tabela]
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # Mantém apenas colunas que existem tanto no arquivo quanto na tabela
    colunas_presentes = [c for c in colunas_esperadas if c in df.columns]
    colunas_ausentes  = [c for c in colunas_esperadas if c not in df.columns]

    if colunas_ausentes:
        logger.warning("Colunas ausentes no arquivo (serão NULL): %s", colunas_ausentes)

    df = df[colunas_presentes].copy()
    # Converte tudo para string (schema bronze é TEXT)
    return df.astype(str).replace("nan", None)


def _inserir_dataframe(df: pd.DataFrame, tabela: str, arquivo_origem: str, engine) -> int:
    """Insere linhas no banco usando INSERT ... ON CONFLICT DO NOTHING.
    Retorna quantidade de linhas inseridas."""
    schema, tabela_curta = tabela.split(".")
    df["_arquivo_origem"] = arquivo_origem

    registros = df.to_dict(orient="records")
    if not registros:
        return 0

    colunas = list(registros[0].keys())
    placeholders = ", ".join(f":{c}" for c in colunas)
    cols_sql = ", ".join(colunas)

    sql = text(
        f"INSERT INTO {tabela} ({cols_sql}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"
    )

    inseridos = 0
    with engine.begin() as conn:
        for rec in registros:
            result = conn.execute(sql, rec)
            inseridos += result.rowcount

    return inseridos


def processar_arquivo(caminho: Path, engine) -> None:
    nome = caminho.name
    tabela = _detectar_tabela(nome)

    if tabela is None:
        logger.warning("Tipo não reconhecido para o arquivo '%s'. Pulando.", nome)
        return

    logger.info("Processando '%s' -> %s", nome, tabela)

    try:
        df = pd.read_excel(caminho, dtype=str)
    except Exception as exc:
        logger.error("Erro ao ler '%s': %s", nome, exc)
        return

    if df.empty:
        logger.warning("Arquivo '%s' está vazio. Pulando.", nome)
        return

    try:
        df = _normalizar_colunas(df, tabela)
        inseridos = _inserir_dataframe(df, tabela, nome, engine)
        logger.info("'%s': %d/%d linhas inseridas.", nome, inseridos, len(df))
    except Exception as exc:
        logger.error("Erro ao inserir dados de '%s': %s", nome, exc)


def main():
    parser = argparse.ArgumentParser(description="Ingere planilhas Excel no schema bronze.")
    parser.add_argument("--pasta",  required=True, help="Caminho da pasta com arquivos .xlsx")
    parser.add_argument("--padrao", default="*.xlsx", help="Padrão glob dos arquivos (padrão: *.xlsx)")
    args = parser.parse_args()

    pasta = Path(args.pasta)
    if not pasta.is_dir():
        logger.error("Pasta não encontrada: %s", pasta)
        raise SystemExit(1)

    arquivos = sorted(pasta.glob(args.padrao))
    if not arquivos:
        logger.warning("Nenhum arquivo encontrado em '%s' com padrão '%s'.", pasta, args.padrao)
        return

    engine = _get_engine()
    logger.info("Iniciando ingestão de %d arquivo(s).", len(arquivos))

    for arquivo in arquivos:
        processar_arquivo(arquivo, engine)

    logger.info("Ingestão concluída.")


if __name__ == "__main__":
    main()
