"""
Ingestão de arquivos CSV/TXT de transportadoras de valores para o schema bronze.

Uso:
    python ingestao_transportadora.py --pasta /data/transportadora
    python ingestao_transportadora.py --pasta /data/transportadora --separador ";"
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

MAPA_TABELAS = {
    "abastecimento": "bronze.abastecimento_atm_raw",
    "atm":           "bronze.abastecimento_atm_raw",
    "movimentacao":  "bronze.movimentacao_raw",
    "custodia":      "bronze.custodia_diaria_raw",
}

COLUNAS_TABELA = {
    "bronze.abastecimento_atm_raw": [
        "data_abastecimento", "cod_atm", "cod_agencia",
        "valor_abastecido", "saldo_anterior", "tecnico",
    ],
    "bronze.movimentacao_raw": [
        "data_movimento", "cod_agencia", "tipo_operacao",
        "denominacao", "quantidade", "valor_total", "operador",
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


def _ler_arquivo(caminho: Path, separador: str, encoding: str) -> pd.DataFrame | None:
    try:
        if caminho.suffix.lower() in (".csv", ".txt"):
            return pd.read_csv(caminho, sep=separador, dtype=str, encoding=encoding)
    except UnicodeDecodeError:
        logger.warning("Encoding '%s' falhou para '%s'. Tentando latin-1.", encoding, caminho.name)
        try:
            return pd.read_csv(caminho, sep=separador, dtype=str, encoding="latin-1")
        except Exception as exc:
            logger.error("Erro ao ler '%s': %s", caminho.name, exc)
    except Exception as exc:
        logger.error("Erro ao ler '%s': %s", caminho.name, exc)
    return None


def _normalizar_colunas(df: pd.DataFrame, tabela: str) -> pd.DataFrame:
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    colunas_esperadas = COLUNAS_TABELA[tabela]
    colunas_presentes = [c for c in colunas_esperadas if c in df.columns]
    colunas_ausentes  = [c for c in colunas_esperadas if c not in df.columns]

    if colunas_ausentes:
        logger.warning("Colunas ausentes no arquivo (serão NULL): %s", colunas_ausentes)

    return df[colunas_presentes].copy().astype(str).replace("nan", None)


def _inserir_dataframe(df: pd.DataFrame, tabela: str, arquivo_origem: str, engine) -> int:
    df = df.copy()
    df["_arquivo_origem"] = arquivo_origem
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


def processar_arquivo(caminho: Path, separador: str, encoding: str, engine) -> None:
    nome = caminho.name
    tabela = _detectar_tabela(nome)

    if tabela is None:
        logger.warning("Tipo não reconhecido para '%s'. Pulando.", nome)
        return

    logger.info("Processando '%s' -> %s", nome, tabela)

    df = _ler_arquivo(caminho, separador, encoding)
    if df is None or df.empty:
        logger.warning("Arquivo '%s' vazio ou ilegível. Pulando.", nome)
        return

    try:
        df = _normalizar_colunas(df, tabela)
        inseridos = _inserir_dataframe(df, tabela, nome, engine)
        logger.info("'%s': %d/%d linhas inseridas.", nome, inseridos, len(df))
    except Exception as exc:
        logger.error("Erro ao inserir dados de '%s': %s", nome, exc)


def main():
    parser = argparse.ArgumentParser(
        description="Ingere arquivos CSV/TXT de transportadoras no schema bronze."
    )
    parser.add_argument("--pasta",     required=True, help="Pasta com os arquivos da transportadora")
    parser.add_argument("--separador", default=";",   help="Separador de campos (padrão: ';')")
    parser.add_argument("--encoding",  default="utf-8", help="Encoding dos arquivos (padrão: utf-8)")
    parser.add_argument("--padrao",    default="*.[ct][sx][tv]", help="Padrão glob (padrão: *.csv e *.txt)")
    args = parser.parse_args()

    pasta = Path(args.pasta)
    if not pasta.is_dir():
        logger.error("Pasta não encontrada: %s", pasta)
        raise SystemExit(1)

    arquivos = sorted(pasta.glob(args.padrao))
    if not arquivos:
        logger.warning("Nenhum arquivo encontrado em '%s'.", pasta)
        return

    engine = _get_engine()
    logger.info("Iniciando ingestão de %d arquivo(s) de transportadora.", len(arquivos))

    for arquivo in arquivos:
        processar_arquivo(arquivo, args.separador, args.encoding, engine)

    logger.info("Ingestão de transportadora concluída.")


if __name__ == "__main__":
    main()
