"""
Testes unitários para a camada bronze.

Executar:
    pytest tests/test_bronze.py -v
"""

import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

# Garante que as variáveis de ambiente mínimas existam para os imports
os.environ.setdefault("DB_HOST",     "localhost")
os.environ.setdefault("DB_PORT",     "5432")
os.environ.setdefault("DB_NAME",     "test_db")
os.environ.setdefault("DB_USER",     "test_user")
os.environ.setdefault("DB_PASSWORD", "test_pass")

from bronze.ingestao_planilhas import (
    MAPA_TABELAS,
    _detectar_tabela,
    _inserir_dataframe,
    _normalizar_colunas,
    processar_arquivo,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture()
def df_movimentacao_valido():
    return pd.DataFrame({
        "data_movimento": ["01/01/2024"],
        "cod_agencia":    ["AG001"],
        "tipo_operacao":  ["ENTRADA"],
        "denominacao":    ["50"],
        "quantidade":     ["100"],
        "valor_total":    ["5000.00"],
        "operador":       ["JOAO"],
    })


@pytest.fixture()
def mock_engine():
    engine = MagicMock()
    conn   = MagicMock()
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__  = MagicMock(return_value=False)
    conn.execute.return_value.rowcount = 1
    engine.begin.return_value = conn
    return engine, conn


# ── Testes: detecção de tabela ────────────────────────────────────────────────

def test_detectar_tabela_movimentacao():
    assert _detectar_tabela("movimentacao_jan2024.xlsx") == "bronze.movimentacao_raw"


def test_detectar_tabela_conferencia():
    assert _detectar_tabela("conferencia_cofre_fev.xlsx") == "bronze.conferencia_cofre_raw"


def test_detectar_tabela_atm():
    assert _detectar_tabela("atm_abastecimento_mar.xlsx") == "bronze.abastecimento_atm_raw"


def test_detectar_tabela_desconhecido():
    assert _detectar_tabela("relatorio_generico.xlsx") is None


# ── Testes: arquivo malformado não interrompe o batch ────────────────────────

def test_arquivo_malformado_nao_interrompe_batch(tmp_path):
    """Arquivo Excel corrompido deve gerar log de erro sem lançar exceção."""
    arquivo_ruim = tmp_path / "movimentacao_corrompido.xlsx"
    arquivo_ruim.write_bytes(b"isso nao e um xlsx valido")

    engine = MagicMock()
    # Não deve lançar exceção
    processar_arquivo(arquivo_ruim, engine)


def test_multiplos_arquivos_um_ruim_nao_para_batch(tmp_path, df_movimentacao_valido, mock_engine):
    """Erro em um arquivo não deve impedir o processamento dos demais."""
    engine, conn = mock_engine

    arquivo_ruim = tmp_path / "movimentacao_corrompido.xlsx"
    arquivo_ruim.write_bytes(b"lixo binario")

    arquivo_bom = tmp_path / "movimentacao_valido.xlsx"
    df_movimentacao_valido.to_excel(arquivo_bom, index=False)

    processados = []

    original_processar = processar_arquivo

    def _mock_processar(caminho, eng):
        processados.append(caminho.name)
        original_processar(caminho, eng)

    from bronze import ingestao_planilhas
    with patch.object(ingestao_planilhas, "processar_arquivo", side_effect=_mock_processar):
        for arq in sorted(tmp_path.glob("*.xlsx")):
            ingestao_planilhas.processar_arquivo(arq, engine)

    assert len(processados) == 2


# ── Testes: inserção duplicada não gera erro (ON CONFLICT DO NOTHING) ─────────

def test_insercao_duplicada_nao_gera_erro(df_movimentacao_valido, mock_engine):
    """rowcount=0 indica conflito ignorado; não deve lançar exceção."""
    engine, conn = mock_engine
    conn.execute.return_value.rowcount = 0  # simula ON CONFLICT DO NOTHING

    df = _normalizar_colunas(df_movimentacao_valido.copy(), "bronze.movimentacao_raw")
    inseridos = _inserir_dataframe(df, "bronze.movimentacao_raw", "arquivo_teste.xlsx", engine)
    assert inseridos == 0  # duplicata ignorada sem erro


def test_insercao_bem_sucedida_retorna_contagem(df_movimentacao_valido, mock_engine):
    engine, conn = mock_engine
    conn.execute.return_value.rowcount = 1

    df = _normalizar_colunas(df_movimentacao_valido.copy(), "bronze.movimentacao_raw")
    inseridos = _inserir_dataframe(df, "bronze.movimentacao_raw", "arquivo_teste.xlsx", engine)
    assert inseridos == 1


# ── Testes: _arquivo_origem é preenchido corretamente ───────────────────────

def test_arquivo_origem_preenchido(df_movimentacao_valido, mock_engine):
    """O campo _arquivo_origem deve conter exatamente o nome passado."""
    engine, conn = mock_engine
    conn.execute.return_value.rowcount = 1

    nome_arquivo = "movimentacao_2024_01.xlsx"
    df = _normalizar_colunas(df_movimentacao_valido.copy(), "bronze.movimentacao_raw")
    _inserir_dataframe(df, "bronze.movimentacao_raw", nome_arquivo, engine)

    chamadas = conn.execute.call_args_list
    assert len(chamadas) > 0
    _, kwargs = chamadas[0]
    params = chamadas[0][0][1] if len(chamadas[0][0]) > 1 else chamadas[0][1].get("parameters", {})
    # Extrai o dicionário de parâmetros passado ao execute
    args = chamadas[0][0]
    parametros = args[1] if len(args) > 1 else {}
    assert parametros.get("_arquivo_origem") == nome_arquivo


# ── Testes: normalização de colunas ──────────────────────────────────────────

def test_normalizar_colunas_converte_para_string():
    df = pd.DataFrame({"data_movimento": [20240101], "cod_agencia": [101]})
    resultado = _normalizar_colunas(df, "bronze.movimentacao_raw")
    assert resultado.dtypes["data_movimento"] == object
    assert resultado.dtypes["cod_agencia"] == object


def test_normalizar_colunas_ignora_extras():
    df = pd.DataFrame({
        "data_movimento": ["01/01/2024"],
        "cod_agencia":    ["AG001"],
        "coluna_extra":   ["ignorar"],
    })
    resultado = _normalizar_colunas(df, "bronze.movimentacao_raw")
    assert "coluna_extra" not in resultado.columns
