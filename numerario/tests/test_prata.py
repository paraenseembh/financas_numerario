"""
Testes unitários para a camada prata.

Executar:
    pytest tests/test_prata.py -v
"""

import os
from datetime import date

import pandas as pd
import pytest

os.environ.setdefault("DB_HOST",     "localhost")
os.environ.setdefault("DB_PORT",     "5432")
os.environ.setdefault("DB_NAME",     "test_db")
os.environ.setdefault("DB_USER",     "test_user")
os.environ.setdefault("DB_PASSWORD", "test_pass")

from prata.limpeza_movimentacao import _parse_data, _parse_numero_br, limpar


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture()
def df_raw_valido():
    return pd.DataFrame({
        "id_raw":          [1, 2, 3],
        "data_movimento":  ["15/03/2024", "2024-03-16", "01/12/2023"],
        "cod_agencia":     ["AG001", "AG002", "AG003"],
        "tipo_operacao":   ["Entrada", "saída", "CONFERÊNCIA"],
        "denominacao":     ["50", "100", "10"],
        "quantidade":      ["200", "50", "1000"],
        "valor_total":     ["10.000,00", "5.000,50", "10000.00"],
        "operador":        ["JOAO", "MARIA", "PEDRO"],
        "_arquivo_origem": ["arq1.xlsx", "arq2.xlsx", "arq3.xlsx"],
        "_ingestado_em":   [None, None, None],
    })


# ── Testes: parse de datas ────────────────────────────────────────────────────

def test_data_formato_br_parseada_corretamente():
    serie = pd.Series(["15/03/2024", "01/01/2023", "31/12/2022"])
    resultado = _parse_data(serie)
    assert resultado[0] == pd.Timestamp("2024-03-15")
    assert resultado[1] == pd.Timestamp("2023-01-01")
    assert resultado[2] == pd.Timestamp("2022-12-31")


def test_data_formato_iso_parseada_corretamente():
    serie = pd.Series(["2024-03-15", "2023-01-01"])
    resultado = _parse_data(serie)
    assert resultado[0] == pd.Timestamp("2024-03-15")
    assert resultado[1] == pd.Timestamp("2023-01-01")


def test_data_invalida_retorna_nat():
    serie = pd.Series(["nao_e_data", "99/99/9999", None])
    resultado = _parse_data(serie)
    assert pd.isna(resultado[0])
    assert pd.isna(resultado[2])


def test_data_ambiguidade_dia_primeiro():
    """01/03/2024 deve ser parseada como 1° de março, não 3° de janeiro."""
    serie = pd.Series(["01/03/2024"])
    resultado = _parse_data(serie)
    assert resultado[0].month == 3
    assert resultado[0].day == 1


# ── Testes: parse de números BR ──────────────────────────────────────────────

def test_numero_br_virgula_decimal():
    serie = pd.Series(["1.234,56"])
    resultado = _parse_numero_br(serie)
    assert abs(resultado[0] - 1234.56) < 1e-6


def test_numero_br_ponto_milhar_virgula_decimal():
    serie = pd.Series(["10.000,00", "1.500,75"])
    resultado = _parse_numero_br(serie)
    assert abs(resultado[0] - 10000.0)  < 1e-6
    assert abs(resultado[1] - 1500.75) < 1e-6


def test_numero_formato_americano_ponto_decimal():
    """Números com ponto decimal (sem vírgula) também devem ser aceitos."""
    serie = pd.Series(["5000.50", "100.00"])
    resultado = _parse_numero_br(serie)
    assert abs(resultado[0] - 5000.50) < 1e-6
    assert abs(resultado[1] - 100.0)   < 1e-6


def test_numero_vazio_retorna_none():
    serie = pd.Series(["", None, "None"])
    resultado = _parse_numero_br(serie)
    assert resultado[0] is None
    assert resultado[1] is None
    assert resultado[2] is None


def test_numero_com_simbolo_moeda():
    serie = pd.Series(["R$ 1.500,00"])
    resultado = _parse_numero_br(serie)
    assert abs(resultado[0] - 1500.0) < 1e-6


# ── Testes: descarte de linhas com cod_agencia nulo ───────────────────────────

def test_linhas_sem_cod_agencia_descartadas():
    df = pd.DataFrame({
        "id_raw":          [1, 2, 3],
        "data_movimento":  ["15/03/2024", "16/03/2024", "17/03/2024"],
        "cod_agencia":     [None, "AG002", ""],
        "tipo_operacao":   ["ENTRADA", "SAIDA", "ENTRADA"],
        "denominacao":     ["50", "100", "50"],
        "quantidade":      ["10", "20", "30"],
        "valor_total":     ["500,00", "2000,00", "1500,00"],
        "operador":        ["A", "B", "C"],
        "_arquivo_origem": ["x", "x", "x"],
        "_ingestado_em":   [None, None, None],
    })
    resultado = limpar(df)
    assert len(resultado) == 1
    assert resultado.iloc[0]["cod_agencia"] == "AG002"


def test_cod_agencia_valido_preservado(df_raw_valido):
    resultado = limpar(df_raw_valido)
    assert len(resultado) == 3


# ── Testes: normalização de tipo_operacao ─────────────────────────────────────

def test_tipo_operacao_uppercase_sem_acento(df_raw_valido):
    resultado = limpar(df_raw_valido)
    ops = resultado["tipo_operacao"].tolist()
    assert "ENTRADA"     in ops
    assert "SAIDA"       in ops
    assert "CONFERENCIA" in ops
    # Nunca deve conter acento após limpeza
    for op in ops:
        assert "Ê" not in op
        assert "É" not in op
        assert "Ã" not in op


# ── Testes: pipeline completo de limpeza ──────────────────────────────────────

def test_limpar_retorna_dataframe_tipado(df_raw_valido):
    resultado = limpar(df_raw_valido)
    assert not resultado.empty
    assert pd.api.types.is_datetime64_any_dtype(resultado["data_movimento"])


def test_limpar_converte_quantidade_para_inteiro(df_raw_valido):
    resultado = limpar(df_raw_valido)
    assert resultado["quantidade"].dtype.name in ("Int64", "int64", "float64")


def test_limpar_converte_valor_total(df_raw_valido):
    resultado = limpar(df_raw_valido)
    # 10.000,00 -> 10000.0
    primeiro = resultado.iloc[0]["valor_total"]
    assert abs(primeiro - 10000.0) < 1e-6
