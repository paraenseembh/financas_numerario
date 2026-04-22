-- DDL Schema Bronze
-- Camada de ingestão bruta: todos os campos TEXT, sem tipagem forçada.
-- Metadados de rastreabilidade (_arquivo_origem, _ingestado_em) em todas as tabelas.

-- Movimentação de caixa/cofre
CREATE TABLE IF NOT EXISTS bronze.movimentacao_raw (
    id_raw          SERIAL PRIMARY KEY,
    data_movimento  TEXT,
    cod_agencia     TEXT,
    tipo_operacao   TEXT,
    denominacao     TEXT,
    quantidade      TEXT,
    valor_total     TEXT,
    operador        TEXT,
    _arquivo_origem TEXT,
    _ingestado_em   TIMESTAMP DEFAULT NOW()
);

-- Conferência de cofre
CREATE TABLE IF NOT EXISTS bronze.conferencia_cofre_raw (
    id_raw          SERIAL PRIMARY KEY,
    data_conferencia TEXT,
    cod_agencia     TEXT,
    turno           TEXT,
    denominacao     TEXT,
    qtd_contada     TEXT,
    qtd_esperada    TEXT,
    diferenca       TEXT,
    conferente      TEXT,
    _arquivo_origem TEXT,
    _ingestado_em   TIMESTAMP DEFAULT NOW()
);

-- Abastecimento de ATMs
CREATE TABLE IF NOT EXISTS bronze.abastecimento_atm_raw (
    id_raw            SERIAL PRIMARY KEY,
    data_abastecimento TEXT,
    cod_atm           TEXT,
    cod_agencia       TEXT,
    valor_abastecido  TEXT,
    saldo_anterior    TEXT,
    tecnico           TEXT,
    _arquivo_origem   TEXT,
    _ingestado_em     TIMESTAMP DEFAULT NOW()
);

-- Custódia diária
CREATE TABLE IF NOT EXISTS bronze.custodia_diaria_raw (
    id_raw          SERIAL PRIMARY KEY,
    data_referencia TEXT,
    cod_agencia     TEXT,
    denominacao     TEXT,
    saldo_fisico    TEXT,
    _arquivo_origem TEXT,
    _ingestado_em   TIMESTAMP DEFAULT NOW()
);
