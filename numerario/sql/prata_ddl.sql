-- DDL Schema Prata
-- Camada de dados limpos e tipados.
-- Constraints UNIQUE garantem deduplicação por chave de negócio.

-- Movimentação de caixa/cofre
CREATE TABLE IF NOT EXISTS prata.movimentacao (
    id                SERIAL PRIMARY KEY,
    data_movimento    DATE           NOT NULL,
    cod_agencia       VARCHAR(20)    NOT NULL,
    tipo_operacao     VARCHAR(50)    NOT NULL,
    denominacao       VARCHAR(30)    NOT NULL,
    quantidade        INTEGER,
    valor_total       NUMERIC(15,2),
    operador          VARCHAR(100),
    _arquivo_origem   TEXT,
    _processado_em    TIMESTAMP DEFAULT NOW(),

    CONSTRAINT uq_movimentacao
        UNIQUE (data_movimento, cod_agencia, tipo_operacao, denominacao, operador)
);

-- Conferência de cofre
CREATE TABLE IF NOT EXISTS prata.conferencia_cofre (
    id               SERIAL PRIMARY KEY,
    data_conferencia DATE           NOT NULL,
    cod_agencia      VARCHAR(20)    NOT NULL,
    turno            VARCHAR(10)    NOT NULL,
    denominacao      VARCHAR(30)    NOT NULL,
    qtd_contada      INTEGER,
    qtd_esperada     INTEGER,
    diferenca        INTEGER,
    conferente       VARCHAR(100),
    _arquivo_origem  TEXT,
    _processado_em   TIMESTAMP DEFAULT NOW(),

    CONSTRAINT uq_conferencia_cofre
        UNIQUE (data_conferencia, cod_agencia, turno, denominacao)
);

-- Abastecimento de ATMs
CREATE TABLE IF NOT EXISTS prata.abastecimento_atm (
    id                 SERIAL PRIMARY KEY,
    data_abastecimento DATE           NOT NULL,
    cod_atm            VARCHAR(30)    NOT NULL,
    cod_agencia        VARCHAR(20),
    valor_abastecido   NUMERIC(15,2),
    saldo_anterior     NUMERIC(15,2),
    tecnico            VARCHAR(100),
    _arquivo_origem    TEXT,
    _processado_em     TIMESTAMP DEFAULT NOW(),

    CONSTRAINT uq_abastecimento_atm
        UNIQUE (data_abastecimento, cod_atm)
);

-- Custódia diária
CREATE TABLE IF NOT EXISTS prata.custodia_diaria (
    id              SERIAL PRIMARY KEY,
    data_referencia DATE           NOT NULL,
    cod_agencia     VARCHAR(20)    NOT NULL,
    denominacao     VARCHAR(30)    NOT NULL,
    saldo_fisico    NUMERIC(15,2),
    _arquivo_origem TEXT,
    _processado_em  TIMESTAMP DEFAULT NOW(),

    CONSTRAINT uq_custodia_diaria
        UNIQUE (data_referencia, cod_agencia, denominacao)
);

-- Tabela de controle de watermark para processamento incremental
CREATE TABLE IF NOT EXISTS prata.controle_processamento (
    tabela_origem   VARCHAR(100) PRIMARY KEY,
    ultimo_id_raw   INTEGER      NOT NULL DEFAULT 0,
    atualizado_em   TIMESTAMP    DEFAULT NOW()
);

INSERT INTO prata.controle_processamento (tabela_origem, ultimo_id_raw)
VALUES
    ('movimentacao_raw',     0),
    ('conferencia_cofre_raw',0),
    ('abastecimento_atm_raw',0),
    ('custodia_diaria_raw',  0)
ON CONFLICT DO NOTHING;
