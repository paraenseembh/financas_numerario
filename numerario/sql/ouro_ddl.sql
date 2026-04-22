-- DDL Schema Ouro
-- Camada analítica: modelo dimensional (estrela) para consumo pelo Power BI.

-- ============================================================
-- DIMENSÕES
-- ============================================================

CREATE TABLE IF NOT EXISTS ouro.dim_agencia (
    sk_agencia    SERIAL PRIMARY KEY,
    cod_agencia   VARCHAR(20)  UNIQUE NOT NULL,
    nome_agencia  VARCHAR(100),
    regiao        VARCHAR(50),
    tipo          VARCHAR(30)  -- 'agencia', 'posto', 'matriz'
);

CREATE TABLE IF NOT EXISTS ouro.dim_denominacao (
    sk_denominacao SERIAL PRIMARY KEY,
    valor_face     NUMERIC(10,2),
    tipo           VARCHAR(10),  -- 'cedula', 'moeda'
    serie          VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS ouro.dim_equipamento (
    sk_equipamento   SERIAL PRIMARY KEY,
    cod_equipamento  VARCHAR(30) UNIQUE NOT NULL,
    tipo_equipamento VARCHAR(20),  -- 'ATM', 'caixa', 'cofre'
    sk_agencia       INT REFERENCES ouro.dim_agencia(sk_agencia)
);

CREATE TABLE IF NOT EXISTS ouro.dim_tempo (
    sk_tempo         SERIAL PRIMARY KEY,
    data             DATE        UNIQUE NOT NULL,
    ano              INT,
    mes              INT,
    dia              INT,
    dia_semana       VARCHAR(15),
    eh_dia_util      BOOLEAN,
    periodo_contabil VARCHAR(7)  -- 'YYYY-MM'
);

CREATE TABLE IF NOT EXISTS ouro.dim_operacao (
    sk_operacao    SERIAL PRIMARY KEY,
    tipo_operacao  VARCHAR(50) UNIQUE NOT NULL,
    categoria      VARCHAR(30)  -- 'entrada', 'saida', 'conferencia'
);

CREATE TABLE IF NOT EXISTS ouro.dim_transportadora (
    sk_transportadora SERIAL PRIMARY KEY,
    nome              VARCHAR(100),
    cnpj              VARCHAR(18),
    contrato          VARCHAR(50)
);

-- ============================================================
-- FATOS
-- ============================================================

CREATE TABLE IF NOT EXISTS ouro.fato_movimentacao_numerario (
    sk_movimentacao SERIAL PRIMARY KEY,
    sk_tempo        INT REFERENCES ouro.dim_tempo(sk_tempo),
    sk_agencia      INT REFERENCES ouro.dim_agencia(sk_agencia),
    sk_denominacao  INT REFERENCES ouro.dim_denominacao(sk_denominacao),
    sk_operacao     INT REFERENCES ouro.dim_operacao(sk_operacao),
    quantidade      INTEGER,
    valor_total     NUMERIC(15,2)
);

CREATE TABLE IF NOT EXISTS ouro.fato_conferencia_cofre (
    sk_conferencia  SERIAL PRIMARY KEY,
    sk_tempo        INT REFERENCES ouro.dim_tempo(sk_tempo),
    sk_agencia      INT REFERENCES ouro.dim_agencia(sk_agencia),
    sk_denominacao  INT REFERENCES ouro.dim_denominacao(sk_denominacao),
    qtd_contada     INTEGER,
    qtd_esperada    INTEGER,
    diferenca       INTEGER,
    valor_diferenca NUMERIC(15,2)
);

CREATE TABLE IF NOT EXISTS ouro.fato_abastecimento_atm (
    sk_abastecimento  SERIAL PRIMARY KEY,
    sk_tempo          INT REFERENCES ouro.dim_tempo(sk_tempo),
    sk_equipamento    INT REFERENCES ouro.dim_equipamento(sk_equipamento),
    valor_abastecido  NUMERIC(15,2),
    saldo_anterior    NUMERIC(15,2)
);

CREATE TABLE IF NOT EXISTS ouro.fato_custodia_diaria (
    sk_custodia    SERIAL PRIMARY KEY,
    sk_tempo       INT REFERENCES ouro.dim_tempo(sk_tempo),
    sk_agencia     INT REFERENCES ouro.dim_agencia(sk_agencia),
    sk_denominacao INT REFERENCES ouro.dim_denominacao(sk_denominacao),
    saldo_fisico   NUMERIC(15,2)
);
