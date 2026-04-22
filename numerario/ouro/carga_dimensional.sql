-- Carga das dimensões e fatos no schema ouro.
-- Executar com psql passando os parâmetros:
--   psql ... -v data_inicio='2024-01-01' -v data_fim='2024-12-31' -f carga_dimensional.sql

-- ============================================================
-- 1. dim_tempo — geração via série de datas
-- ============================================================
INSERT INTO ouro.dim_tempo (data, ano, mes, dia, dia_semana, eh_dia_util, periodo_contabil)
SELECT
    d::DATE                                              AS data,
    EXTRACT(YEAR  FROM d)::INT                           AS ano,
    EXTRACT(MONTH FROM d)::INT                           AS mes,
    EXTRACT(DAY   FROM d)::INT                           AS dia,
    TO_CHAR(d, 'Day')                                    AS dia_semana,
    -- dia útil: segunda a sexta (feriados devem ser tratados fora deste script)
    EXTRACT(DOW FROM d) NOT IN (0, 6)                    AS eh_dia_util,
    TO_CHAR(d, 'YYYY-MM')                                AS periodo_contabil
FROM generate_series(:'data_inicio'::DATE, :'data_fim'::DATE, INTERVAL '1 day') AS d
ON CONFLICT (data) DO NOTHING;

-- ============================================================
-- 2. dim_agencia — valores distintos da camada prata
-- ============================================================
INSERT INTO ouro.dim_agencia (cod_agencia)
SELECT DISTINCT cod_agencia
FROM (
    SELECT cod_agencia FROM prata.movimentacao
    UNION
    SELECT cod_agencia FROM prata.conferencia_cofre
    UNION
    SELECT cod_agencia FROM prata.abastecimento_atm WHERE cod_agencia IS NOT NULL
    UNION
    SELECT cod_agencia FROM prata.custodia_diaria
) t
WHERE cod_agencia IS NOT NULL
ON CONFLICT (cod_agencia) DO NOTHING;

-- ============================================================
-- 3. dim_denominacao — valores distintos da camada prata
-- ============================================================
INSERT INTO ouro.dim_denominacao (valor_face, tipo)
SELECT DISTINCT
    -- extrai o valor numérico da string de denominação (ex: 'R$ 50,00' ou '50')
    CASE
        WHEN denominacao ~ '^\d+([,\.]\d+)?$'
        THEN REPLACE(REPLACE(denominacao, '.', ''), ',', '.')::NUMERIC
        ELSE NULL
    END AS valor_face,
    -- classifica como moeda se valor_face <= 1 (convenção interna)
    CASE
        WHEN REPLACE(REPLACE(denominacao, '.', ''), ',', '.')::NUMERIC <= 1
        THEN 'moeda' ELSE 'cedula'
    END AS tipo
FROM (
    SELECT denominacao FROM prata.movimentacao
    UNION
    SELECT denominacao FROM prata.conferencia_cofre
    UNION
    SELECT denominacao FROM prata.custodia_diaria
) t
WHERE denominacao IS NOT NULL
  AND denominacao ~ '^\d+([,\.]\d+)?$'
ON CONFLICT DO NOTHING;

-- ============================================================
-- 4. dim_operacao — tipos de operação distintos
-- ============================================================
INSERT INTO ouro.dim_operacao (tipo_operacao, categoria)
SELECT DISTINCT
    tipo_operacao,
    CASE
        WHEN tipo_operacao ILIKE '%ENTRADA%'     OR tipo_operacao ILIKE '%DEPOSITO%'  THEN 'entrada'
        WHEN tipo_operacao ILIKE '%SAIDA%'       OR tipo_operacao ILIKE '%SAQUE%'     THEN 'saida'
        WHEN tipo_operacao ILIKE '%CONFERENCIA%' OR tipo_operacao ILIKE '%CONTAGEM%'  THEN 'conferencia'
        ELSE 'outro'
    END AS categoria
FROM prata.movimentacao
WHERE tipo_operacao IS NOT NULL
ON CONFLICT (tipo_operacao) DO NOTHING;

-- ============================================================
-- 5. dim_equipamento — ATMs distintos
-- ============================================================
INSERT INTO ouro.dim_equipamento (cod_equipamento, tipo_equipamento, sk_agencia)
SELECT DISTINCT
    a.cod_atm                       AS cod_equipamento,
    'ATM'                           AS tipo_equipamento,
    ag.sk_agencia
FROM prata.abastecimento_atm a
LEFT JOIN ouro.dim_agencia ag ON ag.cod_agencia = a.cod_agencia
WHERE a.cod_atm IS NOT NULL
ON CONFLICT (cod_equipamento) DO NOTHING;

-- ============================================================
-- 6. fato_movimentacao_numerario
-- ============================================================
INSERT INTO ouro.fato_movimentacao_numerario
    (sk_tempo, sk_agencia, sk_denominacao, sk_operacao, quantidade, valor_total)
SELECT
    t.sk_tempo,
    ag.sk_agencia,
    dn.sk_denominacao,
    op.sk_operacao,
    m.quantidade,
    m.valor_total
FROM prata.movimentacao m
JOIN ouro.dim_tempo     t  ON t.data         = m.data_movimento
JOIN ouro.dim_agencia   ag ON ag.cod_agencia = m.cod_agencia
LEFT JOIN ouro.dim_denominacao dn ON dn.valor_face = (
    CASE
        WHEN m.denominacao ~ '^\d+([,\.]\d+)?$'
        THEN REPLACE(REPLACE(m.denominacao, '.', ''), ',', '.')::NUMERIC
        ELSE NULL
    END
)
LEFT JOIN ouro.dim_operacao op ON op.tipo_operacao = m.tipo_operacao;

-- ============================================================
-- 7. fato_conferencia_cofre
-- ============================================================
INSERT INTO ouro.fato_conferencia_cofre
    (sk_tempo, sk_agencia, sk_denominacao, qtd_contada, qtd_esperada, diferenca, valor_diferenca)
SELECT
    t.sk_tempo,
    ag.sk_agencia,
    dn.sk_denominacao,
    c.qtd_contada,
    c.qtd_esperada,
    c.diferenca,
    -- valor da diferença = diferenca * valor_face da denominação
    c.diferenca * COALESCE(dn.valor_face, 0) AS valor_diferenca
FROM prata.conferencia_cofre c
JOIN ouro.dim_tempo   t  ON t.data         = c.data_conferencia
JOIN ouro.dim_agencia ag ON ag.cod_agencia = c.cod_agencia
LEFT JOIN ouro.dim_denominacao dn ON dn.valor_face = (
    CASE
        WHEN c.denominacao ~ '^\d+([,\.]\d+)?$'
        THEN REPLACE(REPLACE(c.denominacao, '.', ''), ',', '.')::NUMERIC
        ELSE NULL
    END
);

-- ============================================================
-- 8. fato_abastecimento_atm
-- ============================================================
INSERT INTO ouro.fato_abastecimento_atm
    (sk_tempo, sk_equipamento, valor_abastecido, saldo_anterior)
SELECT
    t.sk_tempo,
    eq.sk_equipamento,
    a.valor_abastecido,
    a.saldo_anterior
FROM prata.abastecimento_atm a
JOIN ouro.dim_tempo       t  ON t.data            = a.data_abastecimento
JOIN ouro.dim_equipamento eq ON eq.cod_equipamento = a.cod_atm;

-- ============================================================
-- 9. fato_custodia_diaria
-- ============================================================
INSERT INTO ouro.fato_custodia_diaria
    (sk_tempo, sk_agencia, sk_denominacao, saldo_fisico)
SELECT
    t.sk_tempo,
    ag.sk_agencia,
    dn.sk_denominacao,
    cd.saldo_fisico
FROM prata.custodia_diaria cd
JOIN ouro.dim_tempo   t  ON t.data         = cd.data_referencia
JOIN ouro.dim_agencia ag ON ag.cod_agencia = cd.cod_agencia
LEFT JOIN ouro.dim_denominacao dn ON dn.valor_face = (
    CASE
        WHEN cd.denominacao ~ '^\d+([,\.]\d+)?$'
        THEN REPLACE(REPLACE(cd.denominacao, '.', ''), ',', '.')::NUMERIC
        ELSE NULL
    END
);
