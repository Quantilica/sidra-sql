SELECT
    p.codigo                                                AS periodo,
    l.nc                                                    AS nivel_territorial_id,
    l.nn                                                    AS nivel_territorial,
    l.d1c                                                   AS localidade_id,
    l.d1n                                                   AS localidade,
    dim.d2c                                                 AS variavel_id,
    dim.d2n                                                 AS variavel,
    dim.mc                                                  AS unidade_id,
    dim.mn                                                  AS unidade,
    dim.d4c                                                 AS categoria_id,
    dim.d4n                                                 AS categoria,
    CASE WHEN d.v ~ '^-?[0-9]' THEN d.v::numeric END       AS valor
FROM dados d
JOIN periodo    p   ON d.periodo_id    = p.id
JOIN dimensao   dim ON d.dimensao_id   = dim.id
JOIN localidade l   ON d.localidade_id = l.id
WHERE d.sidra_tabela_id IN (
    '1692', '1693',     -- jul/1989 - dez/1990
    '58',   '61',       -- jan/1991 - jul/1999
    '655',  '656',      -- ago/1999 - jun/2006
    '2938',             -- jul/2006 - dez/2011
    '1419',             -- jan/2012 - dez/2019
    '7060'              -- jan/2020 em diante
)
  AND d.ativo = true
