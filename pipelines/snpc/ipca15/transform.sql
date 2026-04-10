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
    '1646',             -- mai/2000 - jul/2006
    '1387',             -- ago/2006 - jan/2012
    '1705',             -- fev/2012 - jan/2020
    '7062'              -- fev/2020 em diante
)
  AND d.ativo = true
