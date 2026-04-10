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
    '1686', '1690',     -- jul/1989 - dez/1990
    '22',   '23',       -- jan/1991 - jul/1999
    '653',  '654',      -- ago/1999 - jun/2006
    '2951',             -- jul/2006 - dez/2011
    '1100',             -- jan/2012 - dez/2019
    '7063'              -- jan/2020 em diante
)
  AND d.ativo = true
