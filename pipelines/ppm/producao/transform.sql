SELECT
    p.ano                                                   AS ano,
    l.d1c                                                   AS id_municipio,
    CASE d.sidra_tabela_id
        WHEN '74'   THEN 'Pecuária'
        WHEN '3940' THEN 'Aquicultura'
    END                                                     AS grupo_produto,
    dim.d4n                                                 AS produto,
    CASE dim.d2n
        WHEN 'Produção de origem animal' THEN 'Produção'
        WHEN 'Produção da aquicultura'   THEN 'Produção'
        ELSE dim.d2n
    END                                                     AS variavel,
    dim.mn                                                  AS unidade,
    CASE WHEN d.v ~ '^-?[0-9]' THEN d.v::numeric END       AS valor
FROM dados d
JOIN periodo    p   ON d.periodo_id    = p.id
JOIN dimensao   dim ON d.dimensao_id   = dim.id
JOIN localidade l   ON d.localidade_id = l.id
WHERE d.sidra_tabela_id IN ('74', '3940')
  AND d.ativo = true
