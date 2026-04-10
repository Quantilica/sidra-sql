SELECT
    p.ano                                                   AS ano,
    l.d1c                                                   AS id_municipio,
    CASE dim.d2n
        WHEN 'Ovinos tosquiados nos estabelecimentos agropecuários' THEN 'Ovinos tosquiados'
        ELSE dim.d2n
    END                                                     AS variavel,
    dim.mn                                                  AS unidade,
    CASE WHEN d.v ~ '^-?[0-9]' THEN d.v::numeric END       AS valor
FROM dados d
JOIN periodo    p   ON d.periodo_id    = p.id
JOIN dimensao   dim ON d.dimensao_id   = dim.id
JOIN localidade l   ON d.localidade_id = l.id
WHERE d.sidra_tabela_id IN ('94', '95')
  AND d.ativo = true
