SELECT
    p.ano                                                   AS ano,
    l.d1c                                                   AS id_municipio,
    CASE d.sidra_tabela_id
        WHEN '289' THEN 'Extração vegetal'
        WHEN '291' THEN 'Silvicultura'
    END                                                     AS grupo_produto,
    dim.d4n                                                 AS produto,
    CASE dim.d2n
        WHEN 'Quantidade produzida na extração vegetal' THEN 'Quantidade produzida'
        WHEN 'Valor da produção na extração vegetal'    THEN 'Valor da produção'
        WHEN 'Quantidade produzida na silvicultura'     THEN 'Quantidade produzida'
        WHEN 'Valor da produção na silvicultura'        THEN 'Valor da produção'
        ELSE dim.d2n
    END                                                     AS variavel,
    dim.mn                                                  AS unidade,
    CASE WHEN d.v ~ '^-?[0-9]' THEN d.v::numeric END       AS valor
FROM dados d
JOIN periodo    p   ON d.periodo_id    = p.id
JOIN dimensao   dim ON d.dimensao_id   = dim.id
JOIN localidade l   ON d.localidade_id = l.id
WHERE d.sidra_tabela_id IN ('289', '291')
  AND d.ativo = true
