SELECT
    p.ano                                                   AS ano,
    l.d1c                                                   AS id_municipio,
    CASE WHEN d.v ~ '^-?[0-9]' THEN d.v::numeric END       AS n_pessoas
FROM dados d
JOIN periodo    p   ON d.periodo_id    = p.id
JOIN dimensao   dim ON d.dimensao_id   = dim.id
JOIN localidade l   ON d.localidade_id = l.id
WHERE d.sidra_tabela_id IN ('305', '793')
  AND d.ativo = true
