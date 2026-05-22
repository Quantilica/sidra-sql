CREATE TABLE IF NOT EXISTS arquivo_carregado (
    arquivo          text        PRIMARY KEY,
    tabela_sidra_id  text        NOT NULL REFERENCES tabela_sidra(id),
    carregado_em     timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_arquivo_carregado_tabela
    ON arquivo_carregado (tabela_sidra_id);
