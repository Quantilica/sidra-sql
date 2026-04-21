# Como Criar um Plugin de Pipeline (sidra-sql)

O `sidra-sql` utiliza uma arquitetura baseada em plugins via Git. Isso significa que você pode criar, versionar e distribuir seus próprios pipelines de dados do IBGE independentemente do repositório principal do motor `sidra-sql`.

Este guia mostrará como estruturar seu repositório de plugin e como definir o arquivo `manifest.toml`.

---

## 1. Estrutura do Repositório

Um plugin é simplesmente um repositório Git. Não é necessário criar um pacote Python (`setup.py` ou `pyproject.toml`) a menos que você queira. O motor do `sidra-sql` apenas clona o repositório e lê o manifesto.

A estrutura recomendada do repositório é:

```text
meu-plugin-sidra/
├── manifest.toml             # (Obrigatório) O arquivo de registro do plugin
├── README.md                 # Documentação do seu plugin
├── lavouras/                 # Diretório da sua pipeline
│   ├── fetch.toml            # Regras de download da API
│   ├── transform.toml        # Metadados da tabela final
│   └── transform.sql         # SQL de transformação
└── pecuaria/                 # Outra pipeline opcional
    ├── fetch.toml
    ├── transform.toml
    └── transform.sql
```

---

## 2. O arquivo `manifest.toml`

Na raiz do seu repositório, você **deve** criar um arquivo `manifest.toml`. Este arquivo diz ao motor `sidra-sql` quais pipelines estão contidas no seu plugin.

```toml
# manifest.toml
name = "Meu Plugin Agropecuário"
description = "Pipelines para dados agropecuários do IBGE"
version = "1.0.0"

[[pipeline]]
id = "lavouras"
description = "Produção Agrícola (PAM)"
fetch = "lavouras/fetch.toml"
transform = "lavouras/transform.toml"

[[pipeline]]
id = "pecuaria"
description = "Produção Pecuária (PPM)"
fetch = "pecuaria/fetch.toml"
transform = "pecuaria/transform.toml"
```

### Detalhes do Manifesto
- `name`, `description`, `version`: Metadados do plugin inteiro.
- `[[pipeline]]`: Uma entrada para cada pipeline que seu plugin expõe.
  - `id`: O identificador único que o usuário usará na CLI (ex: `sidra-sql run meu-plugin lavouras`).
  - `description`: Descrição amigável.
  - `fetch`: Caminho relativo para o arquivo de declaração de tabelas SIDRA.
  - `transform`: Caminho relativo para o arquivo TOML de transformação SQL.

---

## 3. O arquivo `fetch.toml`

Este arquivo dita quais tabelas e variáveis baixar da API SIDRA.
A sintaxe é exatamente a mesma documentada no README principal do `sidra-sql`.

**Exemplo (`lavouras/fetch.toml`):**
```toml
[[tabelas]]
sidra_tabela = "1613"           # ID da tabela no SIDRA
variables    = ["allxp"]        # Variáveis
territories  = {6 = []}         # Nível territorial (ex: 6 = municípios)
unnest_classifications = true   # Expande categorias dinamicamente
```

---

## 4. Os arquivos de Transformação

### `transform.toml`
Define como a view analítica será materializada. O arquivo `.sql` deve ter **exatamente o mesmo nome base** (`transform.sql`).

**Exemplo (`lavouras/transform.toml`):**
```toml
[table]
name        = "minhas_lavouras"
schema      = "analytics"
strategy    = "replace"
description = "Tabela de Lavouras"
```

### `transform.sql`
A consulta (Query) que consolida os dados do schema normalizado (`ibge_sidra`) em uma tabela plana.

**Exemplo (`lavouras/transform.sql`):**
```sql
SELECT
    d.d3c as periodo,
    l.d1n as municipio,
    dim.d2n as variavel,
    CASE WHEN d.v ~ '^-?[0-9]' THEN d.v::numeric END AS valor
FROM dados d
JOIN dimensao dim ON d.dimensao_id = dim.id
JOIN localidade l ON d.localidade_id = l.id
WHERE d.sidra_tabela_id = '1613' AND d.ativo = true
```

---

## 5. Publicação e Instalação

1.  Suba seu diretório para um repositório no GitHub (ou GitLab, Bitbucket).
2.  O usuário poderá instalar o seu pipeline executando:
    ```bash
    sidra-sql plugin install https://github.com/seu-usuario/meu-plugin-sidra.git --alias agro
    ```
3.  E verificar se foi instalado corretamente:
    ```bash
    sidra-sql plugin list
    ```
4.  Para rodar o pipeline:
    ```bash
    sidra-sql run agro lavouras
    ```

**Dica**: É sempre recomendável criar índices (`table.indexes`) em suas tabelas analíticas para garantir máxima performance no banco de dados.
