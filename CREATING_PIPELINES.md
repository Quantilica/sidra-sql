# Guia: Criando e Usando Pipelines no sidra-sql

Este guia é destinado a desenvolvedores e cientistas de dados brasileiros que querem aproveitar o motor `sidra-sql` para baixar, normalizar e consultar dados do IBGE via API SIDRA.

---

## Índice

- [Conceitos Fundamentais](#conceitos-fundamentais)
- [Criando um plugin com o CLI](#criando-um-plugin-com-o-cli)
- [Encontrando Dados no SIDRA](#encontrando-dados-no-sidra)
- [Estrutura de um Plugin](#estrutura-de-um-plugin)
- [O arquivo `manifest.toml`](#o-arquivo-manifesttoml)
- [O arquivo `fetch.toml`](#o-arquivo-fetchtoml)
  - [Campos principais](#campos-principais)
  - [Níveis territoriais](#níveis-territoriais)
  - [Variáveis e classificações](#variáveis-e-classificações)
  - [Flags especiais](#flags-especiais)
- [Os arquivos de transformação](#os-arquivos-de-transformação)
  - [`transform.toml`](#transformtoml)
  - [`transform.sql`](#transformsql)
  - [Referência do esquema normalizado](#referência-do-esquema-normalizado)
- [Usando a CLI](#usando-a-cli)
- [Publicando e instalando seu plugin](#publicando-e-instalando-seu-plugin)
- [Exemplos completos](#exemplos-completos)
- [Boas práticas](#boas-práticas)

---

## Conceitos Fundamentais

O `sidra-sql` funciona com dois papéis distintos:

- **Motor (`sidra-sql`):** gerencia plugins, orquestra downloads, carrega dados no PostgreSQL e executa transformações SQL.
- **Plugin:** repositório Git independente que declara *quais* tabelas baixar e *como* transformá-las. Um plugin pode conter múltiplas pipelines.

Esse design permite que você versione e compartilhe seus pipelines sem modificar o código do motor. Qualquer pessoa com o `sidra-sql` instalado pode usar seu plugin com um único comando.

```
Plugin (seu repositório Git)
├── manifest.toml       ← registro das pipelines
├── lavouras/
│   ├── fetch.toml      ← o que baixar da API SIDRA
│   ├── transform.toml  ← como materializar a tabela analítica
│   └── transform.sql   ← SQL que transforma o dado normalizado
└── ...

Motor sidra-sql
└── lê o manifest → executa fetch → executa transform
```

---

## Criando um plugin com o CLI

O `sidra-sql` inclui comandos para gerar a estrutura de um plugin automaticamente, com templates comentados que guiam o preenchimento.

### Fluxo completo

```bash
# 1. Criar o plugin — gera manifest.toml, README.md, .gitignore
#    e o primeiro pipeline com fetch.toml, transform.toml e transform.sql
sidra-sql plugin scaffold meu-plugin \
    --description "Dados agropecuários do IBGE" \
    --version "1.0.0"

# A estrutura criada:
# meu-plugin/
# ├── .gitignore
# ├── README.md
# ├── manifest.toml
# └── meu_plugin/
#     ├── fetch.toml
#     ├── transform.toml
#     └── transform.sql

# 2. Editar os templates:
#    - Em fetch.toml: substituir "XXXX" pelo ID da tabela SIDRA
#    - Em transform.toml: ajustar o nome da tabela de saída
#    - Em transform.sql: escrever a query de transformação

# 3. Adicionar mais pipelines conforme necessário
cd meu-plugin
sidra-sql plugin add-pipeline pam --description "Produção Agrícola Municipal"

# Pipeline com caminho aninhado (cria precos/ipca/ e registra no manifest)
sidra-sql plugin add-pipeline ipca --path "precos/ipca" --description "IPCA"

# Informar --plugin-dir se não estiver dentro do diretório do plugin
sidra-sql plugin add-pipeline serie --plugin-dir /caminho/para/meu-plugin

# 4. Validar a estrutura antes de publicar
sidra-sql plugin validate
# Ou se estiver fora do diretório:
sidra-sql plugin validate --plugin-dir ./meu-plugin

# Saída esperada (exemplo):
# manifest.toml
#   [OK] TOML válido
#   [OK] 2 pipeline(s) declarado(s)
# meu_plugin
#   [OK] fetch.toml válido (1 tabela(s))
#   [OK] transform.toml válido
#   [OK] transform.sql presente
# Resultado: Válido, sem erros ou avisos

# 5. Publicar no Git e instalar
git remote add origin https://github.com/seu-usuario/meu-plugin.git
git push -u origin main
sidra-sql plugin install https://github.com/seu-usuario/meu-plugin.git --alias meu-alias
```

### Opções do `scaffold`

| Opção | Padrão | Descrição |
|---|---|---|
| `--description`, `-d` | `""` | Descrição do plugin |
| `--version` | `"1.0.0"` | Versão semântica |
| `--output-dir`, `-o` | `.` | Onde criar o diretório do plugin |
| `--git-init` / `--no-git-init` | git-init | Inicializa repositório Git com commit inicial |

### Opções do `add-pipeline`

| Opção | Padrão | Descrição |
|---|---|---|
| `--description`, `-d` | `""` | Descrição do pipeline |
| `--path`, `-p` | `<pipeline-id>` | Caminho relativo ao plugin (suporta `/` para aninhamento) |
| `--plugin-dir` | `.` | Raiz do plugin (padrão: diretório atual) |

### O que o `validate` verifica

| O que | Erros | Avisos |
|---|---|---|
| `manifest.toml` | TOML inválido, `id`/`path` ausentes, IDs duplicados | `name`/`version` ausentes, nenhum pipeline declarado |
| Diretório do pipeline | Diretório não existe | — |
| `fetch.toml` | TOML inválido, nenhuma `[[tabelas]]`, `sidra_tabela` ausente | — |
| `transform.toml` | TOML inválido, `[table]` ausente, campos `name`/`schema`/`strategy` faltando | — |
| `transform.sql` | Ausente quando `transform.toml` existe | — |

O comando retorna código de saída `1` se houver erros — útil em pipelines de CI.

---

## Encontrando Dados no SIDRA

Toda tabela na API SIDRA tem um **ID numérico**. Para encontrar o ID da tabela que você quer:

1. Acesse o portal SIDRA: `https://sidra.ibge.gov.br`
2. Navegue até a pesquisa desejada (ex: PAM, IPCA, Censo)
3. Clique na tabela — o ID aparece na URL: `https://sidra.ibge.gov.br/tabela/1613`
4. Anote o ID da tabela (ex: `1613`), os IDs das variáveis e os IDs das classificações

Nas páginas de tabela do SIDRA você encontra:

| O que anotar | Onde fica |
|---|---|
| **ID da tabela** | URL da página (`/tabela/XXXX`) |
| **IDs das variáveis** | Seção "Variáveis" na interface da tabela |
| **IDs das classificações** | Seção "Classificações/categorias" |
| **Periodicidade** | Descrição da tabela (anual, mensal, etc.) |
| **Níveis territoriais disponíveis** | Filtro "Unidade territorial" |

> **Dica:** A API SIDRA usa os mesmos IDs mostrados na interface web. Para explorar programaticamente, acesse `https://servicodados.ibge.gov.br/api/v3/agregados/{ID}/metadados`.

---

## Estrutura de um Plugin

Um plugin é um repositório Git com a seguinte estrutura recomendada:

```text
meu-plugin-sidra/
├── manifest.toml                 # (obrigatório) registro das pipelines
├── README.md                     # documentação do plugin
├── agricultura/                  # diretório de uma pipeline
│   ├── fetch.toml                # regras de download
│   ├── transform.toml            # configuração da tabela analítica
│   └── transform.sql             # SQL de transformação
└── pecuaria/                     # outra pipeline (opcional)
    ├── fetch.toml
    ├── transform.toml
    └── transform.sql
```

Não há restrição de profundidade ou nomenclatura de diretórios — o que importa são os caminhos declarados no `manifest.toml`.

---

## O arquivo `manifest.toml`

Na raiz do repositório, o `manifest.toml` declara quais pipelines o plugin expõe:

```toml
name        = "Dados Agropecuários IBGE"
description = "Pipelines para PAM e PPM — IBGE"
version     = "1.0.0"

[[pipeline]]
id          = "agricultura"
description = "Produção Agrícola Municipal (PAM)"
path        = "agricultura"

[[pipeline]]
id          = "pecuaria"
description = "Pesquisa Pecuária Municipal (PPM)"
path        = "pecuaria"
```

| Campo | Tipo | Descrição |
|---|---|---|
| `name` | string | Nome do plugin |
| `description` | string | Descrição geral |
| `version` | string | Versão semântica |
| `[[pipeline]]` | array | Uma entrada por pipeline |
| `pipeline.id` | string | Identificador usado na CLI (`sidra-sql run <alias> <id>`) |
| `pipeline.description` | string | Descrição amigável |
| `pipeline.path` | path | Diretório raiz da pipeline (relativo à raiz do plugin). O motor descobre `fetch.toml` e `transform.toml` dentro dele e percorre subdiretórios recursivamente. |

### Resolução hierárquica

O motor caminha o diretório de uma pipeline em **pós-ordem (depth-first)**: cada subdiretório que contém `fetch.toml` ou `transform.toml` é tratado como uma sub-pipeline e executa **antes** do diretório-pai. Isso permite criar transformações de segundo nível que consomem as tabelas `analytics.*` produzidas pelas filhas.

Exemplo — `path = "populacao"`:

```text
populacao/
├── transform.toml          ← roda por último (UNION das filhas)
├── transform.sql
├── censo_populacao/        ← roda primeiro (fetch + transform)
│   ├── fetch.toml
│   ├── transform.toml
│   └── transform.sql
├── contagem_populacao/     ← roda em seguida
│   └── ...
└── estimapop/              ← roda em seguida
    └── ...
```

Ordem de execução com `sidra-sql run std populacao`:

1. `censo_populacao` (fetch → transform → `analytics.censo_populacao`)
2. `contagem_populacao` (idem)
3. `estimapop` (idem → `analytics.estimativa_populacao`)
4. `populacao` (apenas transform; SQL faz `UNION ALL` referenciando `analytics.*`)

Regras:

- Um diretório pode ter apenas `transform.toml` (rollup puro), apenas `fetch.toml` (raro), ambos (folha clássica) ou nenhum (container de agrupamento).
- Filhas são processadas em ordem alfabética (`sorted(iterdir())`).
- Para SQL de rollup que lê de outras tabelas analíticas, **use o nome qualificado** (`analytics.tabela`) — o `search_path` continua apontando para `ibge_sidra`.

---

## O arquivo `fetch.toml`

O `fetch.toml` declara uma lista de entradas `[[tabelas]]`. Cada entrada é uma chamada à API SIDRA.

### Campos principais

```toml
[[tabelas]]
sidra_tabela    = "1613"            # ID da tabela no SIDRA (obrigatório)
variables       = ["allxp"]         # IDs das variáveis, ou "allxp" para todas
territories     = {6 = []}          # nível territorial → IDs ([] = todos)
classifications = {87 = []}         # ID da classificação → categorias ([] = todas)
```

| Campo | Tipo | Descrição |
|---|---|---|
| `sidra_tabela` | string | ID numérico da tabela SIDRA |
| `variables` | lista | IDs das variáveis, ou `["allxp"]` para todas disponíveis |
| `territories` | mapa | Código do nível → lista de IDs (`[]` ou `["all"]` = todos) |
| `classifications` | mapa | ID da classificação → lista de categorias (`[]` = todas, `["allxt"]` = todas incluindo total) |

### Níveis territoriais

| Código | Descrição |
|---|---|
| `1` | Brasil |
| `2` | Grandes Regiões |
| `3` | Unidades da Federação |
| `6` | Municípios |
| `7` | Regiões Metropolitanas |
| `8` | Mesorregião geográfica |
| `9` | Microrregião geográfica |
| `71` | Regiões Metropolitanas e RIDEs |

Múltiplos níveis na mesma entrada:

```toml
[[tabelas]]
sidra_tabela = "7060"
variables    = ["63", "66"]
territories  = {1 = [], 3 = [], 6 = [], 71 = []}
```

IDs específicos de territórios (ex: apenas Sudeste e Sul):

```toml
[[tabelas]]
sidra_tabela = "5938"
variables    = ["37", "498"]
territories  = {2 = ["3", "4"]}    # Código 3 = Sudeste, 4 = Sul
```

### Variáveis e classificações

**Variáveis:**

```toml
# Todas as variáveis disponíveis
variables = ["allxp"]

# Variáveis específicas
variables = ["63", "66"]
```

**Classificações:**

```toml
# Todas as categorias (exceto total)
classifications = {315 = []}

# Todas as categorias (incluindo total)
classifications = {315 = ["allxt"]}

# Categorias específicas
classifications = {315 = ["7169", "7170", "7445"]}
```

**Múltiplas classificações (produto cartesiano):**

```toml
[[tabelas]]
sidra_tabela    = "2960"
variables       = ["allxp"]
territories     = {3 = []}
classifications = {12762 = [], 2 = []}    # classificação A × classificação B
```

### Flags especiais

#### `unnest_classifications = true`

Quando a tabela tem classificações com muitas categorias, a API pode retornar erro por excesso de parâmetros. Esta flag faz o motor consultar os metadados em tempo de execução e gerar **uma requisição por categoria**:

```toml
[[tabelas]]
sidra_tabela           = "1613"
variables              = ["allxp"]
territories            = {6 = []}
unnest_classifications = true
```

Use quando a tabela tiver classificações com dezenas ou centenas de categorias (ex: produtos agrícolas, municípios por produto).

#### `split_variables = true`

Emite uma requisição separada para cada variável. Útil quando a combinação de múltiplas variáveis com certas classificações gera respostas muito grandes ou erros da API:

```toml
[[tabelas]]
sidra_tabela    = "1002"
variables       = ["109", "216", "214", "112"]
split_variables = true
territories     = {6 = []}
classifications = {81 = ["allxt"]}
```

#### Combinando as duas flags

As flags podem ser combinadas:

```toml
[[tabelas]]
sidra_tabela           = "5457"
variables              = ["allxp"]
territories            = {6 = []}
unnest_classifications = true
split_variables        = true
```

---

## Os arquivos de transformação

Após o download e carga dos dados brutos no schema normalizado (`ibge_sidra`), a transformação gera tabelas planas prontas para ferramentas analíticas (Power BI, Metabase, Excel).

### `transform.toml`

```toml
[table]
name        = "pam_lavouras_permanentes"    # nome da tabela de destino
schema      = "analytics"                   # schema de destino
strategy    = "replace"                     # "replace" ou "view"
description = "PAM — lavouras permanentes por município e produto"
primary_key = ["ano", "id_municipio", "produto", "variavel"]  # opcional

[[table.indexes]]
name    = "idx_pam_lp_ano"
columns = ["ano"]

[[table.indexes]]
name    = "idx_pam_lp_municipio"
columns = ["id_municipio"]
unique  = false
```

**Campos do `[table]`:**

| Campo | Tipo | Obrigatório | Descrição |
|---|---|---|---|
| `name` | string | sim | Nome da tabela de destino |
| `schema` | string | sim | Schema PostgreSQL de destino |
| `strategy` | string | sim | `"replace"` ou `"view"` |
| `description` | string | não | Descrição para documentação |
| `primary_key` | lista | não | Colunas que formam a PK após a carga |

**Estratégias:**

| Estratégia | Comportamento | Quando usar |
|---|---|---|
| `replace` | `DROP TABLE` + `CREATE TABLE AS SELECT` + índices/PK | Importação em Power BI, Excel (refresh completo) |
| `view` | `CREATE OR REPLACE VIEW` | Conexões live, dashboards, zero storage extra |

**Índices (`[[table.indexes]]`):**

| Campo | Tipo | Padrão | Descrição |
|---|---|---|---|
| `name` | string | — | Nome do índice |
| `columns` | lista | — | Colunas do índice |
| `unique` | bool | `false` | Se o índice deve ser único |

### `transform.sql`

O arquivo `.sql` contém um `SELECT` puro. O `search_path` é configurado automaticamente pelo motor para o schema `ibge_sidra`, então **não use prefixo de schema** nas tabelas:

```sql
SELECT
    p.ano                                               AS ano,
    l.d1c                                               AS id_municipio,
    l.d1n                                               AS municipio,
    dim.d4n                                             AS produto,
    dim.d2n                                             AS variavel,
    dim.mn                                              AS unidade,
    CASE WHEN d.v ~ '^-?[0-9]' THEN d.v::numeric END   AS valor
FROM dados d
JOIN periodo    p   ON d.periodo_id    = p.id
JOIN dimensao   dim ON d.dimensao_id   = dim.id
JOIN localidade l   ON d.localidade_id = l.id
WHERE d.sidra_tabela_id IN ('839', '1612')
  AND d.ativo = true
```

> **Importante:** Valores não numéricos do SIDRA (`"..."`, `"-"`, `"X"`, `"C"`) são armazenados como texto na coluna `v`. Use o padrão `CASE WHEN d.v ~ '^-?[0-9]' THEN d.v::numeric END` para convertê-los em `NULL` de forma segura.

### Referência do esquema normalizado

As tabelas abaixo ficam no schema configurado em `config.ini` (padrão: `ibge_sidra`).

#### `dados` — fatos

| Coluna | Tipo | Descrição |
|---|---|---|
| `id` | bigint | PK auto-gerada |
| `sidra_tabela_id` | text | FK → `sidra_tabela.id` |
| `localidade_id` | bigint | FK → `localidade.id` |
| `dimensao_id` | bigint | FK → `dimensao.id` |
| `periodo_id` | int | FK → `periodo.id` |
| `v` | text | Valor (pode ser numérico ou flag como `"..."`) |
| `modificacao` | date | Data de modificação informada pela API |
| `ativo` | boolean | `true` = dado mais recente para o período |

#### `periodo` — períodos

| Coluna | Tipo | Descrição |
|---|---|---|
| `id` | int | PK auto-gerada |
| `codigo` | text | Código original da API (ex: `"202301"`, `"2023"`) |
| `frequencia` | text | Frequência: `"mensal"`, `"anual"`, etc. |
| `ano` | int | Ano do período |
| `mes` | smallint | Mês (1–12), `NULL` se não aplicável |
| `trimestre` | smallint | Trimestre (1–4), `NULL` se não aplicável |
| `semestre` | smallint | Semestre (1–2), `NULL` se não aplicável |
| `data_inicio` | date | Data de início do período |
| `data_fim` | date | Data de fim do período |

#### `localidade` — unidades territoriais

| Coluna | Tipo | Descrição |
|---|---|---|
| `id` | bigint | PK auto-gerada |
| `nc` | text | Código do nível territorial (ex: `"6"` = município) |
| `nn` | text | Nome do nível territorial (ex: `"Município"`) |
| `d1c` | text | Código da unidade (ex: código IBGE do município) |
| `d1n` | text | Nome da unidade (ex: `"São Paulo"`) |

#### `dimensao` — variáveis e classificações

| Coluna | Tipo | Descrição |
|---|---|---|
| `id` | bigint | PK auto-gerada |
| `mc` | text | Código da unidade de medida |
| `mn` | text | Nome da unidade de medida (ex: `"Toneladas"`) |
| `d2c` | text | Código da variável |
| `d2n` | text | Nome da variável |
| `d4c` / `d4n` | text | Código/nome da 1ª classificação |
| `d5c` / `d5n` | text | Código/nome da 2ª classificação |
| `d6c` / `d6n` | text | Código/nome da 3ª classificação |
| `d7c` / `d7n` | text | Código/nome da 4ª classificação |
| `d8c` / `d8n` | text | Código/nome da 5ª classificação |
| `d9c` / `d9n` | text | Código/nome da 6ª classificação |

#### `sidra_tabela` — metadados das tabelas

| Coluna | Tipo | Descrição |
|---|---|---|
| `id` | text | ID da tabela SIDRA |
| `nome` | text | Nome descritivo |
| `periodicidade` | text | Periodicidade da pesquisa |
| `ultima_atualizacao` | date | Data da última atualização |
| `metadados` | jsonb | Metadados completos da API em JSON |

---

## Usando a CLI

### Criando e desenvolvendo plugins

```bash
# Criar a estrutura de um novo plugin
sidra-sql plugin scaffold meu-plugin --description "Meus dados do IBGE"

# Adicionar pipelines a um plugin existente (dentro do diretório do plugin)
cd meu-plugin
sidra-sql plugin add-pipeline nova_serie --description "Nova pesquisa"
sidra-sql plugin add-pipeline ipca --path "precos/ipca" --description "IPCA"

# Validar o plugin (dentro do diretório ou com --plugin-dir)
sidra-sql plugin validate
sidra-sql plugin validate --plugin-dir ./meu-plugin
```

### Gerenciando plugins instalados

```bash
# Instalar um plugin a partir de um repositório Git
sidra-sql plugin install https://github.com/seu-usuario/meu-plugin.git --alias agro

# Instalar sem alias (usa o nome do repositório como alias)
sidra-sql plugin install https://github.com/seu-usuario/sidra-pipeline-pam.git

# Listar plugins instalados e suas pipelines
sidra-sql plugin list

# Validar um plugin já instalado
sidra-sql plugin validate agro

# Atualizar um plugin (git pull)
sidra-sql plugin update agro

# Atualizar todos os plugins
sidra-sql plugin update

# Remover um plugin
sidra-sql plugin remove agro
```

### Executando pipelines

```bash
# Executar um pipeline (download + transformação)
sidra-sql run agro agricultura

# Executar todos os pipelines de um plugin
sidra-sql run agro

# Forçar atualização de metadados (ignora cache de metadados)
sidra-sql run agro agricultura --force-metadata

# Executar apenas a etapa de transformação (sem fetch nem recursão)
sidra-sql transform agro agricultura
```

O comando `run` executa sequencialmente:
1. **Fetch:** baixa os dados da API SIDRA (com cache e retry)
2. **Transform:** executa o SQL e materializa a tabela analítica

Use `sidra-sql transform` quando quiser reiterar apenas a query SQL sem re-baixar os dados.

### Consultando dados no banco

Após executar a pipeline, os dados ficam disponíveis em dois locais:

**Schema normalizado** (dados brutos, todas as tabelas combinadas):
```sql
-- Exemplo: IPCA mensal para São Paulo
SELECT
    p.ano, p.mes,
    l.d1n AS localidade,
    dim.d2n AS variavel,
    dim.d4n AS categoria,
    d.v::numeric AS valor
FROM ibge_sidra.dados d
JOIN ibge_sidra.periodo    p   ON d.periodo_id    = p.id
JOIN ibge_sidra.dimensao   dim ON d.dimensao_id   = dim.id
JOIN ibge_sidra.localidade l   ON d.localidade_id = l.id
WHERE d.sidra_tabela_id = '7060'
  AND l.d1c = '5300108'    -- código IBGE de Brasília
  AND d.ativo = true
ORDER BY p.ano, p.mes;
```

**Schema analítico** (tabela plana, gerada pela transformação):
```sql
-- Exemplo: dados transformados do IPCA
SELECT * FROM analytics.ipca
WHERE localidade_id = '5300108'
ORDER BY periodo DESC;
```

---

## Publicando e instalando seu plugin

### Publicando

1. Valide a estrutura localmente antes de publicar:
   ```bash
   sidra-sql plugin validate
   ```
2. Crie um repositório no GitHub (ou GitLab, Bitbucket, etc.)
3. Suba os arquivos: `manifest.toml` e os diretórios de cada pipeline
4. O repositório pode ser público ou privado (desde que o usuário tenha acesso via Git)

### Instalando

```bash
# GitHub público
sidra-sql plugin install https://github.com/seu-usuario/meu-plugin.git --alias meu-alias

# Repositório privado via SSH
sidra-sql plugin install git@github.com:sua-org/plugin-interno.git --alias interno
```

### Verificando a instalação

```bash
sidra-sql plugin list
```

A saída mostra todos os plugins e pipelines disponíveis:

```
           Installed Pipelines
┌──────────────┬──────────────┬─────────────────────────────────┐
│ Plugin Alias │ Pipeline ID  │ Description                     │
├──────────────┼──────────────┼─────────────────────────────────┤
│ agro         │ agricultura  │ Produção Agrícola Municipal (PAM)│
│ agro         │ pecuaria     │ Pesquisa Pecuária Municipal (PPM)│
└──────────────┴──────────────┴─────────────────────────────────┘
```

---

## Exemplos completos

### Exemplo 1: Pipeline simples — PIB Municipal

**Cenário:** baixar a tabela 5938 do SIDRA (PIB dos Municípios) para todos os municípios.

**`manifest.toml`:**
```toml
name        = "PIB Municipal IBGE"
description = "Pipeline para o PIB dos Municípios"
version     = "1.0.0"

[[pipeline]]
id          = "pib"
description = "PIB dos Municípios (Tabela 5938)"
path        = "pib"
```

**`pib/fetch.toml`:**
```toml
[[tabelas]]
sidra_tabela    = "5938"
variables       = ["37", "498", "513", "517"]
territories     = {6 = []}
classifications = {315 = []}
```

**`pib/transform.toml`:**
```toml
[table]
name        = "pib_municipal"
schema      = "analytics"
strategy    = "replace"
description = "PIB dos Municípios — valor adicionado e PIB total"
primary_key = ["ano", "id_municipio", "variavel"]

[[table.indexes]]
name    = "idx_pib_ano"
columns = ["ano"]

[[table.indexes]]
name    = "idx_pib_municipio"
columns = ["id_municipio"]
```

**`pib/transform.sql`:**
```sql
SELECT
    p.ano                                               AS ano,
    l.d1c                                               AS id_municipio,
    l.d1n                                               AS municipio,
    l.nc                                                AS nivel_id,
    l.nn                                                AS nivel,
    dim.d2n                                             AS variavel,
    dim.mn                                              AS unidade,
    dim.d4n                                             AS categoria,
    CASE WHEN d.v ~ '^-?[0-9]' THEN d.v::numeric END   AS valor
FROM dados d
JOIN periodo    p   ON d.periodo_id    = p.id
JOIN dimensao   dim ON d.dimensao_id   = dim.id
JOIN localidade l   ON d.localidade_id = l.id
WHERE d.sidra_tabela_id = '5938'
  AND d.ativo = true
ORDER BY p.ano, l.d1c;
```

---

### Exemplo 2: Pipeline com múltiplas tabelas — IPCA série histórica

**Cenário:** o IPCA mudou de tabela várias vezes ao longo dos anos. Para a série histórica completa, é preciso combinar múltiplas tabelas SIDRA.

**`ipca/fetch.toml`:**
```toml
# Tabela 58 — IPCA variação mensal (jan/1991 – jul/1999)
[[tabelas]]
sidra_tabela    = "58"
variables       = ["63"]
territories     = {1 = [], 6 = [], 7 = []}
classifications = {72 = []}

# Tabela 655 — IPCA variação mensal (ago/1999 – jun/2006)
[[tabelas]]
sidra_tabela    = "655"
variables       = ["63"]
territories     = {1 = [], 6 = [], 7 = []}
classifications = {315 = []}

# Tabela 7060 — IPCA variação e peso mensal (jan/2020 em diante)
[[tabelas]]
sidra_tabela    = "7060"
variables       = ["63", "66"]
territories     = {1 = [], 6 = [], 7 = [], 71 = []}
classifications = {315 = []}
```

**`ipca/transform.sql`:**
```sql
SELECT
    p.codigo                                            AS periodo,
    p.ano,
    p.mes,
    l.nc                                                AS nivel_territorial_id,
    l.nn                                                AS nivel_territorial,
    l.d1c                                               AS localidade_id,
    l.d1n                                               AS localidade,
    dim.d2n                                             AS variavel,
    dim.d4n                                             AS categoria,
    CASE WHEN d.v ~ '^-?[0-9]' THEN d.v::numeric END   AS valor
FROM dados d
JOIN periodo    p   ON d.periodo_id    = p.id
JOIN dimensao   dim ON d.dimensao_id   = dim.id
JOIN localidade l   ON d.localidade_id = l.id
WHERE d.sidra_tabela_id IN ('58', '61', '655', '656', '2938', '1419', '7060')
  AND d.ativo = true
ORDER BY p.ano, p.mes, l.d1c, dim.d2n, dim.d4n;
```

---

### Exemplo 3: Pipeline com `unnest_classifications` — Lavouras permanentes

**Cenário:** a tabela 1613 (PAM — lavouras permanentes) tem dezenas de produtos como categorias. O `unnest_classifications` gera uma requisição por produto, evitando timeout da API.

**`permanentes/fetch.toml`:**
```toml
[[tabelas]]
sidra_tabela           = "1613"
variables              = ["allxp"]
territories            = {6 = []}
unnest_classifications = true
```

**`permanentes/transform.sql`:**
```sql
SELECT
    p.ano                                               AS ano,
    l.d1c                                               AS id_municipio,
    l.d1n                                               AS municipio,
    dim.d4n                                             AS produto,
    dim.d2n                                             AS variavel,
    dim.mn                                              AS unidade,
    CASE WHEN d.v ~ '^-?[0-9]' THEN d.v::numeric END   AS valor
FROM dados d
JOIN periodo    p   ON d.periodo_id    = p.id
JOIN dimensao   dim ON d.dimensao_id   = dim.id
JOIN localidade l   ON d.localidade_id = l.id
WHERE d.sidra_tabela_id = '1613'
  AND d.ativo = true;
```

---

## Boas práticas

### fetch.toml

- **Separe tabelas por período:** quando uma série histórica usa múltiplas tabelas (uma por faixa de anos), declare cada uma como uma entrada `[[tabelas]]` separada.
- **Use `unnest_classifications` para tabelas com muitos produtos:** evita erros 414 (URL too long) e timeouts da API SIDRA.
- **Use `split_variables` quando necessário:** algumas tabelas retornam dados inconsistentes quando múltiplas variáveis com unidades diferentes são solicitadas juntas.
- **Prefira `[]` a `["all"]`:** ambos retornam todos os IDs, mas `[]` é resolvido dinamicamente, o que é mais correto para tabelas que adicionam territórios com o tempo.

### transform.sql

- **Sempre filtre por `d.ativo = true`:** garante que apenas o dado mais recente para cada período seja retornado.
- **Use o guard numérico:** `CASE WHEN d.v ~ '^-?[0-9]' THEN d.v::numeric END` converte flags do SIDRA (`"..."`, `"-"`, `"X"`, `"C"`) em `NULL`.
- **Não use prefixo de schema:** o `search_path` é configurado automaticamente pelo motor. Escreva `dados`, não `ibge_sidra.dados`.
- **Filtre por `sidra_tabela_id`:** sem esse filtro, a query retorna dados de *todas* as tabelas no banco.
- **Aproveite a tabela `periodo`:** os campos `ano`, `mes`, `trimestre` já estão parseados — use-os ao invés de manipular strings do `codigo`.

### transform.toml

- **Defina `primary_key`:** garante que a tabela analítica não aceite duplicatas e melhora o desempenho de joins.
- **Crie índices nas colunas mais filtradas:** geralmente `ano`, `id_municipio`, `localidade_id`.
- **Use `strategy = "view"` para dados que atualizam constantemente:** zero overhead de storage, sempre reflete o estado atual do banco.
- **Use `strategy = "replace"` para importação em ferramentas BI:** o Power BI e Excel precisam de tabelas físicas para refresh incremental.

### Organização do plugin

- **Um repositório por tema:** separe agropecuária, preços e demografia em plugins distintos.
- **Um diretório por pipeline:** facilita manutenção e versionamento de cada série individualmente.
- **Use `sidra-sql plugin validate` antes de cada commit:** evita publicar plugins com estrutura quebrada.
- **Documente o README:** inclua as tabelas SIDRA usadas, a periodicidade, as variáveis disponíveis na tabela analítica e quaisquer ressalvas metodológicas do IBGE.
- **Versione com tags Git:** permite que usuários fixem uma versão estável do plugin.
