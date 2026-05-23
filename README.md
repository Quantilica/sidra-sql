# sidra-sql: Pipeline ETL para dados do IBGE/SIDRA em PostgreSQL

![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg?style=flat-square) ![Python](https://img.shields.io/badge/python-3.13+-blue.svg?style=flat-square)

**Pipeline ETL robusto para baixar, normalizar e carregar tabelas agregadas do SIDRA/IBGE em PostgreSQL.**

![SIDRA-SQL banner](./assets/banner.png)

Trabalhar com dados do IBGE é uma tarefa que todo analista e cientista de dados brasileiro conhece bem — e sabe que não é simples. A API SIDRA disponibiliza um acervo imenso de séries estatísticas (PIB municipal, população, inflação, agropecuária e muito mais), mas transformar esses dados brutos em um banco de dados relacional, limpo, normalizado e pronto para consulta é trabalhoso e cheio de armadilhas.

Este projeto resolve exatamente esse problema: um pipeline ETL completo, com controle de cache, downloads paralelos, carga em massa via protocolo COPY do PostgreSQL e um esquema de banco de dados cuidadosamente normalizado.

---

## Por que usar este projeto?

- **Zero redundância:** nomes de arquivo determinísticos garantem que a mesma requisição nunca seja baixada duas vezes.
- **Desempenho real:** downloads multi-threaded + carga via `COPY` do PostgreSQL são ordens de magnitude mais rápidos que abordagens ingênuas.
- **Confiabilidade:** retry com backoff exponencial lida com instabilidades da API sem interromper o pipeline.
- **Declarativo:** cada pesquisa é descrita em um arquivo TOML — sem código Python para adicionar novas séries.
- **Transformações:** camada de transformação (TOML + SQL) gera tabelas planas e desnormalizadas, prontas para Power BI, Excel ou qualquer ferramenta analítica.
- **Banco normalizado:** dados separados em cinco tabelas relacionais com constraints de unicidade e índices otimizados para consultas analíticas.

---

## Funcionalidades

| Funcionalidade | Detalhes |
|---|---|
| **Download paralelo** | Pool de threads configurável para baixar múltiplos períodos simultaneamente |
| **Cache inteligente** | Filenames determinísticos — cache-hit evita requisições duplicadas à API |
| **Retry com backoff** | Até 5 tentativas com delay exponencial (5s, 10s, 20s…) em falhas de rede |
| **Carga em massa** | Protocolo COPY nativo do PostgreSQL via `psycopg3` para inserção em alta performance |
| **Upsert idempotente** | `ON CONFLICT DO NOTHING/UPDATE` em todas as operações — re-execuções são seguras |
| **Skip inteligente de carga** | Tabela `arquivo_carregado` rastreia arquivos já carregados; re-execuções pulam o trabalho sem repetir I/O |
| **Normalização completa** | Localidades, dimensões (variável × classificação) e fatos em tabelas separadas |
| **Suporte a 6 classificações** | Produto cartesiano de até 6 níveis de classificação por variável |
| **Metadados persistidos** | Agregados, periodicidade e metadados JSON salvos no banco para consulta |
| **Transformações SQL** | Gera tabelas planas (ou views) prontas para análise, definidas por pares TOML + SQL |
| **Logging detalhado** | Dual-channel (arquivo rotativo + console) com rastreamento de cada etapa |

---

## Arquitetura

O projeto segue uma arquitetura baseada em plugins. O motor core `sidra-sql` gerencia e orquestra a execução de pipelines que são distribuídos via repositórios Git independentes.

```
┌─────────────────────────────────────────────────────────────┐
│              Plugins Independentes (GitHub)                 │
│         manifest.toml + fetch.toml + transform.toml         │
│            (declaração das tabelas a baixar)                │
└──────────────────────────┬──────────────────────────────────┘
                           │ instalado e lido via CLI
┌──────────────────────────▼──────────────────────────────────┐
│              sidra-sql plugin install <url>                 │
│         sidra-sql run <plugin-alias> <pipeline-id>          │
│                                                             │
│                    toml_runner.py                           │
│           TomlScript: download → metadata → load            │
└──────┬───────────────────┬──────────────────┬───────────────┘
       │                   │                  │
┌──────▼──────┐   ┌────────▼───────┐   ┌──────▼──────────────┐
│  sidra.py   │   │  database.py   │   │    storage.py       │
│  (Fetcher)  │   │ (load, upsert, │   │ (filesystem, cache, │
│  API client │   │  DDL builders) │   │  filename hashing)  │
└──────┬──────┘   └────────┬───────┘   └──────┬──────────────┘
       │                   │                  │
┌──────▼──────┐   ┌────────▼───────┐   ┌──────▼──────┐
│  SIDRA API  │   │  PostgreSQL    │   │ Sistema de  │
│  (IBGE)     │   │  (ibge_sidra)  │   │ arquivos    │
└─────────────┘   └────────┬───────┘   └─────────────┘
                           │
       ┌───────────────────▼────────────────────────┐
       │             transform_runner.py            │
       │       (executa o SQL da transformação)     │
       └───────────────────┬────────────────────────┘
                           │
               ┌───────────▼───────────┐
               │  PostgreSQL           │
               │  (analytics schema)   │
               │  tabelas prontas para │
               │  Power BI / Excel     │
               └───────────────────────┘
```

**Princípios de design:**

- **Desacoplado:** os pipelines vivem em repositórios próprios; o motor apenas clona e executa os manifestos TOML.
- **Determinismo:** o mesmo conjunto de parâmetros sempre gera o mesmo nome de arquivo — re-execuções são seguras e baratas.
- **Dois passos de carga:** o primeiro escaneamento coleta chaves únicas de localidades e dimensões; o segundo transmite os dados via COPY, evitando acúmulo em memória.
- **Declarativo:** tanto a carga (scripts TOML) quanto a transformação (TOML + SQL) são definidas por arquivos de configuração.

---

## Esquema do Banco de Dados

O banco é organizado em cinco tabelas no schema `ibge_sidra` (configurável):

```
┌─────────────────┐       ┌──────────────────────────────────────────┐
│  tabela_sidra   │       │              dados (fatos)               │
│─────────────────│       │──────────────────────────────────────────│
│ id (PK)         │◄──────│ tabela_sidra_id (FK)                     │
│ nome            │       │ localidade_id (FK) ──────────────────────┼──►┌─────────────────┐
│ periodicidade   │       │ dimensao_id (FK) ────────────────────────┼──►│   localidade    │
│ metadados (JSON)│       │ periodo_id (FK) ─────────────────────────┼──►│─────────────────│
│ ultima_atualizac│       │ v    (valor como texto)                  │   │ id (PK)         │
└─────────────────┘       │ modificacao (date)                       │   │ nc  (nível id)  │
                          │ ativo (boolean)                          │   │ nn  (nível nome)│
                          └──────────────────────────────────────────┘   │ d1c (unidade id)│
                                                                         │ d1n (unidade nom│
┌──────────────────────────────┐                                         └─────────────────┘
│           periodo            │◄────(periodo_id)
│──────────────────────────────│
│ id (PK)                      │     ┌──────────────────────────────────────────┐
│ codigo  (ex: "202301")       │     │              dimensao                    │
│ ano, mes, trimestre, semestre│     │──────────────────────────────────────────│
│ data_inicio, data_fim        │     │ id (PK)                                  │
└──────────────────────────────┘     │ mc,mn  (unidade de medida id/nome)       │
                                     │ d2c,d2n (variável id/nome)               │
                                     │ d4c–d9c (ids das classificações, ≤6)     │
                                     │ d4n–d9n (nomes das classificações)       │
                                     └──────────────────────────────────────────┘
```

**Constraint de unicidade na tabela `dados`:**
```sql
UNIQUE (tabela_sidra_id, localidade_id, dimensao_id, periodo_id)
```

Isso garante que cada combinação de tabela × localidade × variável/classificação × período exista apenas uma vez, tornando re-execuções completamente seguras.

---

## Pipelines Padrão (Plugin Oficial)

O `sidra-sql` vem pré-configurado com o catálogo oficial de pipelines de referência (hospedado em [Quantilica/sidra-pipelines](https://github.com/Quantilica/sidra-pipelines)). Estas pipelines são instaladas automaticamente com o alias `std` na primeira execução do CLI.

| Comando | Pesquisa | Tabelas SIDRA |
|---|---|---|
| `sidra-sql run std pib_municipal` | **PIB dos Municípios** | 5938 |
| `sidra-sql run std estimativa_populacao` | **Estimativas de População** | 6579 |
| `sidra-sql run std censo_populacao` | **Censo Demográfico** | 200 |
| `sidra-sql run std contagem_populacao` | **Contagem de População** | 305, 793 |
| `sidra-sql run std ipca` | **IPCA** | 1692, 1693, 58, 61, 655, 656, 2938, 1419, 7060 |
| `sidra-sql run std ipca15` | **IPCA-15** | 1646, 1387, 1705, 7062 |
| `sidra-sql run std inpc` | **INPC** | 1686, 1690, 22, 23, 653, 654, 2951, 1100, 7063 |
| `sidra-sql run std ppm_rebanhos` | **PPM — Rebanhos** | 73, 3939 |
| `sidra-sql run std ppm_producao` | **PPM — Produção animal** | 74, 3940 |
| `sidra-sql run std ppm_exploracao` | **PPM — Aquicultura e exploração** | 94, 95 |
| `sidra-sql run std pam_lavouras_temporarias` | **PAM — Lavouras temporárias** | 839, 1000, 1001, 1002, 1612 |
| `sidra-sql run std pam_lavouras_permanentes` | **PAM — Lavouras permanentes** | 1613 |
| `sidra-sql run std pevs_producao` | **PEVS — Produção florestal** | 289, 291 |
| `sidra-sql run std pevs_area_florestal` | **PEVS — Área florestal** | 5930 |

Para criar suas próprias pipelines e distribuí-las como plugin, consulte o **[Guia de Criação de Pipelines](CREATING_PIPELINES.md)**.

---

## Pré-requisitos

- **Python 3.13+**
- **PostgreSQL 14+** (com usuário e banco de dados criados)
- Acesso à internet para consultar a API SIDRA do IBGE
- Biblioteca [`sidra-fetcher`](https://github.com/Quantilica/sidra-fetcher) (instalada automaticamente via `pyproject.toml`)

---

## Instalação

```bash
pip install git+https://github.com/Quantilica/sidra-sql.git
```

Com [uv](https://github.com/astral-sh/uv):

```bash
uv add "git+https://github.com/Quantilica/sidra-sql.git"
```

**Dependências principais:**

| Pacote | Uso |
|---|---|
| [`sidra-fetcher`](https://github.com/Quantilica/sidra-fetcher) | Cliente HTTP para a API SIDRA do IBGE |
| `psycopg[binary] >= 3.2.9` | Adaptador PostgreSQL com extensões C |
| `sqlalchemy >= 2.0.41` | ORM e geração de SQL |
| `orjson >= 3.11.7` | Serialização JSON de alta performance |

---

## Configuração

Crie o arquivo `config.ini` na raiz do projeto:

```ini
[storage]
# Diretório onde os arquivos JSON baixados serão armazenados
data_dir = data

[database]
user       = postgres
password   = sua_senha
host       = localhost
port       = 5432
dbname     = dados
schema     = ibge_sidra
```

> **Nota:** O schema `ibge_sidra` será criado automaticamente na primeira execução, incluindo todas as tabelas, índices e constraints.

---

## Uso

O sistema gerencia pipelines através de uma interface de linha de comando (CLI). Como as pipelines são plugins externos (repositórios git), o primeiro passo é instalar o plugin desejado.

### 1. Gerenciar Plugins

```bash
# Instalar um plugin via URL do Git
sidra-sql plugin install https://github.com/Quantilica/sidra-pipeline-pam.git --alias pam

# Listar os plugins instalados e suas pipelines disponíveis
sidra-sql plugin list

# Atualizar um plugin instalado
sidra-sql plugin update pam

# Remover um plugin
sidra-sql plugin remove pam
```

### 2. Criar e Desenvolver Plugins

O CLI inclui comandos para criar e iterar sobre plugins localmente, sem precisar escrever os arquivos manualmente.

```bash
# Criar a estrutura completa de um novo plugin
sidra-sql plugin scaffold meu-plugin --description "Meus dados do IBGE"

# Adicionar um pipeline a um plugin existente (rodar dentro do diretório do plugin)
cd meu-plugin
sidra-sql plugin add-pipeline nova_serie --description "Nova série de dados"

# Adicionar um pipeline com caminho aninhado
sidra-sql plugin add-pipeline ipca_municipios --path "precos/ipca" --description "IPCA municipal"

# Validar a estrutura do plugin antes de publicar
sidra-sql plugin validate

# Validar um plugin já instalado pelo alias
sidra-sql plugin validate std
```

O comando `scaffold` cria:
```
meu-plugin/
├── .gitignore
├── README.md
├── manifest.toml
└── meu_plugin/
    ├── fetch.toml         ← template comentado com referências ao SIDRA
    ├── transform.toml     ← declara as saídas (uma ou mais por pipeline)
    └── meu_plugin.sql     ← SQL da saída (um arquivo .sql por entrada [[table]])
```

Para o fluxo completo de criação de plugins, veja o **[Guia de Criação de Pipelines](CREATING_PIPELINES.md)**.

### 3. Executar Pipelines

Use o comando `run`, especificando o alias do plugin e o id do pipeline (mostrados via `sidra-sql plugin list`):

```bash
# Baixa os dados e executa a transformação do pipeline 'lavouras_temporarias' do plugin 'pam'
sidra-sql run pam lavouras_temporarias

# Executa todos os pipelines de um plugin
sidra-sql run pam

# Força a atualização de metadados (re-baixa metadados da API mesmo com cache local)
sidra-sql run pam lavouras_temporarias --force-metadata

# Força o recarregamento de todos os arquivos no banco, ignorando o registro de arquivos já carregados
sidra-sql run pam lavouras_temporarias --force-load

# Combinação: metadados frescos + recarga completa
sidra-sql run pam lavouras_temporarias --force-metadata --force-load

# Executar apenas a etapa de transformação (sem fetch nem recursão)
sidra-sql transform pam lavouras_temporarias
```

> **`--force-load`** ignora a tabela `arquivo_carregado` e reprocessa todos os arquivos presentes em disco. Útil quando o schema do banco foi alterado ou quando é necessário reconstruir os dados a partir dos arquivos JSON locais já baixados. Não re-baixa arquivos da API — para isso use `--force-metadata` em conjunto.

---

## Formato TOML

Cada arquivo TOML contém uma lista de entradas `[[tabelas]]`. Cada entrada corresponde a uma chamada à API SIDRA:

```toml
[[tabelas]]
tabela_sidra = "5938"           # ID da tabela no SIDRA
variables    = ["37", "498"]    # IDs das variáveis ("allxp" para todas)
territories  = {6 = ["all"]}   # nível territorial → lista de IDs

[tabelas.classifications]       # classificações e categorias (opcional)
315 = []                        # lista vazia = todas as categorias
```

**Níveis territoriais comuns:**

| Código | Descrição |
|---|---|
| `1` | Brasil |
| `2` | Grandes Regiões |
| `3` | Unidades da Federação |
| `6` | Municípios |
| `7` | Regiões Metropolitanas |
| `71` | Regiões Metropolitanas e RIDEs |

### Flags especiais

**`unnest_classifications = true`**

Busca os metadados da tabela em tempo de execução e gera uma requisição para cada combinação de classificação × categoria:

```toml
[[tabelas]]
tabela_sidra = "1613"
variables    = ["allxp"]
territories  = {6 = []}
unnest_classifications = true
```

**`split_variables = true`**

Emite uma requisição separada para cada variável listada em `variables`:

```toml
[[tabelas]]
tabela_sidra   = "1002"
variables      = ["109", "216", "214", "112"]
split_variables = true
territories    = {6 = []}
classifications = {81 = ["allxt"]}
```

### Adicionar uma nova série

Para aprender a criar o seu próprio repositório de pipelines compatível com este motor, veja a documentação dedicada:
👉 **[Guia: Como Criar Pipelines (Plugins)](CREATING_PIPELINES.md)**

---

## Transformações

Após a carga dos dados brutos no banco normalizado, a camada de transformação gera tabelas planas e desnormalizadas, prontas para consumo por ferramentas analíticas (Power BI, Excel, Metabase, etc.).

Cada pipeline declara suas saídas em um único `transform.toml` no seu diretório:

- **`transform.toml`** — uma ou mais entradas `[[table]]`, cada uma especificando o nome da tabela/view de destino, schema, estratégia e o arquivo `.sql` correspondente
- **`<saída>.sql`** — query SELECT que produz os dados denormalizados (um arquivo por entrada `[[table]]`)

Um pipeline pode produzir múltiplas saídas (ex.: uma tabela detalhada + uma view agregada) declarando múltiplos blocos `[[table]]` no mesmo `transform.toml`. Cada saída é materializada na ordem do array, em sua própria transação — se uma falhar, as anteriores persistem.

### Executar uma transformação

A execução via CLI `sidra-sql run <plugin> <pipeline>` já orquestra de forma inteligente a extração e em seguida a transformação de acordo com o `manifest.toml` do plugin.

### Formato TOML da transformação

```toml
[[table]]
name        = "ipca"           # Nome da tabela de destino
schema      = "analytics"      # Schema de destino (criado automaticamente)
strategy    = "replace"        # Estratégia de materialização ("replace" ou "view")
sql         = "ipca.sql"       # Arquivo SQL (relativo a transform.toml)
description = "IPCA - variação e peso mensal por categoria e localidade"
primary_key = ["periodo", "localidade_id", "variavel", "categoria"] # Opcional: define PK após carga
indexes     = [
    { name = "idx_ipca_periodo",    columns = ["periodo"] },
    { name = "idx_ipca_localidade", columns = ["localidade"] },
]

# Saída adicional opcional — outra tabela/view do mesmo pipeline:
[[table]]
name     = "ipca_resumo_anual"
schema   = "analytics"
strategy = "view"
sql      = "ipca_resumo.sql"
```

**Estratégias disponíveis:**

| Estratégia | Comportamento | Quando usar |
|---|---|---|
| `replace` | `DROP` + `CREATE AS` + `PK/Indexes` | Import em Power BI / Excel (refresh completo) |
| `view` | `CREATE OR REPLACE VIEW` | Conexões live (zero storage, sempre atualizado) |

### SQL da transformação

O arquivo `.sql` contém um SELECT puro. Os nomes de tabela (`dados`, `dimensao`, `localidade`, `periodo`) são resolvidos pelo `search_path` configurado em `config.ini` — não use prefixo de schema:

```sql
SELECT
    p.codigo                                                AS periodo,
    p.ano,
    p.mes,
    l.d1c                                                   AS localidade_id,
    l.d1n                                                   AS localidade,
    dim.d2n                                                 AS variavel,
    dim.d4n                                                 AS categoria,
    CASE WHEN d.v ~ '^-?[0-9]' THEN d.v::numeric END       AS valor
FROM dados d
JOIN periodo    p   ON d.periodo_id    = p.id
JOIN dimensao   dim ON d.dimensao_id   = dim.id
JOIN localidade l   ON d.localidade_id = l.id
WHERE d.tabela_sidra_id IN ('7060', '1419')
  AND d.ativo = true
```

Valores não numéricos do SIDRA (`"..."`, `"-"`, `"X"`) são convertidos em `NULL` pelo guard `CASE WHEN d.v ~ '^-?[0-9]'`.

### Adicionar uma nova transformação

Para detalhes sobre como adicionar ou editar transformações dentro do seu próprio plugin, veja o **[Guia: Como Criar Pipelines](CREATING_PIPELINES.md)**.

### Transformações incluídas

| Pipeline | Tabela de destino | Descrição |
|---|---|---|
| `pipelines/snpc/ipca/` | `analytics.ipca` | IPCA completo |
| `pipelines/snpc/inpc/` | `analytics.inpc` | INPC completo |
| `pipelines/snpc/ipca15/` | `analytics.ipca15` | IPCA-15 completo |
| `pipelines/pib_munic/` | `analytics.pib_municipal` | PIB dos Municípios |
| `pipelines/populacao/estimapop/` | `analytics.estimativa_populacao` | Estimativas de população |
| `pipelines/populacao/censo_populacao/` | `analytics.censo_populacao` | Censo Demográfico |
| `pipelines/populacao/contagem_populacao/` | `analytics.contagem_populacao` | Contagem da População |
| `pipelines/ppm/rebanhos/` | `analytics.ppm_rebanhos` | Efetivo dos rebanhos |
| `pipelines/ppm/producao/` | `analytics.ppm_producao` | Produção de origem animal |
| `pipelines/ppm/exploracao/` | `analytics.ppm_exploracao` | Aquicultura e exploração |
| `pipelines/pam/lavouras_permanentes/` | `analytics.pam_lavouras_permanentes` | Lavouras permanentes |
| `pipelines/pam/lavouras_temporarias/` | `analytics.pam_lavouras_temporarias` | Lavouras temporárias |
| `pipelines/pevs/producao/` | `analytics.pevs_producao` | Extração vegetal e silvicultura |
| `pipelines/pevs/area_florestal/` | `analytics.pevs_area_florestal` | Área de florestas plantadas |

---

## Fluxo de Dados

```
API SIDRA (IBGE)
      │
      │  GET /agregados/{tabela}/periodos/{período}/...
      ▼
┌────────────────────────────────────────────────────┐
│  Fetcher (sidra.py)                                │
│  ┌─────────────────────────────────────────────┐   │
│  │  ThreadPoolExecutor (max_workers=4)         │   │
│  │  → download de cada período em paralelo     │   │
│  │  → retry com backoff (5 tentativas, base 5s)│   │
│  └──────────────────┬──────────────────────────┘   │
└─────────────────────┼──────────────────────────────┘
                      │
                      ▼
         ┌────────────────────────┐
         │  Storage (storage.py)  │
         │  data/t-{id}/          │
         │  ├── arquivo1.json     │  ← nome determinístico
         │  ├── arquivo2.json     │    (tabela+período+terr+
         │  └── ...               │     vars+classif+mod)
         └───────────┬────────────┘
                     │
                     ▼
         ┌────────────────────────────────────────┐
         │  load_dados (database.py)              │
         │                                        │
         │  Passo 1: scan JSON                    │
         │  → coleta chaves únicas de             │
         │    localidades e dimensões             │
         │  → upsert em lotes de 5.000 linhas     │
         │  → constrói lookup dicts em memória    │
         │  Passo 2: scan JSON novamente          │
         │  → resolve IDs via lookup              │
         │  → usa data de modificação da API      │
         │  → stream via COPY para staging table  │
         │  → INSERT com ON CONFLICT DO NOTHING   │
         └───────────────┬────────────────────────┘
                         │
                         ▼
               ┌──────────────────┐
               │   PostgreSQL     │
               │                  │
               │  tabela_sidra    │
               │  localidade      │
               │  dimensao        │
               │  dados           │
               └──────────────────┘
```

---

## Desenvolvimento

```bash
git clone https://github.com/Quantilica/sidra-sql.git
cd sidra-sql
uv sync --dev
uv run pytest
```

A suíte de testes cobre:

| Arquivo | O que testa |
|---|---|
| `tests/test_config.py` | Carregamento de config, setup de logging |
| `tests/test_storage.py` | Geração de nomes, leitura/escrita, caminhos de metadados |
| `tests/test_base.py` | Cache de metadados, deduplicação, download com filepaths |
| `tests/test_sidra.py` | Retry logic, unnesting de classificações, context manager |
| `tests/test_database.py` | Limpeza de dados, criação de engine, builders DDL/DCL |
| `tests/test_utils.py` | Produto cartesiano de dimensões, resolução de unidade |

---

## Estrutura do Repositório

```
sidra-sql/
├── src/sidra_sql/
│   ├── __init__.py
│   ├── cli.py                # Interface de Linha de Comando (Typer)
│   ├── plugin_manager.py     # Gerenciamento de plugins, Git e manifests
│   ├── scaffold.py           # Geração de plugins e pipelines (scaffold, add-pipeline)
│   ├── validator.py          # Validação de estrutura de plugins
│   ├── toml_runner.py        # TomlScript — orquestra o pipeline ETL de extração
│   ├── transform_runner.py   # TransformRunner — materializa TOML+SQL analíticos
│   ├── config.py             # Leitura de config.ini
│   ├── database.py           # SQLAlchemy, carga, DDL/DCL
│   ├── models.py             # ORM models (tabelas, localidades, dimensões, dados)
│   ├── sidra.py              # Cliente da API SIDRA com retry e cache
│   ├── storage.py            # Filesystem: leitura, escrita, filenames
│   └── utils.py              # Produto cartesiano de dimensões
├── tests/
├── config.ini                # Configurações (não versionado)
├── pyproject.toml            # Metadados e dependências do projeto
├── README.md
└── CREATING_PIPELINES.md     # Guia para criação de plugins
```

---

## Licença

MIT — veja [LICENSE](LICENSE).
