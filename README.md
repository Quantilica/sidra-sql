# sidra-sql

**Pipeline ETL robusto para baixar, normalizar e carregar tabelas agregadas do SIDRA/IBGE em PostgreSQL.**

---

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

## Índice

- [Funcionalidades](#funcionalidades)
- [Arquitetura](#arquitetura)
- [Esquema do Banco de Dados](#esquema-do-banco-de-dados)
- [Pipelines de referência incluídas](#pipelines-de-referência-incluídas)
- [Pré-requisitos](#pré-requisitos)
- [Instalação](#instalação)
- [Configuração](#configuração)
- [Uso](#uso)
  - [Executar um pipeline](#executar-um-pipeline)
  - [Executar todos os pipelines](#executar-todos-os-pipelines)
- [Formato TOML](#formato-toml)
- [Transformações](#transformações)
- [Fluxo de Dados](#fluxo-de-dados)
- [Módulos Internos](#módulos-internos)
- [Testes](#testes)
- [Criando seus próprios pipelines](#criando-seus-próprios-pipelines)

---

## Funcionalidades

| Funcionalidade | Detalhes |
|---|---|
| **Download paralelo** | Pool de threads configurável para baixar múltiplos períodos simultaneamente |
| **Cache inteligente** | Filenames determinísticos — cache-hit evita requisições duplicadas à API |
| **Retry com backoff** | Até 5 tentativas com delay exponencial (5s, 10s, 20s…) em falhas de rede |
| **Carga em massa** | Protocolo COPY nativo do PostgreSQL via `psycopg3` para inserção em alta performance |
| **Upsert idempotente** | `ON CONFLICT DO NOTHING/UPDATE` em todas as operações — re-execuções são seguras |
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
│  sidra_tabela   │       │              dados (fatos)               │
│─────────────────│       │──────────────────────────────────────────│
│ id (PK)         │◄──────│ sidra_tabela_id (FK)                     │
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
UNIQUE (sidra_tabela_id, localidade_id, dimensao_id, periodo_id)
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
# 1. Clone o repositório
git clone https://github.com/Quantilica/sidra-sql.git
cd sidra-sql

# 2. Crie e ative o ambiente virtual
python -m venv .venv
source .venv/bin/activate       # Linux/macOS
# .venv\Scripts\activate        # Windows

# 3. Instale as dependências
pip install -e .
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
tablespace = pg_default
readonly_role = readonly_role
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

### 2. Executar uma pipeline

Use o comando `run`, especificando o "alias" do plugin e o "id" da pipeline (mostrados via `sidra-sql plugin list`):

```bash
# Baixa os dados e executa a transformação da pipeline 'lavouras_temporarias' do plugin 'pam'
sidra-sql run pam lavouras_temporarias

# Executa forçando a atualização de metadados
sidra-sql run pam lavouras_temporarias --force-metadata
```

---

## Formato TOML

Cada arquivo TOML contém uma lista de entradas `[[tabelas]]`. Cada entrada corresponde a uma chamada à API SIDRA:

```toml
[[tabelas]]
sidra_tabela = "5938"           # ID da tabela no SIDRA
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
sidra_tabela = "1613"
variables    = ["allxp"]
territories  = {6 = []}
unnest_classifications = true
```

**`split_variables = true`**

Emite uma requisição separada para cada variável listada em `variables`:

```toml
[[tabelas]]
sidra_tabela   = "1002"
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

Cada transformação é definida por um par de arquivos dentro do diretório do pipeline:

- **`transform.toml`** — metadados: nome da tabela de destino, schema e estratégia de materialização
- **`transform.sql`** — query SELECT que produz os dados denormalizados

### Executar uma transformação

A execução via CLI `sidra-sql run <plugin> <pipeline>` já orquestra de forma inteligente a extração e em seguida a transformação de acordo com o `manifest.toml` do plugin.

### Formato TOML da transformação

```toml
[table]
name        = "ipca"           # Nome da tabela de destino
schema      = "analytics"      # Schema de destino (criado automaticamente)
strategy    = "replace"        # Estratégia de materialização ("replace" ou "view")
description = "IPCA - variação e peso mensal por categoria e localidade"
primary_key = ["periodo", "localidade_id", "variavel", "categoria"] # Opcional: define PK após carga

[[table.indexes]]             # Opcional: define índices adicionais
name    = "idx_ipca_periodo"
columns = ["periodo"]

[[table.indexes]]
name    = "idx_ipca_localidade"
columns = ["localidade"]
unique  = false
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
WHERE d.sidra_tabela_id IN ('7060', '1419')
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
               │  sidra_tabela    │
               │  localidade      │
               │  dimensao        │
               │  dados           │
               └──────────────────┘
```

---

## Módulos Internos

### `toml_runner.py` — Pipeline principal

`TomlScript` lê o TOML, expande entradas dinâmicas e orquestra todo o pipeline:

```python
from sidra_sql.toml_runner import TomlScript
from sidra_sql.config import Config
from pathlib import Path

script = TomlScript(Config(), Path("pipelines/pib_munic/pib/fetch.toml"))
script.run()
```

O método `run()` executa automaticamente toda a sequência:
1. Cria as tabelas no banco (idempotente)
2. Busca e persiste os metadados
3. Baixa todos os períodos disponíveis (com cache)
4. Carrega os dados no PostgreSQL

### `transform_runner.py` — Transformações SQL

`TransformRunner` lê um par TOML + SQL e materializa a query como tabela ou view:

```python
from sidra_sql.transform_runner import TransformRunner
from sidra_sql.config import Config
from pathlib import Path

runner = TransformRunner(Config(), Path("pipelines/snpc/ipca/transform.toml"))
runner.run()
```

### `config.py` — Gerenciamento de configuração

Lê `config.ini` e expõe credenciais do banco, diretório de dados e opções de logging.

```python
from sidra_sql.config import Config
config = Config("config.ini")
print(config.database.host)    # "localhost"
print(config.storage.data_dir) # "data"
```

### `sidra.py` — Cliente da API SIDRA

```python
from sidra_sql.sidra import Fetcher

with Fetcher(config=config) as fetcher:
    filepaths = fetcher.download_table(
        sidra_tabela="5938",
        territories={"6": ["all"]},
        variables=["37", "498"],
    )
```

O `Fetcher` gerencia internamente:
- Pool de threads para downloads paralelos
- Detecção de cache-hit (evita re-download)
- Retry com backoff exponencial em falhas de rede

### `storage.py` — Armazenamento em arquivo

Nomes de arquivo são gerados deterministicamente a partir dos parâmetros da requisição:

```
t5938_p202301_f3_n6-all_v37.498_c0_m1717200000.json
│     │        │  │       │      │  │
│     │        │  │       │      │  └─ timestamp de modificação
│     │        │  │       │      └──── classificações
│     │        │  │       └─────────── variáveis
│     │        │  └─────────────────── nível territorial
│     │        └────────────────────── formato
│     └─────────────────────────────── período
└───────────────────────────────────── tabela
```

### `database.py` — Operações no banco

```python
from sidra_sql.database import get_engine, load_dados

engine = get_engine(config)
load_dados(engine, storage, data_files)
```

A carga usa o protocolo COPY do PostgreSQL via `psycopg3`, com inserção em tabela de staging e resolução de conflitos via `ON CONFLICT DO NOTHING`.

### `utils.py` — Utilitários de transformação

```python
from sidra_sql.utils import unnest_dimensoes

dimensoes = list(unnest_dimensoes(variaveis, classificacoes))
```

Gera todas as combinações possíveis de variável × categoria de classificação.

---

## Testes

```bash
pytest -q
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

## Criando seus próprios pipelines

O `sidra-sql` foi projetado para ser extensível. Qualquer série do IBGE disponível na API SIDRA pode ser transformada em um pipeline sem escrever nenhum código Python — apenas arquivos TOML e SQL.

Para criar, publicar e instalar seus próprios plugins:

👉 **[Guia completo: Criando e Usando Pipelines](CREATING_PIPELINES.md)**

O guia cobre:
- Como encontrar IDs de tabelas, variáveis e classificações no portal SIDRA
- Estrutura completa de um repositório de plugin
- Referência de todos os campos de `manifest.toml`, `fetch.toml` e `transform.toml`
- Referência do esquema normalizado (colunas de cada tabela)
- Exemplos completos do zero
- Boas práticas para séries históricas, classificações complexas e transformações SQL

---

## Licença

GNU GPLv3
