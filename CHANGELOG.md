# Changelog

Todas as mudanças notáveis deste projeto serão documentadas neste arquivo.

O formato segue [Keep a Changelog](https://keepachangelog.com/pt-BR/1.1.0/),
e este projeto adere ao [Semantic Versioning](https://semver.org/lang/pt-BR/).

## [2.0.0] - 2026-08-30

### Alterado

- **Quebra de compatibilidade:** migração de `httpx` para `httpx2` (fork mantido pelo Pydantic, API idêntica). As exceções transitórias capturadas/repropagadas por `Fetcher.get_table` passam a ser tipos `httpx2` (`httpx2.ReadTimeout`, `httpx2.ConnectError`, etc.), alinhadas ao `quantilica-core>=0.6.0`; quem trata essas exceções deve importar `httpx2`.
- Dependências: `httpx2>=2.12.0` (substitui `httpx`), `sidra-fetcher>=0.10.3` (versão que restaura `load_agregado`, partida no refactor v0.9.0 do fetcher — o pin anterior aceitava as versões quebradas).

## [1.3.1] - 2026-07-22

### Corrigido

- `Config.__str__` não expõe mais a senha do banco em texto puro (mascarada como
  `***`).
- `Storage.read_data` normaliza os sentinelas de "sem dado" (`"..."`/`"-"`) apenas
  na coluna de valor `V`, sem corromper nomes de dimensão/localidade que
  legitimamente sejam `"-"`.
- `save_agregado` grava tabela, períodos e localidades numa única transação
  (`engine.begin()`) — sem estados parciais em caso de falha.
- `PluginRegistry` passou a usar o namespace `quantilica` (consistente com
  `config.py`), com migração automática do `registry.json` e da pasta `plugins/`
  dos diretórios antigos (`~/.config/sidra-sql`, `~/.local/share/sidra-sql`).
- `git clone`/`git pull` de plugins agora têm `timeout` (300 s / 120 s).

### Alterado

- `_stream_staging` loga uma amostra das chaves (localidade/dimensão/período) que
  não resolveram, tornando o debug de linhas puladas possível.
- `TomlScript`: plano de download extraído para `_build_plan`, compartilhado por
  `download()` e `_run()` (o caminho testado passa a ser o de produção).

### Removido

- Código morto sem chamadores: `build_localidade_lookup`/`build_dimensao_lookup`/
  `build_periodo_lookup` (database), `Storage.read_data_dir`, `Fetcher.download_table`.

## [1.3.0] - 2026-07-16

### Corrigido

- Dependência de `sidra-fetcher` trocada de `git+https://...@v0.7.0` (tag desatualizada)
  para `sidra-fetcher>=0.7.2` (versão publicada no PyPI)
- `rich` e `httpx`, usados diretamente no código, agora declarados explicitamente em
  `dependencies` (antes chegavam apenas de forma transitiva e frágil)
- Imagem do banner no README trocada de path relativo (não renderiza no PyPI) para
  URL absoluta apontando para a tag `v1.3.0`
- Instrução de instalação no README (`pip install sidra-sql`, em vez de git+https)

### Adicionado

- Metadados PEP 621 completos (`classifiers`, `keywords`, `urls`)
- `[tool.hatch.build.targets.sdist]` para excluir scripts operacionais soltos e assets
  grandes do pacote fonte publicado
- Primeiro release público no PyPI
