# Changelog

Todas as mudanças notáveis deste projeto serão documentadas neste arquivo.

O formato segue [Keep a Changelog](https://keepachangelog.com/pt-BR/1.1.0/),
e este projeto adere ao [Semantic Versioning](https://semver.org/lang/pt-BR/).

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
