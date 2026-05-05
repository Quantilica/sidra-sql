# Copyright (c) 2026 Komesu, D.K.
# Licensed under the MIT License.

import subprocess
import tomllib
from pathlib import Path


def _slugify(name: str) -> str:
    return name.lower().replace("-", "_").replace(" ", "_")


def _fetch_toml_template() -> str:
    return (
        "# Busca dados da API SIDRA (IBGE).\n"
        "# Encontre IDs de tabelas em https://sidra.ibge.gov.br\n"
        "#\n"
        '# sidra_tabela    — ID da tabela no SIDRA (ex: "839")\n'
        '# variables       — ["allxp"] para todas, ou IDs específicos: ["109", "216"]\n'
        "# territories     — {6 = []} todos os municípios; {3 = []} todos os estados\n"
        '# classifications — {81 = ["allxt"]} todas as categorias (descomente se precisar)\n'
        "# split_variables — true para enviar uma requisição por variável\n"
        "\n"
        "[[tabelas]]\n"
        'sidra_tabela = "XXXX"        # substitua pelo ID da tabela\n'
        'variables    = ["allxp"]\n'
        "territories  = {6 = []}      # nível 6 = municípios\n"
        '# classifications = {81 = ["allxt"]}\n'
    )


def _transform_toml_template(slug: str) -> str:
    return (
        "[table]\n"
        f'name        = "{slug}"\n'
        'schema      = "analytics"\n'
        'strategy    = "replace"\n'
        'description = "Descrição da tabela de saída"\n'
    )


def _transform_sql_template() -> str:
    return (
        "-- Adapte esta query para o seu pipeline.\n"
        "-- Tabelas normalizadas disponíveis:\n"
        "--   IBGE_SIDRA.DADOS  — valores brutos\n"
        "--   PERIODO           — dimensão temporal (ano, mês, trimestre...)\n"
        "--   DIMENSAO          — variáveis e classificações\n"
        "--   LOCALIDADE        — unidades territoriais\n"
        "\n"
        "SELECT\n"
        "    P.ANO           AS ANO,\n"
        "    L.D1C           AS ID_MUNICIPIO,\n"
        "    L.D1N           AS NOME_MUNICIPIO,\n"
        "    DIM.D2N         AS VARIAVEL,\n"
        "    DIM.MN          AS UNIDADE,\n"
        "    CASE WHEN D.V ~ '^-?[0-9]' THEN D.V::NUMERIC END AS VALOR\n"
        "FROM\n"
        "    IBGE_SIDRA.DADOS D\n"
        "    JOIN PERIODO    P   ON D.PERIODO_ID    = P.ID\n"
        "    JOIN DIMENSAO   DIM ON D.DIMENSAO_ID   = DIM.ID\n"
        "    JOIN LOCALIDADE L   ON D.LOCALIDADE_ID = L.ID\n"
        "WHERE\n"
        "    D.SIDRA_TABELA_ID IN ('XXXX')  -- substitua pelo(s) ID(s) da(s) tabela(s)\n"
        "    AND D.ATIVO = TRUE;\n"
    )


class PluginScaffolder:
    def __init__(
        self,
        name: str,
        description: str,
        version: str,
        output_dir: Path,
        git_init: bool,
    ):
        self.name = name
        self.slug = _slugify(name)
        self.description = description
        self.version = version
        self.plugin_dir = Path(output_dir) / name
        self.git_init = git_init

    def create(self) -> Path:
        if self.plugin_dir.exists():
            raise FileExistsError(
                f"Directory '{self.plugin_dir}' already exists."
            )

        self.plugin_dir.mkdir(parents=True)
        pipeline_dir = self.plugin_dir / self.slug
        pipeline_dir.mkdir()

        self._write(self.plugin_dir / "manifest.toml", self._manifest())
        self._write(self.plugin_dir / "README.md", self._readme())
        self._write(pipeline_dir / "fetch.toml", _fetch_toml_template())
        self._write(pipeline_dir / "transform.toml", _transform_toml_template(self.slug))
        self._write(pipeline_dir / "transform.sql", _transform_sql_template())

        if self.git_init:
            self._write(self.plugin_dir / ".gitignore", self._gitignore())
            self._run_git_init()

        return self.plugin_dir

    def _write(self, path: Path, content: str) -> None:
        path.write_text(content, encoding="utf-8")

    def _manifest(self) -> str:
        return (
            f'name        = "{self.name}"\n'
            f'description = "{self.description}"\n'
            f'version     = "{self.version}"\n'
            f"\n"
            f"[[pipeline]]\n"
            f'id          = "{self.slug}"\n'
            f'description = "Descrição do pipeline"\n'
            f'path        = "{self.slug}"\n'
        )

    def _readme(self) -> str:
        return (
            f"# {self.name}\n"
            "\n"
            f"{self.description or 'Descrição do plugin.'}\n"
            "\n"
            "## Instalação\n"
            "\n"
            "```bash\n"
            "sidra-sql plugin install <git-url>\n"
            "```\n"
            "\n"
            "## Pipelines\n"
            "\n"
            "| ID | Descrição | Path |\n"
            "|---|---|---|\n"
            f"| {self.slug} | Descrição do pipeline | {self.slug}/ |\n"
            "\n"
            "## Desenvolvimento\n"
            "\n"
            "1. Encontre a tabela desejada em https://sidra.ibge.gov.br\n"
            f"2. Edite `{self.slug}/fetch.toml` com o ID da tabela e variáveis\n"
            f"3. Ajuste `{self.slug}/transform.sql` para a transformação desejada\n"
            f"4. Atualize `{self.slug}/transform.toml` com o nome da tabela de saída\n"
            "5. Adicione mais pipelines em `manifest.toml` conforme necessário\n"
            "\n"
            "### Territórios disponíveis\n"
            "\n"
            "| Código | Nível |\n"
            "|---|---|\n"
            "| 1 | Brasil |\n"
            "| 2 | Grandes Regiões |\n"
            "| 3 | Unidades da Federação |\n"
            "| 6 | Municípios |\n"
            "| 7 | Regiões Metropolitanas |\n"
        )

    def _gitignore(self) -> str:
        return (
            "__pycache__/\n"
            "*.py[cod]\n"
            ".env\n"
            ".DS_Store\n"
        )

    def _run_git_init(self) -> None:
        cwd = str(self.plugin_dir)
        try:
            subprocess.run(["git", "init"], cwd=cwd, check=True, capture_output=True)
            subprocess.run(["git", "add", "."], cwd=cwd, check=True, capture_output=True)
            subprocess.run(
                ["git", "commit", "-m", "chore: initial scaffold"],
                cwd=cwd,
                check=True,
                capture_output=True,
            )
        except FileNotFoundError:
            raise RuntimeError(
                "git não encontrado. Instale o Git ou use --no-git-init."
            )
        except subprocess.CalledProcessError as e:
            raise RuntimeError(
                f"Falha ao inicializar repositório Git: {e.stderr.decode().strip()}"
            )


class PipelineAdder:
    def __init__(
        self,
        pipeline_id: str,
        description: str,
        path: str,
        plugin_dir: Path,
    ):
        self.pipeline_id = pipeline_id
        self.slug = _slugify(pipeline_id)
        self.description = description
        self.path = path or self.slug
        self.plugin_dir = Path(plugin_dir)
        self.manifest_path = self.plugin_dir / "manifest.toml"
        self.pipeline_dir = self.plugin_dir / self.path

    def add(self) -> Path:
        if not self.manifest_path.exists():
            raise FileNotFoundError(
                f"manifest.toml não encontrado em '{self.plugin_dir}'. "
                "Execute o comando dentro do diretório do plugin ou use --plugin-dir."
            )

        with open(self.manifest_path, "rb") as f:
            manifest = tomllib.load(f)
        existing_ids = {p["id"] for p in manifest.get("pipeline", [])}
        if self.pipeline_id in existing_ids:
            raise ValueError(
                f"Pipeline '{self.pipeline_id}' já existe no manifest.toml."
            )

        if self.pipeline_dir.exists():
            raise FileExistsError(
                f"Diretório '{self.pipeline_dir}' já existe."
            )

        self.pipeline_dir.mkdir(parents=True)
        self.pipeline_dir.joinpath("fetch.toml").write_text(
            _fetch_toml_template(), encoding="utf-8"
        )
        self.pipeline_dir.joinpath("transform.toml").write_text(
            _transform_toml_template(self.slug), encoding="utf-8"
        )
        self.pipeline_dir.joinpath("transform.sql").write_text(
            _transform_sql_template(), encoding="utf-8"
        )

        self._append_to_manifest()
        return self.pipeline_dir

    def _append_to_manifest(self) -> None:
        entry = (
            "\n"
            "[[pipeline]]\n"
            f'id          = "{self.pipeline_id}"\n'
            f'description = "{self.description}"\n'
            f'path        = "{self.path}"\n'
        )
        with open(self.manifest_path, "a", encoding="utf-8") as f:
            f.write(entry)
