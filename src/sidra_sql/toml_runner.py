# Copyright (c) 2026 Komesu, D.K.
# Licensed under the MIT License.

"""Pipeline runner driven by a TOML configuration file.

``TomlScript`` reads a TOML file that declares which SIDRA tables to fetch
and drives the full ETL pipeline: metadata → download → database load.

TOML schema
-----------
Each ``[[tabelas]]`` entry maps directly to a `sidra.Fetcher.download_table`
call.  Two optional boolean flags extend the static format:

``unnest_classifications = true``
    Fetch the table's metadata at runtime and expand every classification /
    category combination with `sidra.unnest_classificacoes`.  The
    ``classifications`` key, if present, is ignored.

``split_variables = true``
    Issue one request per variable listed in ``variables`` instead of a
    single request with all variables.

Example TOML
~~~~~~~~~~~~
::

    [[tabelas]]
    sidra_tabela = "5938"
    variables = ["37", "498"]
    territories = {6 = ["all"]}

    [[tabelas]]
    sidra_tabela = "1613"
    variables = ["allxp"]
    territories = {6 = []}
    unnest_classifications = true

    [[tabelas]]
    sidra_tabela = "1002"
    variables = ["109", "216", "214", "112"]
    split_variables = true
    territories = {6 = []}
    classifications = {81 = ["allxt"]}
"""

import contextlib
import logging
import tomllib
from pathlib import Path
from typing import Any, Iterable

import sqlalchemy as sa
from rich.console import Console

from . import database, models, sidra
from .config import Config
from .storage import Storage

logger = logging.getLogger(__name__)


class TomlScript:
    """ETL pipeline runner that loads table definitions from a TOML file.

    Drives the full pipeline:

    1. Create ORM tables if they don't exist.
    2. Fetch and save metadata (sidra_tabela, localidade).
    3. Download all data files.
    4. Load data rows into the dados table (also upserts dimensions).
    """

    def __init__(
        self,
        config: Config,
        toml_path: Path,
        max_workers: int = 4,
        force_metadata: bool = False,
        console: Console | None = None,
    ):
        self.config = config
        self.toml_path = toml_path
        self.force_metadata = force_metadata
        self.console = console
        self.storage = Storage.default(config)
        self.fetcher = sidra.Fetcher(
            config, max_workers=max_workers, storage=self.storage
        )

    def _status(self, msg: str):
        if self.console:
            return self.console.status(msg)
        return contextlib.nullcontext()

    def get_tabelas(self) -> Iterable[dict[str, Any]]:
        """Read the TOML file and return an expanded list of table request dicts."""
        with open(self.toml_path, "rb") as f:
            data = tomllib.load(f)

        result: list[dict[str, Any]] = []
        for entry in data.get("tabelas", []):
            entry = dict(entry)
            unnest = entry.pop("unnest_classifications", False)
            split_vars = entry.pop("split_variables", False)

            if unnest:
                entry.pop("classifications", None)
                metadados = self.fetcher.sidra_client.get_agregado_metadados(
                    entry["sidra_tabela"]
                )
                for classificacoes in sidra.unnest_classificacoes(
                    metadados.classificacoes
                ):
                    result.append({**entry, "classifications": classificacoes})
            elif split_vars:
                variables = entry.pop("variables")
                for var in variables:
                    result.append({**entry, "variables": [var]})
            else:
                result.append(entry)

        return result

    def download(
        self, tabelas: Iterable[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Download all tables and return a list of data-file descriptors."""
        data_files = []
        for tabela in tabelas:
            for result in self.fetcher.download_table(**tabela):
                data_files.append(tabela | result)
        return data_files

    def load_metadata(
        self, engine: sa.Engine, tabelas: Iterable[dict[str, Any]]
    ):
        """Fetch and persist metadata for all unique SIDRA tables."""
        seen: set[str] = set()
        for tabela in tabelas:
            sidra_tabela_id = tabela["sidra_tabela"]
            if sidra_tabela_id in seen:
                continue
            seen.add(sidra_tabela_id)

            metadata_filepath = self.storage.get_metadata_filepath(
                sidra_tabela_id
            )
            if metadata_filepath.exists() and not self.force_metadata:
                logger.info(
                    "Reading cached metadata for table %s", sidra_tabela_id
                )
                agregado = self.storage.read_metadata(sidra_tabela_id)
            else:
                logger.info("Fetching metadata for table %s", sidra_tabela_id)
                agregado = self.fetcher.fetch_metadata(sidra_tabela_id)
                self.storage.write_metadata(agregado)

            logger.info(
                "Saving metadata to database for table %s", sidra_tabela_id
            )
            database.save_agregado(engine, agregado)

    def run(self):
        """Execute the full fetch-and-load pipeline."""
        engine = database.get_engine(self.config)
        models.Base.metadata.create_all(engine)

        tabelas = list(self.get_tabelas())
        n = len(tabelas)

        with self.fetcher:
            with self._status(f"[cyan]Carregando metadados[/cyan] ({n} {'tabela' if n == 1 else 'tabelas'})..."):
                self.load_metadata(engine, tabelas)

            data_files = []
            for i, tabela in enumerate(tabelas, 1):
                tid = tabela["sidra_tabela"]
                with self._status(f"[cyan]Baixando tabela[/cyan] {tid} ({i}/{n})..."):
                    for result in self.fetcher.download_table(**tabela):
                        data_files.append(tabela | result)

        if self.console:
            self.console.print(f"  [dim]→ {len(data_files)} arquivo(s) prontos para carga[/dim]")

        with self._status("[cyan]Carregando no banco de dados[/cyan]..."):
            database.load_dados(engine, self.storage, data_files)
