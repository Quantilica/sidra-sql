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

``unnest_classifications = ["id1", "id2"]``
    Same expansion, but only for the listed classification IDs (integers also
    accepted).  The ``classifications`` key, if present, is merged as static
    defaults — unnested entries take precedence on key conflicts.

``split_variables = true``
    Issue one request per variable listed in ``variables`` instead of a
    single request with all variables.

Example TOML
~~~~~~~~~~~~
::

    [[tabelas]]
    tabela_sidra = "5938"
    variables = ["37", "498"]
    territories = {6 = ["all"]}

    [[tabelas]]
    tabela_sidra = "1613"
    variables = ["allxp"]
    territories = {6 = []}
    unnest_classifications = true

    [[tabelas]]
    tabela_sidra = "5938"
    variables = ["allxp"]
    territories = {6 = []}
    classifications = {81 = ["allxt"]}
    unnest_classifications = ["87"]

    [[tabelas]]
    tabela_sidra = "1002"
    variables = ["109", "216", "214", "112"]
    split_variables = true
    territories = {6 = []}
    classifications = {81 = ["allxt"]}
"""

import logging
import tomllib
from pathlib import Path
from typing import Any, Iterable

import sqlalchemy as sa
from rich.console import Console
from rich.table import Table
from rich.text import Text
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TaskID,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

from . import database, models, sidra
from .config import Config
from .storage import Storage

logger = logging.getLogger(__name__)


class _MainOnlyTimeElapsedColumn(TimeElapsedColumn):
    def render(self, task):
        if not task.fields.get("main"):
            return Text("")
        return super().render(task)


class _MainOnlyTimeRemainingColumn(TimeRemainingColumn):
    def render(self, task):
        if not task.fields.get("main"):
            return Text("")
        return super().render(task)


def _make_progress(console: Console | None) -> Progress:
    return Progress(
        SpinnerColumn(finished_text="[green]✓[/green]"),
        TextColumn("[progress.description]{task.description}", table_column=None),
        BarColumn(bar_width=28),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%", style="grey70"),
        _MainOnlyTimeElapsedColumn(),
        _MainOnlyTimeRemainingColumn(),
        console=console,
        transient=False,
        disable=console is None,
    )


def _make_download_progress(console: Console | None) -> Progress:
    return Progress(
        SpinnerColumn(finished_text="[green]✓[/green]"),
        TextColumn("[progress.description]{task.description}", table_column=None),
        BarColumn(bar_width=28),
        MofNCompleteColumn(),
        _MainOnlyTimeElapsedColumn(),
        _MainOnlyTimeRemainingColumn(),
        console=console,
        transient=False,
        disable=console is None,
    )


class TomlScript:
    """ETL pipeline runner that loads table definitions from a TOML file.

    Drives the full pipeline:

    1. Create ORM tables if they don't exist.
    2. Fetch and save metadata (tabela_sidra, localidade).
    3. Download all data files.
    4. Load data rows into the dados table (also upserts dimensions).
    """

    def __init__(
        self,
        config: Config,
        toml_path: Path,
        max_workers: int = 4,
        force_metadata: bool = False,
        force_load: bool = False,
        console: Console | None = None,
    ):
        self.config = config
        self.toml_path = toml_path
        self.force_metadata = force_metadata
        self.force_load = force_load
        self.console = console
        self.storage = Storage.default(config)
        self.fetcher = sidra.Fetcher(
            config, max_workers=max_workers, storage=self.storage
        )

    def get_tabelas(self) -> Iterable[dict[str, Any]]:
        """Read the TOML file and return an expanded list of table request dicts."""
        with open(self.toml_path, "rb") as f:
            data = tomllib.load(f)

        result: list[dict[str, Any]] = []
        for entry in data.get("tabelas", []):
            entry = dict(entry)
            unnest = entry.pop("unnest_classifications", False)
            split_vars = entry.pop("split_variables", False)

            if unnest is not False:
                metadados = self.fetcher.sidra_client.get_agregado_metadados(
                    entry["tabela_sidra"]
                )
                if unnest is True:
                    entry.pop("classifications", None)
                    to_unnest = metadados.classificacoes
                    static_cls: dict[str, list[str]] = {}
                else:
                    unnest_ids = {str(u) for u in unnest}
                    to_unnest = [
                        c for c in metadados.classificacoes if str(c.id) in unnest_ids
                    ]
                    static_cls = entry.pop("classifications", {})
                    for c in metadados.classificacoes:
                        cls_id = str(c.id)
                        if cls_id not in unnest_ids and cls_id not in static_cls:
                            static_cls[cls_id] = ["all"]
                for cls_combo in sidra.unnest_classificacoes(to_unnest):
                    result.append(
                        {
                            **entry,
                            "classifications": {**static_cls, **cls_combo},
                        }
                    )
            elif split_vars:
                variables = entry.pop("variables")
                for var in variables:
                    result.append({**entry, "variables": [var]})
            else:
                result.append(entry)

        return result

    def download(self, tabelas: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
        """Download all tables and return a list of data-file descriptors.

        Builds a flat plan across every requested table and submits it
        through a single ``ThreadPoolExecutor`` so downloads parallelize
        across table boundaries, not just across periods of one table.
        """
        plan: list[tuple[dict[str, Any], Any, str]] = []
        for tabela in tabelas:
            for parameter, modification in self.fetcher.plan_periods(**tabela):
                plan.append((tabela, parameter, modification))
        results = self.fetcher.download_periods(plan)
        return [
            r["key"] | {"filepath": r["filepath"], "modificacao": r["modificacao"]}
            for r in results
        ]

    def load_metadata(self, engine: sa.Engine, tabelas: Iterable[dict[str, Any]]):
        """Fetch and persist metadata for all unique SIDRA tables."""
        seen: set[str] = set()
        for tabela in tabelas:
            tabela_sidra_id = tabela["tabela_sidra"]
            if tabela_sidra_id in seen:
                continue
            seen.add(tabela_sidra_id)

            metadata_filepath = self.storage.get_metadata_filepath(tabela_sidra_id)
            if metadata_filepath.exists() and not self.force_metadata:
                logger.info("Reading cached metadata for table %s", tabela_sidra_id)
                agregado = self.storage.read_metadata(tabela_sidra_id)
            else:
                logger.info("Fetching metadata for table %s", tabela_sidra_id)
                agregado = self.fetcher.fetch_metadata(tabela_sidra_id)
                self.storage.write_metadata(agregado)

            logger.info("Saving metadata to database for table %s", tabela_sidra_id)
            database.save_agregado(engine, agregado)

    def run(self):
        """Execute the full fetch-and-load pipeline."""
        engine = database.get_engine(self.config)
        with engine.begin() as conn:
            conn.exec_driver_sql(
                f'CREATE SCHEMA IF NOT EXISTS "{self.config.db_schema}"'
            )
        models.Base.metadata.create_all(engine)
        database.ensure_vintage_schema(engine)
        try:
            self._run(engine)
        except KeyboardInterrupt:
            if self.console is not None:
                self.console.print("\n[yellow]Interrompido.[/yellow]")
            raise SystemExit(1)

    def _run(self, engine: sa.Engine):
        tabelas = list(self.get_tabelas())
        n = len(tabelas)
        s = "tabela" if n == 1 else "tabelas"
        n_meta = len({t["tabela_sidra"] for t in tabelas})
        s_meta = "tabela" if n_meta == 1 else "tabelas"

        with self.fetcher:
            with _make_progress(self.console) as progress:
                meta_task = progress.add_task(
                    f"Metadados ({n_meta} {s_meta})", total=None, main=True
                )
                self.load_metadata(engine, tabelas)
                progress.update(
                    meta_task,
                    total=1,
                    completed=1,
                    description=f"Metadados ({n_meta} {s_meta})",
                )

            plan: list[tuple[dict[str, Any], Any, str]] = []
            for tabela in tabelas:
                for parameter, modification in self.fetcher.plan_periods(**tabela):
                    plan.append((tabela, parameter, modification))

            n_plan = len(plan)
            if self.console is not None:
                info = Table.grid(padding=(0, 2))
                info.add_column(style="bold")
                info.add_column()
                info.add_row("Pipeline", str(self.toml_path))
                info.add_row("Tabelas", f"{n_meta} {s_meta}")
                info.add_row("Arquivos", str(n_plan))
                info.add_row("Threads", str(self.fetcher.max_workers))
                info.add_row(
                    "Banco",
                    f"{self.config.db_host}:{self.config.db_port}/{self.config.db_name}"
                    f"  schema={self.config.db_schema}",
                )
                info.add_row("Storage", str(self.config.data_dir))
                self.console.print(info)
                self.console.print()

            files_per_table: dict[str, int] = {}
            for tabela, _, _ in plan:
                sid = tabela["tabela_sidra"]
                files_per_table[sid] = files_per_table.get(sid, 0) + 1

            with _make_download_progress(self.console) as progress:
                global_task = progress.add_task("Download", total=n_plan, main=True)
                task_by_table: dict[str, TaskID] = {}
                if len(files_per_table) > 1:
                    for sid, count in files_per_table.items():
                        task_by_table[sid] = progress.add_task(
                            f"Tabela {sid}", total=count
                        )

                def _on_done(key: dict[str, Any]) -> None:
                    sub = task_by_table.get(key["tabela_sidra"])
                    if sub is not None:
                        progress.advance(sub)
                    progress.advance(global_task)

                results = self.fetcher.download_periods(plan, on_file_done=_on_done)
                data_files = [
                    r["key"]
                    | {
                        "filepath": r["filepath"],
                        "modificacao": r["modificacao"],
                    }
                    for r in results
                ]
                for sid, task_id in task_by_table.items():
                    progress.update(task_id, description=f"Tabela {sid} ✓")
                progress.update(global_task, description="Download concluído ✓")

        if not self.force_load:
            loaded = database.get_loaded_filenames(
                engine, {f["filepath"].name for f in data_files}
            )
            data_files = [f for f in data_files if f["filepath"].name not in loaded]
        if not data_files:
            if self.console is not None:
                self.console.print(
                    "[green]Todos os arquivos já foram carregados.[/green]"
                )
            return

        with _make_download_progress(self.console) as progress:
            db_files_per_table: dict[str, int] = {}
            for d in data_files:
                sid = str(d["tabela_sidra"])
                db_files_per_table[sid] = db_files_per_table.get(sid, 0) + 1
            n_db_files = sum(db_files_per_table.values())
            db_global_task = progress.add_task(
                "Carregando no banco de dados", total=n_db_files * 2, main=True
            )
            db_task_by_table: dict[str, TaskID] = {}
            if len(db_files_per_table) > 1:
                for sid, count in db_files_per_table.items():
                    db_task_by_table[sid] = progress.add_task(
                        f"Tabela {sid}", total=count * 2
                    )

            def _on_db_file_done(sid: str) -> None:
                sub = db_task_by_table.get(sid)
                if sub is not None:
                    progress.advance(sub)
                progress.advance(db_global_task)

            def _on_db_table_done(sid: str) -> None:
                sub = db_task_by_table.get(sid)
                if sub is not None:
                    progress.update(sub, description=f"Tabela {sid} ✓")

            database.load_dados(
                engine,
                self.storage,
                data_files,
                on_file_done=_on_db_file_done,
                on_table_done=_on_db_table_done,
            )
            progress.update(db_global_task, description="Carregamento concluído ✓")
