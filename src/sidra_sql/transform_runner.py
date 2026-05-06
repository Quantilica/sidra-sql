# Copyright (c) 2026 Komesu, D.K.
# Licensed under the MIT License.

"""Pipeline runner for SQL transformations defined by TOML + SQL file pairs.

Each transformation is defined by two files sharing the same stem:

``transformations/snpc/ipca.toml``
    Declares the target table name, schema, and materialization strategy.

``transformations/snpc/ipca.sql``
    Contains a SELECT query that produces analysis-ready data.

TOML schema
-----------
::

    [table]
    name        = "ipca"
    schema      = "analytics"
    strategy    = "replace"     # "replace" or "view"
    description = "IPCA - todas as séries históricas unificadas"

Strategies
~~~~~~~~~~
``replace``
    ``DROP TABLE IF EXISTS`` followed by ``CREATE TABLE ... AS``.
    Full refresh — best for batch imports into Power BI, Excel, etc.

``view``
    ``CREATE OR REPLACE VIEW``.  Zero storage cost, always up-to-date,
    best for live database connections.

The SQL query uses unqualified table names (``dados``, ``dimensao``,
``localidade``, ``sidra_tabela``).  They resolve via the database
``search_path`` set by ``get_engine`` from ``config.ini``.
"""

import contextlib
import logging
import tomllib
from pathlib import Path

from rich.console import Console

from . import database
from .config import Config

logger = logging.getLogger(__name__)


class TransformRunner:
    """Run a SQL transformation defined by a TOML + SQL file pair."""

    def __init__(self, config: Config, toml_path: Path, console: Console | None = None):
        self.config = config
        self.toml_path = toml_path
        self.console = console

    def _status(self, msg: str):
        if self.console:
            return self.console.status(msg)
        return contextlib.nullcontext()

    def run(self):
        with open(self.toml_path, "rb") as f:
            data = tomllib.load(f)

        table_config = data["table"]
        name = table_config["name"]
        schema = table_config["schema"]
        strategy = table_config.get("strategy", "replace")
        primary_key = table_config.get("primary_key")
        indexes = table_config.get("indexes", [])

        sql_path = self.toml_path.with_suffix(".sql")
        query = sql_path.read_text(encoding="utf-8").strip()

        engine = database.get_engine(self.config)
        qualified = f'"{schema}"."{name}"'

        strategy_label = {"replace": "tabela", "view": "view"}.get(strategy, strategy)
        with self._status(f"[cyan]Executando transform[/cyan] {qualified} [{strategy_label}]..."):
            with engine.begin() as conn:
                conn.exec_driver_sql(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')

                if strategy == "view":
                    conn.exec_driver_sql(
                        f"CREATE OR REPLACE VIEW {qualified} AS\n{query}"
                    )
                elif strategy == "replace":
                    conn.exec_driver_sql(f"DROP TABLE IF EXISTS {qualified}")
                    conn.exec_driver_sql(f"CREATE TABLE {qualified} AS\n{query}")

                    if primary_key:
                        pk_cols = ", ".join(f'"{c}"' for c in primary_key)
                        conn.exec_driver_sql(
                            f"ALTER TABLE {qualified} ADD PRIMARY KEY ({pk_cols})"
                        )

                    for idx in indexes:
                        idx_name = idx["name"]
                        idx_cols = ", ".join(f'"{c}"' for c in idx["columns"])
                        unique = "UNIQUE" if idx.get("unique") else ""
                        conn.exec_driver_sql(
                            f'CREATE {unique} INDEX "{idx_name}" ON {qualified} ({idx_cols})'
                        )
                else:
                    raise ValueError(
                        f"Unknown strategy {strategy!r} in {self.toml_path}"
                    )
