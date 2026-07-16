"""Export materialized analytics outputs (and as-of vintage snapshots) to CSV.

Reads a pipeline's ``transform.toml`` to discover its ``[[table]]`` outputs and
streams each to a CSV file using the PostgreSQL ``COPY ... TO STDOUT`` protocol
(no DataFrame/Polars dependency).

Two modes:

* **current** — ``COPY (SELECT * FROM "<schema>"."<name>")`` for each
  materialized analytics table/view (the current truth).
* **as-of** — reconstruct the outputs as IBGE published them on a date. A temp
  view ``dados`` shadows the real fact table (``search_path`` puts ``pg_temp``
  first), exposing, per logical key, the row with the greatest
  ``modificacao <= date`` flagged ``ativo``. The pipeline's own transform
  ``.sql`` runs against it unchanged — its ``WHERE ativo = true`` passes exactly
  that vintage — and the result is streamed to CSV. Persisted ``analytics.*``
  tables are left untouched.
"""

import datetime as dt
import tomllib
from pathlib import Path

from rich.console import Console

from . import database
from .config import Config


def read_outputs(toml_path: Path) -> list[dict]:
    """Return the ``[[table]]`` entries declared in a ``transform.toml``."""
    with open(toml_path, "rb") as f:
        data = tomllib.load(f)
    tables = data.get("table")
    if not isinstance(tables, list) or not tables:
        raise ValueError(f"{toml_path}: esperado um ou mais [[table]] (array).")
    for entry in tables:
        missing = [k for k in ("name", "schema", "sql") if k not in entry]
        if missing:
            raise ValueError(
                f"{toml_path}: [[table]] sem campo(s) obrigatório(s): "
                f"{', '.join(missing)}"
            )
    return tables


def _clean_query(sql: str) -> str:
    """Strip trailing whitespace/semicolons so the SELECT can be embedded."""
    return sql.strip().rstrip(";").strip()


def copy_table_sql(schema: str, name: str) -> str:
    """COPY that dumps a materialized table/view to CSV on STDOUT."""
    return (
        f'COPY (SELECT * FROM "{schema}"."{name}") TO STDOUT WITH (FORMAT csv, HEADER)'
    )


def copy_query_sql(query: str) -> str:
    """COPY that dumps an arbitrary SELECT to CSV on STDOUT."""
    return f"COPY ({_clean_query(query)}) TO STDOUT WITH (FORMAT csv, HEADER)"


def asof_view_sql(schema: str, asof: dt.date) -> str:
    """``CREATE TEMP VIEW dados`` exposing the as-of vintage as the active rows.

    Per logical key, picks the row with the greatest ``modificacao <= asof`` and
    flags it ``ativo``, so transforms filtering ``WHERE ativo = true`` see the
    data exactly as published on that date. The inner source is
    schema-qualified so the temp view does not reference itself. The date is a
    validated ``date`` object, inlined as a literal (no parameters allowed in a
    view definition).
    """
    return (
        "CREATE TEMP VIEW dados AS"
        " SELECT id, tabela_sidra_id, dimensao_id, localidade_id, periodo_id,"
        " modificacao, TRUE AS ativo, v FROM ("
        " SELECT DISTINCT ON"
        " (tabela_sidra_id, localidade_id, dimensao_id, periodo_id)"
        " id, tabela_sidra_id, dimensao_id, localidade_id, periodo_id,"
        " modificacao, v"
        f' FROM "{schema}".dados'
        f" WHERE modificacao <= DATE '{asof.isoformat()}'"
        " ORDER BY tabela_sidra_id, localidade_id, dimensao_id, periodo_id,"
        " modificacao DESC"
        " ) AS _asof"
    )


def stamped_name(name: str, asof: dt.date | None) -> str:
    """``name.csv`` for the current truth, ``name@YYYYMMDD.csv`` for as-of."""
    if asof is None:
        return f"{name}.csv"
    return f"{name}@{asof:%Y%m%d}.csv"


class Exporter:
    """Export a pipeline's analytics outputs to CSV files."""

    def __init__(
        self,
        config: Config,
        toml_path: Path,
        console: Console | None = None,
    ):
        self.config = config
        self.toml_path = toml_path
        self.console = console

    def export(self, output_dir: Path, asof: dt.date | None = None) -> list[Path]:
        """Export every declared output; return the paths actually written."""
        outputs = read_outputs(self.toml_path)
        output_dir.mkdir(parents=True, exist_ok=True)
        engine = database.get_engine(self.config)
        written: list[Path] = []

        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            pg_conn = conn.connection.driver_connection
            cur = pg_conn.cursor()

            if asof is not None:
                # pg_temp first so the temp view shadows the real `dados`.
                cur.execute(f"SET search_path = pg_temp, {self.config.db_schema}")
                cur.execute(asof_view_sql(self.config.db_schema, asof))

            for entry in outputs:
                name = entry["name"]
                schema = entry["schema"]
                out_path = output_dir / stamped_name(name, asof)

                if asof is None:
                    sql = copy_table_sql(schema, name)
                else:
                    query = (self.toml_path.parent / entry["sql"]).read_text(
                        encoding="utf-8"
                    )
                    sql = copy_query_sql(query)

                try:
                    self._copy_to_file(cur, sql, out_path)
                except Exception as exc:  # noqa: BLE001
                    self._warn(name, schema, asof, exc)
                    continue

                written.append(out_path)
                if self.console is not None:
                    self.console.print(f"  [green]✓[/green] {out_path}")

        return written

    @staticmethod
    def _copy_to_file(cur, sql: str, out_path: Path) -> None:
        with cur.copy(sql) as copy:
            with open(out_path, "wb") as fh:
                for chunk in copy:
                    fh.write(chunk)

    def _warn(
        self, name: str, schema: str, asof: dt.date | None, exc: Exception
    ) -> None:
        if self.console is None:
            return
        if asof is None:
            self.console.print(
                f"  [yellow]![/yellow] {schema}.{name} não encontrada — "
                "rode 'sidra-sql run' para materializar. (pulando)"
            )
        else:
            self.console.print(
                f"  [yellow]![/yellow] falha ao exportar {name} as-of: {exc}"
            )
