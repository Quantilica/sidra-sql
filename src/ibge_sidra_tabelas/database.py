# Copyright (C) 2026 Komesu, D.K. <daniel@dkko.me>
#
# This file is part of ibge-sidra-tabelas.
#
# ibge-sidra-tabelas is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# ibge-sidra-tabelas is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with ibge-sidra-tabelas.  If not, see <https://www.gnu.org/licenses/>.

"""Database helpers: engine creation and data-loading functions.

Public functions:
- `get_engine`: create a SQLAlchemy engine from `Config`.
- `save_agregado`: upsert SIDRA table metadata, periods, and localidades.
- `build_localidade_lookup`: query localidade IDs by (nc, d1c) keys.
- `build_dimensao_lookup`: query dimensao IDs by dimension key tuples.
- `build_periodo_lookup`: query periodo IDs by (codigo, literals) keys.
- `load_dados`: load data rows into the dados table (also upserts
  localidades and dimensions).
- `build_ddl`: build a CREATE TABLE statement string.
- `build_dcl`: build owner/grant statements for a table.
"""

import itertools
import json
import logging
from typing import Any, Iterable

import sqlalchemy as sa
from sidra_fetcher.agregados import Agregado
from sqlalchemy.dialects.postgresql import insert as pg_insert

from . import models
from .config import Config
from .storage import Storage

logger = logging.getLogger(__name__)

_BATCH_SIZE = 5000


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _coerce(val) -> str | None:
    """Return str(val), or None if val is None."""
    return str(val) if val is not None else None


def _clean_str(val) -> str:
    """Normalize a territory/locality code: strip and remove trailing .0."""
    if val is None:
        return ""
    s = str(val).strip()
    return s[:-2] if s.endswith(".0") else s


def _normalize_nc(nc: str) -> str:
    """Ensure NC uses the 'N<n>' format (e.g. '6' -> 'N6', 'N6' -> 'N6')."""
    if nc and not nc.startswith("N"):
        return "N" + nc
    return nc


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------

def get_engine(config: Config) -> sa.engine.Engine:
    """Create and return a SQLAlchemy engine for the configured DB."""
    connection_string = (
        f"postgresql+psycopg://{config.db_user}:{config.db_password}"
        f"@{config.db_host}:{config.db_port}/{config.db_name}"
    )
    return sa.create_engine(
        connection_string,
        connect_args={"options": f"-c search_path={config.db_schema}"},
    )


# ---------------------------------------------------------------------------
# Metadata
# ---------------------------------------------------------------------------

def save_agregado(engine: sa.engine.Engine, agregado: Agregado):
    """Save SIDRA table metadata, periods, and localidades to the database (idempotent)."""
    sidra_tabela = dict(
        id=str(agregado.id),
        nome=agregado.nome,
        periodicidade=agregado.periodicidade.frequencia,
        metadados=json.loads(json.dumps(agregado.asdict(), default=str)),
    )
    with engine.connect() as conn:
        stmt = pg_insert(models.SidraTabela.__table__).values(sidra_tabela)
        stmt = stmt.on_conflict_do_update(
            index_elements=["id"],
            set_={"metadados": stmt.excluded.metadados},
        )
        conn.execute(stmt)
        conn.commit()

    # Save periods
    periodos_iter = (
        dict(
            codigo=periodo.id,
            literals=periodo.literals,
            frequencia=periodo.frequencia,
            data_inicio=periodo.data_inicio if periodo.data_inicio else None,
            data_fim=periodo.data_fim if periodo.data_fim else None,
            ano=periodo.ano,
            ano_fim=periodo.ano_fim,
            semestre=periodo.semestre,
            trimestre=periodo.trimestre,
            mes=periodo.mes,
        )
        for periodo in agregado.periodos
    )
    with engine.connect() as conn:
        while True:
            batch = list(itertools.islice(periodos_iter, _BATCH_SIZE))
            if not batch:
                break
            stmt = pg_insert(models.Periodo.__table__).values(batch)
            stmt = stmt.on_conflict_do_update(
                constraint="uq_periodo",
                set_={
                    "frequencia": stmt.excluded.frequencia,
                    "data_inicio": stmt.excluded.data_inicio,
                    "data_fim": stmt.excluded.data_fim,
                    "ano": stmt.excluded.ano,
                    "ano_fim": stmt.excluded.ano_fim,
                    "semestre": stmt.excluded.semestre,
                    "trimestre": stmt.excluded.trimestre,
                    "mes": stmt.excluded.mes,
                },
            )
            conn.execute(stmt)
            conn.commit()

    localidades_iter = (
        dict(
            nc=str(localidade.nivel.id),
            nn=localidade.nivel.nome,
            d1c=str(localidade.id),
            d1n=localidade.nome,
        )
        for localidade in agregado.localidades
    )
    with engine.connect() as conn:
        while True:
            batch = list(itertools.islice(localidades_iter, _BATCH_SIZE))
            if not batch:
                break
            stmt = pg_insert(models.Localidade.__table__).values(batch)
            conn.execute(stmt.on_conflict_do_nothing())
            conn.commit()


# ---------------------------------------------------------------------------
# Lookups
# ---------------------------------------------------------------------------

def _localidade_lookup_query(
    conn: sa.Connection, keys: Iterable[tuple] | None = None
) -> dict[tuple, int]:
    """Return a mapping of (nc, d1c) -> localidade.id using an open connection."""
    lookup: dict[tuple, int] = {}
    stmt = sa.select(
        models.Localidade.id,
        models.Localidade.nc,
        models.Localidade.d1c,
    )
    if keys is not None:
        keys = list(keys)
        if not keys:
            return lookup
        for i in range(0, len(keys), _BATCH_SIZE):
            chunk_stmt = stmt.where(
                sa.tuple_(models.Localidade.nc, models.Localidade.d1c).in_(
                    keys[i : i + _BATCH_SIZE]
                )
            )
            for row in conn.execute(chunk_stmt):
                lookup[(row.nc, row.d1c)] = row.id
    else:
        for row in conn.execute(stmt):
            lookup[(row.nc, row.d1c)] = row.id
    return lookup


def build_localidade_lookup(
    engine: sa.Engine, keys: Iterable[tuple] | None = None
) -> dict[tuple, int]:
    """Return a mapping of (nc, d1c) -> localidade.id."""
    with engine.connect() as conn:
        return _localidade_lookup_query(conn, keys)


def _dimensao_lookup_query(
    conn: sa.Connection, keys: Iterable[tuple] | None = None
) -> dict[tuple, int]:
    """Return a mapping of (d2c, d4c...d9c) -> dimensao.id using an open connection."""
    lookup: dict[tuple, int] = {}
    stmt = sa.select(
        models.Dimensao.id,
        models.Dimensao.d2c,
        models.Dimensao.d4c,
        models.Dimensao.d5c,
        models.Dimensao.d6c,
        models.Dimensao.d7c,
        models.Dimensao.d8c,
        models.Dimensao.d9c,
    )
    if keys is not None:
        d2c_keys = list(
            {k[0] for k in keys if k is not None and k[0] is not None}
        )
        if not d2c_keys:
            return lookup
        for i in range(0, len(d2c_keys), _BATCH_SIZE):
            chunk_stmt = stmt.where(
                models.Dimensao.d2c.in_(d2c_keys[i : i + _BATCH_SIZE])
            )
            for row in conn.execute(chunk_stmt):
                lookup[(row.d2c, row.d4c, row.d5c, row.d6c, row.d7c, row.d8c, row.d9c)] = row.id
    else:
        for row in conn.execute(stmt):
            lookup[(row.d2c, row.d4c, row.d5c, row.d6c, row.d7c, row.d8c, row.d9c)] = row.id
    return lookup


def build_dimensao_lookup(
    engine: sa.Engine, keys: Iterable[tuple] | None = None
) -> dict[tuple, int]:
    """Return a mapping of (d2c, d4c...d9c) -> dimensao.id."""
    with engine.connect() as conn:
        return _dimensao_lookup_query(conn, keys)


def _periodo_lookup_query(
    conn: sa.Connection, keys: Iterable[tuple] | None = None
) -> dict[tuple, int]:
    """Return a mapping of (codigo, literals) -> periodo.id using an open connection."""
    lookup: dict[tuple, int] = {}
    stmt = sa.select(
        models.Periodo.id,
        models.Periodo.codigo,
        models.Periodo.literals,
    )
    if keys is not None:
        keys = list(keys)
        if not keys:
            return lookup
        # Extract unique codigos from keys for batch querying
        codigos = list({k[0] for k in keys if k and k[0] is not None})
        if not codigos:
            return lookup
        for i in range(0, len(codigos), _BATCH_SIZE):
            chunk_stmt = stmt.where(
                models.Periodo.codigo.in_(codigos[i : i + _BATCH_SIZE])
            )
            for row in conn.execute(chunk_stmt):
                literals_tuple = tuple(row.literals) if row.literals else ()
                lookup[(row.codigo, literals_tuple)] = row.id
    else:
        for row in conn.execute(stmt):
            literals_tuple = tuple(row.literals) if row.literals else ()
            lookup[(row.codigo, literals_tuple)] = row.id
    return lookup


def build_periodo_lookup(
    engine: sa.Engine, keys: Iterable[tuple] | None = None
) -> dict[tuple, int]:
    """Return a mapping of (codigo, literals) -> periodo.id."""
    with engine.connect() as conn:
        return _periodo_lookup_query(conn, keys)


# ---------------------------------------------------------------------------
# ETL
# ---------------------------------------------------------------------------

_STAGING_DDL = (
    "CREATE TEMP TABLE _staging_dados ("
    "  sidra_tabela_id text,"
    "  localidade_id bigint,"
    "  dimensao_id bigint,"
    "  periodo_id integer,"
    "  modificacao date,"
    "  ativo boolean,"
    "  v text"
    ") ON COMMIT DROP"
)

_STAGING_INSERT = (
    "INSERT INTO dados"
    " (sidra_tabela_id, localidade_id, dimensao_id, periodo_id, modificacao, ativo, v)"
    " SELECT sidra_tabela_id, localidade_id, dimensao_id,"
    "  periodo_id, modificacao, ativo, v"
    " FROM _staging_dados"
    " ON CONFLICT DO NOTHING"
)

_STAGING_DEACTIVATE = (
    "UPDATE dados d"
    " SET ativo = FALSE"
    " FROM ("
    "  SELECT sidra_tabela_id, periodo_id, MAX(modificacao) AS max_mod"
    "  FROM _staging_dados"
    "  GROUP BY sidra_tabela_id, periodo_id"
    " ) latest"
    " WHERE d.sidra_tabela_id = latest.sidra_tabela_id"
    "  AND d.periodo_id = latest.periodo_id"
    "  AND d.modificacao < latest.max_mod"
    "  AND d.ativo = TRUE"
)

_STAGING_COPY = (
    "COPY _staging_dados"
    " (sidra_tabela_id, localidade_id, dimensao_id,"
    "  periodo_id, modificacao, ativo, v)"
    " FROM STDIN"
)


def load_dados(
    engine: sa.Engine,
    storage: Storage,
    data_files: list[dict[str, Any]],
):
    """Load data rows from JSON files into the dados table.

    Also upserts localidades and dimensions found in the data files,
    so a separate upsert call is not needed. Files are grouped by
    SIDRA table and loaded with a two-pass approach:

    * Pass 1 — collect unique localidade/dimension rows and lookup keys
      (small memory footprint).
    * Between passes — upsert localidades and dimensions, then build
      ID lookup dicts.
    * Pass 2 — re-read the JSON files and stream resolved rows into a
      temporary staging table via the PostgreSQL COPY protocol, then
      INSERT into dados with ON CONFLICT DO NOTHING.
    """
    files_by_table: dict[str, list[dict]] = {}
    for data_file in data_files:
        sidra_tabela_id = str(data_file["sidra_tabela"])
        files_by_table.setdefault(sidra_tabela_id, []).append(data_file)

    for sidra_tabela_id, table_files in files_by_table.items():
        # --- Pass 1: collect unique localidades, dimensions, keys ---
        seen_locs: set[tuple] = set()
        loc_dicts: list[dict] = []
        seen_dim_full: set[tuple] = set()
        dim_dicts: list[dict] = []
        seen_dim_lookup: set[tuple] = set()
        seen_periodos: set[str] = set()
        has_data = False

        for data_file in table_files:
            filepath = data_file["filepath"]
            rows = storage.read_data(filepath)
            for r in rows:
                if r.get("V") is None:
                    continue
                has_data = True

                nc = _normalize_nc(_clean_str(r.get("NC")))
                d1c = _clean_str(r.get("D1C"))
                loc_key = (nc, d1c)
                if loc_key not in seen_locs:
                    seen_locs.add(loc_key)
                    loc_dicts.append({
                        "nc": nc,
                        "nn": str(r.get("NN", "")).strip(),
                        "d1c": d1c,
                        "d1n": str(r.get("D1N", "")).strip(),
                    })

                dim_full_key = (
                    _coerce(r.get("MC")),
                    _coerce(r.get("D2C")),
                    _coerce(r.get("D4C")),
                    _coerce(r.get("D5C")),
                    _coerce(r.get("D6C")),
                    _coerce(r.get("D7C")),
                    _coerce(r.get("D8C")),
                    _coerce(r.get("D9C")),
                )
                if dim_full_key not in seen_dim_full:
                    seen_dim_full.add(dim_full_key)
                    dim_dicts.append({
                        "mc":  _coerce(r.get("MC")),
                        "mn":  _coerce(r.get("MN")) or "",
                        "d2c": _coerce(r.get("D2C")) or "",
                        "d2n": _coerce(r.get("D2N")) or "",
                        "d4c": _coerce(r.get("D4C")), "d4n": _coerce(r.get("D4N")),
                        "d5c": _coerce(r.get("D5C")), "d5n": _coerce(r.get("D5N")),
                        "d6c": _coerce(r.get("D6C")), "d6n": _coerce(r.get("D6N")),
                        "d7c": _coerce(r.get("D7C")), "d7n": _coerce(r.get("D7N")),
                        "d8c": _coerce(r.get("D8C")), "d8n": _coerce(r.get("D8N")),
                        "d9c": _coerce(r.get("D9C")), "d9n": _coerce(r.get("D9N")),
                    })

                seen_dim_lookup.add((
                    _coerce(r.get("D2C")),
                    _coerce(r.get("D4C")),
                    _coerce(r.get("D5C")),
                    _coerce(r.get("D6C")),
                    _coerce(r.get("D7C")),
                    _coerce(r.get("D8C")),
                    _coerce(r.get("D9C")),
                ))

                d3c = _coerce(r.get("D3C"))
                if d3c:
                    # Store just codigo for now; we'll fetch literals from DB
                    seen_periodos.add(d3c)
            del rows

        if not has_data:
            logger.info("No data rows found for table %s", sidra_tabela_id)
            continue

        logger.info(
            "Collected %d unique periodo codigos from data for table %s",
            len(seen_periodos), sidra_tabela_id,
        )

        with engine.connect() as conn:
            # Upsert localidades
            for i in range(0, len(loc_dicts), _BATCH_SIZE):
                stmt = pg_insert(models.Localidade.__table__).values(
                    loc_dicts[i : i + _BATCH_SIZE]
                )
                conn.execute(stmt.on_conflict_do_nothing())

            # Upsert dimensions
            for i in range(0, len(dim_dicts), _BATCH_SIZE):
                stmt = pg_insert(models.Dimensao.__table__).values(
                    dim_dicts[i : i + _BATCH_SIZE]
                )
                conn.execute(stmt.on_conflict_do_nothing())
            conn.commit()

            logger.info(
                "Upserted %d localidades and %d dimensions for table %s",
                len(loc_dicts), len(dim_dicts), sidra_tabela_id,
            )

            # Build lookups
            loc_lookup = _localidade_lookup_query(conn, keys=seen_locs)
            dim_lookup = _dimensao_lookup_query(conn, keys=seen_dim_lookup)

            # Build periodo lookup by fetching all periods with the codigos we need.
            # Note: Periodo table has unique constraint on (codigo, literals), so a codigo
            # can map to multiple periodos with different literals arrays. Since the data
            # only has codigo without literals, we handle this by:
            # - If a codigo maps to exactly one periodo, use it
            # - If a codigo maps to multiple periodos, use the first one
            periodo_by_codigo = {}
            n_periodos_found = 0
            for codigo in seen_periodos:
                stmt = sa.select(
                    models.Periodo.id,
                    models.Periodo.codigo,
                    models.Periodo.literals,
                ).where(models.Periodo.codigo == codigo)
                results = list(conn.execute(stmt))
                if results:
                    row = results[0]
                    periodo_by_codigo[codigo] = row.id
                    n_periodos_found += 1
                    if len(results) > 1:
                        logger.warning(
                            "Found %d periodos with codigo '%s' (using id %d)",
                            len(results), codigo, row.id,
                        )
            logger.info(
                "Matched %d periodos out of %d unique codigos from data",
                n_periodos_found, len(seen_periodos),
            )

            # --- Pass 2: re-read files and stream via COPY ---
            raw_conn = conn.connection.dbapi_connection
            missing_dims = 0
            missing_locs = 0
            missing_periodos = 0
            n_rows = 0

            with raw_conn.cursor() as cur:
                cur.execute(_STAGING_DDL)
                with cur.copy(_STAGING_COPY) as copy:
                    for data_file in table_files:
                        filepath = data_file["filepath"]
                        modificacao = data_file["modificacao"]
                        rows = storage.read_data(filepath)
                        for r in rows:
                            if r.get("V") is None:
                                continue

                            nc = _normalize_nc(_clean_str(r.get("NC")))
                            d1c = _clean_str(r.get("D1C"))
                            loc_id = loc_lookup.get((nc, d1c))
                            if loc_id is None:
                                missing_locs += 1
                                continue

                            dim_key = (
                                _coerce(r.get("D2C")),
                                _coerce(r.get("D4C")),
                                _coerce(r.get("D5C")),
                                _coerce(r.get("D6C")),
                                _coerce(r.get("D7C")),
                                _coerce(r.get("D8C")),
                                _coerce(r.get("D9C")),
                            )
                            dim_id = dim_lookup.get(dim_key)
                            if dim_id is None:
                                missing_dims += 1
                                continue

                            d3c = _coerce(r.get("D3C"))
                            periodo_id = periodo_by_codigo.get(d3c)
                            if periodo_id is None:
                                missing_periodos += 1
                                continue

                            copy.write_row((
                                sidra_tabela_id,
                                loc_id,
                                dim_id,
                                periodo_id,
                                modificacao,
                                True,
                                str(r.get("V")),
                            ))
                            n_rows += 1
                        del rows

                cur.execute(_STAGING_INSERT)
                n_inserted = cur.rowcount
                cur.execute(_STAGING_DEACTIVATE)
                n_deactivated = cur.rowcount

            conn.commit()

        if missing_dims > 0:
            logger.warning(
                "Skipping %d rows with unknown dimensao for table %s",
                missing_dims, sidra_tabela_id,
            )
        if missing_locs > 0:
            logger.warning(
                "Skipping %d rows with unknown localidade for table %s",
                missing_locs, sidra_tabela_id,
            )
        if missing_periodos > 0:
            logger.warning(
                "Skipping %d rows with unknown periodo for table %s",
                missing_periodos, sidra_tabela_id,
            )
        logger.info(
            "Loaded %d/%d rows into dados for table %s (%d deactivated)",
            n_inserted, n_rows, sidra_tabela_id, n_deactivated,
        )


# ---------------------------------------------------------------------------
# DDL / DCL builders
# ---------------------------------------------------------------------------

def build_ddl(
    schema: str,
    table_name: str,
    tablespace: str,
    columns: dict[str, str],
    primary_keys: Iterable[str],
    comment: str = "",
) -> str:
    """Build a CREATE TABLE DDL string."""
    table_definition = ", ".join(
        f"{col} {typ}" for col, typ in columns.items()
    )
    primary_keys_definition = ", ".join(primary_keys)

    ddl_create_table = (
        f"CREATE TABLE IF NOT EXISTS {schema}.{table_name} (\n"
        f"    {table_definition},\n"
        f"    CONSTRAINT {table_name}_pkey PRIMARY KEY ({primary_keys_definition})\n"
        ")\n"
        f"TABLESPACE {tablespace};"
    )

    ddl_comment_table = ""
    if comment:
        ddl_comment_table = f"COMMENT ON TABLE {schema}.{table_name} IS '{comment}';"

    return "\n\n".join((ddl_create_table, ddl_comment_table))


def build_dcl(
    schema: str,
    table_name: str,
    table_owner: str,
    table_user: str,
) -> str:
    """Build DCL statements to set table owner and grant SELECT."""
    dcl_table_owner = (
        f"ALTER TABLE IF EXISTS {schema}.{table_name} OWNER TO {table_owner};"
    )
    dcl_grant = f"GRANT SELECT ON TABLE {schema}.{table_name} TO {table_user};"
    return "\n\n".join((dcl_table_owner, dcl_grant))
