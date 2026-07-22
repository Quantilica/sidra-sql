"""Database helpers: engine creation and data-loading functions.

Public functions:
- `get_engine`: create a SQLAlchemy engine from `Config`.
- `save_agregado`: upsert SIDRA table metadata, periods, and localidades.
- `load_dados`: load data rows into the dados table (also upserts
  localidades and dimensions).
"""

import itertools
import json
import logging
from collections.abc import Callable, Iterable
from typing import Any

import sqlalchemy as sa
from sidra_fetcher.agregados import Agregado
from sidra_fetcher.periodos import expected_periodo_frequencias
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
    """Save SIDRA table metadata, periods, and localidades to the database.

    Idempotent.
    """

    tabela_sidra = dict(
        id=str(agregado.id),
        nome=agregado.nome,
        periodicidade=agregado.periodicidade.frequencia,
        metadados=json.loads(json.dumps(agregado.asdict(), default=str)),
    )
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
    localidades_iter = (
        dict(
            nc=str(localidade.nivel.id),
            nn=localidade.nivel.nome,
            d1c=str(localidade.id),
            d1n=localidade.nome,
        )
        for localidade in agregado.localidades
    )

    # Tabela, períodos e localidades numa única transação — ou tudo entra, ou
    # nada; sem estados parciais em caso de falha no meio.
    with engine.begin() as conn:
        stmt = pg_insert(models.TabelaSidra.__table__).values(tabela_sidra)
        stmt = stmt.on_conflict_do_update(
            index_elements=["id"],
            set_={"metadados": stmt.excluded.metadados},
        )
        conn.execute(stmt)

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

        while True:
            batch = list(itertools.islice(localidades_iter, _BATCH_SIZE))
            if not batch:
                break
            stmt = pg_insert(models.Localidade.__table__).values(batch)
            conn.execute(stmt.on_conflict_do_nothing())


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


def _dimensao_lookup_query(
    conn: sa.Connection, keys: Iterable[tuple] | None = None
) -> dict[tuple, int]:
    """Return a mapping of (mc, d2c, d4c...d9c) -> dimensao.id using an open
    connection.
    """
    lookup: dict[tuple, int] = {}
    stmt = sa.select(
        models.Dimensao.id,
        models.Dimensao.mc,
        models.Dimensao.d2c,
        models.Dimensao.d4c,
        models.Dimensao.d5c,
        models.Dimensao.d6c,
        models.Dimensao.d7c,
        models.Dimensao.d8c,
        models.Dimensao.d9c,
    )
    if keys is not None:
        # key format: (mc, d2c, d4c, d5c, d6c, d7c, d8c, d9c) — d2c is at index 1
        d2c_keys = list({k[1] for k in keys if k is not None and k[1] is not None})
        if not d2c_keys:
            return lookup
        for i in range(0, len(d2c_keys), _BATCH_SIZE):
            chunk_stmt = stmt.where(
                models.Dimensao.d2c.in_(d2c_keys[i : i + _BATCH_SIZE])
            )
            for row in conn.execute(chunk_stmt):
                lookup[
                    (
                        row.mc,
                        row.d2c,
                        row.d4c,
                        row.d5c,
                        row.d6c,
                        row.d7c,
                        row.d8c,
                        row.d9c,
                    )
                ] = row.id
    else:
        for row in conn.execute(stmt):
            lookup[
                (
                    row.mc,
                    row.d2c,
                    row.d4c,
                    row.d5c,
                    row.d6c,
                    row.d7c,
                    row.d8c,
                    row.d9c,
                )
            ] = row.id
    return lookup


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


# ---------------------------------------------------------------------------
# ETL
# ---------------------------------------------------------------------------


_STAGING_DDL = (
    "CREATE TEMP TABLE _staging_dados ("
    "  tabela_sidra_id text,"
    "  localidade_id bigint,"
    "  dimensao_id bigint,"
    "  periodo_id integer,"
    "  modificacao date,"
    "  v text"
    ") ON COMMIT DROP"
)

_STAGING_COPY = (
    "COPY _staging_dados"
    " (tabela_sidra_id, localidade_id, dimensao_id,"
    "  periodo_id, modificacao, v)"
    " FROM STDIN"
)

# Vintages distintos presentes no staging, em ordem ascendente. Processar nessa
# ordem garante que o histórico de change-points seja construído corretamente e
# que a revisão mais recente termine ativa.
_STAGING_MODIFICACOES = (
    "SELECT DISTINCT modificacao FROM _staging_dados ORDER BY modificacao"
)

# Desativa a linha ativa de uma chave quando o valor do vintage corrente difere
# (detecção-de-mudança). Recarga idêntica não toca nada.
_DEACTIVATE_GROUP = (
    "UPDATE dados d"
    " SET ativo = FALSE"
    " FROM _staging_dados s"
    " WHERE s.modificacao = %(mod)s"
    "  AND d.tabela_sidra_id = s.tabela_sidra_id"
    "  AND d.localidade_id = s.localidade_id"
    "  AND d.dimensao_id = s.dimensao_id"
    "  AND d.periodo_id = s.periodo_id"
    "  AND d.ativo = TRUE"
    "  AND d.v IS DISTINCT FROM s.v"
)

# Insere o vintage como nova linha ativa quando não há ativa para a chave
# (novo ou recém-desativado acima) ou quando o valor difere. DISTINCT ON evita
# inserir >1 linha por chave no mesmo grupo de modificacao.
_INSERT_GROUP = (
    "INSERT INTO dados"
    " (tabela_sidra_id, localidade_id, dimensao_id, periodo_id, modificacao, ativo, v)"
    " SELECT DISTINCT ON"
    "  (s.tabela_sidra_id, s.localidade_id, s.dimensao_id, s.periodo_id)"
    "  s.tabela_sidra_id, s.localidade_id, s.dimensao_id, s.periodo_id,"
    "  s.modificacao, TRUE, s.v"
    " FROM _staging_dados s"
    " LEFT JOIN dados d"
    "  ON d.tabela_sidra_id = s.tabela_sidra_id"
    "  AND d.localidade_id = s.localidade_id"
    "  AND d.dimensao_id = s.dimensao_id"
    "  AND d.periodo_id = s.periodo_id"
    "  AND d.ativo = TRUE"
    " WHERE s.modificacao = %(mod)s"
    "  AND (d.id IS NULL OR d.v IS DISTINCT FROM s.v)"
    " ORDER BY s.tabela_sidra_id, s.localidade_id, s.dimensao_id, s.periodo_id"
)


def _loc_key(r: dict) -> tuple[str, str]:
    """Return the (nc, d1c) lookup key for a data row."""
    return (_normalize_nc(_clean_str(r.get("NC"))), _clean_str(r.get("D1C")))


def _dim_key(r: dict) -> tuple:
    """Return the (mc, d2c, d4c..d9c) lookup key for a data row."""
    return (
        _coerce(r.get("MC")),
        _coerce(r.get("D2C")),
        _coerce(r.get("D4C")),
        _coerce(r.get("D5C")),
        _coerce(r.get("D6C")),
        _coerce(r.get("D7C")),
        _coerce(r.get("D8C")),
        _coerce(r.get("D9C")),
    )


def _collect_upsert_data(
    storage: Storage,
    table_files: list[dict],
    on_file_done: Callable[[], None] | None = None,
) -> tuple[list[dict], list[dict], set[tuple], Iterable[tuple], set[str], bool]:
    """Scan data files (Pass 1) and collect unique localidades, dimensions,
    and periodo codigos.

    Returns (loc_dicts, dim_dicts, seen_locs, dim_keys, seen_periodos, has_data).
    """
    seen_locs: set[tuple] = set()
    loc_dicts: list[dict] = []
    seen_dim_full: dict[tuple, dict] = {}
    seen_periodos: set[str] = set()
    has_data = False

    for data_file in table_files:
        for row in storage.read_data(data_file["filepath"]):
            if row.get("V") is None:
                continue
            has_data = True

            lk = _loc_key(row)
            if lk not in seen_locs:
                seen_locs.add(lk)
                loc_dicts.append(
                    {
                        "nc": lk[0],
                        "nn": str(row.get("NN", "")).strip(),
                        "d1c": lk[1],
                        "d1n": str(row.get("D1N", "")).strip(),
                    }
                )

            dim_full_key = _dim_key(row)
            if dim_full_key not in seen_dim_full:
                seen_dim_full[dim_full_key] = {
                    "mc": _coerce(row.get("MC")),
                    "mn": _coerce(row.get("MN")) or "",
                    "d2c": _coerce(row.get("D2C")) or "",
                    "d2n": _coerce(row.get("D2N")) or "",
                    "d4c": _coerce(row.get("D4C")),
                    "d4n": _coerce(row.get("D4N")),
                    "d5c": _coerce(row.get("D5C")),
                    "d5n": _coerce(row.get("D5N")),
                    "d6c": _coerce(row.get("D6C")),
                    "d6n": _coerce(row.get("D6N")),
                    "d7c": _coerce(row.get("D7C")),
                    "d7n": _coerce(row.get("D7N")),
                    "d8c": _coerce(row.get("D8C")),
                    "d8n": _coerce(row.get("D8N")),
                    "d9c": _coerce(row.get("D9C")),
                    "d9n": _coerce(row.get("D9N")),
                }

            d3c = _coerce(row.get("D3C"))
            if d3c:
                seen_periodos.add(d3c)

        if on_file_done is not None:
            on_file_done()

    return (
        loc_dicts,
        list(seen_dim_full.values()),
        seen_locs,
        seen_dim_full.keys(),
        seen_periodos,
        has_data,
    )


def _upsert_localidades_and_dims(
    conn: sa.Connection, loc_dicts: list[dict], dim_dicts: list[dict]
):
    """Upsert localidades and dimensoes in batches."""
    for i in range(0, len(loc_dicts), _BATCH_SIZE):
        stmt = pg_insert(models.Localidade.__table__).values(
            loc_dicts[i : i + _BATCH_SIZE]
        )
        conn.execute(stmt.on_conflict_do_nothing())
    for i in range(0, len(dim_dicts), _BATCH_SIZE):
        stmt = pg_insert(models.Dimensao.__table__).values(
            dim_dicts[i : i + _BATCH_SIZE]
        )
        conn.execute(stmt.on_conflict_do_nothing())
    conn.commit()


def _periodo_by_codigo_query(
    conn: sa.Connection,
    codigos: set[str],
    frequencias: set[str] | None = None,
) -> dict[str, int]:
    """Return a mapping of codigo -> periodo.id using a single batched query.

    When a codigo maps to multiple periodos (different literals arrays), the
    first result is used and a warning is logged.
    """
    lookup: dict[str, int] = {}
    codigos_list = list(codigos)
    for i in range(0, len(codigos_list), _BATCH_SIZE):
        stmt = sa.select(models.Periodo.id, models.Periodo.codigo).where(
            models.Periodo.codigo.in_(codigos_list[i : i + _BATCH_SIZE])
        )
        if frequencias:
            stmt = stmt.where(models.Periodo.frequencia.in_(frequencias))
        for row in conn.execute(stmt):
            if row.codigo not in lookup:
                lookup[row.codigo] = row.id
            else:
                logger.warning(
                    "Multiple periodos found for codigo '%s', using id %d",
                    row.codigo,
                    lookup[row.codigo],
                )
    return lookup


def _stream_staging(
    raw_conn,
    storage: Storage,
    table_files: list[dict],
    tabela_sidra_id: str,
    loc_lookup: dict[tuple, int],
    dim_lookup: dict[tuple, int],
    periodo_by_codigo: dict[str, int],
    on_file_done: Callable[[], None] | None = None,
) -> tuple[int, int, int, int, int, int]:
    """Stream resolved rows into the staging table via COPY, then flush to dados.

    Returns (n_rows, n_inserted, n_deactivated, missing_locs, missing_dims,
    missing_periodos).
    """
    missing_locs = missing_dims = missing_periodos = n_rows = 0
    # Amostra das chaves que não resolveram, para tornar o debug possível
    # (contar não basta — é preciso saber QUAIS chaves faltaram).
    _SAMPLE = 5
    sample_locs: list[tuple] = []
    sample_dims: list[tuple] = []
    sample_periodos: list[str | None] = []

    with raw_conn.cursor() as cur:
        cur.execute(_STAGING_DDL)
        with cur.copy(_STAGING_COPY) as copy:
            for data_file in table_files:
                modificacao = data_file["modificacao"]
                for row in storage.read_data(data_file["filepath"]):
                    if row.get("V") is None:
                        continue

                    loc_key = _loc_key(row)
                    loc_id = loc_lookup.get(loc_key)
                    if loc_id is None:
                        missing_locs += 1
                        if len(sample_locs) < _SAMPLE:
                            sample_locs.append(loc_key)
                        continue

                    dim_key = _dim_key(row)
                    dim_id = dim_lookup.get(dim_key)
                    if dim_id is None:
                        missing_dims += 1
                        if len(sample_dims) < _SAMPLE:
                            sample_dims.append(dim_key)
                        continue

                    periodo_codigo = _coerce(row.get("D3C"))
                    periodo_id = periodo_by_codigo.get(periodo_codigo)
                    if periodo_id is None:
                        missing_periodos += 1
                        if len(sample_periodos) < _SAMPLE:
                            sample_periodos.append(periodo_codigo)
                        continue

                    copy.write_row(
                        (
                            tabela_sidra_id,
                            loc_id,
                            dim_id,
                            periodo_id,
                            modificacao,
                            str(row.get("V")),
                        )
                    )
                    n_rows += 1

                if on_file_done is not None:
                    on_file_done()

        cur.execute(_STAGING_MODIFICACOES)
        modificacoes = [r[0] for r in cur.fetchall()]
        n_inserted = n_deactivated = 0
        for mod in modificacoes:
            cur.execute(_DEACTIVATE_GROUP, {"mod": mod})
            n_deactivated += cur.rowcount
            cur.execute(_INSERT_GROUP, {"mod": mod})
            n_inserted += cur.rowcount

    if missing_locs:
        logger.warning(
            "Table %s: %d rows with unknown localidade; sample keys (nc, d1c): %s",
            tabela_sidra_id,
            missing_locs,
            sample_locs,
        )
    if missing_dims:
        logger.warning(
            "Table %s: %d rows with unknown dimensao; sample keys "
            "(mc, d2c, d4c..d9c): %s",
            tabela_sidra_id,
            missing_dims,
            sample_dims,
        )
    if missing_periodos:
        logger.warning(
            "Table %s: %d rows with unknown periodo; sample codigos: %s",
            tabela_sidra_id,
            missing_periodos,
            sample_periodos,
        )

    return (
        n_rows,
        n_inserted,
        n_deactivated,
        missing_locs,
        missing_dims,
        missing_periodos,
    )


def ensure_vintage_schema(engine: sa.Engine) -> None:
    """Migrate an existing `dados` table to the vintage-storage layout.

    Transitional shim — repairs pre-vintage databases that still carry the full
    `uq_dados` unique constraint by dropping it and ensuring the partial unique
    index (one active row per key) plus the as-of support index. Fresh databases
    already get the correct layout from `Base.metadata.create_all`.

    Self-skipping: it inspects the catalog first and only issues DDL for what is
    actually missing. Once a database is migrated it runs **no** DDL at all (just
    a cheap catalog read), so it never re-acquires a table lock on subsequent
    runs. Safe to drop entirely once every deployment is known to be migrated.
    """
    with engine.begin() as conn:
        has_old_constraint = (
            conn.execute(
                sa.text(
                    "SELECT 1 FROM pg_constraint"
                    " WHERE conname = 'uq_dados'"
                    " AND conrelid = to_regclass('dados')"
                )
            ).first()
            is not None
        )
        has_partial_index = (
            conn.execute(sa.text("SELECT to_regclass('uq_dados_ativo')")).scalar()
            is not None
        )
        has_asof_index = (
            conn.execute(sa.text("SELECT to_regclass('ix_dados_asof')")).scalar()
            is not None
        )

        if not has_old_constraint and has_partial_index and has_asof_index:
            return  # already migrated — no DDL, no lock

        if has_old_constraint:
            conn.exec_driver_sql("ALTER TABLE dados DROP CONSTRAINT uq_dados")
        if not has_partial_index:
            conn.exec_driver_sql(
                "CREATE UNIQUE INDEX uq_dados_ativo"
                " ON dados (tabela_sidra_id, localidade_id, dimensao_id, periodo_id)"
                " WHERE ativo"
            )
        if not has_asof_index:
            conn.exec_driver_sql(
                "CREATE INDEX ix_dados_asof"
                " ON dados (tabela_sidra_id, periodo_id, modificacao)"
            )


def get_loaded_filenames(engine: sa.Engine, filenames: set[str]) -> set[str]:
    """Return the subset of filenames already recorded in arquivo_carregado."""
    if not filenames:
        return set()
    with engine.connect() as conn:
        result = conn.execute(
            sa.select(models.ArquivoCarregado.arquivo).where(
                models.ArquivoCarregado.arquivo.in_(filenames)
            )
        )
        return {row.arquivo for row in result}


def load_dados(
    engine: sa.Engine,
    storage: Storage,
    data_files: list[dict[str, Any]],
    on_file_done: Callable[[str], None] | None = None,
    on_table_done: Callable[[str], None] | None = None,
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
      temporary staging table via the PostgreSQL COPY protocol, then merge
      into dados per vintage (modificacao) in ascending order using
      deactivate-then-insert by value difference (vintage storage): each
      distinct value becomes a retained row, the latest stays `ativo`.
    """
    files_by_table: dict[str, list[dict]] = {}
    for data_file in data_files:
        tabela_sidra_id = str(data_file["tabela_sidra"])

        files_by_table.setdefault(tabela_sidra_id, []).append(data_file)

    for tabela_sidra_id, table_files in files_by_table.items():
        _file_done: Callable[[], None] | None = None
        if on_file_done is not None:
            sid = tabela_sidra_id

            def _file_done(s=sid) -> None:
                on_file_done(s)

        (
            loc_dicts,
            dim_dicts,
            seen_locs,
            seen_dim_lookup,
            seen_periodos,
            has_data,
        ) = _collect_upsert_data(storage, table_files, on_file_done=_file_done)

        if not has_data:
            logger.info("No data rows found for table %s", tabela_sidra_id)

            if on_table_done is not None:
                on_table_done(tabela_sidra_id)
            continue

        logger.info(
            "Collected %d unique periodo codigos from data for table %s",
            len(seen_periodos),
            tabela_sidra_id,
        )

        with engine.connect() as conn:
            _upsert_localidades_and_dims(conn, loc_dicts, dim_dicts)
            logger.info(
                "Upserted %d localidades and %d dimensions for table %s",
                len(loc_dicts),
                len(dim_dicts),
                tabela_sidra_id,
            )

            loc_lookup = _localidade_lookup_query(conn, keys=seen_locs)
            dim_lookup = _dimensao_lookup_query(conn, keys=seen_dim_lookup)
            periodicidade = conn.execute(
                sa.select(models.TabelaSidra.periodicidade).where(
                    models.TabelaSidra.id == tabela_sidra_id
                )
            ).scalar_one_or_none()
            periodo_by_codigo = _periodo_by_codigo_query(
                conn,
                seen_periodos,
                frequencias=expected_periodo_frequencias(periodicidade),
            )
            logger.info(
                "Matched %d periodos out of %d unique codigos from data",
                len(periodo_by_codigo),
                len(seen_periodos),
            )

            raw_conn = conn.connection.dbapi_connection
            (
                n_rows,
                n_inserted,
                n_deactivated,
                missing_locs,
                missing_dims,
                missing_periodos,
            ) = _stream_staging(
                raw_conn,
                storage,
                table_files,
                tabela_sidra_id,
                loc_lookup,
                dim_lookup,
                periodo_by_codigo,
                on_file_done=_file_done,
            )
            stmt = pg_insert(models.ArquivoCarregado.__table__).values(
                [
                    {
                        "arquivo": f["filepath"].name,
                        "tabela_sidra_id": tabela_sidra_id,
                    }
                    for f in table_files
                ]
            )
            conn.execute(stmt.on_conflict_do_nothing())
            conn.commit()

        if on_table_done is not None:
            on_table_done(tabela_sidra_id)

        if missing_dims > 0:
            logger.warning(
                "Skipping %d rows with unknown dimensao for table %s",
                missing_dims,
                tabela_sidra_id,
            )
        if missing_locs > 0:
            logger.warning(
                "Skipping %d rows with unknown localidade for table %s",
                missing_locs,
                tabela_sidra_id,
            )
        if missing_periodos > 0:
            logger.warning(
                "Skipping %d rows with unknown periodo for table %s",
                missing_periodos,
                tabela_sidra_id,
            )
        logger.info(
            "Loaded %d/%d rows into dados for table %s (%d deactivated)",
            n_inserted,
            n_rows,
            tabela_sidra_id,
            n_deactivated,
        )
