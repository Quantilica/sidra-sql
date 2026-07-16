"""Utilities to fetch and store SIDRA tables.

This module provides a `Fetcher` context-managed helper that wraps the
`sidra_fetcher` client to download SIDRA tables as CSV-backed pandas
DataFrames and write them to the project's data directory. It also
contains a helper `unnest_classificacoes` which expands nested
classification/category combinations into flat dictionaries suitable for
request parameters.

Public API
- `Fetcher`: context-managed client for downloading SIDRA tables.
- `unnest_classificacoes`: yields classification/category mappings.
"""

import logging
import threading
from collections.abc import Callable, Generator
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import httpx
from sidra_fetcher.agregados import Agregado, Classificacao
from sidra_fetcher.fetcher import SidraClient
from sidra_fetcher.sidra import Formato, Parametro, Precisao

from .config import Config
from .storage import Storage

logger = logging.getLogger(__name__)

_MAX_RETRIES = 5
_RETRY_BASE_DELAY = 5  # seconds; doubles on each attempt (5, 10, 20, 40, 80)

# Transient network conditions that warrant a retry
_TRANSIENT_ERRORS = (
    httpx.ReadTimeout,
    httpx.ConnectTimeout,
    httpx.ConnectError,
    httpx.RemoteProtocolError,
    httpx.NetworkError,
)


class Fetcher:
    """Helper to download SIDRA tables and save them locally.

    This class wraps a `SidraClient` to provide higher-level operations
    to download all periods of a given SIDRA table, write each period's
    result to disk and return the written file paths.

    Usage example::

        with Fetcher() as f:
            files = f.download_table(...)

    Attributes:
        sidra_client: An instance of `SidraClient` used to perform HTTP
            requests to the SIDRA API.
        storage: `Storage` repository where downloaded files are written.
        max_workers: Maximum number of concurrent period downloads.
    """

    def __init__(
        self,
        config: Config,
        max_workers: int = 4,
        storage: Storage | None = None,
    ):
        self.sidra_client = SidraClient(timeout=600)
        self.storage = storage if storage is not None else Storage.default(config)
        self.max_workers = max_workers
        self._cancel = threading.Event()

    def plan_periods(
        self,
        tabela_sidra: str,
        territories: dict[str, list[str]],
        variables: list[str] | None = None,
        classifications: dict[str, list[str]] | None = None,
    ) -> list[tuple[Parametro, str]]:
        """Build (Parametro, modification) tuples for every period of a table.

        Pure planning — no downloads. Use ``download_periods`` to fetch
        a flat plan concurrently across many tables.

        Args:
            tabela_sidra: SIDRA table code (numeric string accepted).
            territories: Mapping of territory type codes to lists of
                territory identifiers.
            variables: Optional list of variable codes. Defaults to ["all"].
            classifications: Optional classification → category mapping.
                If omitted, defaults to empty list for each declared
                classification (read from cached or fetched metadata).

        Returns:
            List of (Parametro, modification_iso_string) tuples — one per
            period of the requested table.
        """
        if variables is None:
            variables = ["all"]

        # Use cached metadata when available — avoids redundant round-trips
        # after load_metadata has already fetched and stored the Agregado.
        metadata_path = self.storage.get_metadata_filepath(tabela_sidra)
        if metadata_path.exists():
            metadados = self.storage.read_metadata(tabela_sidra)
        else:
            metadados = self.sidra_client.get_agregado_metadados(int(tabela_sidra))

        if classifications is None:
            classifications = {str(c.id): [] for c in metadados.classificacoes}

        periodos = getattr(
            metadados, "periodos", None
        ) or self.sidra_client.get_agregado_periodos(agregado_id=int(tabela_sidra))

        period_params: list[tuple[Parametro, str]] = []
        for periodo in periodos:
            parameter = Parametro(
                agregado=tabela_sidra,
                territorios=territories,
                variaveis=variables,
                periodos=[periodo.id],
                classificacoes=classifications,
                decimais={"": Precisao.M},  # Precisão: Máxima
                formato=Formato.A,
            )
            period_params.append((parameter, periodo.modificacao.isoformat()))
        return period_params

    def download_periods(
        self,
        plan: list[tuple[Any, Parametro, str]],
        on_file_done: Callable[[Any], None] | None = None,
    ) -> list[dict[str, Any]]:
        """Download many periods concurrently from a flat plan.

        Submits every (Parametro, modification) entry of ``plan`` to a
        single ``ThreadPoolExecutor`` capped at ``self.max_workers``,
        regardless of which source table each entry came from.

        Args:
            plan: Tuples of (key, parameter, modification). ``key`` is
                opaque metadata returned alongside each result so callers
                can correlate downloads back to their originating request.
            on_file_done: Optional callback fired once per completed
                download (success or failure), useful for progress bars.

        Returns:
            List of dicts with keys "key", "filepath", "modificacao", in
            completion order. Raises the first download error after all
            futures complete.
        """
        results: list[dict[str, Any]] = []
        errors: list[Exception] = []
        executor = ThreadPoolExecutor(max_workers=self.max_workers)
        try:
            future_to_meta = {
                executor.submit(self._download_period, parameter, modification): (
                    key,
                    modification,
                )
                for key, parameter, modification in plan
            }
            for future in as_completed(future_to_meta):
                key, modification = future_to_meta[future]
                try:
                    results.append(
                        {
                            "key": key,
                            "filepath": future.result(),
                            "modificacao": modification,
                        }
                    )
                except Exception as e:
                    logger.error("Period download failed: %s", e)
                    errors.append(e)
                if on_file_done is not None:
                    on_file_done(key)
        except KeyboardInterrupt:
            self._cancel.set()
            executor.shutdown(wait=True, cancel_futures=True)
            raise
        else:
            executor.shutdown(wait=True)
        if errors:
            raise errors[0]
        return results

    def download_table(
        self,
        tabela_sidra: str,
        territories: dict[str, list[str]],
        variables: list[str] | None = None,
        classifications: dict[str, list[str]] | None = None,
        on_file_done: Callable[[], None] | None = None,
    ) -> list[dict[str, Any]]:
        """Download all periods of a single SIDRA table and save them to disk.

        Convenience wrapper around ``plan_periods`` + ``download_periods``
        for callers that only need to fetch one table at a time. To
        parallelize across many tables, build a combined plan via
        ``plan_periods`` and submit it through ``download_periods``.

        Returns:
            A list of dicts with keys "filepath" (Path) and "modificacao" (str).
        """
        plan = [
            (None, parameter, modification)
            for parameter, modification in self.plan_periods(
                tabela_sidra=tabela_sidra,
                territories=territories,
                variables=variables,
                classifications=classifications,
            )
        ]
        results = self.download_periods(plan, on_file_done=on_file_done)
        return [
            {"filepath": r["filepath"], "modificacao": r["modificacao"]}
            for r in results
        ]

    def fetch_metadata(self, tabela_sidra: str) -> Agregado:
        """Fetch full metadata for a SIDRA table including localidades and periodos."""
        agregado = self.sidra_client.get_agregado_metadados(int(tabela_sidra))

        all_niveis = (
            agregado.nivel_territorial.administrativo
            + agregado.nivel_territorial.ibge
            + agregado.nivel_territorial.especial
        )

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            loc_futures = [
                executor.submit(
                    self.sidra_client.get_agregado_localidades,
                    agregado_id=int(tabela_sidra),
                    localidades_nivel=nivel,
                )
                for nivel in all_niveis
            ]

        localidades = []
        for f in loc_futures:
            localidades.extend(f.result())

        agregado.localidades = localidades
        agregado.periodos = self.sidra_client.get_agregado_periodos(int(tabela_sidra))
        return agregado

    def _download_period(
        self,
        parameter: Parametro,
        modification: str,
    ) -> Path:
        """Download a single period and save it; return the destination path."""
        if self._cancel.is_set():
            raise InterruptedError("cancelled")
        if self.storage.exists(parameter, modification):
            filepath = self.storage.get_data_filepath(parameter, modification)
            logger.debug("File already exists (cache hit): %s", filepath)
            return filepath
        logger.info(
            "Downloading %s",
            self.storage.get_data_filepath(parameter, modification).name,
        )
        data = self.get_table(parameter)
        return self.storage.write_data(
            data=data, parameter=parameter, modification=modification
        )

    def get_table(self, parameter: Parametro) -> dict:
        """Request a SIDRA table and return it as a dictionary.

        Retries up to `_MAX_RETRIES` times on transient network errors
        using exponential backoff (5 s, 10 s, 20 s, …).  Raises the
        underlying exception once all attempts are exhausted.

        Args:
            parameter: A `Parametro` instance with the desired request
                configuration.

        Returns:
            A `dict` constructed from the JSON response.
        """
        url = parameter.url()
        for attempt in range(_MAX_RETRIES):
            if self._cancel.is_set():
                raise InterruptedError("cancelled")
            try:
                return self.sidra_client.get(url)
            except _TRANSIENT_ERRORS as e:
                if attempt >= _MAX_RETRIES - 1:
                    raise
                delay = _RETRY_BASE_DELAY * (2**attempt)
                logger.error("%s while fetching data: %s", type(e).__name__, e)
                logger.info(
                    "Retrying in %d s (attempt %d/%d)…",
                    delay,
                    attempt + 1,
                    _MAX_RETRIES,
                )
                if self._cancel.wait(delay):
                    raise InterruptedError("cancelled") from None

    def __enter__(self):
        """Enter the context manager and return this `Fetcher`."""
        self.sidra_client.__enter__()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Close resources held by the fetcher.

        Delegates to the `SidraClient` context manager to ensure any
        network resources are cleaned up. Arguments are forwarded from
        the context manager protocol.
        """
        self.sidra_client.__exit__(exc_type, exc_value, traceback)


def unnest_classificacoes(
    classificacoes: list[Classificacao],
    data: dict[str, list[str]] | None = None,
) -> Generator[dict[str, list[str]], None, None]:
    """Recursively enumerate classification/category combinations.

    SIDRA classifications can be nested. This generator produces a flat
    sequence of mappings suitable to pass as the ``classificacoes``
    parameter when requesting aggregated data: each yielded dict maps a
    classification id (string) to a single-element list containing a
    category id (string).

    The function skips categories with id "0" which usually represent
    an undefined or "all" category.

    Args:
        classificacoes: List of `Classificacao` objects (from
            `sidra_fetcher`) to expand.
        data: Internal accumulator used by recursion; callers should
            normally omit this argument.

    Yields:
        Dictionaries mapping classification id to a singleton list of
        category ids, representing one combination of categories across
        the provided classifications.
    """
    if data is None:
        data = {}
    if not classificacoes:
        return
    classificacao = classificacoes[0]
    classificacao_id = str(classificacao.id)
    for categoria in classificacao.categorias:
        categoria_id = str(categoria.id)
        if categoria_id == "0":
            continue
        new_data = {**data, classificacao_id: [categoria_id]}
        if len(classificacoes) == 1:
            yield new_data
        else:
            yield from unnest_classificacoes(classificacoes[1:], new_data)
