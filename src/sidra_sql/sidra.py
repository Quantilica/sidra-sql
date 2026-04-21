# Copyright (C) 2026 Komesu, D.K. <daniel@dkko.me>
#
# This file is part of sidra-sql.
#
# sidra-sql is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# sidra-sql is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with sidra-sql.  If not, see <https://www.gnu.org/licenses/>.

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
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Generator

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

    def download_table(
        self,
        sidra_tabela: str,
        territories: dict[str, list[str]],
        variables: list[str] | None = None,
        classifications: dict[str, list[str]] | None = None,
    ) -> list[dict[str, Any]]:
        """Download all periods of a SIDRA table and save them to disk.

        For each period returned by the SIDRA API this method builds a
        `Parametro`, requests the data and writes a CSV to
        ``data_dir / f"t-{sidra_tabela}" / filename``. If a destination
        file already exists it is skipped.

        Args:
            sidra_tabela: SIDRA table code (numeric string accepted).
            territories: Mapping of territory type codes to lists of
                territory identifiers (e.g. {"6": ["1234567"]}).
            variables: Optional list of variable codes to request. If
                omitted, the special value ["all"] is used.
            classifications: Optional mapping of classification id to a
                list of category ids. If omitted the method will fetch
                metadata and default to empty category lists for every
                classification.

        Returns:
            A list of dicts with keys "filepath" (Path) and "modificacao" (str).
        """

        if variables is None:
            variables = ["all"]

        # Use cached metadata when available — avoids redundant round-trips
        # after load_metadata has already fetched and stored the Agregado.
        metadata_path = self.storage.get_metadata_filepath(sidra_tabela)
        if metadata_path.exists():
            metadados = self.storage.read_metadata(sidra_tabela)
        else:
            metadados = self.sidra_client.get_agregado_metadados(int(sidra_tabela))

        if classifications is None:
            classifications = {
                str(c.id): [] for c in metadados.classificacoes
            }

        periodos = getattr(metadados, "periodos", None) or \
            self.sidra_client.get_agregado_periodos(agregado_id=int(sidra_tabela))

        period_params: list[tuple[Parametro, str]] = []
        for periodo in periodos:
            parameter = Parametro(
                agregado=sidra_tabela,
                territorios=territories,
                variaveis=variables,
                periodos=[periodo.id],
                classificacoes=classifications,
                decimais={"": Precisao.M},  # Precisão: Máxima
                formato=Formato.A,
            )
            period_params.append((parameter, periodo.modificacao.isoformat()))

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [
                (executor.submit(self._download_period, parameter, modification), modification)
                for parameter, modification in period_params
            ]

        results: list[dict[str, Any]] = []
        errors: list[Exception] = []
        for f, modification in futures:
            try:
                results.append({
                    "filepath": f.result(),
                    "modificacao": modification,
                })
            except Exception as e:
                logger.error("Period download failed: %s", e)
                errors.append(e)
        if errors:
            raise errors[0]
        return results

    def fetch_metadata(self, sidra_tabela: str) -> Agregado:
        """Fetch full metadata for a SIDRA table including localidades and periodos."""
        agregado = self.sidra_client.get_agregado_metadados(int(sidra_tabela))

        all_niveis = (
            agregado.nivel_territorial.administrativo
            + agregado.nivel_territorial.ibge
            + agregado.nivel_territorial.especial
        )

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            loc_futures = [
                executor.submit(
                    self.sidra_client.get_agregado_localidades,
                    agregado_id=int(sidra_tabela),
                    localidades_nivel=nivel,
                )
                for nivel in all_niveis
            ]

        localidades = []
        for f in loc_futures:
            localidades.extend(f.result())

        agregado.localidades = localidades
        agregado.periodos = self.sidra_client.get_agregado_periodos(
            int(sidra_tabela)
        )
        return agregado

    def _download_period(
        self,
        parameter: Parametro,
        modification: str,
    ) -> Path:
        """Download a single period and save it; return the destination path."""
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
            try:
                return self.sidra_client.get(url)
            except _TRANSIENT_ERRORS as e:
                if attempt >= _MAX_RETRIES - 1:
                    raise
                delay = _RETRY_BASE_DELAY * (2 ** attempt)
                logger.error("%s while fetching data: %s", type(e).__name__, e)
                logger.info(
                    "Retrying in %d s (attempt %d/%d)…",
                    delay, attempt + 1, _MAX_RETRIES,
                )
                time.sleep(delay)

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
