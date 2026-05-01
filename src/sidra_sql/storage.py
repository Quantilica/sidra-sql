# Copyright (c) 2026 Komesu, D.K.
# Licensed under the MIT License.

"""Storage repository for SIDRA data files.

This module provides the `Storage` class, which centralizes all
filesystem operations for SIDRA tables: constructing deterministic file
paths from a `Parametro`, checking whether a file already exists, and
reading/writing JSON data files.
"""

import logging
from pathlib import Path

import orjson
from sidra_fetcher.agregados import Agregado
from sidra_fetcher.reader import load_agregado, save_agregado
from sidra_fetcher.sidra import Parametro

from .config import Config

logger = logging.getLogger(__name__)


class Storage:
    def __init__(self, data_dir: Path | str):
        self.data_dir = Path(data_dir)

    @classmethod
    def default(cls, config: Config) -> "Storage":
        """Create a Storage rooted at the data directory from config."""
        data_dir = config.data_dir
        data_dir.mkdir(exist_ok=True, parents=True)
        return cls(data_dir)

    @staticmethod
    def build_data_filename(parameter: Parametro, modification: str) -> str:
        """Build a deterministic filename for a SIDRA `Parametro`.

        The filename encodes the table id, periods, format, territorial
        levels, variables and classifications so that each unique request
        maps to a unique file name. The returned filename ends with
        ``@{modification}.json`` where ``modification`` is typically the
        period modification timestamp used by the API.

        Args:
            parameter: A `Parametro` instance containing request
                configuration (table, periods, territorios, variaveis,
                classificacoes, formato).
            modification: A string representing the modification
                timestamp to append to the filename.

        Returns:
            A string suitable for use as a JSON filename.
        """
        sidra_table = parameter.agregado
        periods = ",".join(parameter.periodos)
        formato = parameter.formato.value
        name = f"t-{sidra_table}_p-{periods}_f-{formato}"
        for territorial_level in parameter.territorios:
            territorial_codes = ",".join(
                str(code) for code in parameter.territorios[territorial_level]
            )
            if territorial_codes == "":
                territorial_codes = "all"
            name += f"_n{territorial_level}-{territorial_codes}"
        if parameter.variaveis:
            variables = ",".join(str(var) for var in parameter.variaveis)
            name += f"_v-{variables}"
        for classification, categories in parameter.classificacoes.items():
            str_categories = ",".join(categories)
            name += f"_c{classification}-{str_categories}"
        name += f"@{modification}"
        name += ".json"
        return name

    def get_metadata_filepath(self, agregado: int | str) -> Path:
        """Return the full path for a table's metadata JSON file."""
        return self.data_dir / f"t-{agregado}" / "metadados.json"

    def get_data_filepath(
        self,
        parameter: Parametro,
        modification: str,
    ) -> Path:
        """Return the full path for the given parameter and modification."""
        filename = self.build_data_filename(parameter, modification)
        return self.data_dir / f"t-{parameter.agregado}" / filename

    def exists(self, parameter: Parametro, modification: str) -> bool:
        """Return True if the file for the given parameter already exists."""
        return self.get_data_filepath(parameter, modification).exists()

    def write_data(
        self,
        data: dict,
        parameter: Parametro,
        modification: str,
    ) -> Path:
        """Write *data* to disk as JSON and return the destination path."""
        filepath = self.get_data_filepath(parameter, modification)
        filepath.parent.mkdir(parents=True, exist_ok=True)
        logger.info("Writing file %s", filepath)
        with filepath.open("w", encoding="utf-8") as f:
            f.write(orjson.dumps(data, option=orjson.OPT_INDENT_2).decode())
        return filepath

    def read_data(self, filepath: Path) -> list[dict]:
        """Read a JSON file previously written by `write`.

        Args:
            filepath: Path to the JSON file to read.

        Returns:
            A list of dicts containing the table data.
        """
        logger.info("Reading file %s", filepath)
        with filepath.open("rb") as f:
            data = orjson.loads(f.read())

        if len(data) > 1:
            rows = data[1:]
            for row in rows:
                for k, v in row.items():
                    if v in ("...", "-"):
                        row[k] = None
            return rows
        return []

    def write_metadata(self, agregado: Agregado) -> Path:
        """Write *agregado* metadata to disk as JSON and return the destination path."""
        filepath = self.get_metadata_filepath(agregado.id)
        filepath.parent.mkdir(parents=True, exist_ok=True)
        logger.info("Writing file %s", filepath)
        save_agregado(agregado, filepath)
        return filepath

    def read_metadata(self, agregado: int | str) -> Agregado:
        """Read metadata from disk as JSON and return the destination path."""
        filepath = self.get_metadata_filepath(agregado)
        agregado = load_agregado(filepath)
        return agregado

    def read_data_dir(self, dirpath: Path) -> list[dict]:
        # Group files by base name (before the @modification suffix) and keep
        # only the file with the latest modification per parameter combination.
        latest: dict[str, tuple[Path, str]] = {}
        for f in dirpath.glob("*.json"):
            stem = f.stem
            if "@" in stem:
                base, mod = stem.rsplit("@", 1)
            else:
                base, mod = stem, ""
            if base not in latest or mod > latest[base][1]:
                latest[base] = (f, mod)
        data = []
        for f, _ in latest.values():
            data.extend(self.read_data(f))
        return data
