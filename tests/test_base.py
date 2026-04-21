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

import tempfile
import unittest
from pathlib import Path
from unittest import mock

from sidra_sql.toml_runner import TomlScript

SIMPLE_TOML = b"""
[[tabelas]]
sidra_tabela = "1"
territories = {6 = ["1"]}
"""


class DummyConfig:
    def __init__(self):
        self.db_schema = None
        self.data_dir = Path(tempfile.mkdtemp())


def make_script() -> TomlScript:
    tmp = Path(tempfile.mkdtemp())
    toml_path = tmp / "test.toml"
    toml_path.write_bytes(SIMPLE_TOML)
    return TomlScript(DummyConfig(), toml_path)


class TestTomlScript(unittest.TestCase):
    def test_download_attaches_filepaths(self):
        script = make_script()

        class FakeFetcher:
            def download_table(self, **kwargs):
                return [Path("/tmp/f1.csv")]

        script.fetcher = FakeFetcher()

        tabelas = script.get_tabelas()
        data_files = script.download(tabelas)
        self.assertEqual(len(data_files), 1)
        self.assertIn("filepath", data_files[0])

    def test_load_metadata_reads_from_cache_when_file_exists(self):
        """load_metadata uses the cached file and never calls the API."""
        script = make_script()
        engine = mock.MagicMock()

        cached_path = mock.MagicMock()
        cached_path.exists.return_value = True
        fake_agregado = mock.MagicMock()
        script.storage.get_metadata_filepath = mock.MagicMock(
            return_value=cached_path
        )
        script.storage.read_metadata = mock.MagicMock(return_value=fake_agregado)
        script.fetcher.fetch_metadata = mock.MagicMock()

        with mock.patch("sidra_sql.database.save_agregado") as save_mock:
            script.load_metadata(engine, [{"sidra_tabela": "99"}])

        script.storage.read_metadata.assert_called_once_with("99")
        script.fetcher.fetch_metadata.assert_not_called()
        save_mock.assert_called_once_with(engine, fake_agregado)

    def test_load_metadata_fetches_from_api_when_not_cached(self):
        """load_metadata calls the API and writes to disk when no cache exists."""
        script = make_script()
        engine = mock.MagicMock()

        missing_path = mock.MagicMock()
        missing_path.exists.return_value = False
        fake_agregado = mock.MagicMock()
        script.storage.get_metadata_filepath = mock.MagicMock(
            return_value=missing_path
        )
        script.fetcher.fetch_metadata = mock.MagicMock(return_value=fake_agregado)
        script.storage.write_metadata = mock.MagicMock()

        with mock.patch("sidra_sql.database.save_agregado") as save_mock:
            script.load_metadata(engine, [{"sidra_tabela": "7"}])

        script.fetcher.fetch_metadata.assert_called_once_with("7")
        script.storage.write_metadata.assert_called_once_with(fake_agregado)
        save_mock.assert_called_once_with(engine, fake_agregado)

    def test_load_metadata_deduplicates_repeated_table_ids(self):
        """load_metadata processes each unique sidra_tabela only once."""
        script = make_script()
        engine = mock.MagicMock()

        cached_path = mock.MagicMock()
        cached_path.exists.return_value = True
        script.storage.get_metadata_filepath = mock.MagicMock(
            return_value=cached_path
        )
        script.storage.read_metadata = mock.MagicMock(
            return_value=mock.MagicMock()
        )

        with mock.patch("sidra_sql.database.save_agregado") as save_mock:
            script.load_metadata(engine, [
                {"sidra_tabela": "5"},
                {"sidra_tabela": "5"},  # duplicate
            ])

        self.assertEqual(save_mock.call_count, 1)


if __name__ == "__main__":
    unittest.main()
