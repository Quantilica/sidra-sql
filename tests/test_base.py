import tempfile
import unittest
from pathlib import Path
from unittest import mock

from sidra_sql.toml_runner import TomlScript

SIMPLE_TOML = b"""
[[tabelas]]
tabela_sidra = "1"
territories = {6 = ["1"]}
"""

PARTIAL_UNNEST_TOML = b"""
[[tabelas]]
tabela_sidra = "5938"
variables = ["allxp"]
territories = {6 = []}
classifications = {81 = ["allxt"]}
unnest_classifications = ["87"]
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
            def plan_periods(self, **kwargs):
                return [("param-1", "2025-01-01")]

            def download_periods(self, plan, on_file_done=None):
                return [
                    {
                        "key": key,
                        "filepath": Path("/tmp/f1.csv"),
                        "modificacao": mod,
                    }
                    for key, _param, mod in plan
                ]

        script.fetcher = FakeFetcher()

        tabelas = script.get_tabelas()
        data_files = script.download(tabelas)
        self.assertEqual(len(data_files), 1)
        self.assertIn("filepath", data_files[0])
        self.assertIn("modificacao", data_files[0])
        self.assertEqual(data_files[0]["tabela_sidra"], "1")

    def test_load_metadata_reads_from_cache_when_file_exists(self):
        """load_metadata uses the cached file and never calls the API."""
        script = make_script()
        engine = mock.MagicMock()

        cached_path = mock.MagicMock()
        cached_path.exists.return_value = True
        fake_agregado = mock.MagicMock()
        script.storage.get_metadata_filepath = mock.MagicMock(return_value=cached_path)
        script.storage.read_metadata = mock.MagicMock(return_value=fake_agregado)
        script.fetcher.fetch_metadata = mock.MagicMock()

        with mock.patch("sidra_sql.database.save_agregado") as save_mock:
            script.load_metadata(engine, [{"tabela_sidra": "99"}])

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
        script.storage.get_metadata_filepath = mock.MagicMock(return_value=missing_path)
        script.fetcher.fetch_metadata = mock.MagicMock(return_value=fake_agregado)
        script.storage.write_metadata = mock.MagicMock()

        with mock.patch("sidra_sql.database.save_agregado") as save_mock:
            script.load_metadata(engine, [{"tabela_sidra": "7"}])

        script.fetcher.fetch_metadata.assert_called_once_with("7")
        script.storage.write_metadata.assert_called_once_with(fake_agregado)
        save_mock.assert_called_once_with(engine, fake_agregado)

    def test_load_metadata_deduplicates_repeated_table_ids(self):
        """load_metadata processes each unique tabela_sidra only once."""
        script = make_script()
        engine = mock.MagicMock()

        cached_path = mock.MagicMock()
        cached_path.exists.return_value = True
        script.storage.get_metadata_filepath = mock.MagicMock(return_value=cached_path)
        script.storage.read_metadata = mock.MagicMock(return_value=mock.MagicMock())

        with mock.patch("sidra_sql.database.save_agregado") as save_mock:
            script.load_metadata(
                engine,
                [
                    {"tabela_sidra": "5"},
                    {"tabela_sidra": "5"},  # duplicate
                ],
            )

        self.assertEqual(save_mock.call_count, 1)

    def test_get_tabelas_partial_unnest(self):
        """unnest_classifications=[list] unnests only named IDs, merges static classifications."""
        tmp = Path(tempfile.mkdtemp())
        toml_path = tmp / "test.toml"
        toml_path.write_bytes(PARTIAL_UNNEST_TOML)
        script = TomlScript(DummyConfig(), toml_path)

        class _Cat:
            def __init__(self, id):
                self.id = id

        class _Cls:
            def __init__(self, id, cat_ids):
                self.id = id
                self.categorias = [_Cat(c) for c in cat_ids]

        class _Meta:
            classificacoes = [_Cls(87, [10, 20]), _Cls(99, [5])]

        script.fetcher.sidra_client = mock.MagicMock()
        script.fetcher.sidra_client.get_agregado_metadados.return_value = _Meta()

        result = list(script.get_tabelas())

        # Two categories of 87, each merged with static 81
        self.assertEqual(len(result), 2)
        for entry in result:
            self.assertEqual(entry["classifications"]["81"], ["allxt"])
            self.assertIn(entry["classifications"]["87"], [["10"], ["20"]])
        # Classification 99 not unnested — auto-set to "all"
        for entry in result:
            self.assertEqual(entry["classifications"]["99"], ["all"])


if __name__ == "__main__":
    unittest.main()
