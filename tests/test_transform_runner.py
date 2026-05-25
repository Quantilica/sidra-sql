import tempfile
import unittest
from pathlib import Path
from unittest import mock

from sidra_sql.transform_runner import TransformRunner


class DummyConfig:
    pass


class FakeConn:
    def __init__(self, log: list[str]):
        self.log = log

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def exec_driver_sql(self, sql: str):
        self.log.append(sql)


class FakeEngine:
    def __init__(self):
        self.log: list[str] = []

    def begin(self):
        return FakeConn(self.log)


def _write_pipeline(tmp: Path, toml: str, sql_files: dict[str, str]) -> Path:
    toml_path = tmp / "transform.toml"
    toml_path.write_text(toml, encoding="utf-8")
    for name, content in sql_files.items():
        (tmp / name).write_text(content, encoding="utf-8")
    return toml_path


class TestTransformRunner(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())

    def _run(self, toml_path: Path) -> FakeEngine:
        engine = FakeEngine()
        with mock.patch(
            "sidra_sql.transform_runner.database.get_engine",
            return_value=engine,
        ):
            TransformRunner(DummyConfig(), toml_path).run()
        return engine

    def test_single_table_replace(self):
        toml = """
[[table]]
name = "ipca"
schema = "analytics"
strategy = "replace"
sql = "ipca.sql"
"""
        toml_path = _write_pipeline(self.tmp, toml, {"ipca.sql": "SELECT 1"})
        engine = self._run(toml_path)
        joined = "\n".join(engine.log)
        self.assertIn('CREATE SCHEMA IF NOT EXISTS "analytics"', joined)
        self.assertIn('DROP TABLE IF EXISTS "analytics"."ipca"', joined)
        self.assertIn('CREATE TABLE "analytics"."ipca" AS\nSELECT 1', joined)

    def test_multiple_tables_replace_and_view(self):
        toml = """
[[table]]
name = "ipca"
schema = "analytics"
strategy = "replace"
sql = "ipca.sql"

[[table]]
name = "ipca_resumo"
schema = "analytics"
strategy = "view"
sql = "resumo.sql"
"""
        toml_path = _write_pipeline(
            self.tmp,
            toml,
            {"ipca.sql": "SELECT 1", "resumo.sql": "SELECT 2"},
        )
        engine = self._run(toml_path)
        joined = "\n".join(engine.log)
        self.assertIn('CREATE TABLE "analytics"."ipca" AS\nSELECT 1', joined)
        self.assertIn(
            'CREATE OR REPLACE VIEW "analytics"."ipca_resumo" AS\nSELECT 2',
            joined,
        )
        # Order: ipca materialized before resumo
        ipca_idx = next(
            i
            for i, s in enumerate(engine.log)
            if "ipca" in s and "ipca_resumo" not in s
        )
        view_idx = next(i for i, s in enumerate(engine.log) if "ipca_resumo" in s)
        self.assertLess(ipca_idx, view_idx)

    def test_legacy_singular_table_raises(self):
        toml = """
[table]
name = "ipca"
schema = "analytics"
strategy = "replace"
"""
        toml_path = self.tmp / "transform.toml"
        toml_path.write_text(toml, encoding="utf-8")
        with self.assertRaises(ValueError) as ctx:
            self._run(toml_path)
        self.assertIn("[[table]]", str(ctx.exception))

    def test_missing_sql_file_raises(self):
        toml = """
[[table]]
name = "x"
schema = "s"
strategy = "replace"
sql = "missing.sql"
"""
        toml_path = self.tmp / "transform.toml"
        toml_path.write_text(toml, encoding="utf-8")
        with self.assertRaises(FileNotFoundError):
            self._run(toml_path)

    def test_missing_required_field_raises(self):
        toml = """
[[table]]
name = "x"
schema = "s"
sql = "x.sql"
"""
        toml_path = _write_pipeline(self.tmp, toml, {"x.sql": "SELECT 1"})
        with self.assertRaises(ValueError) as ctx:
            self._run(toml_path)
        self.assertIn("strategy", str(ctx.exception))

    def test_primary_key_and_indexes(self):
        toml = """
[[table]]
name = "t"
schema = "s"
strategy = "replace"
sql = "t.sql"
primary_key = ["a", "b"]
indexes = [
    { name = "ix_t_a", columns = ["a"] },
    { name = "ix_t_b", columns = ["b"], unique = true },
]
"""
        toml_path = _write_pipeline(self.tmp, toml, {"t.sql": "SELECT 1"})
        engine = self._run(toml_path)
        joined = "\n".join(engine.log)
        self.assertIn('ADD PRIMARY KEY ("a", "b")', joined)
        self.assertIn('CREATE  INDEX "ix_t_a" ON "s"."t" ("a")', joined)
        self.assertIn('CREATE UNIQUE INDEX "ix_t_b" ON "s"."t" ("b")', joined)


if __name__ == "__main__":
    unittest.main()
