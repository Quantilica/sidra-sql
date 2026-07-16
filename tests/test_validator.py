import tempfile
import unittest
from pathlib import Path

from sidra_sql.validator import PluginValidator, Severity

MANIFEST = """
name = "test"
version = "1.0.0"

[[pipeline]]
id = "p1"
description = "test"
path = "p1"
"""

FETCH = """
[[tabelas]]
tabela_sidra = "1"
"""


def _setup_plugin(tmp: Path, transform_toml: str, sql_files: dict[str, str]) -> Path:
    plugin = tmp / "plugin"
    plugin.mkdir()
    (plugin / "manifest.toml").write_text(MANIFEST, encoding="utf-8")
    pipe = plugin / "p1"
    pipe.mkdir()
    (pipe / "fetch.toml").write_text(FETCH, encoding="utf-8")
    (pipe / "transform.toml").write_text(transform_toml, encoding="utf-8")
    for name, content in sql_files.items():
        (pipe / name).write_text(content, encoding="utf-8")
    return plugin


def _section(report, title):
    return next(s for s in report.sections if s.title == title)


def _errors(section):
    return [i.message for i in section.issues if i.severity == Severity.ERROR]


class TestValidatorTransformToml(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())

    def test_array_with_one_entry_valid(self):
        toml = """
[[table]]
name = "t"
schema = "s"
strategy = "replace"
sql = "t.sql"
"""
        plugin = _setup_plugin(self.tmp, toml, {"t.sql": "SELECT 1"})
        report = PluginValidator(plugin).validate()
        self.assertTrue(report.is_valid, _errors(_section(report, "p1")))

    def test_array_with_multiple_entries_valid(self):
        toml = """
[[table]]
name = "t1"
schema = "s"
strategy = "replace"
sql = "t1.sql"

[[table]]
name = "t2"
schema = "s"
strategy = "view"
sql = "t2.sql"
"""
        plugin = _setup_plugin(
            self.tmp, toml, {"t1.sql": "SELECT 1", "t2.sql": "SELECT 2"}
        )
        report = PluginValidator(plugin).validate()
        self.assertTrue(report.is_valid)

    def test_legacy_singular_table_errors(self):
        toml = """
[table]
name = "t"
schema = "s"
strategy = "replace"
"""
        plugin = _setup_plugin(self.tmp, toml, {})
        report = PluginValidator(plugin).validate()
        errs = _errors(_section(report, "p1"))
        self.assertTrue(any("[table] singular" in e for e in errs), errs)

    def test_missing_sql_file_errors(self):
        toml = """
[[table]]
name = "t"
schema = "s"
strategy = "replace"
sql = "missing.sql"
"""
        plugin = _setup_plugin(self.tmp, toml, {})
        report = PluginValidator(plugin).validate()
        errs = _errors(_section(report, "p1"))
        self.assertTrue(any("missing.sql" in e for e in errs), errs)

    def test_missing_required_field_errors(self):
        toml = """
[[table]]
name = "t"
schema = "s"
sql = "t.sql"
"""
        plugin = _setup_plugin(self.tmp, toml, {"t.sql": "SELECT 1"})
        report = PluginValidator(plugin).validate()
        errs = _errors(_section(report, "p1"))
        self.assertTrue(any("strategy" in e for e in errs), errs)

    def test_invalid_strategy_errors(self):
        toml = """
[[table]]
name = "t"
schema = "s"
strategy = "merge"
sql = "t.sql"
"""
        plugin = _setup_plugin(self.tmp, toml, {"t.sql": "SELECT 1"})
        report = PluginValidator(plugin).validate()
        errs = _errors(_section(report, "p1"))
        self.assertTrue(any("strategy" in e and "merge" in e for e in errs), errs)

    def test_duplicate_output_errors(self):
        toml = """
[[table]]
name = "t"
schema = "s"
strategy = "replace"
sql = "t.sql"

[[table]]
name = "t"
schema = "s"
strategy = "view"
sql = "t.sql"
"""
        plugin = _setup_plugin(self.tmp, toml, {"t.sql": "SELECT 1"})
        report = PluginValidator(plugin).validate()
        errs = _errors(_section(report, "p1"))
        self.assertTrue(any("duplicada" in e for e in errs), errs)

    def test_empty_table_array_errors(self):
        toml = "# nothing\n"
        plugin = _setup_plugin(self.tmp, toml, {})
        report = PluginValidator(plugin).validate()
        errs = _errors(_section(report, "p1"))
        self.assertTrue(any("nenhum [[table]]" in e for e in errs), errs)


if __name__ == "__main__":
    unittest.main()
