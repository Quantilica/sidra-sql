import datetime as dt
import tempfile
import unittest
from pathlib import Path

from sidra_sql import exporter


class TestReadOutputs(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())

    def _write(self, toml: str) -> Path:
        p = self.tmp / "transform.toml"
        p.write_text(toml, encoding="utf-8")
        return p

    def test_reads_multiple_entries(self):
        p = self._write(
            """
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
        )
        outputs = exporter.read_outputs(p)
        self.assertEqual([o["name"] for o in outputs], ["ipca", "ipca_resumo"])

    def test_missing_required_field_raises(self):
        p = self._write(
            """
[[table]]
name = "ipca"
schema = "analytics"
"""
        )
        with self.assertRaises(ValueError) as ctx:
            exporter.read_outputs(p)
        self.assertIn("sql", str(ctx.exception))

    def test_legacy_singular_table_raises(self):
        p = self._write(
            """
[table]
name = "ipca"
schema = "analytics"
sql = "ipca.sql"
"""
        )
        with self.assertRaises(ValueError):
            exporter.read_outputs(p)


class TestCopySql(unittest.TestCase):
    def test_copy_table_sql(self):
        sql = exporter.copy_table_sql("analytics", "ipca")
        self.assertIn('SELECT * FROM "analytics"."ipca"', sql)
        self.assertIn("TO STDOUT WITH (FORMAT csv, HEADER)", sql)

    def test_copy_query_strips_trailing_semicolon(self):
        sql = exporter.copy_query_sql("SELECT 1 ;  \n")
        self.assertEqual(
            sql, "COPY (SELECT 1) TO STDOUT WITH (FORMAT csv, HEADER)"
        )

    def test_copy_query_preserves_percent(self):
        # transform SQL uses LIKE '...%' — must survive verbatim (psycopg sends
        # the COPY statement with no params, so % is literal).
        sql = exporter.copy_query_sql("SELECT x WHERE v LIKE 'PIB%'")
        self.assertIn("LIKE 'PIB%'", sql)


class TestAsofViewSql(unittest.TestCase):
    def test_shadows_dados_with_qualified_source(self):
        sql = exporter.asof_view_sql("ibge_sidra", dt.date(2024, 3, 1))
        self.assertIn("CREATE TEMP VIEW dados AS", sql)
        # Inner source must be schema-qualified to avoid self-reference.
        self.assertIn('FROM "ibge_sidra".dados', sql)
        self.assertIn("TRUE AS ativo", sql)

    def test_inlines_date_literal_and_picks_latest(self):
        sql = exporter.asof_view_sql("ibge_sidra", dt.date(2024, 3, 1))
        self.assertIn("modificacao <= DATE '2024-03-01'", sql)
        self.assertIn(
            "DISTINCT ON"
            " (tabela_sidra_id, localidade_id, dimensao_id, periodo_id)",
            sql,
        )
        self.assertIn("modificacao DESC", sql)


class TestStampedName(unittest.TestCase):
    def test_current(self):
        self.assertEqual(exporter.stamped_name("ipca", None), "ipca.csv")

    def test_asof(self):
        self.assertEqual(
            exporter.stamped_name("ipca", dt.date(2024, 3, 1)),
            "ipca@20240301.csv",
        )


if __name__ == "__main__":
    unittest.main()
