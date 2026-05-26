import unittest
from types import SimpleNamespace
from unittest.mock import patch

import sqlalchemy as sa

from sidra_sql import database

# ---------------------------------------------------------------------------
# Internal helpers — no DB connection required
# ---------------------------------------------------------------------------


class TestCoerce(unittest.TestCase):
    def test_none_returns_none(self):
        self.assertIsNone(database._coerce(None))

    def test_integer_returns_string(self):
        self.assertEqual(database._coerce(42), "42")

    def test_zero_returns_string_not_none(self):
        self.assertEqual(database._coerce(0), "0")

    def test_string_passes_through(self):
        self.assertEqual(database._coerce("hello"), "hello")


class TestCleanStr(unittest.TestCase):
    def test_none_returns_empty_string(self):
        self.assertEqual(database._clean_str(None), "")

    def test_trailing_dot_zero_removed(self):
        self.assertEqual(database._clean_str("1100015.0"), "1100015")

    def test_surrounding_whitespace_stripped(self):
        self.assertEqual(database._clean_str("  abc  "), "abc")

    def test_plain_string_unchanged(self):
        self.assertEqual(database._clean_str("ABC"), "ABC")

    def test_zero_string_unchanged(self):
        self.assertEqual(database._clean_str("0"), "0")


class TestNormalizeNc(unittest.TestCase):
    def test_bare_number_gets_n_prefix(self):
        self.assertEqual(database._normalize_nc("6"), "N6")

    def test_already_prefixed_is_unchanged(self):
        self.assertEqual(database._normalize_nc("N6"), "N6")

    def test_empty_string_is_unchanged(self):
        self.assertEqual(database._normalize_nc(""), "")

    def test_multi_digit_number_gets_prefix(self):
        self.assertEqual(database._normalize_nc("101"), "N101")


class DummyConfig:
    def __init__(self, user, password, host, port, name, table, schema=None):
        self.db_user = user
        self.db_password = password
        self.db_host = host
        self.db_port = port
        self.db_name = name
        self.db_table = table
        self.db_schema = schema


class TestDatabaseHelpers(unittest.TestCase):
    def test_get_engine_fields(self):
        cfg = DummyConfig("u", "p", "h", 5432, "db", "t")
        eng = database.get_engine(cfg)
        # Engine URL object should expose the provided fields
        url = eng.url
        self.assertEqual(url.username, "u")
        self.assertEqual(url.password, "p")
        self.assertEqual(url.host, "h")
        self.assertEqual(url.port, 5432)
        self.assertEqual(url.database, "db")
        # drivername should contain postgresql
        self.assertIn("postgresql", url.drivername)


class TestPeriodoByCodigoQuery(unittest.TestCase):
    """Verify the (codigo, frequencias) lookup that disambiguates ambiguous
    period codes like "200702" across frequencies."""

    def setUp(self):
        self.engine = sa.create_engine("sqlite:///:memory:")
        meta = sa.MetaData()
        self.periodo = sa.Table(
            "periodo",
            meta,
            sa.Column("id", sa.Integer, primary_key=True),
            sa.Column("codigo", sa.Text, nullable=False),
            sa.Column("frequencia", sa.Text),
        )
        meta.create_all(self.engine)
        with self.engine.begin() as conn:
            conn.execute(
                self.periodo.insert(),
                [
                    {"id": 1, "codigo": "200702", "frequencia": "mensal"},
                    {"id": 2, "codigo": "200702", "frequencia": "semestral"},
                    {"id": 3, "codigo": "200701", "frequencia": "mensal"},
                ],
            )
        self._patcher = patch.object(
            database.models,
            "Periodo",
            SimpleNamespace(
                id=self.periodo.c.id,
                codigo=self.periodo.c.codigo,
                frequencia=self.periodo.c.frequencia,
            ),
        )
        self._patcher.start()
        self.addCleanup(self._patcher.stop)

    def _query(self, codigos, **kwargs):
        with self.engine.connect() as conn:
            return database._periodo_by_codigo_query(conn, codigos, **kwargs)

    def test_mensal_resolves_to_monthly_row(self):
        result = self._query({"200702", "200701"}, frequencias={"mensal"})
        self.assertEqual(result, {"200702": 1, "200701": 3})

    def test_semestral_resolves_to_semestral_row(self):
        result = self._query({"200702"}, frequencias={"semestral"})
        self.assertEqual(result, {"200702": 2})

    def test_no_filter_first_match_wins(self):
        result = self._query({"200702"}, frequencias=None)
        self.assertEqual(set(result.keys()), {"200702"})
        self.assertIn(result["200702"], {1, 2})

    def test_empty_frequencia_set_acts_as_no_filter(self):
        result = self._query({"200702"}, frequencias=set())
        self.assertEqual(set(result.keys()), {"200702"})
        self.assertIn(result["200702"], {1, 2})


class TestVintageStagingSql(unittest.TestCase):
    """Lock in the vintage-storage merge semantics (deactivate-then-insert by
    value difference, processed per modificacao group)."""

    def test_staging_table_has_no_ativo_column(self):
        # ativo is derived at merge time, not carried through staging.
        self.assertNotIn("ativo", database._STAGING_DDL)
        self.assertNotIn("ativo", database._STAGING_COPY)

    def test_deactivate_uses_value_difference(self):
        self.assertIn("ativo = FALSE", database._DEACTIVATE_GROUP)
        self.assertIn("d.v IS DISTINCT FROM s.v", database._DEACTIVATE_GROUP)
        self.assertIn("%(mod)s", database._DEACTIVATE_GROUP)

    def test_insert_is_change_point_only_and_dedups_per_key(self):
        self.assertIn("DISTINCT ON", database._INSERT_GROUP)
        self.assertIn(
            "d.id IS NULL OR d.v IS DISTINCT FROM s.v", database._INSERT_GROUP
        )
        self.assertIn("%(mod)s", database._INSERT_GROUP)
        # New rows are always inserted active.
        self.assertIn("TRUE", database._INSERT_GROUP)

    def test_no_on_conflict_do_nothing_in_merge(self):
        # The old lossy strategy discarded revisions via ON CONFLICT DO NOTHING.
        self.assertNotIn("ON CONFLICT", database._INSERT_GROUP)

    def test_modificacoes_sorted_ascending(self):
        self.assertIn("ORDER BY modificacao", database._STAGING_MODIFICACOES)


if __name__ == "__main__":
    unittest.main()
