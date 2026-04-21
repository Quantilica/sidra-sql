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

import unittest

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


if __name__ == "__main__":
    unittest.main()
