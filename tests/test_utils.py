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

from sidra_sql.utils import unnest_dimensoes

# ---------------------------------------------------------------------------
# Stub domain objects — duck-typed, no sidra_fetcher import needed
# ---------------------------------------------------------------------------

class _Cat:
    def __init__(self, id, nome="cat", unidade=None):
        self.id = id
        self.nome = nome
        self.unidade = unidade


class _Cls:
    def __init__(self, categorias):
        self.categorias = categorias


class _Var:
    def __init__(self, id, nome="var", unidade="BRL"):
        self.id = id
        self.nome = nome
        self.unidade = unidade


class TestUnnestDimensoes(unittest.TestCase):
    def test_no_classifications_yields_one_row_per_variable(self):
        variaveis = [_Var(1, "GDP"), _Var(2, "POP")]
        rows = list(unnest_dimensoes(variaveis, []))
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["d2c"], "1")
        self.assertEqual(rows[1]["d2c"], "2")

    def test_no_classifications_all_dimension_slots_are_none(self):
        rows = list(unnest_dimensoes([_Var(1)], []))
        for slot in ("d4c", "d4n", "d5c", "d5n", "d6c", "d6n",
                     "d7c", "d7n", "d8c", "d8n", "d9c", "d9n"):
            self.assertIsNone(rows[0][slot], msg=f"{slot} should be None")

    def test_no_classifications_uses_variable_unit_of_measure(self):
        rows = list(unnest_dimensoes([_Var(1, unidade="BRL")], []))
        self.assertIsNone(rows[0]["mc"])
        self.assertEqual(rows[0]["mn"], "BRL")

    def test_one_classification_yields_one_row_per_category(self):
        cats = [_Cat(10, "A"), _Cat(20, "B")]
        rows = list(unnest_dimensoes([_Var(1)], [_Cls(cats)]))
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["d4c"], "10")
        self.assertEqual(rows[1]["d4c"], "20")

    def test_one_classification_higher_slots_are_none(self):
        rows = list(unnest_dimensoes([_Var(1)], [_Cls([_Cat(1)])]))
        for slot in ("d5c", "d6c", "d7c", "d8c", "d9c"):
            self.assertIsNone(rows[0][slot], msg=f"{slot} should be None")

    def test_two_classifications_yields_cartesian_product(self):
        cls1 = _Cls([_Cat(1), _Cat(2)])
        cls2 = _Cls([_Cat(10), _Cat(20)])
        rows = list(unnest_dimensoes([_Var(99)], [cls1, cls2]))
        self.assertEqual(len(rows), 4)
        self.assertEqual({r["d4c"] for r in rows}, {"1", "2"})
        self.assertEqual({r["d5c"] for r in rows}, {"10", "20"})

    def test_six_classifications_fills_all_slots(self):
        classifications = [_Cls([_Cat(i)]) for i in range(6)]
        rows = list(unnest_dimensoes([_Var(1)], classifications))
        self.assertEqual(len(rows), 1)
        for slot in ("d4c", "d5c", "d6c", "d7c", "d8c", "d9c"):
            self.assertIsNotNone(rows[0][slot], msg=f"{slot} should not be None")

    def test_category_unit_overrides_variable_unit(self):
        cats = [_Cat(1, "A", unidade="USD")]
        rows = list(unnest_dimensoes([_Var(1, unidade="BRL")], [_Cls(cats)]))
        self.assertEqual(rows[0]["mn"], "USD")

    def test_category_without_unit_falls_back_to_variable_unit(self):
        cats = [_Cat(1, "A", unidade=None)]
        rows = list(unnest_dimensoes([_Var(1, unidade="BRL")], [_Cls(cats)]))
        self.assertEqual(rows[0]["mn"], "BRL")

    def test_variable_and_category_ids_are_converted_to_strings(self):
        rows = list(unnest_dimensoes([_Var(42)], [_Cls([_Cat(7)])]))
        self.assertEqual(rows[0]["d2c"], "42")
        self.assertEqual(rows[0]["d4c"], "7")

    def test_multiple_variables_each_produce_their_own_rows(self):
        cats = [_Cat(1)]
        rows = list(unnest_dimensoes([_Var(10), _Var(20)], [_Cls(cats)]))
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["d2c"], "10")
        self.assertEqual(rows[1]["d2c"], "20")


if __name__ == "__main__":
    unittest.main()
