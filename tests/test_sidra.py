# Copyright (c) 2026 Komesu, D.K.
# Licensed under the MIT License.

import tempfile
import unittest
from pathlib import Path

import httpx

from sidra_sql.sidra import Fetcher, unnest_classificacoes


class _DummyConfig:
    def __init__(self):
        self.data_dir = Path(tempfile.mkdtemp())


class _Cat:
    def __init__(self, id):
        self.id = id


class _Cls:
    def __init__(self, id, categorias):
        self.id = id
        self.categorias = [_Cat(c) for c in categorias]


class TestSidra(unittest.TestCase):
    def test_unnest_classificacoes_nested(self):
        # Two-level classification: first has two categories, second has one
        classificacoes = [_Cls(1, [10, 20]), _Cls(2, [100])]
        combos = list(unnest_classificacoes(classificacoes))
        expected = [{"1": ["10"], "2": ["100"]}, {"1": ["20"], "2": ["100"]}]
        self.assertEqual(combos, expected)

    def test_unnest_classificacoes_skips_zero(self):
        classificacoes = [_Cls(1, [0, 5])]
        combos = list(unnest_classificacoes(classificacoes))
        expected = [{"1": ["5"]}]
        self.assertEqual(combos, expected)

    def test_unnest_classificacoes_three_levels_full_product(self):
        # Verifies no stale dict keys bleed across the recursion.
        # 2 × 2 × 2 = 8 combinations expected.
        classificacoes = [
            _Cls(1, [10, 11]),
            _Cls(2, [20, 21]),
            _Cls(3, [30, 31]),
        ]
        combos = list(unnest_classificacoes(classificacoes))
        self.assertEqual(len(combos), 8)
        # Every combo must have exactly 3 keys
        for combo in combos:
            self.assertEqual(set(combo.keys()), {"1", "2", "3"})
        # All combinations must be distinct
        combo_tuples = [
            (combo["1"][0], combo["2"][0], combo["3"][0]) for combo in combos
        ]
        self.assertEqual(len(set(combo_tuples)), 8)

    def test_get_table_retries_on_timeout(self):
        fetcher = Fetcher(_DummyConfig())

        # Replace sidra_client with a fake that fails once then returns data
        calls = {"n": 0}

        class FakeClient:
            def get(self_inner, url):
                calls["n"] += 1
                if calls["n"] == 1:
                    raise httpx.ReadTimeout("timeout")
                return [{"col": 1}, {"col": 2}]

        fetcher.sidra_client = FakeClient()

        class P:
            def url(self):
                return "http://example"

        import sidra_sql.sidra as sidra_module

        sleep_calls = []
        orig_sleep = sidra_module.time.sleep
        sidra_module.time.sleep = lambda s: sleep_calls.append(s)
        try:
            result = fetcher.get_table(P())
        finally:
            sidra_module.time.sleep = orig_sleep

        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 2)
        # One sleep call with the base delay (first attempt, exponent 0)
        self.assertEqual(len(sleep_calls), 1)
        self.assertEqual(sleep_calls[0], sidra_module._RETRY_BASE_DELAY)

    def test_get_table_raises_after_max_retries(self):
        import sidra_sql.sidra as sidra_module

        fetcher = Fetcher(_DummyConfig())

        class AlwaysTimesOut:
            def get(self_inner, url):
                raise httpx.ReadTimeout("timeout")

        fetcher.sidra_client = AlwaysTimesOut()

        class P:
            def url(self):
                return "http://example"

        orig_sleep = sidra_module.time.sleep
        sidra_module.time.sleep = lambda s: None
        try:
            with self.assertRaises(httpx.ReadTimeout):
                fetcher.get_table(P())
        finally:
            sidra_module.time.sleep = orig_sleep

    def test_get_table_retries_on_connect_error(self):
        """Broader error types beyond ReadTimeout are also retried."""
        import sidra_sql.sidra as sidra_module

        fetcher = Fetcher(_DummyConfig())
        calls = {"n": 0}

        class FakeClient:
            def get(self_inner, url):
                calls["n"] += 1
                if calls["n"] == 1:
                    raise httpx.ConnectError("refused")
                return [{"col": 1}]

        fetcher.sidra_client = FakeClient()

        class P:
            def url(self):
                return "http://example"

        orig_sleep = sidra_module.time.sleep
        sidra_module.time.sleep = lambda s: None
        try:
            result = fetcher.get_table(P())
        finally:
            sidra_module.time.sleep = orig_sleep

        self.assertEqual(len(result), 1)

    def test_context_manager_enter_delegates(self):
        fetcher = Fetcher(_DummyConfig())

        class FakeClient:
            def __init__(self):
                self.entered = False

            def __enter__(self):
                self.entered = True
                return self

            def __exit__(self, *a):
                pass

        client = FakeClient()
        fetcher.sidra_client = client
        result = fetcher.__enter__()
        self.assertTrue(client.entered)
        self.assertIs(result, fetcher)

    def test_context_manager_exit_delegates(self):
        fetcher = Fetcher(_DummyConfig())

        class FakeClient:
            def __init__(self):
                self.exited = False

            def __exit__(self, exc_type, exc_value, traceback):
                self.exited = True

        client = FakeClient()
        fetcher.sidra_client = client
        fetcher.__exit__(None, None, None)
        self.assertTrue(client.exited)


if __name__ == "__main__":
    unittest.main()
