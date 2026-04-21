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

import os
import tempfile
import unittest
from pathlib import Path

from sidra_sql.config import Config, setup_logging


class TestConfig(unittest.TestCase):
    def test_config_reads_values_and_str(self):
        content = """
[storage]
data_dir = /tmp/test_data

[database]
user = alice
password = secret
host = db.example
port = 5432
dbname = sample_db
schema = public
tablespace = pg_default
readonly_role = readonly
"""

        cwd = os.getcwd()
        td = tempfile.mkdtemp()
        try:
            os.chdir(td)
            (Path(td) / "config.ini").write_text(content)

            cfg = Config()
            self.assertEqual(cfg.db_user, "alice")
            self.assertEqual(cfg.db_password, "secret")
            self.assertEqual(cfg.db_host, "db.example")
            self.assertEqual(cfg.db_port, "5432")
            self.assertEqual(cfg.db_name, "sample_db")
            self.assertEqual(cfg.db_schema, "public")
            self.assertEqual(cfg.db_tablespace, "pg_default")
            self.assertEqual(cfg.db_readonly_role, "readonly")

            s = str(cfg)
            self.assertIn("db_user: alice", s)
        finally:
            os.chdir(cwd)

    def test_setup_logging_creates_handlers_and_file(self):
        td = tempfile.mkdtemp()
        log_path = Path(td) / "test.log"

        logger = setup_logging("test_logger_for_unit", log_path)
        # logger should have handlers attached and propagation disabled
        self.assertFalse(logger.propagate)
        self.assertGreaterEqual(len(logger.handlers), 2)

        # Emit a log and ensure the file is created
        logger.info("hello")
        self.assertTrue(log_path.exists())


if __name__ == "__main__":
    unittest.main()
