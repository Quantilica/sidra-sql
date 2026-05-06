# Copyright (c) 2026 Komesu, D.K.
# Licensed under the MIT License.

import configparser
import logging
from logging import handlers
from pathlib import Path

import platformdirs

APP_NAME = "sidra-sql"

GLOBAL_CONFIG_PATH = Path(platformdirs.user_config_dir(APP_NAME, appauthor=False)) / "config.ini"
LOCAL_CONFIG_PATH = Path("config.ini")


_REQUIRED_KEYS = {
    "database": ["user", "password", "host", "port", "dbname", "schema", "tablespace", "readonly_role"],
    "storage": ["data_dir"],
}

_SETUP_HINT = """\
No configuration found. Run the following commands to get started:

  sidra-sql config set database.host     <host>
  sidra-sql config set database.port     5432
  sidra-sql config set database.user     <user>
  sidra-sql config set database.password <password>
  sidra-sql config set database.dbname   <dbname>
  sidra-sql config set database.schema   <schema>
  sidra-sql config set database.tablespace    pg_default
  sidra-sql config set database.readonly_role <role>
  sidra-sql config set storage.data_dir  <path>

Add --global to write to the user-level config (~/.config/sidra-sql/config.ini).\
"""


class ConfigError(Exception):
    pass


class Config:
    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config.read([GLOBAL_CONFIG_PATH, LOCAL_CONFIG_PATH])

        self._validate()

        self.data_dir = Path(self.config["storage"]["data_dir"])

        self.db_user = self.config["database"]["user"]
        self.db_password = self.config["database"]["password"]
        self.db_host = self.config["database"]["host"]
        self.db_port = self.config["database"]["port"]

        self.db_name = self.config["database"]["dbname"]
        self.db_schema = self.config["database"]["schema"]
        self.db_tablespace = self.config["database"]["tablespace"]
        self.db_readonly_role = self.config["database"]["readonly_role"]

    def _validate(self):
        missing = []
        for section, keys in _REQUIRED_KEYS.items():
            for key in keys:
                if not self.config.has_option(section, key):
                    missing.append(f"{section}.{key}")

        if missing:
            if len(missing) == sum(len(v) for v in _REQUIRED_KEYS.values()):
                raise ConfigError(_SETUP_HINT)
            lines = "\n".join(f"  sidra-sql config set {k} <value>" for k in missing)
            raise ConfigError(f"Missing configuration keys:\n\n{lines}")

    def __str__(self):
        return (
            f"db_user: {self.db_user}\n"
            f"db_password: {self.db_password}\n"
            f"db_host: {self.db_host}\n"
            f"db_port: {self.db_port}\n"
            f"db_name: {self.db_name}\n"
            f"db_schema: {self.db_schema}\n"
            f"db_tablespace: {self.db_tablespace}\n"
            f"db_readonly_role: {self.db_readonly_role}\n"
        )


def setup_logging(logger_name: str, log_filepath: Path):
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    logger.propagate = False

    log_formatter = logging.Formatter(
        fmt="%(asctime)s.%(msecs)03d %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # File log
    filehandler = handlers.RotatingFileHandler(
        filename=log_filepath,
        mode="a",
        maxBytes=50 * 2**20,
        backupCount=100,
    )
    filehandler.setFormatter(log_formatter)
    filehandler.setLevel(logging.INFO)
    logger.addHandler(filehandler)

    # Console log — only warnings and above; progress output is handled by Rich
    streamhandler = logging.StreamHandler()
    streamhandler.setFormatter(log_formatter)
    streamhandler.setLevel(logging.WARNING)
    logger.addHandler(streamhandler)

    return logger
