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

import argparse

from sidra_sql import database, models
from sidra_sql.config import Config
from sidra_sql.sidra import Fetcher
from sidra_sql.storage import Storage


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch metadata of a table from IBGE Sidra"
    )
    parser.add_argument("table", type=str, help="Table ID")
    return parser.parse_args()


def main():
    args = get_args()
    config = Config()
    engine = database.get_engine(config)
    models.Base.metadata.create_all(engine)

    storage = Storage.default(config)
    metadata_filepath = storage.get_metadata_filepath(int(args.table))

    if not metadata_filepath.exists():
        with Fetcher() as fetcher:
            agregado = fetcher.fetch_metadata(args.table)
        storage.write_metadata(agregado)
    else:
        agregado = storage.read_metadata(int(args.table))

    database.save_agregado(engine, agregado)


if __name__ == "__main__":
    main()
