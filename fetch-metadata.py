# Copyright (c) 2026 Komesu, D.K.
# Licensed under the MIT License.

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
