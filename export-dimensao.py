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
import csv
from pathlib import Path

from sidra_sql.config import Config
from sidra_sql.storage import Storage
from sidra_sql.utils import unnest_dimensoes


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Save dimensao.csv for a given SIDRA table using already fetched data"
    )
    parser.add_argument("table", type=str, help="Table ID")
    parser.add_argument(
        "--output",
        type=str,
        default="dimensao.csv",
        help="Output CSV file path (default: dimensao.csv)",
    )
    return parser.parse_args()


def main():
    args = get_args()
    config = Config()
    storage = Storage.default(config)
    table_dir = storage.data_dir / f"t-{args.table}"

    if not table_dir.exists():
        print(
            f"Error: No data directory found for table {args.table} at {table_dir}"
        )
        return

    # 1. Use unnest_dimensoes to get exhaustive combinations
    agregado = storage.read_metadata(args.table)
    if not agregado:
        print(f"Error: Could not load metadata for table {args.table}")
        return

    base_dims = list(
        unnest_dimensoes(agregado.variaveis, agregado.classificacoes)
    )

    if not base_dims:
        print(f"No dimensions found in the metadata for table {args.table}.")
        return

    # 2. Extract unit keys (combo mapping + mn->mc fallback) from data files (Formato.A)
    combo_to_mc = {}
    mn_to_mc = {}

    for filepath in table_dir.glob("*.json"):
        try:
            rows = storage.read_data(filepath)
        except Exception as e:
            print(f"Warning: Failed to read {filepath.name}: {e}")
            continue

        if not rows:
            continue

        for row in rows:
            mc = row.get("MC")
            if mc is None:
                continue

            mc_str = str(mc).strip()
            mn = row.get("MN")
            if mn is not None:
                mn_str = str(mn).strip()
                if mn_str and mc_str:
                    mn_to_mc[mn_str] = mc_str

            def _s(c):
                v = row.get(c)
                return str(v) if v is not None else "None"

            combo_key = (
                _s("D2C"),
                _s("D4C"),
                _s("D5C"),
                _s("D6C"),
                _s("D7C"),
                _s("D8C"),
                _s("D9C"),
            )
            combo_to_mc[combo_key] = mc_str

    # 3. Update 'mc' based on Formato.A mapped keys
    for dim in base_dims:

        def _str_match(c):
            return str(dim.get(c)) if dim.get(c) is not None else "None"

        combo_key = (
            _str_match("d2c"),
            _str_match("d4c"),
            _str_match("d5c"),
            _str_match("d6c"),
            _str_match("d7c"),
            _str_match("d8c"),
            _str_match("d9c"),
        )

        if combo_key in combo_to_mc:
            dim["mc"] = combo_to_mc[combo_key]
        elif dim.get("mn") and dim["mn"] in mn_to_mc:
            dim["mc"] = mn_to_mc[dim["mn"]]

    output_path = Path(args.output)
    fieldnames = [
        "mc",
        "mn",
        "d2c",
        "d2n",
        "d4c",
        "d4n",
        "d5c",
        "d5n",
        "d6c",
        "d6n",
        "d7c",
        "d7n",
        "d8c",
        "d8n",
        "d9c",
        "d9n",
    ]

    with output_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(base_dims)

    print(f"Successfully saved {len(base_dims)} dimensions to {output_path}")


if __name__ == "__main__":
    main()
