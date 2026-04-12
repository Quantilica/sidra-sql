# Copyright (C) 2026 Komesu, D.K. <daniel@dkko.me>
#
# This file is part of ibge-sidra-tabelas.
#
# ibge-sidra-tabelas is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# ibge-sidra-tabelas is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with ibge-sidra-tabelas.  If not, see <https://www.gnu.org/licenses/>.

"""CLI entry point for running a dataset pipeline.

Reads a pipeline directory containing fetch.toml and/or transform.toml
and executes fetch → transform in sequence.  Each step is optional: if
the corresponding file is absent it is silently skipped.

Usage::

    # Run both steps
    python scripts/run.py pipelines/snpc/ipca

    # Run only the fetch step
    python scripts/run.py pipelines/snpc/ipca --fetch-only

    # Run only the transform step
    python scripts/run.py pipelines/snpc/ipca --transform-only
"""

import argparse
import logging
from pathlib import Path

from ibge_sidra_tabelas.config import Config
from ibge_sidra_tabelas.toml_runner import TomlScript
from ibge_sidra_tabelas.transform_runner import TransformRunner


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    parser = argparse.ArgumentParser(
        description="Run fetch and/or transform for a pipeline directory",
    )
    parser.add_argument(
        "pipeline_dir",
        type=Path,
        help="Directory containing fetch.toml and/or transform.toml",
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--fetch-only",
        action="store_true",
        help="Only run the fetch step (skips transform.toml)",
    )
    group.add_argument(
        "--transform-only",
        action="store_true",
        help="Only run the transform step (skips fetch.toml)",
    )
    parser.add_argument(
        "--force-metadata",
        action="store_true",
        help="Force re-download of agregado metadata (metadados, localidades, periodos) even if cached",
    )
    args = parser.parse_args()

    pipeline_dir = args.pipeline_dir
    config = Config()

    run_fetch = not args.transform_only
    run_transform = not args.fetch_only

    if run_fetch:
        fetch_toml = pipeline_dir / "fetch.toml"
        if fetch_toml.exists():
            TomlScript(config, fetch_toml, force_metadata=args.force_metadata).run()

    if run_transform:
        transform_toml = pipeline_dir / "transform.toml"
        if transform_toml.exists():
            TransformRunner(config, transform_toml).run()


if __name__ == "__main__":
    main()
