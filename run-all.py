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

"""Run all pipelines found under a given directory.

Usage::

    python run-all.py
    python run-all.py pipelines/snpc
"""

import argparse
import subprocess
import sys
from pathlib import Path


def find_pipelines(root: Path) -> list[Path]:
    dirs = set()
    for name in ("fetch.toml", "transform.toml"):
        for p in root.rglob(name):
            dirs.add(p.parent)
    return sorted(dirs)


def main():
    parser = argparse.ArgumentParser(
        description="Run all pipelines under a directory",
    )
    parser.add_argument(
        "pipelines_dir",
        nargs="?",
        type=Path,
        default=Path("pipelines"),
        help="Root directory to search for pipelines (default: pipelines)",
    )
    parser.add_argument(
        "--force-metadata",
        action="store_true",
        help="Force re-download of agregado metadata even if cached",
    )
    args = parser.parse_args()

    pipelines_dir: Path = args.pipelines_dir
    if not pipelines_dir.is_dir():
        print(f"Error: Directory '{pipelines_dir}' does not exist.")
        sys.exit(1)

    pipelines = find_pipelines(pipelines_dir)
    if not pipelines:
        print(f"No pipelines found in '{pipelines_dir}'.")
        sys.exit(0)

    print(f"Starting execution of all pipelines in '{pipelines_dir}/'...")

    failed = []
    for pipeline in pipelines:
        print("=" * 40)
        print(f"Running: {pipeline}")
        print("=" * 40)
        cmd = [sys.executable, "scripts/run.py", str(pipeline)]
        if args.force_metadata:
            cmd.append("--force-metadata")
        result = subprocess.run(cmd)
        if result.returncode != 0:
            print(f"Warning: '{pipeline}' exited with code {result.returncode}")
            failed.append(pipeline)

    print("All pipelines finished.")
    if failed:
        print(f"{len(failed)} pipeline(s) failed:")
        for p in failed:
            print(f"  {p}")
        sys.exit(1)


if __name__ == "__main__":
    main()
