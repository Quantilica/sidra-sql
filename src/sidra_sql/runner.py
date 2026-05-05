# Copyright (c) 2026 Komesu, D.K.
# Licensed under the MIT License.

"""Hierarchical pipeline runner.

Walks a pipeline directory depth-first, post-order:
each child subtree (any subdirectory containing ``fetch.toml`` or
``transform.toml``) runs to completion before the parent's own
``fetch.toml`` / ``transform.toml`` execute. This lets a parent's
SQL transform consume the materialized outputs of its children.
"""

import logging
from pathlib import Path

from rich.console import Console

from .config import Config
from .toml_runner import TomlScript
from .transform_runner import TransformRunner

logger = logging.getLogger(__name__)


def _is_pipeline_dir(p: Path) -> bool:
    return p.is_dir() and (
        (p / "fetch.toml").exists() or (p / "transform.toml").exists()
    )


def run_subtree(
    config: Config,
    path: Path,
    force_metadata: bool = False,
    console: Console | None = None,
):
    """Run all sub-pipelines under ``path`` post-order, then ``path`` itself."""
    if not path.exists() or not path.is_dir():
        raise FileNotFoundError(f"Pipeline directory not found: {path}")

    for child in sorted(path.iterdir()):
        if _is_pipeline_dir(child):
            run_subtree(config, child, force_metadata, console)

    fetch_path = path / "fetch.toml"
    transform_path = path / "transform.toml"

    if fetch_path.exists():
        if console:
            console.print(f"[blue]-> Fetch:[/blue] {path.name}")
        TomlScript(config, fetch_path, force_metadata=force_metadata).run()

    if transform_path.exists():
        if console:
            console.print(f"[blue]-> Transform:[/blue] {path.name}")
        TransformRunner(config, transform_path).run()
