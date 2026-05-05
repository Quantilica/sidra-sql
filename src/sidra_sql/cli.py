# Copyright (c) 2026 Komesu, D.K.
# Licensed under the MIT License.

import logging
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from sidra_sql.config import Config
from sidra_sql.plugin_manager import PluginManager
from sidra_sql.runner import run_subtree
from sidra_sql.transform_runner import TransformRunner

app = typer.Typer(help="Sidra-SQL CLI - Manage and run data pipelines")
plugin_app = typer.Typer(help="Manage pipeline plugins")
app.add_typer(plugin_app, name="plugin")

console = Console()
manager = PluginManager()


@app.callback()
def bootstrap():
    """Inicialização automática do sistema."""
    manager.ensure_defaults()


@plugin_app.command("install")
def install_plugin(
    url: str,
    alias: Optional[str] = typer.Option(None, help="Alias for the plugin"),
):
    """Install a new plugin from a Git URL."""
    try:
        manager.install(url, alias)
        console.print("[green]Plugin installed successfully.[/green]")
    except Exception as e:
        console.print(f"[red]Error installing plugin:[/red] {e}")


@plugin_app.command("update")
def update_plugin(
    alias: Optional[str] = typer.Argument(
        None, help="Alias of the plugin to update (updates all if omitted)"
    ),
):
    """Update installed plugin(s) from Git."""
    try:
        manager.update(alias)
        console.print("[green]Update completed.[/green]")
    except Exception as e:
        console.print(f"[red]Error updating plugin:[/red] {e}")


@plugin_app.command("remove")
def remove_plugin(
    alias: str = typer.Argument(..., help="Alias of the plugin to remove"),
):
    """Remove an installed plugin."""
    try:
        manager.remove(alias)
        console.print(f"[green]Plugin '{alias}' removed.[/green]")
    except Exception as e:
        console.print(f"[red]Error removing plugin:[/red] {e}")


@plugin_app.command("list")
def list_plugins():
    """List installed plugins and their pipelines."""
    try:
        pipelines = manager.list_pipelines()

        table = Table(title="Installed Pipelines")
        table.add_column("Plugin Alias", style="cyan")
        table.add_column("Pipeline ID", style="magenta")
        table.add_column("Description", style="green")

        for alias, plugin_name, pipeline in pipelines:
            table.add_row(alias, pipeline.id, pipeline.description)

        console.print(table)
    except Exception as e:
        console.print(f"[red]Error listing plugins:[/red] {e}")


@app.command("run")
def run_pipeline(
    alias: str = typer.Argument(..., help="Plugin alias"),
    pipeline_id: Optional[str] = typer.Argument(
        None, help="Pipeline ID to run (omit to run all)"
    ),
    force_metadata: bool = typer.Option(
        False, "--force-metadata", help="Force refresh metadata"
    ),
):
    """Run pipeline(s) from an installed plugin. Omit pipeline_id to run all."""
    try:
        config = Config()

        if pipeline_id is None:
            manifest = manager.read_manifest(alias)
            pipelines = manifest.pipelines
            if not pipelines:
                console.print(
                    f"[yellow]No pipelines found in '{alias}'.[/yellow]"
                )
                return
            console.print(
                f"[bold blue]Running all {len(pipelines)} pipeline(s) from '{alias}'[/bold blue]"
            )
            for p in pipelines:
                console.print(f"\n[cyan]→ {p.id}[/cyan]")
                run_subtree(
                    config,
                    p.path,
                    force_metadata=force_metadata,
                    console=console,
                )
            console.print(
                "\n[bold green]All pipelines completed successfully![/bold green]"
            )
        else:
            pipeline = manager.get_pipeline(alias, pipeline_id)

            console.print(
                f"[bold blue]Running pipeline {pipeline_id} from {alias}[/bold blue]"
            )

            run_subtree(
                config,
                pipeline.path,
                force_metadata=force_metadata,
                console=console,
            )

            console.print(
                "[bold green]Pipeline completed successfully![/bold green]"
            )

    except Exception as e:
        console.print(f"[bold red]Pipeline failed:[/bold red] {e}")
        import traceback

        traceback.print_exc()


@app.command("transform")
def transform_pipeline(
    alias: str = typer.Argument(..., help="Plugin alias"),
    pipeline_id: str = typer.Argument(..., help="Pipeline ID to transform"),
):
    """Run only the transform step of a pipeline, without fetch or recursion."""
    try:
        config = Config()
        pipeline = manager.get_pipeline(alias, pipeline_id)

        transform_path = pipeline.path / "transform.toml"
        if not transform_path.exists():
            console.print(
                f"[red]No transform.toml found at {transform_path}[/red]"
            )
            raise typer.Exit(1)

        console.print(
            f"[bold blue]Transforming {pipeline_id} from {alias}[/bold blue]"
        )
        TransformRunner(config, transform_path).run()
        console.print("[bold green]Transform completed successfully![/bold green]")

    except typer.Exit:
        raise
    except Exception as e:
        console.print(f"[bold red]Transform failed:[/bold red] {e}")
        import traceback

        traceback.print_exc()


def main():
    logging.basicConfig(level=logging.WARNING)
    app()


if __name__ == "__main__":
    main()
