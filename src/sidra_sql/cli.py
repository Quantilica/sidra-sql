# Copyright (c) 2026 Komesu, D.K.
# Licensed under the MIT License.

import logging
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from sidra_sql.config import Config
from sidra_sql.plugin_manager import PluginManager
from sidra_sql.runner import run_subtree
from sidra_sql.scaffold import PipelineAdder, PluginScaffolder
from sidra_sql.validator import PluginValidator, Severity
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


@plugin_app.command("scaffold")
def scaffold_plugin(
    name: str = typer.Argument(..., help="Nome do plugin (vira o diretório raiz)"),
    description: str = typer.Option(
        "", "--description", "-d", help="Descrição do plugin"
    ),
    version: str = typer.Option("1.0.0", "--version", help="Versão semântica"),
    output_dir: Path = typer.Option(
        Path("."), "--output-dir", "-o", help="Diretório de saída"
    ),
    git_init: bool = typer.Option(
        True, "--git-init/--no-git-init", help="Inicializar repositório Git"
    ),
):
    """Cria a estrutura de arquivos para um novo plugin com templates prontos."""
    try:
        scaffolder = PluginScaffolder(name, description, version, output_dir, git_init)
        plugin_dir = scaffolder.create()
        slug = scaffolder.slug

        console.print(
            f"\n[bold green]Plugin '{name}' criado em {plugin_dir}[/bold green]\n"
        )
        console.print("  manifest.toml")
        console.print(f"  {slug}/")
        console.print("    fetch.toml")
        console.print("    transform.toml")
        console.print("    transform.sql")
        console.print("  README.md")
        if git_init:
            console.print("  .gitignore")

        console.print("\n[bold]Próximos passos:[/bold]")
        console.print("  1. Edite [cyan]manifest.toml[/cyan] e ajuste a descrição do pipeline")
        console.print(
            f"  2. Em [cyan]{slug}/fetch.toml[/cyan], substitua XXXX pelo ID da tabela SIDRA"
        )
        console.print(
            f"  3. Ajuste [cyan]{slug}/transform.sql[/cyan] para a sua transformação"
        )
        console.print(
            "  4. Publique o repositório e instale: "
            "[dim]sidra-sql plugin install <git-url>[/dim]\n"
        )
    except FileExistsError as e:
        console.print(f"[red]Erro:[/red] {e}")
        raise typer.Exit(1)
    except RuntimeError as e:
        console.print(f"[red]Erro:[/red] {e}")
        raise typer.Exit(1)


@plugin_app.command("add-pipeline")
def add_pipeline(
    pipeline_id: str = typer.Argument(..., help="ID do pipeline (usado em 'sidra-sql run')"),
    description: str = typer.Option(
        "", "--description", "-d", help="Descrição do pipeline"
    ),
    path: str = typer.Option(
        "", "--path", "-p", help="Caminho do diretório relativo ao plugin (default: pipeline-id)"
    ),
    plugin_dir: Path = typer.Option(
        Path("."), "--plugin-dir", help="Diretório raiz do plugin (default: diretório atual)"
    ),
):
    """Adiciona um novo pipeline a um plugin existente."""
    try:
        adder = PipelineAdder(pipeline_id, description, path, plugin_dir)
        pipeline_dir_created = adder.add()

        console.print(
            f"\n[bold green]Pipeline '{pipeline_id}' adicionado[/bold green]\n"
        )
        console.print(f"  {adder.path}/")
        console.print("    fetch.toml")
        console.print("    transform.toml")
        console.print("    transform.sql")
        console.print("  manifest.toml [dim](atualizado)[/dim]\n")

        console.print("[bold]Próximos passos:[/bold]")
        console.print(
            f"  1. Em [cyan]{adder.path}/fetch.toml[/cyan], substitua XXXX pelo ID da tabela SIDRA"
        )
        console.print(
            f"  2. Ajuste [cyan]{adder.path}/transform.sql[/cyan] para a sua transformação"
        )
        console.print(
            f"  3. Execute: [dim]sidra-sql run <alias> {pipeline_id}[/dim]\n"
        )
    except (FileNotFoundError, FileExistsError, ValueError) as e:
        console.print(f"[red]Erro:[/red] {e}")
        raise typer.Exit(1)
    except Exception as e:
        console.print(f"[red]Erro ao adicionar pipeline:[/red] {e}")
        raise typer.Exit(1)


@plugin_app.command("validate")
def validate_plugin(
    alias: Optional[str] = typer.Argument(
        None, help="Alias do plugin instalado (omitir para usar --plugin-dir)"
    ),
    plugin_dir: Path = typer.Option(
        Path("."), "--plugin-dir", help="Diretório raiz do plugin (default: diretório atual)"
    ),
):
    """Valida a estrutura e os arquivos de um plugin."""
    if alias is not None:
        target_dir = manager.registry.get_plugin_path(alias)
        if not target_dir.exists():
            console.print(f"[red]Erro:[/red] Plugin '{alias}' não encontrado.")
            raise typer.Exit(1)
    else:
        target_dir = plugin_dir

    console.print(f"\n[bold]Validando plugin em[/bold] {target_dir}\n")

    report = PluginValidator(target_dir).validate()

    severity_style = {
        Severity.OK: "[green]OK[/green]",
        Severity.WARN: "[yellow]AVISO[/yellow]",
        Severity.ERROR: "[red]ERRO[/red]",
    }

    for section in report.sections:
        console.print(f"[bold cyan]{section.title}[/bold cyan]")
        for issue in section.issues:
            tag = severity_style[issue.severity]
            console.print(f"  [{tag}] {issue.message}")
        console.print()

    if report.is_valid:
        summary = "[bold green]Válido[/bold green]"
    else:
        summary = "[bold red]Inválido[/bold red]"

    parts = [summary]
    if report.error_count:
        parts.append(f"[red]{report.error_count} erro(s)[/red]")
    if report.warning_count:
        parts.append(f"[yellow]{report.warning_count} aviso(s)[/yellow]")
    if not report.error_count and not report.warning_count:
        parts.append("sem erros ou avisos")

    console.print("Resultado: " + ", ".join(parts) + "\n")

    if not report.is_valid:
        raise typer.Exit(1)


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


@app.command("run-path")
def run_pipeline_path(
    path: Path = typer.Argument(..., help="Path to the pipeline directory"),
    force_metadata: bool = typer.Option(
        False, "--force-metadata", help="Force refresh metadata"
    ),
):
    """Run a pipeline directly from a directory path, without a registered plugin."""
    try:
        resolved = path.resolve()
        if not resolved.is_dir():
            console.print(f"[bold red]Directory not found:[/bold red] {resolved}")
            raise typer.Exit(1)

        config = Config()
        console.print(f"[bold blue]Running pipeline from {resolved}[/bold blue]")
        run_subtree(config, resolved, force_metadata=force_metadata, console=console)
        console.print("[bold green]Pipeline completed successfully![/bold green]")
    except Exception as e:
        console.print(f"[bold red]Pipeline failed:[/bold red] {e}")
        import traceback

        traceback.print_exc()
        raise typer.Exit(1)


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
