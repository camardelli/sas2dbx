"""CLI principal do SAS2DBX — Typer + Rich."""

from __future__ import annotations

import logging
from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

app = typer.Typer(
    name="sas2dbx",
    help="SAS Query to Databricks Migrator — transpilação AI-first de jobs SAS.",
    no_args_is_help=True,
)
knowledge_app = typer.Typer(
    name="knowledge",
    help="Gerenciamento do Knowledge Store (harvest, build-mappings, validate).",
    no_args_is_help=True,
)
app.add_typer(knowledge_app, name="knowledge")

console = Console()

# ---------------------------------------------------------------------------
# Global options
# ---------------------------------------------------------------------------

_verbose_state: dict[str, bool] = {"value": False}


@app.callback()
def main(
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Ativar logging DEBUG."),
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suprimir output de progresso."),
) -> None:
    """SAS2DBX — migra jobs SAS para notebooks e workflows Databricks."""
    _verbose_state["value"] = verbose
    level = logging.DEBUG if verbose else (logging.ERROR if quiet else logging.WARNING)
    logging.basicConfig(
        level=level,
        format="%(levelname)s %(name)s: %(message)s",
    )


# ---------------------------------------------------------------------------
# sas2dbx knowledge harvest
# ---------------------------------------------------------------------------

@knowledge_app.command()
def harvest(
    source: str = typer.Argument(..., help="sas | pyspark | databricks | custom"),
    mode: str = typer.Option("offline", "--mode", "-m", help="offline | online"),
    version: str = typer.Option(None, "--version", help="Versão da fonte (ex: 9.4, 3.5)"),
    path: str = typer.Option(None, "--path", "-p", help="Path para --source custom"),
    base_path: str = typer.Option("./knowledge", "--base-path", help="Raiz do Knowledge Store"),
) -> None:
    """Coleta documentação de fontes técnicas para o Knowledge Store."""
    from sas2dbx.knowledge.populate.harvester import HarvestMode, KnowledgeHarvester

    try:
        harvest_mode = HarvestMode(mode)
    except ValueError:
        console.print(f"[red]Erro: mode inválido '{mode}'. Use 'offline' ou 'online'.[/red]")
        raise typer.Exit(1) from None

    harvester = KnowledgeHarvester(base_path=base_path)

    with console.status(f"[bold]Coletando [cyan]{source}[/cyan] ({mode})..."):
        try:
            kwargs: dict = {"mode": harvest_mode}
            if version:
                kwargs["version"] = version
            if path:
                from pathlib import Path as _Path
                kwargs["custom_path"] = _Path(path)

            harvester.harvest(source=source, **kwargs)
        except ValueError as exc:
            console.print(f"[red]Erro: {exc}[/red]")
            raise typer.Exit(1) from exc
        except FileNotFoundError as exc:
            console.print(f"[red]Arquivo não encontrado: {exc}[/red]")
            raise typer.Exit(1) from exc

    console.print(f"[green]✓[/green] Harvest [cyan]{source}[/cyan] concluído.")


# ---------------------------------------------------------------------------
# sas2dbx knowledge build-mappings
# ---------------------------------------------------------------------------

@knowledge_app.command(name="build-mappings")
def build_mappings(
    base_path: str = typer.Option("./knowledge", "--base-path", help="Raiz do Knowledge Store"),
) -> None:
    """Merge generated/ + curated/ → merged/ (curated sempre vence conflitos)."""
    from sas2dbx.knowledge.populate.normalizer import build_mappings as _build

    with console.status("[bold]Gerando merged/..."):
        results = _build(base_path=base_path)

    table = Table(title="Knowledge Store — Mappings Gerados", show_header=True)
    table.add_column("Arquivo", style="cyan")
    table.add_column("Entradas", justify="right", style="bold")

    total = 0
    for filename, count in sorted(results.items()):
        table.add_row(filename, str(count))
        total += count

    table.add_section()
    table.add_row("[bold]Total[/bold]", f"[bold]{total}[/bold]")

    console.print(table)
    console.print(f"[green]✓[/green] merged/ atualizado em [cyan]{base_path}/mappings/merged/[/cyan]")


# ---------------------------------------------------------------------------
# sas2dbx knowledge validate
# ---------------------------------------------------------------------------

@knowledge_app.command()
def validate(
    base_path: str = typer.Option("./knowledge", "--base-path", help="Raiz do Knowledge Store"),
) -> None:
    """Valida integridade e cobertura do Knowledge Store."""
    from sas2dbx.knowledge.validate import validate_knowledge_store

    with console.status("[bold]Validando Knowledge Store..."):
        report = validate_knowledge_store(base_path=base_path)

    # Entries table
    table = Table(title="Knowledge Store — Cobertura", show_header=True)
    table.add_column("Arquivo", style="cyan")
    table.add_column("Entradas", justify="right")

    total = 0
    for filename, count in sorted(report.total_entries.items()):
        table.add_row(filename, str(count))
        total += count
    table.add_section()
    table.add_row("[bold]Total[/bold]", f"[bold]{total}[/bold]")

    console.print(table)
    console.print(f"Cobertura (confidence ≥ 0.7): [bold]{report.coverage:.0%}[/bold]")

    if report.warnings:
        console.print("\n[yellow]Warnings:[/yellow]")
        for w in report.warnings:
            console.print(f"  [yellow]⚠[/yellow] {w}")

    if report.missing_references:
        console.print("\n[yellow]Referências .md ausentes:[/yellow]")
        for ref in report.missing_references:
            console.print(f"  [yellow]-[/yellow] {ref}")

    status = "[green]✓ VÁLIDO[/green]" if report.is_valid else "[red]✗ INVÁLIDO[/red]"
    console.print(f"\nStatus: {status}")

    if not report.is_valid:
        raise typer.Exit(1)
