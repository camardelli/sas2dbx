"""CLI principal do SAS2DBX — Typer + Rich."""

from __future__ import annotations

import logging
from pathlib import Path

import typer
from rich.console import Console
from rich.progress import BarColumn, Progress, SpinnerColumn, TextColumn, TimeElapsedColumn
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
    config: Path | None = typer.Option(None, "--config", help="Path para sas2dbx.yaml"),
) -> None:
    """SAS2DBX — migra jobs SAS para notebooks e workflows Databricks."""
    _verbose_state["value"] = verbose
    level = logging.DEBUG if verbose else (logging.ERROR if quiet else logging.WARNING)
    logging.basicConfig(
        level=level,
        format="%(levelname)s %(name)s: %(message)s",
    )
    if config and not config.exists():
        console.print(
            f"[yellow]Aviso: --config '{config}' não encontrado — usando padrões.[/yellow]"
        )


# ---------------------------------------------------------------------------
# sas2dbx migrate
# ---------------------------------------------------------------------------

_TIER_STYLE = {"rule": "green", "llm": "yellow", "manual": "red"}


@app.command()
def migrate(
    source_dir: Path = typer.Argument(..., help="Diretório com arquivos .sas (ou arquivo único)"),
    output: Path | None = typer.Option(None, "--output", "-o", help="Diretório de saída"),
    resume: bool = typer.Option(False, "--resume", help="Retomar migração interrompida"),
    recursive: bool = typer.Option(True, "--recursive/--no-recursive", help="Busca recursiva"),
) -> None:
    """Migra jobs SAS para notebooks Databricks (inventário no Sprint 1)."""
    from sas2dbx.analyze.classifier import classify_block
    from sas2dbx.ingest.reader import read_sas_file, split_blocks
    from sas2dbx.ingest.scanner import scan_directory

    if resume:
        console.print("[yellow]--resume: funcionalidade disponível na Story 3.1.[/yellow]")

    # 1 — Scan
    try:
        sas_files = scan_directory(source_dir, recursive=recursive)
    except FileNotFoundError as exc:
        console.print(f"[red]Erro: {exc}[/red]")
        raise typer.Exit(1) from exc

    if not sas_files:
        console.print(f"[yellow]Nenhum arquivo .sas encontrado em {source_dir}[/yellow]")
        raise typer.Exit(0)

    console.print(f"\nEncontrados [bold]{len(sas_files)}[/bold] arquivo(s) .sas\n")

    # 2 — Inventário: ler + dividir + classificar com progress bar
    inventory_rows: list[tuple] = []

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total}"),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("Analisando blocos...", total=len(sas_files))

        for sas_file in sas_files:
            code, encoding = read_sas_file(sas_file.path)
            sas_file.encoding = encoding  # propaga encoding detectado (QA M3)
            blocks = split_blocks(code, source_file=sas_file.path)

            if not blocks:
                inventory_rows.append((
                    sas_file.path.name, "—", "—", "—",
                ))
            else:
                for block in blocks:
                    result = classify_block(block.raw_code)
                    block.classification = result
                    inventory_rows.append((
                        sas_file.path.name,
                        result.construct_type,
                        result.tier.value.upper(),
                        f"{result.confidence:.1f}",
                    ))

            progress.advance(task)

    # 3 — Exibir tabela de inventário
    table = Table(title="Inventário de Blocos SAS", show_header=True)
    table.add_column("Arquivo", style="cyan")
    table.add_column("Construct")
    table.add_column("Tier", justify="center")
    table.add_column("Conf.", justify="right")

    tier_counts: dict[str, int] = {"RULE": 0, "LLM": 0, "MANUAL": 0}
    for filename, construct, tier, conf in inventory_rows:
        style = _TIER_STYLE.get(tier.lower(), "")
        table.add_row(filename, construct, f"[{style}]{tier}[/{style}]", conf)
        if tier in tier_counts:
            tier_counts[tier] += 1

    console.print(table)

    total = sum(tier_counts.values())
    if total:
        console.print(
            f"\nResumo: "
            f"[green]{tier_counts['RULE']} RULE[/green] · "
            f"[yellow]{tier_counts['LLM']} LLM[/yellow] · "
            f"[red]{tier_counts['MANUAL']} MANUAL[/red] "
            f"de {total} bloco(s)"
        )

    if output:
        console.print(
            f"\n[dim]--output {output}: geração de notebooks disponível no Sprint 4.[/dim]"
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
                kwargs["custom_path"] = Path(path)

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
    console.print(
        f"[green]✓[/green] merged/ atualizado em [cyan]{base_path}/mappings/merged/[/cyan]"
    )


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


# ---------------------------------------------------------------------------
# sas2dbx analyze
# ---------------------------------------------------------------------------


@app.command()
def analyze(
    source_dir: Path = typer.Argument(..., help="Diretório com arquivos .sas (ou arquivo único)"),
    autoexec: Path | None = typer.Option(
        None, "--autoexec", help="Path para autoexec.sas com LIBNAMEs globais"
    ),
    libnames_yaml: Path | None = typer.Option(
        None, "--libnames", help="Path para libnames.yaml com depends_on_jobs"
    ),
    recursive: bool = typer.Option(True, "--recursive/--no-recursive", help="Busca recursiva"),
    show_order: bool = typer.Option(True, "--order/--no-order", help="Exibir ordem de execução"),
) -> None:
    """Analisa dependências entre jobs SAS e exibe o grafo de execução."""
    from sas2dbx.analyze.classifier import classify_block
    from sas2dbx.analyze.dependency import DependencyAnalyzer
    from sas2dbx.ingest.reader import read_sas_file, split_blocks
    from sas2dbx.ingest.scanner import scan_directory

    # 1 — Scan
    try:
        sas_files = scan_directory(source_dir, recursive=recursive)
    except FileNotFoundError as exc:
        console.print(f"[red]Erro: {exc}[/red]")
        raise typer.Exit(1) from exc

    # Exclui autoexec da análise de jobs
    autoexec_path = autoexec or (source_dir / "autoexec.sas" if source_dir.is_dir() else None)
    if autoexec_path and autoexec_path.exists():
        sas_files = [f for f in sas_files if f.path != autoexec_path]

    if not sas_files:
        console.print(f"[yellow]Nenhum arquivo .sas encontrado em {source_dir}[/yellow]")
        raise typer.Exit(0)

    console.print(f"\nAnalisando [bold]{len(sas_files)}[/bold] job(s)...\n")

    # 2 — Classificação de constructs por arquivo (cross-check Knowledge Store)
    construct_counts: dict[str, int] = {}
    manual_constructs: list[str] = []

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total}"),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("Classificando constructs...", total=len(sas_files))
        for sas_file in sas_files:
            sas_file.encoding = read_sas_file(sas_file.path)[1]  # QA M3
            code, _ = read_sas_file(sas_file.path)
            for block in split_blocks(code, source_file=sas_file.path):
                result = classify_block(block.raw_code)
                construct_counts[result.construct_type] = (
                    construct_counts.get(result.construct_type, 0) + 1
                )
                if result.tier.value == "manual":
                    manual_constructs.append(
                        f"{sas_file.path.name}:{block.start_line} [{result.construct_type}]"
                    )
            progress.advance(task)

    # 3 — Dependency analysis
    analyzer = DependencyAnalyzer(
        autoexec_path=autoexec_path if (autoexec_path and autoexec_path.exists()) else None,
        libnames_yaml=libnames_yaml,
    )
    graph = analyzer.analyze(sas_files)

    # 4 — Tabela de jobs e dependências
    job_table = Table(title="Jobs SAS — Dependências", show_header=True)
    job_table.add_column("Job", style="cyan", no_wrap=True)
    job_table.add_column("Inputs", style="dim")
    job_table.add_column("Outputs", style="dim")
    job_table.add_column("Macros")
    job_table.add_column("Libs")

    for job_name in sorted(graph.jobs.keys()):
        node = graph.jobs[job_name]
        job_table.add_row(
            job_name,
            ", ".join(node.inputs[:3]) + ("…" if len(node.inputs) > 3 else ""),
            ", ".join(node.outputs[:3]) + ("…" if len(node.outputs) > 3 else ""),
            ", ".join(node.macros_called[:2]) + ("…" if len(node.macros_called) > 2 else ""),
            ", ".join(node.libnames_declared[:3]),
        )
    console.print(job_table)

    # 5 — Arestas implícitas
    implicit = graph.get_implicit_dependencies()
    if implicit:
        console.print("\n[yellow]Dependências implícitas detectadas:[/yellow]")
        for dep, prereq, ds in implicit:
            console.print(f"  [yellow]⚠[/yellow] [cyan]{dep}[/cyan] ← {ds} ← [cyan]{prereq}[/cyan]")

    # 6 — Ordem de execução
    if show_order:
        order = graph.get_execution_order()
        console.print("\n[bold]Ordem de execução sugerida:[/bold]")
        for i, job in enumerate(order, 1):
            prefix = "└─" if i == len(order) else "├─"
            console.print(f"  {prefix} {i}. [cyan]{job}[/cyan]")

    # 7 — Constructs Tier.MANUAL (gap analysis Knowledge Store)
    if manual_constructs:
        console.print(f"\n[red]Constructs Tier MANUAL ({len(manual_constructs)}):[/red]")
        for item in manual_constructs[:10]:
            console.print(f"  [red]-[/red] {item}")
        if len(manual_constructs) > 10:
            console.print(f"  … e mais {len(manual_constructs) - 10}")

    # 8 — Warnings do grafo
    if graph.warnings:
        console.print(
            f"\n[yellow]{len(graph.warnings)} warning(s) — use --verbose para detalhes[/yellow]"
        )
