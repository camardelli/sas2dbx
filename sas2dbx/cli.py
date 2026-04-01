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

    if resume and not output:
        console.print("[red]Erro: --resume requer --output para localizar o state file.[/red]")
        raise typer.Exit(1)

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
            f"\n[bold]Iniciando transpilação → {output}[/bold]\n"
        )
        from sas2dbx.analyze.dependency import DependencyAnalyzer
        from sas2dbx.transpile.engine import TranspilationEngine

        autoexec_path = source_dir / "autoexec.sas" if source_dir.is_dir() else None
        analyzer = DependencyAnalyzer(
            autoexec_path=autoexec_path if (autoexec_path and autoexec_path.exists()) else None,
        )
        graph = analyzer.analyze(sas_files)
        execution_order = graph.get_execution_order()

        engine = TranspilationEngine(
            output_dir=output,
            resume=resume,
        )

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("{task.completed}/{task.total}"),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            eng_task = progress.add_task("Transpilando jobs...", total=len(execution_order))
            results = engine.run(sas_files, execution_order)
            progress.update(eng_task, completed=len(execution_order))

        done = sum(1 for r in results if r.status == "done")
        failed = len(results) - done
        console.print(
            f"\nConcluído: [green]{done} transpilado(s)[/green]"
            + (f" · [red]{failed} com erro[/red]" if failed else "")
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
# sas2dbx knowledge status  (K.1)
# ---------------------------------------------------------------------------


@knowledge_app.command(name="status")
def status_knowledge(
    base_path: str = typer.Option("./knowledge", "--base-path", help="Raiz do Knowledge Store"),
) -> None:
    """Exibe estatísticas do Knowledge Store (entradas, confidence, tokens)."""
    from sas2dbx.knowledge.manifest import update_from_merged

    with console.status("[bold]Calculando estatísticas do Knowledge Store..."):
        manifest = update_from_merged(base_path)

    table = Table(title="Knowledge Store — Status", show_header=True)
    table.add_column("Arquivo", style="cyan")
    table.add_column("Entradas", justify="right", style="bold")

    total = 0
    for filename, count in sorted(manifest.get("entries_per_file", {}).items()):
        table.add_row(filename, str(count))
        total += count
    table.add_section()
    table.add_row("[bold]Total[/bold]", f"[bold]{total}[/bold]")

    console.print(table)

    avg_conf = manifest.get("avg_confidence", 0.0)
    conf_color = "green" if avg_conf >= 0.8 else ("yellow" if avg_conf >= 0.6 else "red")
    tokens = manifest.get("tokens_used_total", 0)
    on_demand = manifest.get("on_demand_harvests", 0)
    last_updated = manifest.get("last_updated") or "—"

    console.print(f"Confidence médio: [{conf_color}]{avg_conf:.2f}[/{conf_color}]")
    console.print(f"Tokens consumidos: [dim]{tokens:,}[/dim]")
    console.print(f"On-demand harvests: [dim]{on_demand}[/dim]")
    console.print(f"Última atualização: [dim]{last_updated}[/dim]")


# ---------------------------------------------------------------------------
# sas2dbx knowledge update
# ---------------------------------------------------------------------------


_ALL_SOURCES = ["sas", "pyspark", "databricks", "custom"]


@knowledge_app.command()
def update(
    sources: list[str] = typer.Argument(
        default=None,
        help="Fontes a re-processar: sas pyspark databricks custom (padrão: todas exceto custom)",
    ),
    mode: str = typer.Option("offline", "--mode", "-m", help="offline | online"),
    base_path: str = typer.Option("./knowledge", "--base-path", help="Raiz do Knowledge Store"),
    custom_path: str = typer.Option(
        None, "--custom-path", help="Path dos arquivos custom (obrigatório se 'custom' incluído)"
    ),
    skip_validate: bool = typer.Option(False, "--skip-validate", help="Pula validação final"),
) -> None:
    """Re-harvesta fontes selecionadas, reconstrói merged/ e valida.

    Equivale a rodar harvest + build-mappings + validate em sequência.
    Sem argumentos, re-processa sas, pyspark e databricks (omite custom).

    Exemplos:
      sas2dbx knowledge update                    # todas as fontes padrão
      sas2dbx knowledge update sas pyspark        # só SAS e PySpark
      sas2dbx knowledge update custom --custom-path ./env/
    """
    from sas2dbx.knowledge.populate.harvester import HarvestMode, KnowledgeHarvester
    from sas2dbx.knowledge.populate.normalizer import build_mappings as _build
    from sas2dbx.knowledge.validate import validate_knowledge_store

    try:
        harvest_mode = HarvestMode(mode)
    except ValueError:
        console.print(f"[red]Erro: mode inválido '{mode}'. Use 'offline' ou 'online'.[/red]")
        raise typer.Exit(1) from None

    # Padrão: todas exceto custom (requer --custom-path)
    resolved_sources = list(sources) if sources else ["sas", "pyspark", "databricks"]

    # Valida que custom_path foi fornecido se 'custom' está nas fontes
    if "custom" in resolved_sources and not custom_path:
        console.print(
            "[red]Erro: --custom-path é obrigatório quando 'custom' está nas fontes.[/red]"
        )
        raise typer.Exit(1) from None

    # Valida fontes
    invalid = [s for s in resolved_sources if s not in _ALL_SOURCES]
    if invalid:
        console.print(f"[red]Fonte(s) inválida(s): {invalid}. Use: {_ALL_SOURCES}[/red]")
        raise typer.Exit(1) from None

    harvester = KnowledgeHarvester(base_path=base_path)
    errors: list[str] = []

    sources_str = ", ".join(resolved_sources)
    console.print(f"\n[bold]Knowledge Store Update[/bold] — fontes: [cyan]{sources_str}[/cyan]\n")

    # 1 — Harvest por fonte
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total}"),
        console=console,
    ) as progress:
        harvest_task = progress.add_task("Harvesting...", total=len(resolved_sources))

        for source in resolved_sources:
            progress.update(harvest_task, description=f"Harvest [cyan]{source}[/cyan]...")
            try:
                kwargs: dict = {"mode": harvest_mode}
                if source == "custom":
                    kwargs["custom_path"] = Path(custom_path)  # type: ignore[arg-type]
                harvester.harvest(source=source, **kwargs)
                console.print(f"  [green]✓[/green] {source}")
            except Exception as exc:  # noqa: BLE001
                console.print(f"  [red]✗[/red] {source}: {exc}")
                errors.append(f"{source}: {exc}")
            finally:
                progress.advance(harvest_task)

    if errors:
        console.print(
            f"\n[yellow]⚠ {len(errors)} fonte(s) com erro"
            " — continuando para build-mappings.[/yellow]"
        )

    # 2 — build-mappings
    console.print("\n[bold]Reconstruindo merged/...[/bold]")
    try:
        with console.status("Merging generated/ + curated/ → merged/..."):
            mapping_results = _build(base_path=base_path)
        total_entries = sum(mapping_results.values())
        console.print(f"  [green]✓[/green] merged/ atualizado — {total_entries} entradas totais")
    except Exception as exc:  # noqa: BLE001
        console.print(f"  [red]✗ Erro em build-mappings: {exc}[/red]")
        raise typer.Exit(1) from exc

    # 3 — validate (opcional)
    if not skip_validate:
        console.print("\n[bold]Validando Knowledge Store...[/bold]")
        try:
            with console.status("Validando..."):
                report = validate_knowledge_store(base_path=base_path)

            coverage_color = "green" if report.coverage >= 0.8 else "yellow"
            console.print(
                f"  Cobertura: [{coverage_color}]{report.coverage:.0%}[/{coverage_color}]"
                f"  |  Entradas: {sum(report.total_entries.values())}"
            )

            if report.warnings:
                for w in report.warnings[:5]:  # máximo 5 warnings inline
                    console.print(f"  [yellow]⚠[/yellow] {w}")
                if len(report.warnings) > 5:
                    extra = len(report.warnings) - 5
                    console.print(
                        f"  [yellow]... +{extra} warnings. "
                        "Use `knowledge validate` para detalhes.[/yellow]"
                    )

            status_str = (
                "[green]✓ VÁLIDO[/green]" if report.is_valid else "[yellow]⚠ COM WARNINGS[/yellow]"
            )
            console.print(f"  Status: {status_str}")
        except Exception as exc:  # noqa: BLE001
            console.print(f"  [yellow]⚠ Validação falhou: {exc}[/yellow]")

    # Sumário final
    console.print()
    if errors:
        console.print(
            f"[yellow]Update concluído com {len(errors)} erro(s) de harvest.[/yellow] "
            "Verifique os logs acima."
        )
        raise typer.Exit(1)
    else:
        console.print("[green]✓ Knowledge Store atualizado com sucesso.[/green]")


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
            code, sas_file.encoding = read_sas_file(sas_file.path)
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


# ---------------------------------------------------------------------------
# sas2dbx document
# ---------------------------------------------------------------------------


@app.command()
def document(
    source_dir: Path = typer.Argument(..., help="Diretório com arquivos .sas"),
    output: Path = typer.Option(
        Path("./docs"), "--output", "-o", help="Diretório de saída para documentação"
    ),
    fmt: str = typer.Option(
        "all", "--format", "-f", help="Formato de saída: md | html | all"
    ),
    provider: str = typer.Option(
        "anthropic", "--provider", help="LLM provider: anthropic | khon"
    ),
    model: str = typer.Option(
        "claude-sonnet-4-6", "--model", help="Modelo LLM"
    ),
    api_key: str | None = typer.Option(
        None, "--api-key", envvar="ANTHROPIC_API_KEY", help="Chave API Anthropic"
    ),
    recursive: bool = typer.Option(True, "--recursive/--no-recursive", help="Busca recursiva"),
) -> None:
    """Gera documentação técnica (README.md por job + ARCHITECTURE.md + Explorer HTML)."""
    from sas2dbx.analyze.classifier import classify_block
    from sas2dbx.analyze.dependency import DependencyAnalyzer
    from sas2dbx.analyze.parser import BlockDeps, extract_block_deps
    from sas2dbx.document.architecture import ArchitectureDocumentor
    from sas2dbx.document.job_doc import JobDocumentor
    from sas2dbx.document.visual import ArchitectureExplorer
    from sas2dbx.ingest.reader import read_sas_file, split_blocks
    from sas2dbx.ingest.scanner import scan_directory
    from sas2dbx.models.migration_result import JobStatus, MigrationResult
    from sas2dbx.transpile.llm.client import LLMClient, LLMConfig

    if fmt not in ("md", "html", "all"):
        console.print(
            f"[red]Erro: --format deve ser 'md', 'html' ou 'all' (recebido: '{fmt}')[/red]"
        )
        raise typer.Exit(1)

    if not api_key:
        console.print(
            "[red]Erro: ANTHROPIC_API_KEY não definida. "
            "Use --api-key ou export ANTHROPIC_API_KEY=sk-...[/red]"
        )
        raise typer.Exit(1)

    # 1 — Scan
    try:
        sas_files = scan_directory(source_dir, recursive=recursive)
    except FileNotFoundError as exc:
        console.print(f"[red]Erro: {exc}[/red]")
        raise typer.Exit(1) from exc

    if not sas_files:
        console.print(f"[yellow]Nenhum arquivo .sas encontrado em {source_dir}[/yellow]")
        raise typer.Exit(0)

    console.print(f"\nDocumentando [bold]{len(sas_files)}[/bold] job(s)...\n")

    # 2 — Analyze: build dependency graph + classifications per job
    autoexec_path = source_dir / "autoexec.sas" if source_dir.is_dir() else None
    analyzer = DependencyAnalyzer(
        autoexec_path=autoexec_path if (autoexec_path and autoexec_path.exists()) else None,
    )
    graph = analyzer.analyze(sas_files)

    jobs_code: dict[str, str] = {}
    jobs_block_deps: dict[str, list[BlockDeps]] = {}
    jobs_classifications: dict[str, list] = {}

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total}"),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        parse_task = progress.add_task("Analisando código...", total=len(sas_files))
        for sas_file in sas_files:
            code, _ = read_sas_file(sas_file.path)
            job_name = sas_file.path.stem
            jobs_code[job_name] = code
            blocks = split_blocks(code, source_file=sas_file.path)
            jobs_block_deps[job_name] = [extract_block_deps(b) for b in blocks]
            jobs_classifications[job_name] = [classify_block(b.raw_code) for b in blocks]
            progress.advance(parse_task)

    # 3 — JobDocumentor (LLM) per job
    llm_config = LLMConfig(provider=provider, model=model, api_key=api_key)
    llm_client = LLMClient(llm_config)
    doc_engine = JobDocumentor(llm_client=llm_client)

    jobs_dir = output / "jobs"
    job_docs: dict[str, str] = {}
    results: list[MigrationResult] = []

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total}"),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        doc_task = progress.add_task("Gerando documentação por job...", total=len(sas_files))
        for sas_file in sas_files:
            job_name = sas_file.path.stem
            try:
                result = doc_engine.generate_doc_sync(
                    job_name=job_name,
                    sas_code=jobs_code[job_name],
                    block_deps=jobs_block_deps.get(job_name, []),
                    classification_results=jobs_classifications.get(job_name, []),
                    graph=graph,
                )
                if fmt in ("md", "all"):
                    doc_engine.write_doc(result, jobs_dir)
                job_docs[job_name] = result.content
                results.append(
                    MigrationResult(
                        job_id=job_name,
                        status=JobStatus.DONE,
                        confidence=1.0,
                    )
                )
            except Exception as exc:  # noqa: BLE001
                console.print(f"  [red]✗[/red] {job_name}: {exc}")
                results.append(
                    MigrationResult(
                        job_id=job_name,
                        status=JobStatus.FAILED,
                        error=str(exc),
                    )
                )
            progress.advance(doc_task)

    # 4 — ArchitectureDocumentor (no LLM)
    if fmt in ("md", "all"):
        arch_doc = ArchitectureDocumentor()
        arch_md = arch_doc.generate_architecture_md(graph, results, job_docs)
        arch_path = arch_doc.write(arch_md, output)
        console.print(f"[green]✓[/green] ARCHITECTURE.md → {arch_path}")

    # 5 — ArchitectureExplorer HTML
    if fmt in ("html", "all"):
        explorer = ArchitectureExplorer(project_name=source_dir.name)
        html = explorer.generate_html(graph, results, job_docs)
        html_path = explorer.write(html, output)
        console.print(f"[green]✓[/green] architecture_explorer.html → {html_path}")

    done = sum(1 for r in results if r.status.value == "done")
    failed = len(results) - done
    console.print(
        f"\nConcluído: [green]{done} jobs documentados[/green]"
        + (f" · [red]{failed} com erro[/red]" if failed else "")
    )


# ---------------------------------------------------------------------------
# sas2dbx status
# ---------------------------------------------------------------------------


@app.command()
def serve(
    port: int = typer.Option(8000, "--port", "-p", help="Porta TCP do servidor"),
    work_dir: str = typer.Option(
        "./sas2dbx_work", "--work-dir", help="Diretório de trabalho para migrações"
    ),
    reload: bool = typer.Option(False, "--reload", help="Auto-reload (apenas dev)"),
) -> None:
    """Inicia o servidor web para o piloto SKY (FastAPI + uvicorn)."""
    try:
        import uvicorn
    except ImportError:
        console.print(
            "[red]Erro: dependências web não instaladas. "
            "Execute: pip install sas2dbx[web][/red]"
        )
        raise typer.Exit(1) from None

    console.print(
        f"[bold]SAS2DBX Web[/bold] iniciando na porta [cyan]{port}[/cyan] "
        f"— work-dir: [cyan]{work_dir}[/cyan]"
    )
    console.print(f"  API docs: [link]http://localhost:{port}/api/docs[/link]")
    console.print("  Ctrl+C para encerrar\n")

    import os
    os.environ.setdefault("SAS2DBX_WORK_DIR", work_dir)

    uvicorn.run(
        "sas2dbx.web.app:create_app",
        factory=True,
        host="0.0.0.0",
        port=port,
        reload=reload,
        log_level="info",
    )


# ---------------------------------------------------------------------------
# sas2dbx validate
# ---------------------------------------------------------------------------


@app.command()
def validate_deploy(
    output_dir: Path = typer.Argument(..., help="Diretório de saída com notebooks .py gerados"),
    host: str = typer.Option(
        None, "--host", envvar="DATABRICKS_HOST", help="URL do workspace Databricks"
    ),
    token: str = typer.Option(
        None, "--token", envvar="DATABRICKS_TOKEN", help="Personal Access Token Databricks"
    ),
    catalog: str = typer.Option("main", "--catalog", help="Unity Catalog de destino"),
    db_schema: str = typer.Option("migrated", "--schema", help="Schema de destino"),
    node_type_id: str = typer.Option(
        "i3.xlarge", "--node-type", help="Tipo de nó do cluster de execução"
    ),
    spark_version: str = typer.Option(
        "13.3.x-scala2.12", "--spark-version", help="Databricks Runtime version"
    ),
    warehouse_id: str | None = typer.Option(
        None, "--warehouse-id", envvar="DATABRICKS_WAREHOUSE_ID", help="SQL Warehouse ID"
    ),
    deploy_only: bool = typer.Option(
        False, "--deploy-only", help="Apenas deploy — não executa o workflow"
    ),
    collect_only: bool = typer.Option(
        False, "--collect-only", help="Apenas coleta resultados de tabelas existentes"
    ),
    tables: list[str] = typer.Option(
        None, "--table", "-t", help="Tabelas a validar (repita para múltiplas)"
    ),
    report_path: Path | None = typer.Option(
        None, "--report", "-r", help="Caminho para salvar relatório JSON"
    ),
) -> None:
    """Valida notebooks gerados — deploy para Databricks, executa e coleta resultados."""
    import json

    try:
        from sas2dbx.validate.config import DatabricksConfig
        from sas2dbx.validate.deployer import DatabricksDeployer
        from sas2dbx.validate.executor import WorkflowExecutor
        from sas2dbx.validate.collector import DatabricksCollector
        from sas2dbx.validate.report import generate_validation_report
    except ImportError:
        console.print(
            "[red]Erro: dependências Databricks não instaladas. "
            "Execute: pip install sas2dbx[databricks][/red]"
        )
        raise typer.Exit(1) from None

    if not host or not token:
        console.print(
            "[red]Erro: --host/DATABRICKS_HOST e --token/DATABRICKS_TOKEN são obrigatórios.[/red]"
        )
        raise typer.Exit(1)

    cfg = DatabricksConfig(
        host=host.rstrip("/"),
        token=token,
        catalog=catalog,
        schema=db_schema,
        node_type_id=node_type_id,
        spark_version=spark_version,
        warehouse_id=warehouse_id or None,
    )

    # 1 — Localiza notebooks
    notebooks = sorted(output_dir.glob("*.py"))
    if not notebooks:
        console.print(f"[yellow]Nenhum notebook .py encontrado em {output_dir}[/yellow]")
        raise typer.Exit(0)

    console.print(
        f"\n[bold]Validação Databricks[/bold] — {len(notebooks)} notebook(s)\n"
        f"  Host: [cyan]{cfg.host}[/cyan]  Catalog: [cyan]{cfg.catalog}[/cyan]"
        f"  Schema: [cyan]{cfg.schema}[/cyan]\n"
    )

    deployer = DatabricksDeployer(cfg)
    deploy_results = []

    # 2 — Deploy
    if not collect_only:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("{task.completed}/{task.total}"),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            dep_task = progress.add_task("Fazendo deploy...", total=len(notebooks))
            for nb in notebooks:
                try:
                    dr = deployer.deploy(nb, nb.stem)
                    deploy_results.append(dr)
                    progress.advance(dep_task)
                except Exception as exc:  # noqa: BLE001
                    console.print(f"  [red]✗[/red] {nb.stem}: {exc}")
                    progress.advance(dep_task)

        console.print(f"[green]✓[/green] {len(deploy_results)} notebook(s) enviados\n")

        if deploy_only or not deploy_results:
            if deploy_results:
                console.print("[dim]--deploy-only: finalizando sem execução.[/dim]")
            raise typer.Exit(0)

    # 3 — Execução
    executor = WorkflowExecutor(cfg)
    exec_results = []
    if deploy_results:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            exec_task = progress.add_task("Executando workflows...", total=len(deploy_results))
            for dr in deploy_results:
                er = executor.execute(dr.job_id)
                dr.run_id = er.run_id
                exec_results.append(er)
                status_color = "green" if er.status == "SUCCESS" else "red"
                console.print(
                    f"  [{status_color}]{'✓' if er.status == 'SUCCESS' else '✗'}[/{status_color}]"
                    f" job_id={dr.job_id} → {er.status} ({er.duration_ms}ms)"
                )
                progress.advance(exec_task)

    # 4 — Coleta de tabelas
    table_validations = []
    if tables:
        collector = DatabricksCollector(cfg)
        with console.status("[bold]Coletando resultados das tabelas..."):
            table_validations = collector.collect(list(tables))

        for tv in table_validations:
            status_str = (
                f"[green]{tv.row_count} linhas, {tv.column_count} colunas[/green]"
                if tv.error is None
                else f"[red]{tv.error}[/red]"
            )
            console.print(f"  [cyan]{tv.table_name}[/cyan]: {status_str}")

    # 5 — Relatório
    if deploy_results and exec_results:
        # Usa o primeiro deploy+exec como representativo do relatório
        report = generate_validation_report(deploy_results[0], exec_results[0], table_validations)
        overall = report["summary"]["overall_status"]
        status_color = "green" if overall == "success" else ("yellow" if overall == "partial" else "red")
        console.print(f"\nStatus geral: [{status_color}]{overall.upper()}[/{status_color}]")

        if report_path:
            report_path.parent.mkdir(parents=True, exist_ok=True)
            report_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
            console.print(f"[green]✓[/green] Relatório salvo em [cyan]{report_path}[/cyan]")


@app.command()
def status(
    output_dir: Path = typer.Argument(..., help="Diretório de saída com .sas2dbx_state.json"),
) -> None:
    """Exibe o status atual de uma migração pelo state file."""
    from sas2dbx.models.migration_result import JobStatus
    from sas2dbx.transpile.state import MigrationStateManager

    state = MigrationStateManager(output_dir)
    if not state.state_path.exists():
        console.print(
            f"[yellow]Nenhuma migração encontrada em {output_dir}[/yellow]"
        )
        raise typer.Exit(0)

    state.load()
    statuses = state.get_all_statuses()

    if not statuses:
        console.print("[yellow]State file vazio.[/yellow]")
        raise typer.Exit(0)

    _STATUS_STYLE = {
        JobStatus.DONE: "green",
        JobStatus.FAILED: "red",
        JobStatus.IN_PROGRESS: "yellow",
        JobStatus.PENDING: "dim",
    }

    table = Table(title=f"Migration Status — {output_dir}", show_header=True)
    table.add_column("Job", style="cyan", no_wrap=True)
    table.add_column("Status", justify="center")

    counts: dict[JobStatus, int] = {s: 0 for s in JobStatus}
    for job_id, job_status in sorted(statuses.items()):
        style = _STATUS_STYLE.get(job_status, "")
        table.add_row(job_id, f"[{style}]{job_status.value.upper()}[/{style}]")
        counts[job_status] += 1

    console.print(table)
    console.print(
        f"\nResumo: "
        f"[green]{counts[JobStatus.DONE]} DONE[/green] · "
        f"[red]{counts[JobStatus.FAILED]} FAILED[/red] · "
        f"[yellow]{counts[JobStatus.IN_PROGRESS]} IN_PROGRESS[/yellow] · "
        f"[dim]{counts[JobStatus.PENDING]} PENDING[/dim]"
    )
