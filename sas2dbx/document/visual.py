"""ArchitectureExplorer — HTML standalone interativo para visualização do projeto.

Decisões arquiteturais aplicadas:
  R13: HTML gerado com string.Template da stdlib — sem Jinja2.
  R15: SVG estático para o grafo + JS vanilla inline (~80 linhas) para
       painel de detalhe clicável. Sem bibliotecas JS externas.

O arquivo HTML gerado é completamente standalone (sem CDN, sem servidor),
abre offline em qualquer browser moderno.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

from sas2dbx.models.dependency_graph import DependencyGraph
from sas2dbx.models.migration_result import JobStatus, MigrationResult

logger = logging.getLogger(__name__)

# Tier → cor do node SVG
_TIER_COLORS: dict[str, str] = {
    "rule": "#16a34a",    # verde — Tier 1
    "llm": "#2563eb",     # azul — Tier 2
    "manual": "#dc2626",  # vermelho — Tier 3
    "mixed": "#d97706",   # âmbar — múltiplos tiers
    "unknown": "#6b7280", # cinza — não classificado
}

# Status → ícone texto
_STATUS_ICON: dict[str, str] = {
    "done": "✅",
    "failed": "❌",
    "in_progress": "🔄",
    "pending": "⏳",
}

# ---------------------------------------------------------------------------
# ArchitectureExplorer
# ---------------------------------------------------------------------------


class ArchitectureExplorer:
    """Gera HTML standalone interativo do projeto SAS migrado.

    Args:
        project_name: Nome exibido no HTML Explorer.
    """

    def __init__(self, project_name: str = "SAS Migration") -> None:
        self._project_name = project_name

    def generate_data_json(
        self,
        graph: DependencyGraph,
        migration_results: list[MigrationResult],
        job_docs: dict[str, str],
        job_tiers: dict[str, str] | None = None,
    ) -> dict:
        """Retorna o dict de dados que alimenta o HTML Explorer.

        Args:
            graph: Grafo de dependências resolvido.
            migration_results: Resultados de migração por job.
            job_docs: Mapa job_name → conteúdo do README.md.
            job_tiers: Mapa job_name → tier string ("rule"|"llm"|"manual").
                Se None, tier é inferido do status (done=rule, failed=manual).

        Returns:
            Dict estruturado pronto para json.dumps().
        """
        results_by_id = {r.job_id: r for r in migration_results}
        execution_order = graph.get_execution_order()
        all_edges = graph.get_all_edges()
        job_tiers = job_tiers or {}

        total = len(execution_order)
        def _status(j: str) -> JobStatus:
            return results_by_id.get(j, MigrationResult(job_id=j)).status

        done_count = sum(1 for j in execution_order if _status(j) == JobStatus.DONE)
        failed_count = sum(1 for j in execution_order if _status(j) == JobStatus.FAILED)
        done_results = [
            results_by_id[j]
            for j in execution_order
            if j in results_by_id and results_by_id[j].status == JobStatus.DONE
        ]
        avg_conf = (
            sum(r.confidence for r in done_results) / len(done_results)
            if done_results else 0.0
        )

        jobs_data: dict[str, dict] = {}
        for job_name in execution_order:
            result = results_by_id.get(job_name)
            status = result.status.value if result else "pending"
            confidence = result.confidence if result else 0.0
            warnings = result.warnings if result else []
            error = result.error if result else None
            tier = job_tiers.get(job_name, _infer_tier(status))

            # Extrai objetivo do README gerado (primeira frase após "## Objetivo")
            objective = _extract_objective(job_docs.get(job_name, ""))

            # Extrai inputs/outputs do grafo
            node = graph.jobs.get(job_name)
            inputs = node.inputs if node else []
            outputs = node.outputs if node else []
            macros = (node.macros_called or []) + (node.macros_defined or []) if node else []

            prereqs = sorted(prereq for dep, prereq in all_edges if dep == job_name)
            dependents = sorted(dep for dep, prereq in all_edges if prereq == job_name)

            jobs_data[job_name] = {
                "name": job_name,
                "status": status,
                "tier": tier,
                "confidence": round(confidence, 4),
                "inputs": inputs,
                "outputs": outputs,
                "macros": macros,
                "objective": objective,
                "warnings": warnings,
                "error": error,
                "prereqs": prereqs,
                "dependents": dependents,
            }

        return {
            "project": self._project_name,
            "summary": {
                "total_jobs": total,
                "done": done_count,
                "failed": failed_count,
                "auto_pct": round(done_count / total, 4) if total else 0.0,
                "avg_confidence": round(avg_conf, 4),
            },
            "jobs": jobs_data,
            "edges": [[dep, prereq] for dep, prereq in all_edges],
            "execution_order": execution_order,
        }

    def generate_html(
        self,
        graph: DependencyGraph,
        migration_results: list[MigrationResult],
        job_docs: dict[str, str],
        job_tiers: dict[str, str] | None = None,
    ) -> str:
        """Retorna o HTML completo (standalone, abre offline).

        Args:
            graph: Grafo de dependências resolvido.
            migration_results: Resultados de migração por job.
            job_docs: Mapa job_name → conteúdo do README.md.
            job_tiers: Mapa job_name → tier string opcional.

        Returns:
            String HTML completa com dados JSON embutidos inline.
        """
        data = self.generate_data_json(graph, migration_results, job_docs, job_tiers)
        return _render_html(data, self._project_name)

    def write(
        self,
        html_content: str,
        output_dir: Path,
        filename: str = "architecture_explorer.html",
    ) -> Path:
        """Grava o HTML em disco.

        Args:
            html_content: Conteúdo gerado por generate_html().
            output_dir: Diretório de destino.
            filename: Nome do arquivo HTML.

        Returns:
            Caminho do arquivo gravado.
        """
        output_dir.mkdir(parents=True, exist_ok=True)
        out = output_dir / filename
        out.write_text(html_content, encoding="utf-8")
        logger.info("ArchitectureExplorer: HTML gravado em %s", out)
        return out


# ---------------------------------------------------------------------------
# HTML rendering (R13: string.Template — sem Jinja2)
# ---------------------------------------------------------------------------

# JS vanilla inline ~80 linhas (R15)
_JS_VANILLA = r"""
const DATA = __DATA_JSON__;

function showDetail(jobKey) {
  const job = DATA.jobs[jobKey];
  if (!job) return;

  document.getElementById('d-name').textContent = job.name;
  document.getElementById('d-status').textContent =
    ({'done':'✅ done','failed':'❌ failed',
      'pending':'⏳ pending','in_progress':'🔄 in_progress'}[job.status] || job.status);
  document.getElementById('d-tier').textContent = job.tier;
  document.getElementById('d-tier').className = 'badge tier-' + job.tier;
  document.getElementById('d-conf').textContent = (job.confidence * 100).toFixed(0) + '%';
  document.getElementById('d-objective').textContent = job.objective || '—';

  const inList = document.getElementById('d-inputs');
  inList.innerHTML = job.inputs.length
    ? job.inputs.map(i => '<li><code>' + i + '</code></li>').join('')
    : '<li><em>nenhum detectado</em></li>';

  const outList = document.getElementById('d-outputs');
  outList.innerHTML = job.outputs.length
    ? job.outputs.map(o => '<li><code>' + o + '</code></li>').join('')
    : '<li><em>nenhum detectado</em></li>';

  const warnDiv = document.getElementById('d-warnings');
  warnDiv.innerHTML = job.warnings && job.warnings.length
    ? job.warnings.map(w => '<div class="warn-item">' + w + '</div>').join('')
    : '<em>nenhum</em>';

  if (job.error) {
    document.getElementById('d-error-row').style.display = 'block';
    document.getElementById('d-error').textContent = job.error;
  } else {
    document.getElementById('d-error-row').style.display = 'none';
  }

  document.getElementById('d-prereqs').textContent = job.prereqs.join(', ') || '—';
  document.getElementById('d-deps').textContent = job.dependents.join(', ') || '—';

  document.getElementById('detail-panel').style.display = 'flex';

  document.querySelectorAll('.job-node').forEach(n => n.classList.remove('selected'));
  const node = document.getElementById('node-' + jobKey.replace(/[^a-zA-Z0-9_]/g, '_'));
  if (node) node.classList.add('selected');
}

function closeDetail() {
  document.getElementById('detail-panel').style.display = 'none';
  document.querySelectorAll('.job-node').forEach(n => n.classList.remove('selected'));
}
"""

_CSS = """
*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
       background: #f8fafc; color: #1e293b; }
header { background: #1e293b; color: #f8fafc; padding: 16px 24px;
         display: flex; align-items: center; gap: 12px; }
header h1 { font-size: 1.25rem; font-weight: 600; }
.subtitle { font-size: 0.85rem; color: #94a3b8; }
.metrics { display: flex; gap: 16px; padding: 20px 24px; flex-wrap: wrap; }
.card { background: #fff; border: 1px solid #e2e8f0; border-radius: 8px;
        padding: 16px 20px; min-width: 140px; }
.card .label { font-size: 0.75rem; color: #64748b; text-transform: uppercase;
               letter-spacing: .05em; margin-bottom: 4px; }
.card .value { font-size: 1.75rem; font-weight: 700; }
.card.green .value { color: #16a34a; }
.card.red .value { color: #dc2626; }
.card.blue .value { color: #2563eb; }
.graph-section { padding: 0 24px 20px; }
.graph-section h2 { font-size: 1rem; font-weight: 600; margin-bottom: 10px;
                    color: #475569; }
.svg-wrapper { background: #fff; border: 1px solid #e2e8f0; border-radius: 8px;
               overflow-x: auto; padding: 16px; }
.job-node { cursor: pointer; transition: filter 0.15s; }
.job-node:hover rect { filter: brightness(0.88); }
.job-node.selected rect { stroke: #f59e0b !important; stroke-width: 3 !important; }
.edge { stroke: #94a3b8; stroke-width: 1.5; fill: none;
        marker-end: url(#arrow); }
.node-label { fill: #fff; font-size: 11px; font-weight: 500;
              pointer-events: none; dominant-baseline: middle; text-anchor: middle; }
.legend { display: flex; gap: 16px; padding: 0 24px 8px; flex-wrap: wrap; }
.legend-item { display: flex; align-items: center; gap: 6px;
               font-size: 0.78rem; color: #475569; }
.legend-dot { width: 12px; height: 12px; border-radius: 3px; }
#detail-panel { display: none; position: fixed; top: 0; right: 0; height: 100%;
                width: 360px; background: #fff; border-left: 1px solid #e2e8f0;
                box-shadow: -4px 0 16px rgba(0,0,0,.08); flex-direction: column;
                overflow-y: auto; z-index: 100; }
.detail-header { display: flex; justify-content: space-between; align-items: center;
                 padding: 16px 20px; background: #1e293b; color: #f8fafc; }
.detail-header h3 { font-size: 0.95rem; word-break: break-all; }
.close-btn { background: none; border: none; color: #94a3b8; font-size: 1.2rem;
             cursor: pointer; padding: 0 4px; }
.close-btn:hover { color: #f8fafc; }
.detail-body { padding: 16px 20px; display: flex; flex-direction: column; gap: 14px; }
.detail-row { display: flex; flex-direction: column; gap: 4px; }
.detail-row .lbl { font-size: 0.72rem; text-transform: uppercase; letter-spacing: .05em;
                   color: #64748b; font-weight: 600; }
.detail-row .val { font-size: 0.88rem; color: #1e293b; }
.detail-row ul { list-style: none; display: flex; flex-direction: column; gap: 3px; }
.detail-row li code { background: #f1f5f9; padding: 1px 6px; border-radius: 4px;
                       font-size: 0.82rem; }
.warn-item { background: #fef9c3; border-left: 3px solid #eab308; padding: 6px 10px;
             border-radius: 0 4px 4px 0; font-size: 0.82rem; margin-bottom: 4px; }
.badge { display: inline-block; padding: 2px 8px; border-radius: 99px;
         font-size: 0.75rem; font-weight: 600; color: #fff; }
.tier-rule { background: #16a34a; }
.tier-llm { background: #2563eb; }
.tier-manual { background: #dc2626; }
.tier-mixed { background: #d97706; }
.tier-unknown { background: #6b7280; }
#d-error-row { background: #fee2e2; border-radius: 6px; padding: 8px 10px;
               font-size: 0.82rem; color: #991b1b; }
"""


def _render_html(data: dict, project_name: str) -> str:
    """Constrói HTML completo com dados embutidos inline."""
    json_str = json.dumps(data, indent=2, ensure_ascii=False)
    js_code = _JS_VANILLA.replace("__DATA_JSON__", json_str)

    summary = data["summary"]
    execution_order = data["execution_order"]
    jobs = data["jobs"]
    edges = data["edges"]

    metrics_html = _build_metrics(summary)
    svg_html = _build_svg(execution_order, jobs, edges)
    legend_html = _build_legend()

    return (
        "<!DOCTYPE html>\n"
        '<html lang="pt-BR">\n'
        "<head>\n"
        '<meta charset="UTF-8">\n'
        '<meta name="viewport" content="width=device-width, initial-scale=1.0">\n'
        f"<title>{project_name} — Architecture Explorer</title>\n"
        f"<style>{_CSS}</style>\n"
        "</head>\n"
        "<body>\n"
        "<header>\n"
        f'  <div><h1>{project_name}</h1>'
        f'  <div class="subtitle">Architecture Explorer · SAS2DBX</div></div>\n'
        "</header>\n"
        f"{metrics_html}\n"
        f"{legend_html}\n"
        '<div class="graph-section">\n'
        "  <h2>Grafo de Dependências</h2>\n"
        f'  <div class="svg-wrapper">{svg_html}</div>\n'
        "</div>\n"
        + _build_detail_panel()
        + f"<script>{js_code}</script>\n"
        "</body>\n"
        "</html>"
    )


def _build_metrics(summary: dict) -> str:
    auto_pct = int(summary["auto_pct"] * 100)
    avg_conf = int(summary["avg_confidence"] * 100)
    return (
        '<div class="metrics">\n'
        '  <div class="card"><div class="label">Total Jobs</div>'
        f'  <div class="value">{summary["total_jobs"]}</div></div>\n'
        '  <div class="card green"><div class="label">Migrados</div>'
        f'  <div class="value">{summary["done"]}</div></div>\n'
        '  <div class="card red"><div class="label">Falharam</div>'
        f'  <div class="value">{summary["failed"]}</div></div>\n'
        '  <div class="card blue"><div class="label">% Automático</div>'
        f'  <div class="value">{auto_pct}%</div></div>\n'
        '  <div class="card"><div class="label">Confiança Média</div>'
        f'  <div class="value">{avg_conf}%</div></div>\n'
        "</div>"
    )


def _build_legend() -> str:
    items = [
        ("rule", "Tier 1 — Rule"),
        ("llm", "Tier 2 — LLM"),
        ("manual", "Tier 3 — Manual"),
        ("unknown", "Não classificado"),
    ]
    parts = []
    for tier, label in items:
        color = _TIER_COLORS[tier]
        parts.append(
            f'<div class="legend-item">'
            f'<div class="legend-dot" style="background:{color}"></div>'
            f'{label}</div>'
        )
    return '<div class="legend">' + "".join(parts) + "</div>"


def _build_svg(
    execution_order: list[str],
    jobs: dict[str, dict],
    edges: list[list[str]],
) -> str:
    if not execution_order:
        return (
            '<svg width="200" height="60">'
            '<text x="10" y="35" fill="#6b7280">Nenhum job encontrado</text>'
            '</svg>'
        )

    # Calcula camadas topológicas
    layers = _compute_layers(execution_order, edges)
    max_layer = max(layers.values()) if layers else 0

    # Agrupa jobs por camada
    by_layer: dict[int, list[str]] = {}
    for job, layer in layers.items():
        by_layer.setdefault(layer, []).append(job)
    for layer_jobs in by_layer.values():
        layer_jobs.sort()

    # Dimensões do SVG
    node_w, node_h = 140, 36
    h_gap, v_gap = 60, 54  # gaps entre nodes
    pad_x, pad_y = 24, 24

    layer_width = node_w + h_gap
    max_in_layer = max(len(v) for v in by_layer.values())
    canvas_w = (max_layer + 1) * layer_width + pad_x * 2
    canvas_h = max_in_layer * (node_h + v_gap) + pad_y * 2

    # Posições dos centros de cada node
    positions: dict[str, tuple[int, int]] = {}
    for layer, layer_jobs in by_layer.items():
        count = len(layer_jobs)
        total_height = count * (node_h + v_gap) - v_gap
        start_y = (canvas_h - total_height) // 2
        cx = pad_x + layer * layer_width + node_w // 2
        for i, job in enumerate(layer_jobs):
            cy = start_y + i * (node_h + v_gap) + node_h // 2
            positions[job] = (cx, cy)

    svg_parts = [
        f'<svg width="{canvas_w}" height="{canvas_h}" '
        f'xmlns="http://www.w3.org/2000/svg">',
        "<defs>",
        '  <marker id="arrow" markerWidth="8" markerHeight="8" '
        '    refX="7" refY="3" orient="auto">',
        '    <path d="M0,0 L0,6 L8,3 z" fill="#94a3b8"/>',
        "  </marker>",
        "</defs>",
    ]

    # Arestas (desenhadas antes dos nodes para ficarem atrás)
    for dep, prereq in edges:
        if dep not in positions or prereq not in positions:
            continue
        px, py = positions[prereq]
        dx, dy = positions[dep]
        x1 = px + node_w // 2
        x2 = dx - node_w // 2
        svg_parts.append(
            f'  <line class="edge" x1="{x1}" y1="{py}" x2="{x2}" y2="{dy}"/>'
        )

    # Nodes
    for job in execution_order:
        if job not in positions:
            continue
        cx, cy = positions[job]
        x = cx - node_w // 2
        y = cy - node_h // 2
        tier = jobs.get(job, {}).get("tier", "unknown")
        color = _TIER_COLORS.get(tier, _TIER_COLORS["unknown"])
        node_id = "node-" + job.replace("-", "_").replace(".", "_")
        # Trunca label se muito longo
        label = job if len(job) <= 18 else job[:15] + "…"
        onclick = f"showDetail('{job}')"
        svg_parts.extend([
            f'  <g class="job-node" id="{node_id}" onclick="{onclick}">',
            f'    <rect x="{x}" y="{y}" width="{node_w}" height="{node_h}" '
            f'      rx="6" fill="{color}" stroke="{color}" stroke-width="1.5"/>',
            f'    <text class="node-label" x="{cx}" y="{cy}">{label}</text>',
            "  </g>",
        ])

    svg_parts.append("</svg>")
    return "\n".join(svg_parts)


def _build_detail_panel() -> str:
    return (
        '<div id="detail-panel">\n'
        '  <div class="detail-header">\n'
        '    <h3 id="d-name">—</h3>\n'
        '    <button class="close-btn" onclick="closeDetail()">✕</button>\n'
        "  </div>\n"
        '  <div class="detail-body">\n'
        '    <div class="detail-row">\n'
        '      <span class="lbl">Status</span>'
        '      <span class="val" id="d-status">—</span></div>\n'
        '    <div class="detail-row">\n'
        '      <span class="lbl">Tier</span>'
        '      <span class="badge" id="d-tier">—</span></div>\n'
        '    <div class="detail-row">\n'
        '      <span class="lbl">Confiança</span>'
        '      <span class="val" id="d-conf">—</span></div>\n'
        '    <div class="detail-row">\n'
        '      <span class="lbl">Objetivo</span>'
        '      <span class="val" id="d-objective">—</span></div>\n'
        '    <div class="detail-row">\n'
        '      <span class="lbl">Inputs</span>'
        '      <ul id="d-inputs"></ul></div>\n'
        '    <div class="detail-row">\n'
        '      <span class="lbl">Outputs</span>'
        '      <ul id="d-outputs"></ul></div>\n'
        '    <div class="detail-row">\n'
        '      <span class="lbl">Warnings</span>'
        '      <div id="d-warnings">—</div></div>\n'
        '    <div class="detail-row" id="d-error-row" style="display:none">\n'
        '      <span class="lbl">Erro</span>'
        '      <span id="d-error"></span></div>\n'
        '    <div class="detail-row">\n'
        '      <span class="lbl">Prerequisitos</span>'
        '      <span class="val" id="d-prereqs">—</span></div>\n'
        '    <div class="detail-row">\n'
        '      <span class="lbl">Dependentes</span>'
        '      <span class="val" id="d-deps">—</span></div>\n'
        "  </div>\n"
        "</div>\n"
    )


# ---------------------------------------------------------------------------
# Helpers internos
# ---------------------------------------------------------------------------


def _compute_layers(execution_order: list[str], edges: list[list[str]]) -> dict[str, int]:
    """Atribui camada topológica a cada job (0 = raiz sem dependências)."""
    layer: dict[str, int] = {j: 0 for j in execution_order}
    for _ in range(len(execution_order)):
        for dep, prereq in edges:
            if prereq in layer and dep in layer:
                layer[dep] = max(layer[dep], layer[prereq] + 1)
    return layer


def _infer_tier(status: str) -> str:
    """Tier inferido do status quando job_tiers não fornecido."""
    return "rule" if status == "done" else "unknown"


def _extract_objective(doc_content: str) -> str:
    """Extrai o texto do ## Objetivo do README.md gerado."""
    if not doc_content:
        return ""
    in_objective = False
    lines: list[str] = []
    for line in doc_content.splitlines():
        if line.strip().startswith("## Objetivo"):
            in_objective = True
            continue
        if in_objective:
            if line.startswith("##"):
                break
            if line.strip():
                lines.append(line.strip())
    return " ".join(lines)[:200]
