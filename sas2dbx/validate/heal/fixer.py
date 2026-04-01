"""Fixer — aplica patches nos notebooks gerados com base no diagnóstico."""

from __future__ import annotations

import logging
import re
import shutil
from dataclasses import dataclass
from pathlib import Path

from sas2dbx.validate.heal.diagnostics import ErrorDiagnostic

logger = logging.getLogger(__name__)

# Registro de handlers: deterministic_fix_key → nome do método interno
_HANDLERS: dict[str, str] = {
    "create_placeholder_table": "_fix_create_placeholder_table",
    "add_missing_import": "_fix_add_missing_import",
    "increase_cluster_config": "_fix_increase_cluster_config",
}


@dataclass
class PatchResult:
    """Resultado da tentativa de patch de um notebook.

    Attributes:
        patched: True se o patch foi aplicado com sucesso.
        backup_path: Path do backup criado antes do patch (None se patch não aplicado).
        description: Descrição da ação tomada.
        error: Mensagem de erro se o patch falhou.
    """

    patched: bool
    backup_path: Path | None = None
    description: str = ""
    error: str | None = None


class NotebookFixer:
    """Aplica patches determinísticos em notebooks .py com base no diagnóstico.

    Classe stateless — cada chamada a apply_fix() opera de forma independente.
    O notebook original é preservado como .py.bak antes de qualquer modificação.
    """

    def apply_fix(self, notebook_path: Path, diagnostic: ErrorDiagnostic) -> PatchResult:
        """Aplica patch no notebook com base no diagnóstico.

        Fluxo:
          1. Verifica se diagnostic.deterministic_fix tem handler registrado
          2. Se não tem: retorna PatchResult(patched=False)
          3. Cria backup (.py.bak) via shutil.copy2()
          4. Chama o handler específico
          5. Se handler falhar: restaura backup, retorna PatchResult(patched=False, error=...)
          6. Retorna PatchResult(patched=True, ...)

        Args:
            notebook_path: Path local do notebook .py a ser modificado.
            diagnostic: ErrorDiagnostic com deterministic_fix preenchido.

        Returns:
            PatchResult indicando sucesso ou falha.
        """
        fix_key = diagnostic.deterministic_fix
        if not fix_key or fix_key not in _HANDLERS:
            return PatchResult(
                patched=False,
                description=f"Sem fix determinístico disponível para '{fix_key or 'None'}'",
            )

        if not notebook_path.exists():
            return PatchResult(
                patched=False,
                description="",
                error=f"Notebook não encontrado: {notebook_path}",
            )

        backup_path = notebook_path.with_suffix(".py.bak")
        try:
            if not backup_path.exists():
                shutil.copy2(notebook_path, backup_path)
            logger.debug("Fixer: backup criado em %s", backup_path)

            method_name = _HANDLERS[fix_key]
            handler = getattr(self, method_name)
            description = handler(notebook_path, diagnostic.entities)

            logger.info("Fixer: patch '%s' aplicado em %s — %s", fix_key, notebook_path, description)
            return PatchResult(patched=True, backup_path=backup_path, description=description)

        except Exception as exc:  # noqa: BLE001
            logger.warning("Fixer: falha ao aplicar '%s': %s", fix_key, exc)
            # Restaura backup se existir
            if backup_path.exists():
                shutil.copy2(backup_path, notebook_path)
                logger.info("Fixer: backup restaurado após falha")
            return PatchResult(patched=False, backup_path=None, error=str(exc))

    # ------------------------------------------------------------------
    # Handlers
    # ------------------------------------------------------------------

    def _fix_create_placeholder_table(
        self, notebook_path: Path, entities: dict[str, str]
    ) -> str:
        """Handler para category='missing_table'.

        Insere CREATE TABLE IF NOT EXISTS antes do primeiro bloco de código.
        """
        table_name = entities.get("table_name", "unknown_table")
        create_stmt = (
            f'# [AUTO-FIX] Placeholder criado pelo self-healing pipeline\n'
            f'spark.sql("CREATE TABLE IF NOT EXISTS {table_name} (id BIGINT) USING DELTA")\n\n'
        )

        content = notebook_path.read_text(encoding="utf-8")

        # Insere antes do primeiro spark.sql() ou # COMMAND ou df = spark
        insert_match = re.search(
            r"(spark\.sql\(|# COMMAND|df\s*=\s*spark)", content, re.MULTILINE
        )
        if insert_match:
            pos = insert_match.start()
            content = content[:pos] + create_stmt + content[pos:]
        else:
            content = create_stmt + content

        notebook_path.write_text(content, encoding="utf-8")
        return f"Created placeholder table: {table_name}"

    def _fix_add_missing_import(
        self, notebook_path: Path, entities: dict[str, str]
    ) -> str:
        """Handler para category='missing_import'.

        Insere import do módulo ausente após os imports existentes.
        """
        module_name = entities.get("module_name", "")
        if not module_name:
            return "No module name found in error — import not added"

        import_line = f"import {module_name}\n"
        content = notebook_path.read_text(encoding="utf-8")

        # Verifica se já importado
        if import_line.strip() in content:
            return f"Import already present: {module_name}"

        # Localiza última linha de import
        lines = content.splitlines(keepends=True)
        last_import_idx = -1
        for i, line in enumerate(lines):
            if re.match(r"^(import |from )", line.strip()):
                last_import_idx = i

        if last_import_idx >= 0:
            lines.insert(last_import_idx + 1, import_line)
        else:
            lines.insert(0, import_line)

        notebook_path.write_text("".join(lines), encoding="utf-8")
        return f"Added import: {module_name}"

    def _fix_increase_cluster_config(
        self, notebook_path: Path, entities: dict[str, str]
    ) -> str:
        """Handler para category='resource'.

        Adiciona configurações de memória do Spark ao notebook.
        """
        memory_conf = (
            '# [AUTO-FIX] Configuração de memória aumentada pelo self-healing pipeline\n'
            'spark.conf.set("spark.executor.memory", "4g")\n'
            'spark.conf.set("spark.driver.memory", "4g")\n\n'
        )

        content = notebook_path.read_text(encoding="utf-8")

        # Verifica se já configurado
        if 'spark.executor.memory' in content:
            return "Memory config already present — skipped"

        # Insere após imports, antes do primeiro bloco de código
        insert_match = re.search(
            r"(spark\.read|df\s*=\s*spark|spark\.sql\()", content, re.MULTILINE
        )
        if insert_match:
            pos = insert_match.start()
            content = content[:pos] + memory_conf + content[pos:]
        else:
            content = memory_conf + content

        notebook_path.write_text(content, encoding="utf-8")
        return "Increased memory config: executor=4g, driver=4g"
