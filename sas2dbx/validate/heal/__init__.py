"""Self-Healing Pipeline — diagnóstico, correção e re-teste automático.

Requer databricks-sdk para retest:
    pip install sas2dbx[databricks]
"""

from sas2dbx.validate.heal.advisor import FixSuggestion, HealingAdvisor
from sas2dbx.validate.heal.diagnostics import DiagnosticsEngine, ErrorDiagnostic
from sas2dbx.validate.heal.fixer import NotebookFixer, PatchResult
from sas2dbx.validate.heal.pipeline import HealingReport, SelfHealingPipeline
from sas2dbx.validate.heal.retest import RetestEngine, RetestResult

__all__ = [
    "DiagnosticsEngine",
    "ErrorDiagnostic",
    "FixSuggestion",
    "HealingAdvisor",
    "HealingReport",
    "NotebookFixer",
    "PatchResult",
    "RetestEngine",
    "RetestResult",
    "SelfHealingPipeline",
]
