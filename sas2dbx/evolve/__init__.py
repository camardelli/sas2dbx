"""sas2dbx.evolve — Self-Evolution Pipeline.

O migrador aprende com cada erro não resolvido:
  1. UnresolvedError captura contexto completo
  2. EvolutionAnalyzer (via LLM) propõe fix no código-fonte
  3. QualityGate valida em sandbox antes de aplicar
  4. FixApplier aplica com hot-reload (sem rebuild Docker)
  5. HealthMonitor monitora impacto e faz rollback se necessário
"""

from sas2dbx.evolve.unresolved import UnresolvedError
from sas2dbx.evolve.health import PipelineHealth, HealthMonitor, HealthAction

__all__ = [
    "UnresolvedError",
    "PipelineHealth",
    "HealthMonitor",
    "HealthAction",
]
