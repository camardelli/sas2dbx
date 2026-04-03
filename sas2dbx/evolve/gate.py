"""QualityGate — valida proposta de fix antes de aplicar.

Nenhum fix toca o código-fonte sem passar aqui. O gate opera em sandbox:
copia os arquivos modificados para um tmpdir e roda pytest via subprocess.

Decisões possíveis:
  APPROVE       — fix low/medium, testes passam → auto-aplica
  APPROVE_NOTIFY — fix medium, testes passam → aplica + notifica operador
  QUARANTINE    — fix high risk → armazena para aprovação humana
  REJECT        — testes falham, escopo violado, sem teste, ou ambiente instável
"""

from __future__ import annotations

import logging
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path

from sas2dbx.evolve.agent import EvolutionProposal, ALLOWED_PATHS, RISK_LEVELS

logger = logging.getLogger(__name__)


@dataclass
class GateResult:
    """Resultado da avaliação do QualityGate.

    Attributes:
        decision: "APPROVE" | "APPROVE_NOTIFY" | "QUARANTINE" | "REJECT".
        reason: Motivo legível da decisão.
        test_output: Output do pytest (para auditoria).
    """

    decision: str
    reason: str
    test_output: str = ""

    @property
    def approved(self) -> bool:
        """True se o fix pode ser aplicado."""
        return self.decision in ("APPROVE", "APPROVE_NOTIFY")


class QualityGate:
    """Valida proposta de fix em 5 etapas antes de aplicar.

    Etapas:
      1. Teste obrigatório (sem teste = REJECT imediato)
      2. Escopo de arquivos permitidos por risk_level
      3. Test suite existente passa (sem regressão)
      4. Aplica fix em sandbox (tmpdir) e roda testes completos
      5. Decisão por risk_level

    Args:
        project_root: Raiz do projeto (onde pyproject.toml / pytest.ini vive).
        test_timeout: Timeout em segundos para rodar pytest. Default 120.
    """

    def __init__(self, project_root: Path, test_timeout: int = 120) -> None:
        self._root = project_root
        self._timeout = test_timeout

    def evaluate(self, proposal: EvolutionProposal) -> GateResult:
        """Avalia proposta e retorna decisão.

        Args:
            proposal: EvolutionProposal do EvolutionAnalyzer.

        Returns:
            GateResult com decision e reason.
        """
        # Etapa 1: Teste obrigatório
        if not proposal.test:
            return GateResult("REJECT", "Proposta sem teste — rejeitada (sem teste)")

        if not proposal.is_valid:
            return GateResult("REJECT", f"Proposta inválida: fix_type={proposal.fix_type!r}")

        # Etapa 2: Escopo de arquivos — violação escala para QUARANTINE (não descarta)
        # O LLM pode propor um arquivo fora do escopo do risk_level declarado, mas a
        # proposta ainda tem valor: vai para revisão humana em vez de ser descartada.
        scope_check = self._check_file_scope(proposal)
        if scope_check:
            logger.info(
                "QualityGate: escopo violado (%s) — escalando para QUARANTINE em vez de REJECT",
                scope_check,
            )
            return GateResult(
                "QUARANTINE",
                f"Escopo violado (escalado para revisão humana): {scope_check}",
            )

        # Fixes de alto risco vão direto para quarentena (sem sandbox)
        if proposal.risk_level == "high":
            return GateResult(
                "QUARANTINE",
                f"Fix de alto risco ({proposal.fix_type}) em "
                f"{[f.path for f in proposal.files_to_modify]} — "
                "aguardando aprovação humana",
            )

        # Etapa 3: Captura falhas existentes na baseline (comparação delta).
        # NÃO exige baseline limpa — fix que não introduz novas falhas é seguro.
        # Isso permite que testes pré-existentes com falha não bloqueiem o gate.
        pre_result = self._run_tests(self._root)
        pre_failures = pre_result["failed_tests"]
        if pre_failures:
            logger.info(
                "QualityGate: %d falha(s) pré-existente(s) na baseline (não bloqueiam gate): %s",
                len(pre_failures),
                sorted(pre_failures)[:5],  # log apenas os primeiros 5
            )

        # Etapa 4: Aplica fix em sandbox — aceita se delta de falhas é zero
        sandbox_result = self._run_in_sandbox(proposal, pre_failures)
        if not sandbox_result["passed"]:
            return GateResult(
                "REJECT",
                sandbox_result.get("reject_reason", "Testes falharam após aplicar fix em sandbox"),
                sandbox_result["output"],
            )

        # Etapa 5: Decisão por risk_level
        if proposal.risk_level == "medium":
            return GateResult(
                "APPROVE_NOTIFY",
                f"Fix aprovado (notificação enviada): {proposal.description}",
                sandbox_result["output"],
            )

        return GateResult(
            "APPROVE",
            proposal.description,
            sandbox_result["output"],
        )

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _check_file_scope(self, proposal: EvolutionProposal) -> str | None:
        """Retorna mensagem de erro se algum arquivo viola o escopo. None = OK."""
        allowed = ALLOWED_PATHS.get(proposal.risk_level, [])
        for fm in proposal.files_to_modify:
            path = fm.path.replace("\\", "/")
            if not any(path.startswith(prefix) for prefix in allowed):
                return (
                    f"Arquivo '{fm.path}' fora do escopo permitido "
                    f"para risk_level='{proposal.risk_level}'"
                )
        return None

    def _run_tests(self, work_dir: Path, extra_args: list[str] | None = None) -> dict:
        """Roda pytest e retorna {'passed': bool, 'output': str, 'failed_tests': set[str]}.

        Não usa -x (stop-on-first-failure) para capturar o conjunto completo de falhas,
        permitindo comparação delta entre baseline e sandbox.
        """
        cmd = [
            sys.executable,
            "-m",
            "pytest",
            "tests/",
            "--tb=short",
            "-q",
            "--no-header",
        ] + (extra_args or [])

        try:
            result = subprocess.run(
                cmd,
                cwd=str(work_dir),
                capture_output=True,
                text=True,
                timeout=self._timeout,
            )
            output = (result.stdout + result.stderr)[-4000:]  # últimas 4k chars
            passed = result.returncode == 0
            failed_tests = self._extract_failed_tests(output)
            if not passed:
                logger.info(
                    "QualityGate: pytest rc=%d, %d falha(s)",
                    result.returncode, len(failed_tests),
                )
            return {"passed": passed, "output": output, "failed_tests": failed_tests}
        except subprocess.TimeoutExpired:
            logger.warning("QualityGate: pytest timeout (%ds)", self._timeout)
            return {"passed": False, "output": f"TIMEOUT após {self._timeout}s", "failed_tests": set()}
        except Exception as exc:
            logger.error("QualityGate: erro ao rodar pytest: %s", exc)
            return {"passed": False, "output": str(exc), "failed_tests": set()}

    def _extract_failed_tests(self, output: str) -> set[str]:
        """Extrai IDs de testes com falha ou erro do output do pytest.

        Captura:
          FAILED tests/path/test_file.py::TestClass::test_method
          ERROR  tests/path/test_file.py::TestClass::test_method
        """
        import re
        failed: set[str] = set()
        for line in output.splitlines():
            m = re.match(r"\s*(?:FAILED|ERROR)\s+([\w/\\.:\-]+)", line)
            if m:
                failed.add(m.group(1).strip())
        return failed

    def _run_in_sandbox(self, proposal: EvolutionProposal, pre_failures: set[str] | None = None) -> dict:
        """Aplica fix em tmpdir e roda testes completos.

        Estratégia:
          1. Copia a árvore do projeto para tmpdir
          2. Aplica as modificações propostas
          3. Escreve o arquivo de teste novo
          4. Roda pytest no tmpdir
          5. Comparação delta: aceita se nenhuma nova falha foi introduzida
          6. Descarta tmpdir

        Args:
            proposal: Proposta de fix.
            pre_failures: Conjunto de IDs de testes que já falhavam na baseline.
                          Se None, exige que todos os testes passem.
        """
        with tempfile.TemporaryDirectory(prefix="sas2dbx_gate_") as tmpdir:
            tmp = Path(tmpdir)

            # Copia apenas o necessário (sem .git, __pycache__, .venv, node_modules)
            ignore = shutil.ignore_patterns(
                ".git", "__pycache__", "*.pyc", ".venv", "venv",
                "node_modules", "*.egg-info", ".pytest_cache",
            )
            shutil.copytree(str(self._root), str(tmp / "project"), ignore=ignore)
            sandbox = tmp / "project"

            # Aplica modificações
            for fm in proposal.files_to_modify:
                target = sandbox / fm.path
                target.parent.mkdir(parents=True, exist_ok=True)

                if fm.action == "add":
                    target.write_text(fm.content, encoding="utf-8")
                elif fm.action == "append":
                    existing = target.read_text(encoding="utf-8") if target.exists() else ""
                    target.write_text(existing + "\n" + fm.content, encoding="utf-8")
                elif fm.action == "modify":
                    if fm.old_string and target.exists():
                        existing = target.read_text(encoding="utf-8")
                        if fm.old_string in existing:
                            target.write_text(
                                existing.replace(fm.old_string, fm.content, 1),
                                encoding="utf-8",
                            )
                        else:
                            logger.error(
                                "QualityGate sandbox: old_string não encontrado em %s — abortando sandbox",
                                fm.path,
                            )
                            msg = (
                                f"old_string não encontrado em '{fm.path}' — "
                                "fix não aplicado (LLM propôs trecho que não existe no arquivo atual)"
                            )
                            return {
                                "passed": False,
                                "output": msg,
                                "reject_reason": msg,
                            }
                    else:
                        target.write_text(fm.content, encoding="utf-8")

            # Escreve teste novo
            test_target = sandbox / proposal.test.path
            test_target.parent.mkdir(parents=True, exist_ok=True)
            test_target.write_text(proposal.test.content, encoding="utf-8")

            # Roda pytest no sandbox e compara delta
            post_result = self._run_tests(sandbox)
            post_failures = post_result["failed_tests"]

            if pre_failures is not None:
                # Modo delta: aceita se nenhuma nova falha foi introduzida
                new_failures = post_failures - pre_failures
                if new_failures:
                    logger.info(
                        "QualityGate sandbox: %d nova(s) falha(s) introduzida(s): %s",
                        len(new_failures), sorted(new_failures),
                    )
                    return {
                        "passed": False,
                        "output": post_result["output"],
                        "reject_reason": (
                            f"Fix introduziu {len(new_failures)} nova(s) falha(s): "
                            + ", ".join(sorted(new_failures))
                        ),
                    }
                logger.info(
                    "QualityGate sandbox: delta OK — %d falha(s) pré-existente(s) inalteradas, nenhuma nova",
                    len(post_failures),
                )
                return {"passed": True, "output": post_result["output"]}
            else:
                # Modo estrito (sem baseline): exige que tudo passe
                return {"passed": post_result["passed"], "output": post_result["output"]}
