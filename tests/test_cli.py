"""Smoke tests para o CLI sas2dbx (Typer)."""

from pathlib import Path

import yaml
from typer.testing import CliRunner

from sas2dbx.cli import app

runner = CliRunner()


# ---------------------------------------------------------------------------
# Help commands
# ---------------------------------------------------------------------------

class TestHelpCommands:
    def test_root_help(self) -> None:
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0
        assert "sas2dbx" in result.output.lower() or "knowledge" in result.output.lower()

    def test_knowledge_help(self) -> None:
        result = runner.invoke(app, ["knowledge", "--help"])
        assert result.exit_code == 0

    def test_harvest_help(self) -> None:
        result = runner.invoke(app, ["knowledge", "harvest", "--help"])
        assert result.exit_code == 0
        assert "source" in result.output.lower()

    def test_build_mappings_help(self) -> None:
        result = runner.invoke(app, ["knowledge", "build-mappings", "--help"])
        assert result.exit_code == 0

    def test_validate_help(self) -> None:
        result = runner.invoke(app, ["knowledge", "validate", "--help"])
        assert result.exit_code == 0


# ---------------------------------------------------------------------------
# build-mappings
# ---------------------------------------------------------------------------

class TestBuildMappings:
    def test_build_mappings_empty_dir(self, tmp_path: Path) -> None:
        """build-mappings num dir vazio não levanta exceção."""
        result = runner.invoke(app, ["knowledge", "build-mappings", "--base-path", str(tmp_path)])
        assert result.exit_code == 0

    def test_build_mappings_creates_merged_dir(self, tmp_path: Path) -> None:
        result = runner.invoke(app, ["knowledge", "build-mappings", "--base-path", str(tmp_path)])
        assert result.exit_code == 0
        assert (tmp_path / "mappings" / "merged").is_dir()

    def test_build_mappings_shows_table(self, tmp_path: Path) -> None:
        result = runner.invoke(app, ["knowledge", "build-mappings", "--base-path", str(tmp_path)])
        assert result.exit_code == 0
        # Rich table output ou ao menos confirmação
        out = result.output.lower()
        assert "merged" in out or "mapeamento" in out or "mapping" in out


# ---------------------------------------------------------------------------
# harvest
# ---------------------------------------------------------------------------

class TestHarvest:
    def test_harvest_sas_offline_no_files(self, tmp_path: Path) -> None:
        """harvest sas offline sem arquivos locais não levanta exceção."""
        result = runner.invoke(app, [
            "knowledge", "harvest", "sas",
            "--mode", "offline",
            "--base-path", str(tmp_path),
        ])
        assert result.exit_code == 0

    def test_harvest_pyspark_offline(self, tmp_path: Path) -> None:
        result = runner.invoke(app, [
            "knowledge", "harvest", "pyspark",
            "--mode", "offline",
            "--base-path", str(tmp_path),
        ])
        assert result.exit_code == 0

    def test_harvest_invalid_mode_exits_1(self, tmp_path: Path) -> None:
        result = runner.invoke(app, [
            "knowledge", "harvest", "sas",
            "--mode", "invalid_mode",
            "--base-path", str(tmp_path),
        ])
        assert result.exit_code == 1

    def test_harvest_unknown_source_exits_1(self, tmp_path: Path) -> None:
        result = runner.invoke(app, [
            "knowledge", "harvest", "unknown_source",
            "--base-path", str(tmp_path),
        ])
        assert result.exit_code == 1


# ---------------------------------------------------------------------------
# validate
# ---------------------------------------------------------------------------

class TestValidateCli:
    def test_validate_empty_store_exits_1(self, tmp_path: Path) -> None:
        """Store vazio → validate retorna exit code 1."""
        result = runner.invoke(app, ["knowledge", "validate", "--base-path", str(tmp_path)])
        assert result.exit_code == 1

    def test_validate_populated_store_exits_0(self, tmp_path: Path) -> None:
        """Store com merged/ populado → validate retorna exit code 0."""
        merged = tmp_path / "mappings" / "merged"
        merged.mkdir(parents=True)
        for f in ["functions_map.yaml", "formats_map.yaml", "informats_map.yaml",
                  "options_map.yaml", "proc_map.yaml", "sql_dialect_map.yaml"]:
            (merged / f).write_text(yaml.dump({}), encoding="utf-8")

        result = runner.invoke(app, ["knowledge", "validate", "--base-path", str(tmp_path)])
        assert result.exit_code == 0


# ---------------------------------------------------------------------------
# migrate (QA L2 — smoke test do comando principal)
# ---------------------------------------------------------------------------

FIXTURES_DIR = Path(__file__).parent / "fixtures" / "sas"


class TestMigrateCli:
    def test_migrate_help(self) -> None:
        result = runner.invoke(app, ["migrate", "--help"])
        assert result.exit_code == 0
        assert "source" in result.output.lower() or "migrate" in result.output.lower()

    def test_migrate_fixtures_exits_0(self) -> None:
        """smoke: migrate com fixtures reais deve sair com código 0."""
        result = runner.invoke(app, ["migrate", str(FIXTURES_DIR)])
        assert result.exit_code == 0, result.output

    def test_migrate_shows_tiers(self) -> None:
        """migrate deve exibir ao menos um tier na tabela."""
        result = runner.invoke(app, ["migrate", str(FIXTURES_DIR)])
        output = result.output.upper()
        assert "RULE" in output or "LLM" in output or "MANUAL" in output

    def test_migrate_nonexistent_dir_exits_1(self) -> None:
        result = runner.invoke(app, ["migrate", "/nao/existe/xyz"])
        assert result.exit_code == 1


# ---------------------------------------------------------------------------
# analyze (Sprint 2)
# ---------------------------------------------------------------------------

class TestAnalyzeCli:
    def test_analyze_help(self) -> None:
        result = runner.invoke(app, ["analyze", "--help"])
        assert result.exit_code == 0

    def test_analyze_fixtures_exits_0(self) -> None:
        result = runner.invoke(app, ["analyze", str(FIXTURES_DIR)])
        assert result.exit_code == 0, result.output

    def test_analyze_shows_jobs(self) -> None:
        result = runner.invoke(app, ["analyze", str(FIXTURES_DIR)])
        assert "job_001" in result.output or "job_002" in result.output

    def test_analyze_nonexistent_dir_exits_1(self) -> None:
        result = runner.invoke(app, ["analyze", "/nao/existe/xyz"])
        assert result.exit_code == 1


# ---------------------------------------------------------------------------
# document (Sprint 6 — Story 6.4)
# ---------------------------------------------------------------------------


_STUB_DOC_RESPONSE = """## Objetivo
Job de teste para validação do CLI.

## Datasets
| Dataset | Tipo |
|---------|------|
| WORK.OUT | Output |

## Transformações
1. DATA step simples

## Riscos e observações
Nenhum."""


class TestDocumentCli:
    def test_document_help(self) -> None:
        result = runner.invoke(app, ["document", "--help"])
        assert result.exit_code == 0
        assert "format" in result.output.lower() or "output" in result.output.lower()

    def test_document_no_api_key_exits_1(self, tmp_path: Path) -> None:
        """Sem ANTHROPIC_API_KEY deve sair com código 1."""
        import os
        env = {k: v for k, v in os.environ.items() if k != "ANTHROPIC_API_KEY"}
        result = runner.invoke(
            app,
            ["document", str(FIXTURES_DIR), "--output", str(tmp_path)],
            env=env,
        )
        assert result.exit_code == 1
        assert "ANTHROPIC_API_KEY" in result.output

    def test_document_invalid_format_exits_1(self, tmp_path: Path) -> None:
        result = runner.invoke(app, [
            "document", str(FIXTURES_DIR),
            "--output", str(tmp_path),
            "--format", "csv",
            "--api-key", "sk-fake",
        ])
        assert result.exit_code == 1
        assert "format" in result.output.lower()

    def test_document_nonexistent_dir_exits_1(self, tmp_path: Path) -> None:
        result = runner.invoke(app, [
            "document", "/nao/existe/xyz",
            "--output", str(tmp_path),
            "--api-key", "sk-fake",
        ])
        assert result.exit_code == 1

    def test_document_md_format_creates_files(self, tmp_path: Path, monkeypatch) -> None:
        """--format md: cria README.md por job e ARCHITECTURE.md (stub LLM)."""
        from sas2dbx.transpile.llm.client import LLMResponse

        async def _stub_complete(self_inner, prompt: str) -> LLMResponse:
            return LLMResponse(
                content=_STUB_DOC_RESPONSE,
                provider_used="stub",
                tokens_used=100,
                latency_ms=1.0,
            )

        from sas2dbx.transpile.llm import client as llm_module
        monkeypatch.setattr(llm_module.LLMClient, "complete", _stub_complete)

        result = runner.invoke(app, [
            "document", str(FIXTURES_DIR),
            "--output", str(tmp_path),
            "--format", "md",
            "--api-key", "sk-fake",
        ])
        assert result.exit_code == 0, result.output
        assert (tmp_path / "ARCHITECTURE.md").exists()
        # Deve criar pelo menos um README de job
        job_files = list((tmp_path / "jobs").glob("*_README.md"))
        assert len(job_files) >= 1

    def test_document_html_format_creates_explorer(self, tmp_path: Path, monkeypatch) -> None:
        """--format html: cria architecture_explorer.html."""
        from sas2dbx.transpile.llm.client import LLMResponse

        async def _stub_complete(self_inner, prompt: str) -> LLMResponse:
            return LLMResponse(
                content=_STUB_DOC_RESPONSE,
                provider_used="stub",
                tokens_used=100,
                latency_ms=1.0,
            )

        from sas2dbx.transpile.llm import client as llm_module
        monkeypatch.setattr(llm_module.LLMClient, "complete", _stub_complete)

        result = runner.invoke(app, [
            "document", str(FIXTURES_DIR),
            "--output", str(tmp_path),
            "--format", "html",
            "--api-key", "sk-fake",
        ])
        assert result.exit_code == 0, result.output
        assert (tmp_path / "architecture_explorer.html").exists()

    def test_document_all_format_creates_all_artifacts(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        """--format all: cria md + html."""
        from sas2dbx.transpile.llm.client import LLMResponse

        async def _stub_complete(self_inner, prompt: str) -> LLMResponse:
            return LLMResponse(
                content=_STUB_DOC_RESPONSE,
                provider_used="stub",
                tokens_used=100,
                latency_ms=1.0,
            )

        from sas2dbx.transpile.llm import client as llm_module
        monkeypatch.setattr(llm_module.LLMClient, "complete", _stub_complete)

        result = runner.invoke(app, [
            "document", str(FIXTURES_DIR),
            "--output", str(tmp_path),
            "--format", "all",
            "--api-key", "sk-fake",
        ])
        assert result.exit_code == 0, result.output
        assert (tmp_path / "ARCHITECTURE.md").exists()
        assert (tmp_path / "architecture_explorer.html").exists()

    def test_document_empty_dir_exits_0(self, tmp_path: Path) -> None:
        """Diretório sem .sas: deve sair com código 0 (sem trabalho)."""
        empty_dir = tmp_path / "empty"
        empty_dir.mkdir()
        result = runner.invoke(app, [
            "document", str(empty_dir),
            "--output", str(tmp_path / "out"),
            "--api-key", "sk-fake",
        ])
        assert result.exit_code == 0
