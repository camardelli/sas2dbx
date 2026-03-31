"""Testes para sas2dbx/web/worker.py — MigrationWorker."""

from __future__ import annotations

import io
import zipfile
from pathlib import Path

import pytest

from sas2dbx.transpile.llm.client import LLMConfig, LLMResponse
from sas2dbx.web.storage import MigrationStorage
from sas2dbx.web.worker import MigrationWorker, _extract_zip

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _llm_config() -> LLMConfig:
    """LLMConfig sem api_key real — stub será injetado nos testes."""
    return LLMConfig(api_key=None)


@pytest.fixture()
def storage(tmp_path: Path) -> MigrationStorage:
    return MigrationStorage(tmp_path / "work")


@pytest.fixture()
def worker(storage: MigrationStorage) -> MigrationWorker:
    return MigrationWorker(storage=storage, llm_config=_llm_config())


def _make_zip(files: dict[str, str]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, content in files.items():
            zf.writestr(name, content)
    return buf.getvalue()


_SIMPLE_SAS = "DATA out; SET in; RUN;"
_SIMPLE_ZIP = _make_zip({"job_a.sas": _SIMPLE_SAS})


def _setup_migration(storage: MigrationStorage, zip_bytes: bytes = _SIMPLE_ZIP) -> str:
    """Cria migração e salva upload.zip — prepara para o worker processar."""
    migration_id = "test-worker-001"
    migration_dir = storage.create_migration(migration_id, config={
        "autoexec_filename": "autoexec.sas",
        "encoding": "auto",
        "catalog": "main",
        "schema": "migrated",
        "original_filename": "test.zip",
    })
    (migration_dir / "upload.zip").write_bytes(zip_bytes)
    return migration_id


# ---------------------------------------------------------------------------
# _extract_zip
# ---------------------------------------------------------------------------


class TestExtractZip:
    def test_extracts_files(self, tmp_path: Path) -> None:
        zip_bytes = _make_zip({"a.sas": "DATA x; RUN;", "sub/b.sas": "DATA y; RUN;"})
        dest = tmp_path / "out"
        zip_path = tmp_path / "test.zip"
        zip_path.write_bytes(zip_bytes)
        _extract_zip(zip_path, dest)
        assert (dest / "a.sas").exists()
        assert (dest / "sub" / "b.sas").exists()

    def test_creates_dest_if_missing(self, tmp_path: Path) -> None:
        zip_path = tmp_path / "t.zip"
        zip_path.write_bytes(_SIMPLE_ZIP)
        dest = tmp_path / "new_dest"
        _extract_zip(zip_path, dest)
        assert dest.exists()

    def test_zip_slip_raises(self, tmp_path: Path) -> None:
        """Entry com ../ que escapa de dest deve levantar ValueError."""
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("../evil.txt", "pwned")
        zip_path = tmp_path / "evil.zip"
        zip_path.write_bytes(buf.getvalue())
        dest = tmp_path / "safe_dest"
        with pytest.raises(ValueError, match="Zip-slip"):
            _extract_zip(zip_path, dest)

    def test_bad_zip_raises(self, tmp_path: Path) -> None:
        zip_path = tmp_path / "bad.zip"
        zip_path.write_bytes(b"not a zip")
        dest = tmp_path / "out"
        with pytest.raises(zipfile.BadZipFile):
            _extract_zip(zip_path, dest)


# ---------------------------------------------------------------------------
# MigrationWorker — status transitions
# ---------------------------------------------------------------------------


class TestWorkerStatusTransitions:
    def test_status_becomes_processing_then_done(
        self, worker: MigrationWorker, storage: MigrationStorage, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """_run() deve levar status de pending → processing → done."""
        mid = _setup_migration(storage)

        # Monkeypatch LLMClient.complete para retornar resposta stub
        from sas2dbx.transpile.llm.client import LLMClient
        async def _stub_complete(self, prompt: str) -> LLMResponse:
            return LLMResponse(content="## Documentação\nStub.", tokens_used=10)
        monkeypatch.setattr(LLMClient, "complete", _stub_complete)

        worker._run(mid)  # executa sincrono (sem thread)

        assert storage.get_meta(mid)["status"] == "done"

    def test_status_becomes_failed_on_infrastructure_error(
        self, worker: MigrationWorker, storage: MigrationStorage, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Exceção de infra em _execute_pipeline deve resultar em status=failed."""
        mid = _setup_migration(storage)

        def _boom(*args, **kwargs):
            raise RuntimeError("Disk full")
        monkeypatch.setattr(worker, "_execute_pipeline", _boom)

        worker._run(mid)

        meta = storage.get_meta(mid)
        assert meta["status"] == "failed"
        assert "RuntimeError" in meta["error"]

    def test_error_message_stored_in_meta(
        self, worker: MigrationWorker, storage: MigrationStorage, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        mid = _setup_migration(storage)
        monkeypatch.setattr(worker, "_execute_pipeline", lambda mid: (_ for _ in ()).throw(
            OSError("no space left")
        ))

        worker._run(mid)

        error = storage.get_meta(mid)["error"]
        assert "no space left" in error


# ---------------------------------------------------------------------------
# MigrationWorker — pipeline outputs
# ---------------------------------------------------------------------------


class TestWorkerPipelineOutputs:
    def _run_pipeline(
        self,
        worker: MigrationWorker,
        storage: MigrationStorage,
        monkeypatch: pytest.MonkeyPatch,
        zip_bytes: bytes = _SIMPLE_ZIP,
    ) -> str:
        """Helper: configura stub LLM, cria migração e executa pipeline."""
        from sas2dbx.transpile.llm.client import LLMClient
        async def _stub_complete(self, prompt: str) -> LLMResponse:
            return LLMResponse(content="## Documentação\nStub gerado pelo teste.", tokens_used=5)
        monkeypatch.setattr(LLMClient, "complete", _stub_complete)

        mid = _setup_migration(storage, zip_bytes)
        worker._run(mid)
        return mid

    def test_status_is_done_after_run(
        self, worker: MigrationWorker, storage: MigrationStorage, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        mid = self._run_pipeline(worker, storage, monkeypatch)
        assert storage.get_meta(mid)["status"] == "done"

    def test_input_dir_has_sas_files(
        self, worker: MigrationWorker, storage: MigrationStorage, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        mid = self._run_pipeline(worker, storage, monkeypatch)
        input_dir = storage.get_migration_dir(mid) / "input"
        sas_files = list(input_dir.glob("**/*.sas"))
        assert len(sas_files) >= 1

    def test_output_dir_has_state_json(
        self, worker: MigrationWorker, storage: MigrationStorage, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        mid = self._run_pipeline(worker, storage, monkeypatch)
        state_path = storage.get_migration_dir(mid) / "output" / ".sas2dbx_state.json"
        assert state_path.exists()

    def test_explorer_html_generated(
        self, worker: MigrationWorker, storage: MigrationStorage, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        mid = self._run_pipeline(worker, storage, monkeypatch)
        explorer_path = storage.get_migration_dir(mid) / "explorer.html"
        assert explorer_path.exists()
        assert "<html" in explorer_path.read_text(encoding="utf-8").lower()

    def test_docs_dir_has_architecture_md(
        self, worker: MigrationWorker, storage: MigrationStorage, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        mid = self._run_pipeline(worker, storage, monkeypatch)
        arch_path = storage.get_migration_dir(mid) / "docs" / "ARCHITECTURE.md"
        assert arch_path.exists()

    def test_get_status_shows_jobs(
        self, worker: MigrationWorker, storage: MigrationStorage, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Após pipeline, get_status() deve retornar jobs com contagens."""
        mid = self._run_pipeline(worker, storage, monkeypatch)
        status = storage.get_status(mid)
        assert status["progress"]["total"] >= 1

    def test_empty_zip_no_sas_files_still_done(
        self, worker: MigrationWorker, storage: MigrationStorage, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Zip sem .sas não deve lançar exceção — pipeline termina silenciosamente."""
        from sas2dbx.transpile.llm.client import LLMClient
        async def _stub(self, prompt):
            return LLMResponse(content="stub", tokens_used=0)
        monkeypatch.setattr(LLMClient, "complete", _stub)

        empty_zip = _make_zip({"readme.txt": "nothing here"})
        mid = _setup_migration(storage, empty_zip)
        worker._run(mid)
        # Sem .sas → retorna cedo no pipeline, mas status ainda é "done"
        assert storage.get_meta(mid)["status"] == "done"


# ---------------------------------------------------------------------------
# MigrationWorker — start() dispara thread
# ---------------------------------------------------------------------------


class TestWorkerStart:
    def test_start_fires_background_thread(
        self, worker: MigrationWorker, storage: MigrationStorage, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """start() deve disparar uma thread (daemon) sem bloquear."""
        import threading

        from sas2dbx.transpile.llm.client import LLMClient
        async def _stub(self, prompt):
            return LLMResponse(content="stub", tokens_used=0)
        monkeypatch.setattr(LLMClient, "complete", _stub)

        mid = _setup_migration(storage)
        threads_before = set(t.name for t in threading.enumerate())
        worker.start(mid)
        threads_after = set(t.name for t in threading.enumerate())
        new_threads = threads_after - threads_before
        # Ao menos uma nova thread com nome "worker-..."
        assert any("worker-" in name for name in new_threads)
