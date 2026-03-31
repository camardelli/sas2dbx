"""Testes para sas2dbx/web/api/routes.py — endpoints REST."""

from __future__ import annotations

import io
import json
import zipfile
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from sas2dbx.web.app import create_app

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _no_worker(monkeypatch: pytest.MonkeyPatch) -> None:
    """Impede o worker de iniciar threads durante testes de routes."""
    from sas2dbx.web.worker import MigrationWorker
    monkeypatch.setattr(MigrationWorker, "start", lambda self, mid: None)


@pytest.fixture()
def client(tmp_path: Path) -> TestClient:
    """TestClient com app apontando para tmp_path."""
    app = create_app(work_dir=str(tmp_path / "work"))
    return TestClient(app, raise_server_exceptions=True)


def _make_zip(files: dict[str, str]) -> bytes:
    """Cria um .zip em memória com os arquivos fornecidos."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, content in files.items():
            zf.writestr(name, content)
    return buf.getvalue()


_SIMPLE_ZIP = _make_zip({"job_001.sas": "DATA out; SET in; RUN;"})


# ---------------------------------------------------------------------------
# POST /api/migrations
# ---------------------------------------------------------------------------


class TestCreateMigration:
    def test_returns_201(self, client: TestClient) -> None:
        resp = client.post(
            "/api/migrations",
            files={"file": ("jobs.zip", _SIMPLE_ZIP, "application/zip")},
        )
        assert resp.status_code == 201

    def test_response_has_migration_id(self, client: TestClient) -> None:
        resp = client.post(
            "/api/migrations",
            files={"file": ("jobs.zip", _SIMPLE_ZIP, "application/zip")},
        )
        data = resp.json()
        assert "migration_id" in data
        assert len(data["migration_id"]) > 0

    def test_response_status_pending(self, client: TestClient) -> None:
        resp = client.post(
            "/api/migrations",
            files={"file": ("jobs.zip", _SIMPLE_ZIP, "application/zip")},
        )
        assert resp.json()["status"] == "pending"

    def test_response_has_created_at(self, client: TestClient) -> None:
        resp = client.post(
            "/api/migrations",
            files={"file": ("jobs.zip", _SIMPLE_ZIP, "application/zip")},
        )
        assert "created_at" in resp.json()

    def test_invalid_content_type_returns_415(self, client: TestClient) -> None:
        resp = client.post(
            "/api/migrations",
            files={"file": ("jobs.txt", b"not a zip", "text/plain")},
        )
        assert resp.status_code == 415

    def test_oversized_file_returns_413(self, tmp_path: Path) -> None:
        app = create_app(work_dir=str(tmp_path / "work"), max_upload_mb=1)
        small_client = TestClient(app)
        big_zip = _make_zip({"big.sas": "x" * (2 * 1024 * 1024)})
        resp = small_client.post(
            "/api/migrations",
            files={"file": ("big.zip", big_zip, "application/zip")},
        )
        assert resp.status_code == 413

    def test_zip_saved_to_disk(self, client: TestClient, tmp_path: Path) -> None:
        resp = client.post(
            "/api/migrations",
            files={"file": ("jobs.zip", _SIMPLE_ZIP, "application/zip")},
        )
        migration_id = resp.json()["migration_id"]
        zip_path = tmp_path / "work" / "migrations" / migration_id / "upload.zip"
        assert zip_path.exists()

    def test_custom_config_stored(self, client: TestClient, tmp_path: Path) -> None:
        resp = client.post(
            "/api/migrations",
            files={"file": ("jobs.zip", _SIMPLE_ZIP, "application/zip")},
            data={"catalog": "dev", "db_schema": "test"},
        )
        migration_id = resp.json()["migration_id"]
        meta_path = tmp_path / "work" / "migrations" / migration_id / "meta.json"
        meta = json.loads(meta_path.read_text())
        assert meta["config"]["catalog"] == "dev"
        assert meta["config"]["schema"] == "test"


# ---------------------------------------------------------------------------
# GET /api/migrations/
# ---------------------------------------------------------------------------


class TestListMigrations:
    def test_empty_returns_empty_list(self, client: TestClient) -> None:
        resp = client.get("/api/migrations/")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_lists_after_create(self, client: TestClient) -> None:
        client.post(
            "/api/migrations",
            files={"file": ("jobs.zip", _SIMPLE_ZIP, "application/zip")},
        )
        resp = client.get("/api/migrations/")
        assert resp.status_code == 200
        assert len(resp.json()) == 1

    def test_each_entry_has_required_fields(self, client: TestClient) -> None:
        client.post(
            "/api/migrations",
            files={"file": ("jobs.zip", _SIMPLE_ZIP, "application/zip")},
        )
        entries = client.get("/api/migrations/").json()
        for e in entries:
            assert "migration_id" in e
            assert "status" in e
            assert "created_at" in e

    def test_multiple_migrations_listed(self, client: TestClient) -> None:
        for _ in range(3):
            client.post(
                "/api/migrations",
                files={"file": ("jobs.zip", _SIMPLE_ZIP, "application/zip")},
            )
        assert len(client.get("/api/migrations/").json()) == 3


# ---------------------------------------------------------------------------
# GET /api/migrations/{migration_id}
# ---------------------------------------------------------------------------


class TestGetMigrationStatus:
    def _create(self, client: TestClient) -> str:
        resp = client.post(
            "/api/migrations",
            files={"file": ("jobs.zip", _SIMPLE_ZIP, "application/zip")},
        )
        return resp.json()["migration_id"]

    def test_returns_200(self, client: TestClient) -> None:
        mid = self._create(client)
        assert client.get(f"/api/migrations/{mid}").status_code == 200

    def test_status_is_pending(self, client: TestClient) -> None:
        mid = self._create(client)
        data = client.get(f"/api/migrations/{mid}").json()
        assert data["status"] == "pending"

    def test_has_progress_fields(self, client: TestClient) -> None:
        mid = self._create(client)
        data = client.get(f"/api/migrations/{mid}").json()
        p = data["progress"]
        for key in ("total", "done", "failed", "pending", "in_progress"):
            assert key in p

    def test_has_jobs_list(self, client: TestClient) -> None:
        mid = self._create(client)
        data = client.get(f"/api/migrations/{mid}").json()
        assert isinstance(data["jobs"], list)

    def test_invalid_uuid_returns_422(self, client: TestClient) -> None:
        resp = client.get("/api/migrations/not-a-uuid")
        assert resp.status_code == 422

    def test_nonexistent_uuid_returns_404(self, client: TestClient) -> None:
        resp = client.get("/api/migrations/00000000-0000-0000-0000-000000000000")
        assert resp.status_code == 404

    def test_migration_id_in_response(self, client: TestClient) -> None:
        mid = self._create(client)
        data = client.get(f"/api/migrations/{mid}").json()
        assert data["migration_id"] == mid

    def test_created_at_in_response(self, client: TestClient) -> None:
        mid = self._create(client)
        data = client.get(f"/api/migrations/{mid}").json()
        assert "created_at" in data


# ---------------------------------------------------------------------------
# Story 7.3 — Endpoints de resultado
# ---------------------------------------------------------------------------


def _create_done(client: TestClient, tmp_path: Path) -> str:
    """Cria migração e força status=done no meta.json."""
    resp = client.post(
        "/api/migrations",
        files={"file": ("jobs.zip", _SIMPLE_ZIP, "application/zip")},
    )
    mid = resp.json()["migration_id"]
    meta_path = tmp_path / "work" / "migrations" / mid / "meta.json"
    meta = json.loads(meta_path.read_text())
    meta["status"] = "done"
    meta_path.write_text(json.dumps(meta))
    return mid


def _seed_artifacts(tmp_path: Path, mid: str) -> None:
    """Popula output/ e docs/ com artefatos simulados de pipeline concluído."""
    base = tmp_path / "work" / "migrations" / mid
    (base / "output").mkdir(parents=True, exist_ok=True)
    (base / "docs" / "jobs").mkdir(parents=True, exist_ok=True)

    (base / "output" / "job_a.py").write_text("# notebook job_a\nprint('ok')")
    (base / "docs" / "ARCHITECTURE.md").write_text("# Architecture")
    (base / "docs" / "jobs" / "job_a.md").write_text("# job_a\nDoc aqui.")
    (base / "explorer.html").write_text("<html><body>explorer</body></html>")

    # state.json com job concluído
    state = {"jobs": {"job_a": {"status": "done", "confidence": 0.95}}}
    (base / "output" / ".sas2dbx_state.json").write_text(json.dumps(state))


class TestResults:
    def test_returns_409_when_not_done(self, client: TestClient) -> None:
        resp = client.post(
            "/api/migrations",
            files={"file": ("jobs.zip", _SIMPLE_ZIP, "application/zip")},
        )
        mid = resp.json()["migration_id"]
        assert client.get(f"/api/migrations/{mid}/results").status_code == 409

    def test_returns_404_for_unknown(self, client: TestClient) -> None:
        assert client.get(
            "/api/migrations/00000000-0000-0000-0000-000000000000/results"
        ).status_code == 404

    def test_returns_200_when_done(self, client: TestClient, tmp_path: Path) -> None:
        mid = _create_done(client, tmp_path)
        assert client.get(f"/api/migrations/{mid}/results").status_code == 200

    def test_response_has_required_fields(self, client: TestClient, tmp_path: Path) -> None:
        mid = _create_done(client, tmp_path)
        _seed_artifacts(tmp_path, mid)
        data = client.get(f"/api/migrations/{mid}/results").json()
        assert data["migration_id"] == mid
        assert data["status"] == "done"
        assert "summary" in data
        assert "jobs" in data
        assert "explorer_url" in data
        assert "download_url" in data

    def test_summary_counts_jobs(self, client: TestClient, tmp_path: Path) -> None:
        mid = _create_done(client, tmp_path)
        _seed_artifacts(tmp_path, mid)
        data = client.get(f"/api/migrations/{mid}/results").json()
        assert data["summary"]["total"] == 1
        assert data["summary"]["done"] == 1

    def test_job_has_notebook_and_doc_flags(self, client: TestClient, tmp_path: Path) -> None:
        mid = _create_done(client, tmp_path)
        _seed_artifacts(tmp_path, mid)
        data = client.get(f"/api/migrations/{mid}/results").json()
        job = data["jobs"][0]
        assert job["has_notebook"] is True
        assert job["has_doc"] is True

    def test_job_without_notebook_flags_false(
        self, client: TestClient, tmp_path: Path
    ) -> None:
        mid = _create_done(client, tmp_path)
        _seed_artifacts(tmp_path, mid)
        (tmp_path / "work" / "migrations" / mid / "output" / "job_a.py").unlink()
        data = client.get(f"/api/migrations/{mid}/results").json()
        assert data["jobs"][0]["has_notebook"] is False

    def test_explorer_url_points_to_correct_endpoint(
        self, client: TestClient, tmp_path: Path
    ) -> None:
        mid = _create_done(client, tmp_path)
        data = client.get(f"/api/migrations/{mid}/results").json()
        assert data["explorer_url"] == f"/api/migrations/{mid}/explorer"

    def test_download_url_points_to_correct_endpoint(
        self, client: TestClient, tmp_path: Path
    ) -> None:
        mid = _create_done(client, tmp_path)
        data = client.get(f"/api/migrations/{mid}/results").json()
        assert data["download_url"] == f"/api/migrations/{mid}/download"


class TestExplorer:
    def test_returns_404_when_html_missing(
        self, client: TestClient, tmp_path: Path
    ) -> None:
        mid = _create_done(client, tmp_path)
        assert client.get(f"/api/migrations/{mid}/explorer").status_code == 404

    def test_returns_200_when_html_exists(
        self, client: TestClient, tmp_path: Path
    ) -> None:
        mid = _create_done(client, tmp_path)
        _seed_artifacts(tmp_path, mid)
        resp = client.get(f"/api/migrations/{mid}/explorer")
        assert resp.status_code == 200
        assert "html" in resp.headers["content-type"]

    def test_html_content_served(self, client: TestClient, tmp_path: Path) -> None:
        mid = _create_done(client, tmp_path)
        _seed_artifacts(tmp_path, mid)
        resp = client.get(f"/api/migrations/{mid}/explorer")
        assert "explorer" in resp.text

    def test_returns_404_for_unknown(self, client: TestClient) -> None:
        assert client.get(
            "/api/migrations/00000000-0000-0000-0000-000000000000/explorer"
        ).status_code == 404


class TestDownload:
    def test_returns_409_when_not_done(self, client: TestClient) -> None:
        resp = client.post(
            "/api/migrations",
            files={"file": ("jobs.zip", _SIMPLE_ZIP, "application/zip")},
        )
        mid = resp.json()["migration_id"]
        assert client.get(f"/api/migrations/{mid}/download").status_code == 409

    def test_returns_404_for_unknown(self, client: TestClient) -> None:
        assert client.get(
            "/api/migrations/00000000-0000-0000-0000-000000000000/download"
        ).status_code == 404

    def test_returns_200_when_done(self, client: TestClient, tmp_path: Path) -> None:
        mid = _create_done(client, tmp_path)
        assert client.get(f"/api/migrations/{mid}/download").status_code == 200

    def test_content_type_is_zip(self, client: TestClient, tmp_path: Path) -> None:
        mid = _create_done(client, tmp_path)
        resp = client.get(f"/api/migrations/{mid}/download")
        assert resp.headers["content-type"] == "application/zip"

    def test_content_disposition_attachment(
        self, client: TestClient, tmp_path: Path
    ) -> None:
        mid = _create_done(client, tmp_path)
        resp = client.get(f"/api/migrations/{mid}/download")
        assert "attachment" in resp.headers["content-disposition"]
        assert ".zip" in resp.headers["content-disposition"]

    def test_zip_contains_notebook(self, client: TestClient, tmp_path: Path) -> None:
        mid = _create_done(client, tmp_path)
        _seed_artifacts(tmp_path, mid)
        resp = client.get(f"/api/migrations/{mid}/download")
        zf = zipfile.ZipFile(io.BytesIO(resp.content))
        names = zf.namelist()
        assert any(n.startswith("output/") and n.endswith(".py") for n in names)

    def test_zip_contains_docs(self, client: TestClient, tmp_path: Path) -> None:
        mid = _create_done(client, tmp_path)
        _seed_artifacts(tmp_path, mid)
        resp = client.get(f"/api/migrations/{mid}/download")
        zf = zipfile.ZipFile(io.BytesIO(resp.content))
        names = zf.namelist()
        assert any(n.startswith("docs/") and n.endswith(".md") for n in names)

    def test_zip_contains_explorer_html(self, client: TestClient, tmp_path: Path) -> None:
        mid = _create_done(client, tmp_path)
        _seed_artifacts(tmp_path, mid)
        resp = client.get(f"/api/migrations/{mid}/download")
        zf = zipfile.ZipFile(io.BytesIO(resp.content))
        assert "explorer.html" in zf.namelist()

    def test_zip_valid_even_without_artifacts(
        self, client: TestClient, tmp_path: Path
    ) -> None:
        """Download deve retornar zip válido (possivelmente vazio) mesmo sem artefatos."""
        mid = _create_done(client, tmp_path)
        resp = client.get(f"/api/migrations/{mid}/download")
        assert resp.status_code == 200
        zf = zipfile.ZipFile(io.BytesIO(resp.content))
        assert isinstance(zf.namelist(), list)


# ---------------------------------------------------------------------------
# OpenAPI / docs
# ---------------------------------------------------------------------------


class TestApiDocs:
    def test_openapi_json_available(self, client: TestClient) -> None:
        resp = client.get("/api/openapi.json")
        assert resp.status_code == 200

    def test_docs_available(self, client: TestClient) -> None:
        resp = client.get("/api/docs")
        assert resp.status_code == 200
