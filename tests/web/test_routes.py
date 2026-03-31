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
# Placeholder endpoints (Story 7.3)
# ---------------------------------------------------------------------------


class TestPlaceholderEndpoints:
    def _create_done(self, client: TestClient, tmp_path: Path) -> str:
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

    def test_results_409_when_not_done(self, client: TestClient) -> None:
        resp = client.post(
            "/api/migrations",
            files={"file": ("jobs.zip", _SIMPLE_ZIP, "application/zip")},
        )
        mid = resp.json()["migration_id"]
        assert client.get(f"/api/migrations/{mid}/results").status_code == 409

    def test_results_200_when_done(self, client: TestClient, tmp_path: Path) -> None:
        mid = self._create_done(client, tmp_path)
        assert client.get(f"/api/migrations/{mid}/results").status_code == 200

    def test_explorer_404_when_html_missing(
        self, client: TestClient, tmp_path: Path
    ) -> None:
        mid = self._create_done(client, tmp_path)
        assert client.get(f"/api/migrations/{mid}/explorer").status_code == 404

    def test_explorer_200_when_html_exists(
        self, client: TestClient, tmp_path: Path
    ) -> None:
        mid = self._create_done(client, tmp_path)
        html_path = tmp_path / "work" / "migrations" / mid / "explorer.html"
        html_path.write_text("<html><body>ok</body></html>")
        resp = client.get(f"/api/migrations/{mid}/explorer")
        assert resp.status_code == 200
        assert "html" in resp.headers["content-type"]

    def test_download_409_when_not_done(self, client: TestClient) -> None:
        resp = client.post(
            "/api/migrations",
            files={"file": ("jobs.zip", _SIMPLE_ZIP, "application/zip")},
        )
        mid = resp.json()["migration_id"]
        assert client.get(f"/api/migrations/{mid}/download").status_code == 409


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
