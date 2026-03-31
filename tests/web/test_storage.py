"""Testes para sas2dbx/web/storage.py — MigrationStorage."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from sas2dbx.web.storage import MigrationNotFoundError, MigrationStorage


@pytest.fixture()
def storage(tmp_path: Path) -> MigrationStorage:
    return MigrationStorage(tmp_path / "work")


@pytest.fixture()
def migration_id(storage: MigrationStorage) -> str:
    storage.create_migration("abc-123", config={"catalog": "main"})
    return "abc-123"


# ---------------------------------------------------------------------------
# create_migration
# ---------------------------------------------------------------------------


class TestCreateMigration:
    def test_creates_subdirectories(self, storage: MigrationStorage, tmp_path: Path) -> None:
        migration_dir = storage.create_migration("m1", config={})
        assert (migration_dir / "input").is_dir()
        assert (migration_dir / "output").is_dir()
        assert (migration_dir / "docs").is_dir()

    def test_creates_meta_json(self, storage: MigrationStorage) -> None:
        migration_dir = storage.create_migration("m1", config={"catalog": "main"})
        meta_path = migration_dir / "meta.json"
        assert meta_path.exists()
        meta = json.loads(meta_path.read_text())
        assert meta["migration_id"] == "m1"
        assert meta["status"] == "pending"
        assert meta["config"]["catalog"] == "main"

    def test_initial_status_is_pending(self, storage: MigrationStorage) -> None:
        storage.create_migration("m1", config={})
        meta = storage.get_meta("m1")
        assert meta["status"] == "pending"

    def test_returns_migration_dir_path(self, storage: MigrationStorage, tmp_path: Path) -> None:
        migration_dir = storage.create_migration("m1", config={})
        assert migration_dir.exists()
        assert migration_dir.name == "m1"


# ---------------------------------------------------------------------------
# get_migration_dir
# ---------------------------------------------------------------------------


class TestGetMigrationDir:
    def test_returns_path_for_existing(
        self, storage: MigrationStorage, migration_id: str
    ) -> None:
        d = storage.get_migration_dir(migration_id)
        assert d.exists()

    def test_raises_for_missing(self, storage: MigrationStorage) -> None:
        with pytest.raises(MigrationNotFoundError):
            storage.get_migration_dir("nonexistent")


# ---------------------------------------------------------------------------
# get_meta / save_meta / update_status
# ---------------------------------------------------------------------------


class TestMeta:
    def test_get_meta_returns_dict(
        self, storage: MigrationStorage, migration_id: str
    ) -> None:
        meta = storage.get_meta(migration_id)
        assert isinstance(meta, dict)
        assert "migration_id" in meta

    def test_get_meta_raises_for_missing(self, storage: MigrationStorage) -> None:
        with pytest.raises(MigrationNotFoundError):
            storage.get_meta("nonexistent")

    def test_save_meta_roundtrip(
        self, storage: MigrationStorage, migration_id: str
    ) -> None:
        meta = storage.get_meta(migration_id)
        meta["status"] = "processing"
        storage.save_meta(migration_id, meta)
        assert storage.get_meta(migration_id)["status"] == "processing"

    def test_meta_json_is_atomic(
        self, storage: MigrationStorage, migration_id: str
    ) -> None:
        """save_meta não deve deixar arquivo .tmp após escrita."""
        storage.save_meta(migration_id, storage.get_meta(migration_id))
        migration_dir = storage.get_migration_dir(migration_id)
        assert not (migration_dir / "meta.tmp").exists()

    def test_update_status_done(
        self, storage: MigrationStorage, migration_id: str
    ) -> None:
        storage.update_status(migration_id, "done")
        assert storage.get_meta(migration_id)["status"] == "done"

    def test_update_status_failed_with_error(
        self, storage: MigrationStorage, migration_id: str
    ) -> None:
        storage.update_status(migration_id, "failed", error="unzip failed")
        meta = storage.get_meta(migration_id)
        assert meta["status"] == "failed"
        assert meta["error"] == "unzip failed"


# ---------------------------------------------------------------------------
# get_status
# ---------------------------------------------------------------------------


class TestGetStatus:
    def test_status_pending_without_state_json(
        self, storage: MigrationStorage, migration_id: str
    ) -> None:
        data = storage.get_status(migration_id)
        assert data["status"] == "pending"
        assert data["progress"]["total"] == 0
        assert data["jobs"] == []

    def test_status_reads_state_json(
        self, storage: MigrationStorage, migration_id: str
    ) -> None:
        migration_dir = storage.get_migration_dir(migration_id)
        state = {
            "version": "1.0",
            "jobs": {
                "job_a": {"status": "done", "confidence": 0.92},
                "job_b": {"status": "failed"},
                "job_c": {"status": "pending"},
            },
        }
        state_path = migration_dir / "output" / ".sas2dbx_state.json"
        state_path.write_text(json.dumps(state), encoding="utf-8")

        data = storage.get_status(migration_id)
        assert data["progress"]["total"] == 3
        assert data["progress"]["done"] == 1
        assert data["progress"]["failed"] == 1
        assert data["progress"]["pending"] == 1

    def test_status_job_list(
        self, storage: MigrationStorage, migration_id: str
    ) -> None:
        migration_dir = storage.get_migration_dir(migration_id)
        state = {
            "jobs": {"job_a": {"status": "done", "confidence": 0.9}},
        }
        (migration_dir / "output" / ".sas2dbx_state.json").write_text(
            json.dumps(state), encoding="utf-8"
        )
        data = storage.get_status(migration_id)
        assert len(data["jobs"]) == 1
        assert data["jobs"][0]["job_id"] == "job_a"
        assert data["jobs"][0]["confidence"] == 0.9

    def test_status_raises_for_missing(self, storage: MigrationStorage) -> None:
        with pytest.raises(MigrationNotFoundError):
            storage.get_status("nonexistent")

    def test_corrupted_state_json_is_tolerated(
        self, storage: MigrationStorage, migration_id: str
    ) -> None:
        migration_dir = storage.get_migration_dir(migration_id)
        (migration_dir / "output" / ".sas2dbx_state.json").write_text(
            "NOT JSON", encoding="utf-8"
        )
        data = storage.get_status(migration_id)
        assert data["progress"]["total"] == 0


# ---------------------------------------------------------------------------
# list_migrations
# ---------------------------------------------------------------------------


class TestListMigrations:
    def test_empty_returns_empty_list(self, storage: MigrationStorage) -> None:
        assert storage.list_migrations() == []

    def test_lists_existing_migrations(self, storage: MigrationStorage) -> None:
        storage.create_migration("m1", config={})
        storage.create_migration("m2", config={})
        result = storage.list_migrations()
        ids = [m["migration_id"] for m in result]
        assert "m1" in ids
        assert "m2" in ids

    def test_sorted_most_recent_first(self, storage: MigrationStorage) -> None:
        import time
        storage.create_migration("m1", config={})
        time.sleep(0.01)  # garante created_at diferente
        storage.create_migration("m2", config={})
        result = storage.list_migrations()
        assert result[0]["migration_id"] == "m2"

    def test_each_entry_has_required_keys(self, storage: MigrationStorage) -> None:
        storage.create_migration("m1", config={})
        result = storage.list_migrations()
        for m in result:
            assert "migration_id" in m
            assert "status" in m
            assert "created_at" in m

    def test_skips_corrupt_meta(self, storage: MigrationStorage, tmp_path: Path) -> None:
        storage.create_migration("m1", config={})
        # Cria diretório com meta.json corrompido
        bad_dir = storage._migration_dir("bad")
        bad_dir.mkdir(parents=True)
        (bad_dir / "meta.json").write_text("INVALID", encoding="utf-8")
        result = storage.list_migrations()
        ids = [m["migration_id"] for m in result]
        assert "bad" not in ids
        assert "m1" in ids
