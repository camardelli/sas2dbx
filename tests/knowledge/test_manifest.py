"""Testes para knowledge/manifest.py — read/write/update_from_merged."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from sas2dbx.knowledge.manifest import (
    increment_on_demand_counter,
    read_manifest,
    update_from_merged,
    write_manifest,
)


# ---------------------------------------------------------------------------
# read_manifest
# ---------------------------------------------------------------------------


class TestReadManifest:
    def test_returns_defaults_when_file_absent(self, tmp_path: Path) -> None:
        manifest = read_manifest(tmp_path)
        assert "version" in manifest
        assert "entries_per_file" in manifest
        assert manifest["avg_confidence"] == 0.0

    def test_reads_existing_manifest(self, tmp_path: Path) -> None:
        (tmp_path / "manifest.yaml").write_text(
            yaml.dump({"version": "2.0.0", "avg_confidence": 0.85}),
            encoding="utf-8",
        )
        manifest = read_manifest(tmp_path)
        assert manifest["version"] == "2.0.0"
        assert manifest["avg_confidence"] == 0.85

    def test_fills_missing_keys_with_defaults(self, tmp_path: Path) -> None:
        (tmp_path / "manifest.yaml").write_text(
            yaml.dump({"version": "1.0.0"}),
            encoding="utf-8",
        )
        manifest = read_manifest(tmp_path)
        # Campo ausente → default
        assert manifest["on_demand_harvests"] == 0

    def test_bad_yaml_returns_defaults(self, tmp_path: Path) -> None:
        (tmp_path / "manifest.yaml").write_text("{{{invalid yaml", encoding="utf-8")
        manifest = read_manifest(tmp_path)
        assert "version" in manifest


# ---------------------------------------------------------------------------
# write_manifest — M2: mkdir implícito
# ---------------------------------------------------------------------------


class TestWriteManifest:
    def test_creates_file(self, tmp_path: Path) -> None:
        write_manifest(tmp_path, {"version": "1.0.0", "avg_confidence": 0.9})
        assert (tmp_path / "manifest.yaml").exists()

    def test_roundtrip(self, tmp_path: Path) -> None:
        data = {"version": "1.0.0", "avg_confidence": 0.75, "on_demand_harvests": 3}
        write_manifest(tmp_path, data)
        result = read_manifest(tmp_path)
        assert result["avg_confidence"] == 0.75
        assert result["on_demand_harvests"] == 3

    def test_creates_base_path_if_absent(self, tmp_path: Path) -> None:
        """M2: write_manifest não deve falhar se base_path não existir ainda."""
        new_ks = tmp_path / "brand_new_ks"
        # Diretório NÃO existe — deve ser criado automaticamente
        write_manifest(new_ks, {"version": "1.0.0"})
        assert (new_ks / "manifest.yaml").exists()

    def test_atomic_write_via_tmp(self, tmp_path: Path) -> None:
        """Arquivo .tmp não deve sobrar após write_manifest."""
        write_manifest(tmp_path, {"version": "1.0.0"})
        tmp_files = list(tmp_path.glob("*.tmp"))
        assert tmp_files == []


# ---------------------------------------------------------------------------
# update_from_merged
# ---------------------------------------------------------------------------


class TestUpdateFromMerged:
    def test_counts_entries_per_file(self, tmp_path: Path) -> None:
        merged = tmp_path / "mappings" / "merged"
        merged.mkdir(parents=True)
        (merged / "functions_map.yaml").write_text(
            yaml.dump({
                "INTCK": {"pyspark": "months_between()", "notes": "ok", "confidence": 0.9},
                "CATX": {"pyspark": "concat_ws()", "notes": "ok", "confidence": 0.95},
            }),
            encoding="utf-8",
        )
        manifest = update_from_merged(tmp_path)
        assert manifest["entries_per_file"]["functions_map.yaml"] == 2

    def test_calculates_avg_confidence(self, tmp_path: Path) -> None:
        merged = tmp_path / "mappings" / "merged"
        merged.mkdir(parents=True)
        (merged / "functions_map.yaml").write_text(
            yaml.dump({
                "A": {"pyspark": "x()", "notes": "", "confidence": 0.8},
                "B": {"pyspark": "y()", "notes": "", "confidence": 1.0},
            }),
            encoding="utf-8",
        )
        manifest = update_from_merged(tmp_path)
        assert abs(manifest["avg_confidence"] - 0.9) < 1e-9

    def test_empty_merged_returns_zero_entries(self, tmp_path: Path) -> None:
        manifest = update_from_merged(tmp_path)
        assert manifest["entries_per_file"] == {}
        assert manifest["avg_confidence"] == 0.0

    def test_updates_last_updated_timestamp(self, tmp_path: Path) -> None:
        manifest = update_from_merged(tmp_path)
        assert manifest["last_updated"] is not None
        assert "T" in manifest["last_updated"]  # formato ISO 8601

    def test_persists_manifest_to_disk(self, tmp_path: Path) -> None:
        update_from_merged(tmp_path)
        assert (tmp_path / "manifest.yaml").exists()


# ---------------------------------------------------------------------------
# increment_on_demand_counter
# ---------------------------------------------------------------------------


class TestIncrementOnDemandCounter:
    def test_increments_from_zero(self, tmp_path: Path) -> None:
        increment_on_demand_counter(tmp_path)
        manifest = read_manifest(tmp_path)
        assert manifest["on_demand_harvests"] == 1

    def test_increments_additively(self, tmp_path: Path) -> None:
        increment_on_demand_counter(tmp_path)
        increment_on_demand_counter(tmp_path)
        increment_on_demand_counter(tmp_path)
        assert read_manifest(tmp_path)["on_demand_harvests"] == 3
