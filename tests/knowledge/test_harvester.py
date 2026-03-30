"""Testes para KnowledgeHarvester — modo offline-first e dispatch por fonte."""

import logging
from pathlib import Path

import pytest
import yaml

from sas2dbx.knowledge.populate.harvester import (
    HarvestMode,
    KnowledgeHarvester,
    _has_files,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def harvester(tmp_path: Path) -> KnowledgeHarvester:
    return KnowledgeHarvester(base_path=tmp_path)


@pytest.fixture
def sas_input_with_file(tmp_path: Path) -> Path:
    """raw_input/sas/ com um arquivo HTML de exemplo."""
    sas_dir = tmp_path / "raw_input" / "sas"
    sas_dir.mkdir(parents=True)
    (sas_dir / "proc_sort.html").write_text("<html>PROC SORT</html>", encoding="utf-8")
    return tmp_path


# ---------------------------------------------------------------------------
# HarvestMode — offline é o default
# ---------------------------------------------------------------------------

class TestHarvestMode:
    def test_offline_is_default_mode(self) -> None:
        """HarvestMode.OFFLINE deve ser o valor padrão."""
        assert HarvestMode.OFFLINE == "offline"

    def test_harvest_sas_offline_by_default(self, harvester: KnowledgeHarvester, tmp_path: Path) -> None:
        """harvest_sas() sem mode arg usa OFFLINE."""
        import inspect
        sig = inspect.signature(harvester.harvest_sas)
        default_mode = sig.parameters["mode"].default
        assert default_mode == HarvestMode.OFFLINE

    def test_harvest_method_offline_by_default(self, harvester: KnowledgeHarvester) -> None:
        """harvest() sem mode arg usa OFFLINE."""
        import inspect
        sig = inspect.signature(harvester.harvest)
        default_mode = sig.parameters["mode"].default
        assert default_mode == HarvestMode.OFFLINE


# ---------------------------------------------------------------------------
# Dispatch por fonte — AC-1, AC-2
# ---------------------------------------------------------------------------

class TestHarvestDispatch:
    def test_dispatch_sas_offline(self, tmp_path: Path, sas_input_with_file: Path) -> None:
        """harvest('sas') com arquivos locais não levanta exceção."""
        h = KnowledgeHarvester(base_path=sas_input_with_file)
        h.harvest(source="sas", mode=HarvestMode.OFFLINE)  # must not raise

    def test_dispatch_sas_online(self, harvester: KnowledgeHarvester) -> None:
        """harvest('sas', mode=ONLINE) chama stub sem exceção."""
        harvester.harvest(source="sas", mode=HarvestMode.ONLINE)  # stub — must not raise

    def test_dispatch_pyspark(self, harvester: KnowledgeHarvester) -> None:
        harvester.harvest(source="pyspark", mode=HarvestMode.OFFLINE)

    def test_dispatch_databricks(self, harvester: KnowledgeHarvester) -> None:
        harvester.harvest(source="databricks", mode=HarvestMode.OFFLINE)

    def test_dispatch_unknown_source_raises(self, harvester: KnowledgeHarvester) -> None:
        with pytest.raises(ValueError, match="Fonte desconhecida"):
            harvester.harvest(source="unknown_source")

    def test_dispatch_custom_without_path_raises(self, harvester: KnowledgeHarvester) -> None:
        with pytest.raises(ValueError, match="--path é obrigatório"):
            harvester.harvest(source="custom")


# ---------------------------------------------------------------------------
# Modo offline — aviso quando sem arquivos — AC-1
# ---------------------------------------------------------------------------

class TestOfflineModeWarnings:
    def test_warns_when_sas_input_empty(
        self, harvester: KnowledgeHarvester, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Deve avisar (WARNING) quando raw_input/sas/ está vazio, sem exceção."""
        with caplog.at_level(logging.WARNING):
            harvester.harvest_sas(mode=HarvestMode.OFFLINE)
        assert any("nenhum arquivo encontrado" in r.message.lower() for r in caplog.records)

    def test_warns_when_pyspark_input_empty(
        self, harvester: KnowledgeHarvester, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level(logging.WARNING):
            harvester.harvest_pyspark(mode=HarvestMode.OFFLINE)
        assert any("nenhum arquivo encontrado" in r.message.lower() for r in caplog.records)

    def test_no_exception_when_input_missing(self, harvester: KnowledgeHarvester) -> None:
        """Diretório raw_input inexistente não levanta exceção — apenas avisa."""
        harvester.harvest_sas(mode=HarvestMode.OFFLINE)  # must not raise


# ---------------------------------------------------------------------------
# harvest_custom — AC-1 (custom source)
# ---------------------------------------------------------------------------

class TestHarvestCustom:
    def test_copies_yaml_file_to_custom_dir(
        self, harvester: KnowledgeHarvester, tmp_path: Path
    ) -> None:
        src = tmp_path / "client"
        src.mkdir()
        libnames = src / "libnames.yaml"
        libnames.write_text(
            yaml.dump({"SASDATA": {"catalog": "main", "schema": "raw"}}), encoding="utf-8"
        )

        harvester.harvest_custom(input_path=src)

        dest = harvester.base_path / "custom" / "libnames.yaml"
        assert dest.exists()
        data = yaml.safe_load(dest.read_text())
        assert "SASDATA" in data

    def test_copies_multiple_yaml_files(
        self, harvester: KnowledgeHarvester, tmp_path: Path
    ) -> None:
        src = tmp_path / "client"
        src.mkdir()
        (src / "libnames.yaml").write_text("SASDATA: {}", encoding="utf-8")
        (src / "macros.yaml").write_text("MACRO_SCD2: {}", encoding="utf-8")

        harvester.harvest_custom(input_path=src)

        assert (harvester.base_path / "custom" / "libnames.yaml").exists()
        assert (harvester.base_path / "custom" / "macros.yaml").exists()

    def test_copies_single_file(
        self, harvester: KnowledgeHarvester, tmp_path: Path
    ) -> None:
        single_file = tmp_path / "libnames.yaml"
        single_file.write_text("SASDATA: {}", encoding="utf-8")

        harvester.harvest_custom(input_path=single_file)

        assert (harvester.base_path / "custom" / "libnames.yaml").exists()

    def test_raises_for_nonexistent_path(self, harvester: KnowledgeHarvester) -> None:
        with pytest.raises(FileNotFoundError, match="não encontrado"):
            harvester.harvest_custom(input_path="/caminho/inexistente/")


# ---------------------------------------------------------------------------
# Helper _has_files
# ---------------------------------------------------------------------------

class TestHasFiles:
    def test_returns_true_with_file(self, tmp_path: Path) -> None:
        (tmp_path / "test.html").write_text("content")
        assert _has_files(tmp_path) is True

    def test_returns_false_for_empty_dir(self, tmp_path: Path) -> None:
        assert _has_files(tmp_path) is False

    def test_returns_false_for_nonexistent_dir(self, tmp_path: Path) -> None:
        assert _has_files(tmp_path / "nonexistent") is False
