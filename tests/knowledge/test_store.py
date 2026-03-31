"""Testes para KnowledgeStore — lookup e retrieval via mappings/merged/."""

from pathlib import Path

import pytest
import yaml

from sas2dbx.knowledge.store import KnowledgeStore

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def knowledge_dir(tmp_path: Path) -> Path:
    """Cria estrutura mínima de knowledge store com dados de teste."""
    # mappings/generated/
    generated = tmp_path / "mappings" / "generated"
    generated.mkdir(parents=True)
    (generated / "functions_map.yaml").write_text(
        yaml.dump({
            "INTCK": {"pyspark": "months_between_generated", "confidence": 0.9},
            "SUBSTR": {"pyspark": "substring", "confidence": 0.95},
            "SCAN": {"pyspark": "split", "confidence": 0.85},
        }),
        encoding="utf-8",
    )
    (generated / "proc_map.yaml").write_text(
        yaml.dump({"PROC_SORT": {"approach": "rule", "confidence": 1.0}}),
        encoding="utf-8",
    )

    # mappings/curated/ — INTCK tem override manual
    curated = tmp_path / "mappings" / "curated"
    curated.mkdir(parents=True)
    (curated / "functions_map.yaml").write_text(
        yaml.dump({
            "INTCK": {"pyspark": "months_between_curated", "confidence": 1.0, "curated": True},
        }),
        encoding="utf-8",
    )

    # mappings/merged/ — resultado do build-mappings (curated > generated)
    merged = tmp_path / "mappings" / "merged"
    merged.mkdir(parents=True)
    (merged / "functions_map.yaml").write_text(
        yaml.dump({
            "INTCK": {"pyspark": "months_between_curated", "confidence": 1.0, "curated": True},
            "SUBSTR": {"pyspark": "substring", "confidence": 0.95},
            "SCAN": {"pyspark": "split", "confidence": 0.85},
        }),
        encoding="utf-8",
    )
    (merged / "proc_map.yaml").write_text(
        yaml.dump({"PROC_SORT": {"approach": "rule", "confidence": 1.0}}),
        encoding="utf-8",
    )

    return tmp_path


# ---------------------------------------------------------------------------
# lookup_function
# ---------------------------------------------------------------------------

class TestLookupFunction:
    def test_returns_merged_entry(self, knowledge_dir: Path) -> None:
        """Deve retornar a entrada de merged/ (que já reflete curadoria)."""
        store = KnowledgeStore(base_path=knowledge_dir)
        result = store.lookup_function("INTCK")
        assert result is not None
        assert result["pyspark"] == "months_between_curated"

    def test_curated_takes_precedence_over_generated(self, knowledge_dir: Path) -> None:
        """Curadoria manual vence gerado automaticamente."""
        store = KnowledgeStore(base_path=knowledge_dir)
        result = store.lookup_function("INTCK")
        assert result is not None
        assert result.get("curated") is True

    def test_generated_only_key_accessible(self, knowledge_dir: Path) -> None:
        """Chave que existe só em generated/ deve ser acessível via merged/."""
        store = KnowledgeStore(base_path=knowledge_dir)
        result = store.lookup_function("SUBSTR")
        assert result is not None
        assert result["pyspark"] == "substring"

    def test_case_insensitive(self, knowledge_dir: Path) -> None:
        """Lookup deve ser case-insensitive."""
        store = KnowledgeStore(base_path=knowledge_dir)
        assert store.lookup_function("intck") == store.lookup_function("INTCK")
        assert store.lookup_function("Substr") == store.lookup_function("SUBSTR")

    def test_returns_none_for_unknown_function(self, knowledge_dir: Path) -> None:
        """Função desconhecida retorna None."""
        store = KnowledgeStore(base_path=knowledge_dir)
        assert store.lookup_function("NONEXISTENT_SAS_FUNCTION") is None

    def test_fallback_to_generated_when_no_merged(self, tmp_path: Path) -> None:
        """Sem merged/, deve cair no fallback de generated/."""
        generated = tmp_path / "mappings" / "generated"
        generated.mkdir(parents=True)
        (generated / "functions_map.yaml").write_text(
            yaml.dump({"CATX": {"pyspark": "concat_ws", "confidence": 0.95}}),
            encoding="utf-8",
        )
        # Nota: sem criar merged/
        store = KnowledgeStore(base_path=tmp_path)
        result = store.lookup_function("CATX")
        assert result is not None
        assert result["pyspark"] == "concat_ws"

    def test_returns_none_when_no_mapping_files(self, tmp_path: Path) -> None:
        """Sem arquivos de mapping, retorna None sem exceção."""
        store = KnowledgeStore(base_path=tmp_path)
        assert store.lookup_function("INTCK") is None


# ---------------------------------------------------------------------------
# lookup_proc
# ---------------------------------------------------------------------------

class TestLookupProc:
    def test_returns_proc_entry(self, knowledge_dir: Path) -> None:
        store = KnowledgeStore(base_path=knowledge_dir)
        result = store.lookup_proc("PROC_SORT")
        assert result is not None
        assert result["approach"] == "rule"

    def test_returns_none_for_unknown_proc(self, knowledge_dir: Path) -> None:
        store = KnowledgeStore(base_path=knowledge_dir)
        assert store.lookup_proc("PROC_NONEXISTENT") is None


# ---------------------------------------------------------------------------
# get_reference
# ---------------------------------------------------------------------------

class TestGetReference:
    def test_returns_content_for_existing_sas_ref(self, tmp_path: Path) -> None:
        procs_dir = tmp_path / "sas_reference" / "procs"
        procs_dir.mkdir(parents=True)
        (procs_dir / "proc_sort.md").write_text("# PROC SORT\nSorts datasets.", encoding="utf-8")

        store = KnowledgeStore(base_path=tmp_path)
        content = store.get_reference("sas", "procs", "proc_sort")
        assert content is not None
        assert "PROC SORT" in content

    def test_returns_content_for_pyspark_ref(self, tmp_path: Path) -> None:
        api_dir = tmp_path / "pyspark_reference" / "dataframe_api"
        api_dir.mkdir(parents=True)
        (api_dir / "groupby_agg.md").write_text(
            "# groupBy().agg()\nAggregations.", encoding="utf-8"
        )

        store = KnowledgeStore(base_path=tmp_path)
        content = store.get_reference("pyspark", "dataframe_api", "groupby_agg")
        assert content is not None
        assert "groupBy" in content

    def test_returns_none_for_missing_file(self, knowledge_dir: Path) -> None:
        store = KnowledgeStore(base_path=knowledge_dir)
        assert store.get_reference("sas", "procs", "proc_nonexistent") is None

    def test_returns_none_for_unknown_source(self, knowledge_dir: Path) -> None:
        store = KnowledgeStore(base_path=knowledge_dir)
        assert store.get_reference("unknown", "procs", "proc_sort") is None


# ---------------------------------------------------------------------------
# get_custom
# ---------------------------------------------------------------------------

class TestGetCustom:
    def test_returns_parsed_yaml(self, tmp_path: Path) -> None:
        custom_dir = tmp_path / "custom"
        custom_dir.mkdir()
        (custom_dir / "libnames.yaml").write_text(
            yaml.dump({"SASDATA": {"catalog": "main", "schema": "raw"}}),
            encoding="utf-8",
        )
        store = KnowledgeStore(base_path=tmp_path)
        result = store.get_custom("libnames")
        assert result == {"SASDATA": {"catalog": "main", "schema": "raw"}}

    def test_returns_empty_dict_for_missing_file(self, tmp_path: Path) -> None:
        store = KnowledgeStore(base_path=tmp_path)
        assert store.get_custom("nonexistent") == {}


# ---------------------------------------------------------------------------
# Cache em memória
# ---------------------------------------------------------------------------

class TestMappingCache:
    def test_second_lookup_uses_cache(self, knowledge_dir: Path) -> None:
        """Segunda chamada de lookup_function não relê o arquivo do disco."""
        from unittest.mock import patch

        store = KnowledgeStore(base_path=knowledge_dir)

        # Primeira chamada preenche o cache
        result1 = store.lookup_function("INTCK")

        # Patch open — não deve ser chamado na segunda vez para o mesmo arquivo
        with patch("builtins.open") as mock_open:
            result2 = store.lookup_function("INTCK")
            mock_open.assert_not_called()

        assert result1 == result2

    def test_different_keys_same_file_uses_cache(self, knowledge_dir: Path) -> None:
        """Lookup de chave diferente no mesmo arquivo usa cache (não relê disco)."""
        from unittest.mock import patch

        store = KnowledgeStore(base_path=knowledge_dir)
        # Preenche cache para o functions_map
        store.lookup_function("INTCK")

        with patch("builtins.open") as mock_open:
            store.lookup_function("SUBSTR")
            mock_open.assert_not_called()

    def test_invalidate_cache_forces_reload(self, knowledge_dir: Path) -> None:
        """invalidate_cache() faz próximo lookup reler do disco."""
        store = KnowledgeStore(base_path=knowledge_dir)
        store.lookup_function("INTCK")
        assert len(store._cache) > 0

        store.invalidate_cache()
        assert len(store._cache) == 0

    def test_invalidate_cache_returns_correct_data(self, knowledge_dir: Path) -> None:
        """Após invalidate_cache(), lookup retorna dados corretos."""
        store = KnowledgeStore(base_path=knowledge_dir)
        result_before = store.lookup_function("INTCK")
        store.invalidate_cache()
        result_after = store.lookup_function("INTCK")
        assert result_before == result_after
