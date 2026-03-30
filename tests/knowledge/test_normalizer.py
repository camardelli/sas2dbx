"""Testes para normalizer.build_mappings — lógica de merge curated > generated."""

from pathlib import Path

import pytest
import yaml

from sas2dbx.knowledge.populate.normalizer import build_mappings, _load_yaml, MAPPING_FILES


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def knowledge_dir(tmp_path: Path) -> Path:
    """Cria estrutura de mappings vazia."""
    (tmp_path / "mappings" / "generated").mkdir(parents=True)
    (tmp_path / "mappings" / "curated").mkdir(parents=True)
    return tmp_path


# ---------------------------------------------------------------------------
# Estrutura de diretórios
# ---------------------------------------------------------------------------

class TestDirectoryCreation:
    def test_creates_merged_dir(self, knowledge_dir: Path) -> None:
        build_mappings(base_path=knowledge_dir)
        assert (knowledge_dir / "mappings" / "merged").is_dir()

    def test_creates_all_dirs_from_scratch(self, tmp_path: Path) -> None:
        """build_mappings deve criar generated/, curated/ e merged/ se não existirem."""
        build_mappings(base_path=tmp_path)
        assert (tmp_path / "mappings" / "generated").is_dir()
        assert (tmp_path / "mappings" / "curated").is_dir()
        assert (tmp_path / "mappings" / "merged").is_dir()

    def test_creates_all_mapping_files_in_merged(self, knowledge_dir: Path) -> None:
        build_mappings(base_path=knowledge_dir)
        for filename in MAPPING_FILES:
            assert (knowledge_dir / "mappings" / "merged" / filename).exists()


# ---------------------------------------------------------------------------
# Lógica de merge — AC-6
# ---------------------------------------------------------------------------

class TestMergeLogic:
    def test_curated_overrides_generated_on_conflict(self, knowledge_dir: Path) -> None:
        """Chave em curated/ vence a mesma chave em generated/."""
        generated = knowledge_dir / "mappings" / "generated"
        curated = knowledge_dir / "mappings" / "curated"

        (generated / "functions_map.yaml").write_text(
            yaml.dump({"INTCK": {"pyspark": "months_between_gen", "confidence": 0.9}}),
            encoding="utf-8",
        )
        (curated / "functions_map.yaml").write_text(
            yaml.dump({"INTCK": {"pyspark": "months_between_curated", "confidence": 1.0}}),
            encoding="utf-8",
        )

        build_mappings(base_path=knowledge_dir)

        merged = _load_yaml(knowledge_dir / "mappings" / "merged" / "functions_map.yaml")
        assert merged["INTCK"]["pyspark"] == "months_between_curated"

    def test_generated_only_keys_present_in_merged(self, knowledge_dir: Path) -> None:
        """Chaves só em generated/ aparecem em merged/."""
        generated = knowledge_dir / "mappings" / "generated"
        (generated / "functions_map.yaml").write_text(
            yaml.dump({
                "INTCK": {"pyspark": "months_between"},
                "SUBSTR": {"pyspark": "substring"},
            }),
            encoding="utf-8",
        )

        build_mappings(base_path=knowledge_dir)

        merged = _load_yaml(knowledge_dir / "mappings" / "merged" / "functions_map.yaml")
        assert "INTCK" in merged
        assert "SUBSTR" in merged

    def test_curated_only_keys_present_in_merged(self, knowledge_dir: Path) -> None:
        """Chaves só em curated/ (não em generated/) aparecem em merged/."""
        curated = knowledge_dir / "mappings" / "curated"
        (curated / "functions_map.yaml").write_text(
            yaml.dump({"CUSTOM_FUNC": {"pyspark": "custom_pyspark", "confidence": 1.0}}),
            encoding="utf-8",
        )

        build_mappings(base_path=knowledge_dir)

        merged = _load_yaml(knowledge_dir / "mappings" / "merged" / "functions_map.yaml")
        assert "CUSTOM_FUNC" in merged

    def test_merged_contains_union_of_generated_and_curated(self, knowledge_dir: Path) -> None:
        """merged/ = union(generated, curated) com curated vencendo conflitos."""
        generated = knowledge_dir / "mappings" / "generated"
        curated = knowledge_dir / "mappings" / "curated"

        (generated / "functions_map.yaml").write_text(
            yaml.dump({
                "INTCK": {"pyspark": "gen_intck"},
                "SCAN": {"pyspark": "split"},
            }),
            encoding="utf-8",
        )
        (curated / "functions_map.yaml").write_text(
            yaml.dump({
                "INTCK": {"pyspark": "curated_intck"},
                "CATX": {"pyspark": "concat_ws"},
            }),
            encoding="utf-8",
        )

        build_mappings(base_path=knowledge_dir)

        merged = _load_yaml(knowledge_dir / "mappings" / "merged" / "functions_map.yaml")
        assert merged["INTCK"]["pyspark"] == "curated_intck"  # curated wins
        assert "SCAN" in merged                                # from generated
        assert "CATX" in merged                                # from curated


# ---------------------------------------------------------------------------
# Garantia de não sobrescrita do curated/ — AC-4
# ---------------------------------------------------------------------------

class TestCuratedNotOverwritten:
    def test_curated_unchanged_after_single_run(self, knowledge_dir: Path) -> None:
        curated = knowledge_dir / "mappings" / "curated"
        original_content = yaml.dump({"INTCK": {"pyspark": "curated_version"}})
        (curated / "functions_map.yaml").write_text(original_content, encoding="utf-8")

        build_mappings(base_path=knowledge_dir)

        assert (curated / "functions_map.yaml").read_text(encoding="utf-8") == original_content

    def test_curated_unchanged_after_multiple_runs(self, knowledge_dir: Path) -> None:
        """Executar build_mappings múltiplas vezes não altera curated/."""
        curated = knowledge_dir / "mappings" / "curated"
        original_content = yaml.dump({"INTCK": {"pyspark": "curated_version"}})
        (curated / "functions_map.yaml").write_text(original_content, encoding="utf-8")

        for _ in range(3):
            build_mappings(base_path=knowledge_dir)

        assert (curated / "functions_map.yaml").read_text(encoding="utf-8") == original_content


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

class TestEdgeCases:
    def test_empty_inputs_creates_empty_merged(self, knowledge_dir: Path) -> None:
        build_mappings(base_path=knowledge_dir)
        merged = _load_yaml(knowledge_dir / "mappings" / "merged" / "functions_map.yaml")
        assert merged == {}

    def test_returns_count_per_file(self, knowledge_dir: Path) -> None:
        generated = knowledge_dir / "mappings" / "generated"
        (generated / "functions_map.yaml").write_text(
            yaml.dump({"A": {}, "B": {}, "C": {}}),
            encoding="utf-8",
        )
        results = build_mappings(base_path=knowledge_dir)
        assert results["functions_map.yaml"] == 3

    def test_idempotent(self, knowledge_dir: Path) -> None:
        """Rodar build_mappings duas vezes produz o mesmo resultado."""
        generated = knowledge_dir / "mappings" / "generated"
        (generated / "functions_map.yaml").write_text(
            yaml.dump({"INTCK": {"pyspark": "months_between"}}),
            encoding="utf-8",
        )

        build_mappings(base_path=knowledge_dir)
        first_run = _load_yaml(knowledge_dir / "mappings" / "merged" / "functions_map.yaml")

        build_mappings(base_path=knowledge_dir)
        second_run = _load_yaml(knowledge_dir / "mappings" / "merged" / "functions_map.yaml")

        assert first_run == second_run
