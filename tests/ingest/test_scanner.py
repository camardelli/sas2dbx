"""Testes para ingest/scanner.py."""

from pathlib import Path

import pytest

from sas2dbx.ingest.scanner import scan_directory
from sas2dbx.models.sas_ast import SASFile

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures" / "sas"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def sas_dir(tmp_path: Path) -> Path:
    """Diretório temporário com 2 arquivos .sas e 1 .txt (não-SAS)."""
    (tmp_path / "job_a.sas").write_text("DATA x; SET y; RUN;", encoding="utf-8")
    (tmp_path / "job_b.sas").write_text("PROC SQL; SELECT 1; QUIT;", encoding="utf-8")
    (tmp_path / "notas.txt").write_text("not sas", encoding="utf-8")
    return tmp_path


@pytest.fixture
def nested_sas_dir(tmp_path: Path) -> Path:
    """Diretório com subpasta contendo .sas."""
    (tmp_path / "job_root.sas").write_text("DATA x; SET y; RUN;", encoding="utf-8")
    sub = tmp_path / "macros"
    sub.mkdir()
    (sub / "macro_util.sas").write_text("%MACRO m; %MEND m;", encoding="utf-8")
    return tmp_path


# ---------------------------------------------------------------------------
# Scan básico
# ---------------------------------------------------------------------------

class TestScanDirectory:
    def test_finds_sas_files(self, sas_dir: Path) -> None:
        files = scan_directory(sas_dir)
        assert len(files) == 2

    def test_ignores_non_sas_files(self, sas_dir: Path) -> None:
        files = scan_directory(sas_dir)
        extensions = {f.path.suffix for f in files}
        assert extensions == {".sas"}

    def test_returns_sas_file_instances(self, sas_dir: Path) -> None:
        files = scan_directory(sas_dir)
        for f in files:
            assert isinstance(f, SASFile)

    def test_size_bytes_populated(self, sas_dir: Path) -> None:
        files = scan_directory(sas_dir)
        for f in files:
            assert f.size_bytes > 0

    def test_empty_dir_returns_empty_list(self, tmp_path: Path) -> None:
        files = scan_directory(tmp_path)
        assert files == []

    def test_nonexistent_path_raises(self) -> None:
        with pytest.raises(FileNotFoundError):
            scan_directory("/caminho/inexistente/xyz")

    def test_sorted_by_path(self, sas_dir: Path) -> None:
        files = scan_directory(sas_dir)
        paths = [f.path for f in files]
        assert paths == sorted(paths)


# ---------------------------------------------------------------------------
# Recursão
# ---------------------------------------------------------------------------

class TestRecursion:
    def test_recursive_finds_nested_files(self, nested_sas_dir: Path) -> None:
        files = scan_directory(nested_sas_dir, recursive=True)
        assert len(files) == 2

    def test_non_recursive_ignores_subdirs(self, nested_sas_dir: Path) -> None:
        files = scan_directory(nested_sas_dir, recursive=False)
        assert len(files) == 1
        assert files[0].path.name == "job_root.sas"


# ---------------------------------------------------------------------------
# Arquivo único
# ---------------------------------------------------------------------------

class TestSingleFile:
    def test_accepts_single_sas_file(self, sas_dir: Path) -> None:
        single = sas_dir / "job_a.sas"
        files = scan_directory(single)
        assert len(files) == 1
        assert files[0].path == single

    def test_rejects_non_sas_file(self, tmp_path: Path) -> None:
        txt = tmp_path / "file.txt"
        txt.write_text("text")
        with pytest.raises(ValueError, match=".sas"):
            scan_directory(txt)


# ---------------------------------------------------------------------------
# Fixtures reais
# ---------------------------------------------------------------------------

class TestRealFixtures:
    def test_finds_fixture_files(self) -> None:
        files = scan_directory(FIXTURES_DIR)
        # autoexec.sas + 3 jobs = 4 arquivos
        assert len(files) == 4

    def test_fixture_names(self) -> None:
        files = scan_directory(FIXTURES_DIR)
        names = {f.path.name for f in files}
        assert "job_001_carga_clientes.sas" in names
        assert "job_002_transform_vendas.sas" in names
        assert "job_003_report_mensal.sas" in names
        assert "autoexec.sas" in names
