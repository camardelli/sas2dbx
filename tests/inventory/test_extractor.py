"""Testes para inventory/extractor.py — TableExtractor, map_libnames, classify_roles."""

from __future__ import annotations

from pathlib import Path

import pytest

from sas2dbx.inventory.extractor import TableExtractor, classify_roles, map_libnames
from sas2dbx.models.sas_ast import SASBlock


# Libname map padrão para os testes
LIBNAME_MAP = {
    "TELCO": {"catalog": "telcostar", "schema": "operacional"},
    "STAGING": {"catalog": "telcostar", "schema": "staging"},
    "DW": {"catalog": "telcostar", "schema": "datawarehouse"},
}


def _block(code: str, start: int = 1) -> SASBlock:
    lines = code.splitlines()
    return SASBlock(raw_code=code, start_line=start, end_line=start + len(lines) - 1)


def _extract(code: str, source_file: str = "test_job.sas") -> list:
    blocks = [_block(code)]
    return TableExtractor().extract(blocks, source_file=source_file)


# ---------------------------------------------------------------------------
# DATA step patterns
# ---------------------------------------------------------------------------

class TestDataStep:
    def test_data_target_extracted(self) -> None:
        entries = _extract("DATA STAGING.output;\n    SET TELCO.input;\nRUN;")
        roles = {e.role for e in entries}
        assert "TARGET" in roles
        assert "SOURCE" in roles

    def test_data_set_source(self) -> None:
        entries = _extract("DATA STAGING.out; SET TELCO.vendas_raw; RUN;")
        sources = [e for e in entries if e.sas_context == "SET"]
        assert len(sources) == 1
        assert sources[0].table_short == "vendas_raw"
        assert sources[0].libname == "TELCO"

    def test_data_target_libname(self) -> None:
        entries = _extract("DATA STAGING.output; SET TELCO.input; RUN;")
        targets = [e for e in entries if e.role == "TARGET"]
        assert targets[0].libname == "STAGING"
        assert targets[0].table_short == "output"

    def test_merge_source(self) -> None:
        entries = _extract("DATA DW.out;\n    MERGE TELCO.a STAGING.b;\n    BY id;\nRUN;")
        merges = [e for e in entries if e.sas_context == "MERGE"]
        assert len(merges) == 2
        shorts = {e.table_short for e in merges}
        assert "a" in shorts
        assert "b" in shorts

    def test_work_library_ignored(self) -> None:
        entries = _extract("DATA WORK.temp; SET TELCO.input; RUN;")
        # WORK.temp deve ser ignorado
        table_shorts = {e.table_short for e in entries}
        assert "temp" not in table_shorts

    def test_no_dots_not_extracted(self) -> None:
        """Referência sem ponto (sem LIBNAME) não é extraída como FROM/JOIN."""
        entries = _extract("PROC SQL; SELECT * FROM vendas; QUIT;")
        # 'vendas' sem ponto não deve ser capturado
        assert not any(e.table_short == "vendas" for e in entries)


# ---------------------------------------------------------------------------
# PROC SQL patterns
# ---------------------------------------------------------------------------

class TestProcSQL:
    def test_from_extracted(self) -> None:
        sas = "PROC SQL;\n    SELECT * FROM TELCO.vendas_raw;\nQUIT;"
        entries = _extract(sas)
        sources = [e for e in entries if e.sas_context == "FROM"]
        assert len(sources) == 1
        assert sources[0].table_short == "vendas_raw"

    def test_join_extracted(self) -> None:
        sas = (
            "PROC SQL;\n"
            "    SELECT * FROM TELCO.vendas_raw v\n"
            "    JOIN TELCO.clientes_raw c ON v.id = c.id;\n"
            "QUIT;"
        )
        entries = _extract(sas)
        join_entries = [e for e in entries if e.sas_context == "JOIN"]
        assert len(join_entries) == 1
        assert join_entries[0].table_short == "clientes_raw"

    def test_create_table_target(self) -> None:
        sas = "PROC SQL;\n    CREATE TABLE STAGING.out AS SELECT * FROM TELCO.in;\nQUIT;"
        entries = _extract(sas)
        targets = [e for e in entries if e.role == "TARGET"]
        assert any(e.table_short == "out" for e in targets)

    def test_two_sources_one_target(self) -> None:
        sas = """PROC SQL;
            CREATE TABLE STAGING.out AS
            SELECT * FROM TELCO.vendas_raw v
            JOIN TELCO.clientes_raw c ON v.id = c.id;
        QUIT;"""
        entries = _extract(sas)
        sources = [e for e in entries if e.role == "SOURCE"]
        targets = [e for e in entries if e.role == "TARGET"]
        assert len(sources) == 2
        assert len(targets) == 1

    def test_subquery_not_extracted(self) -> None:
        """FROM sem ponto (subquery ou CTE) não deve ser extraído."""
        sas = "PROC SQL;\n    SELECT * FROM (SELECT id FROM TELCO.src) sub;\nQUIT;"
        entries = _extract(sas)
        # 'sub' sem ponto não deve aparecer
        assert not any(e.table_short == "sub" for e in entries)


# ---------------------------------------------------------------------------
# PROC com DATA= / OUT=
# ---------------------------------------------------------------------------

class TestProcDataOut:
    def test_proc_sort_data_source(self) -> None:
        sas = "PROC SORT DATA=TELCO.vendas_raw; BY id; RUN;"
        entries = _extract(sas)
        sources = [e for e in entries if e.sas_context == "DATA="]
        assert any(e.table_short == "vendas_raw" for e in sources)

    def test_proc_sort_out_target(self) -> None:
        sas = "PROC SORT DATA=TELCO.input OUT=STAGING.output; BY id; RUN;"
        entries = _extract(sas)
        targets = [e for e in entries if e.role == "TARGET"]
        assert any(e.table_short == "output" for e in targets)

    def test_nodupkey_before_data(self) -> None:
        """PROC SORT NODUPKEY DATA=... deve capturar DATA=."""
        sas = "PROC SORT NODUPKEY DATA=TELCO.clientes_raw OUT=STAGING.clientes_dedup; BY id; RUN;"
        entries = _extract(sas)
        table_shorts = {e.table_short for e in entries}
        assert "clientes_raw" in table_shorts
        assert "clientes_dedup" in table_shorts


# ---------------------------------------------------------------------------
# map_libnames
# ---------------------------------------------------------------------------

class TestMapLibnames:
    def test_known_libname_resolves_fqn(self) -> None:
        entries = _extract("DATA STAGING.out; SET TELCO.vendas_raw; RUN;")
        map_libnames(entries, LIBNAME_MAP)
        source = next(e for e in entries if e.table_short == "vendas_raw")
        assert source.table_fqn == "telcostar.operacional.vendas_raw"

    def test_unknown_libname_becomes_unresolved(self) -> None:
        entries = _extract("DATA UNKNOWN.out; SET UNKNOWN.src; RUN;")
        map_libnames(entries, LIBNAME_MAP)
        assert all(e.role == "UNRESOLVED" for e in entries)

    def test_empty_libname_becomes_unresolved(self) -> None:
        """Referência sem LIBNAME (apenas table_short) → UNRESOLVED."""
        entries = _extract("PROC SQL; SELECT * FROM TELCO.src; QUIT;")
        # Adiciona entry manual sem libname para testar
        from sas2dbx.inventory import InventoryEntry
        bare = InventoryEntry(table_short="local_table", libname="", role="SOURCE", sas_context="FROM")
        map_libnames([bare], LIBNAME_MAP)
        assert bare.role == "UNRESOLVED"


# ---------------------------------------------------------------------------
# classify_roles
# ---------------------------------------------------------------------------

class TestClassifyRoles:
    def test_source_and_target_becomes_intermediate(self) -> None:
        """Tabela que é SOURCE e TARGET → INTERMEDIATE."""
        from sas2dbx.inventory import InventoryEntry
        entries = [
            InventoryEntry(table_short="staging_out", libname="STAGING", role="TARGET",
                           sas_context="DATA", table_fqn="telcostar.staging.staging_out"),
            InventoryEntry(table_short="staging_out", libname="STAGING", role="SOURCE",
                           sas_context="SET", table_fqn="telcostar.staging.staging_out"),
        ]
        classify_roles(entries)
        assert all(e.role == "INTERMEDIATE" for e in entries)

    def test_only_source_stays_source(self) -> None:
        from sas2dbx.inventory import InventoryEntry
        entries = [
            InventoryEntry(table_short="vendas_raw", libname="TELCO", role="SOURCE",
                           sas_context="FROM", table_fqn="telcostar.operacional.vendas_raw"),
        ]
        classify_roles(entries)
        assert entries[0].role == "SOURCE"

    def test_only_target_stays_target(self) -> None:
        from sas2dbx.inventory import InventoryEntry
        entries = [
            InventoryEntry(table_short="result", libname="DW", role="TARGET",
                           sas_context="DATA", table_fqn="telcostar.datawarehouse.result"),
        ]
        classify_roles(entries)
        assert entries[0].role == "TARGET"

    def test_classify_after_mapping(self) -> None:
        """INTERMEDIATE só funciona depois do map_libnames (table_fqn resolvido)."""
        sas = (
            "DATA STAGING.inter; SET TELCO.src; RUN;\n"
            "DATA DW.final; SET STAGING.inter; RUN;"
        )
        from sas2dbx.ingest.reader import split_blocks
        blocks = split_blocks(sas)
        entries = TableExtractor().extract(blocks)
        map_libnames(entries, LIBNAME_MAP)
        classify_roles(entries)
        inter = [e for e in entries if e.table_short == "inter"]
        # STAGING.inter é TARGET no job1 e SOURCE no job2 → INTERMEDIATE
        assert any(e.role == "INTERMEDIATE" for e in inter)


# ---------------------------------------------------------------------------
# Deduplicação e rastreabilidade
# ---------------------------------------------------------------------------

class TestDeduplicationAndTraceability:
    def test_same_table_in_two_jobs_deduped(self) -> None:
        blocks1 = [_block("PROC SQL; SELECT * FROM TELCO.vendas_raw; QUIT;")]
        blocks2 = [_block("DATA STAGING.out; SET TELCO.vendas_raw; RUN;")]

        extractor = TableExtractor()
        entries1 = extractor.extract(blocks1, source_file="job1.sas")
        entries2 = extractor.extract(blocks2, source_file="job2.sas")
        all_entries = entries1 + entries2
        map_libnames(all_entries, LIBNAME_MAP)

        sources = [e for e in all_entries if e.table_short == "vendas_raw" and e.role == "SOURCE"]
        # Cada extração produz sua própria entry — referenced_in rastreia o job
        assert all("vendas_raw" in e.table_short for e in sources)

    def test_referenced_in_contains_job_name(self) -> None:
        entries = _extract("PROC SQL; SELECT * FROM TELCO.vendas_raw; QUIT;", source_file="job_107.sas")
        assert entries[0].referenced_in == ["job_107"]
