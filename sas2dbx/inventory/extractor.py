"""TableExtractor — extrai referências a tabelas do código SAS.

Consome list[SASBlock] (saída de split_blocks()), não parseia SAS raw.
Classifica cada referência como SOURCE, TARGET, INTERMEDIATE ou UNRESOLVED.

Fluxo:
  extract(blocks) → entries com role=SOURCE/TARGET (antes de mapping)
  map_libnames(entries, libname_map) → resolve table_fqn
  classify_roles(entries) → promove SOURCE+TARGET → INTERMEDIATE
"""

from __future__ import annotations

import logging
import re
from pathlib import Path

from sas2dbx.inventory import InventoryEntry
from sas2dbx.models.sas_ast import SASBlock

logger = logging.getLogger(__name__)

# Pattern para extrair TODOS os datasets de uma cláusula MERGE:
# MERGE captura tudo entre MERGE e (;|BY|RUN), depois extrai cada lib.table.
_MERGE_STMT = re.compile(r"\bMERGE\b(.*?)(?:;|\bBY\b|\bRUN\b)", re.IGNORECASE | re.DOTALL)
_LIBNAME_TABLE = re.compile(r"\b([\w]+\.[\w]+)\b")

# Patterns por tipo de bloco SAS.
# Cada entry: (pattern, role, context_label)
# FROM/JOIN exigem ponto ([\w]+\.[\w]+) para evitar subqueries e palavras reservadas.
_PATTERNS: dict[str, list[tuple[re.Pattern, str, str]]] = {
    "DATA": [
        (re.compile(r"^\s*DATA\s+([\w]+\.[\w]+)\s*[;(]", re.IGNORECASE | re.MULTILINE), "TARGET", "DATA"),
        (re.compile(r"\bSET\s+([\w]+\.[\w]+)", re.IGNORECASE), "SOURCE", "SET"),
        # MERGE usa extração especial (_extract_merge_tables) — não incluído aqui
    ],
    "PROC": [
        # DATA= e OUT= podem aparecer em qualquer PROC
        (re.compile(r"\bDATA\s*=\s*([\w]+\.[\w]+)", re.IGNORECASE), "SOURCE", "DATA="),
        (re.compile(r"\bOUT\s*=\s*([\w]+\.[\w]+)", re.IGNORECASE), "TARGET", "OUT="),
        # SQL patterns — FROM/JOIN exigem ponto para distinguir de palavras-chave
        (re.compile(r"\bFROM\s+([\w]+\.[\w]+)", re.IGNORECASE), "SOURCE", "FROM"),
        (re.compile(r"\bJOIN\s+([\w]+\.[\w]+)", re.IGNORECASE), "SOURCE", "JOIN"),
        (re.compile(r"\bCREATE\s+TABLE\s+([\w.]+)", re.IGNORECASE), "TARGET", "CREATE TABLE"),
        (re.compile(r"\bINTO\s+([\w.]+)", re.IGNORECASE), "TARGET", "INTO"),
    ],
    "%MACRO": [
        # Dentro de macros: mesmos patterns SQL
        (re.compile(r"\bFROM\s+([\w]+\.[\w]+)", re.IGNORECASE), "SOURCE", "FROM"),
        (re.compile(r"\bJOIN\s+([\w]+\.[\w]+)", re.IGNORECASE), "SOURCE", "JOIN"),
        (re.compile(r"\bCREATE\s+TABLE\s+([\w.]+)", re.IGNORECASE), "TARGET", "CREATE TABLE"),
        (re.compile(r"\bSET\s+([\w]+\.[\w]+)", re.IGNORECASE), "SOURCE", "SET"),
        (re.compile(r"\bDATA\s*=\s*([\w]+\.[\w]+)", re.IGNORECASE), "SOURCE", "DATA="),
    ],
    "LIBNAME": [],  # LIBNAME não referencia tabelas diretamente
}

# Blocos sem tipo explícito — trata como PROC por default
_DEFAULT_PATTERNS = _PATTERNS["PROC"]

# SAS work library — tabelas temporárias, ignorar
_IGNORE_LIBNAMES = frozenset({"WORK", "_WEBOUT_", "SASHELP", "SASUSER"})


def _block_type(block: SASBlock) -> str:
    """Detecta o tipo do bloco pelo início do raw_code."""
    stripped = block.raw_code.strip().upper()
    for key in ("DATA", "PROC", "%MACRO", "LIBNAME"):
        if stripped.startswith(key):
            return key
    return "PROC"  # fallback


def _split_libname_table(ref: str) -> tuple[str, str]:
    """Separa 'LIBNAME.tabela' em (libname, tabela_short).

    Para referências sem ponto, retorna ('', ref).
    Para 'a.b.c', retorna ('a', 'c') — ignora schema intermediário.
    """
    parts = ref.split(".")
    if len(parts) == 1:
        return "", parts[0].lower()
    return parts[0].upper(), parts[-1].lower()


def _extract_merge_tables(raw_code: str) -> list[str]:
    """Extrai TODOS os datasets de cláusulas MERGE no código SAS.

    MERGE aceita N datasets separados por espaço: MERGE TELCO.a STAGING.b;
    O pattern genérico só captura o primeiro — este helper captura todos.
    """
    refs: list[str] = []
    for merge_m in _MERGE_STMT.finditer(raw_code):
        body = merge_m.group(1)
        for tref_m in _LIBNAME_TABLE.finditer(body):
            refs.append(tref_m.group(1))
    return refs


class TableExtractor:
    """Extrai referências a tabelas de uma lista de SASBlock."""

    def extract(
        self,
        blocks: list[SASBlock],
        source_file: str | Path = "",
    ) -> list[InventoryEntry]:
        """Extrai todas as referências a tabelas dos blocos SAS.

        Não aplica LIBNAME mapping — as entries terão role=SOURCE/TARGET mas
        table_fqn=None até que map_libnames() seja chamado.

        Args:
            blocks: Blocos SAS (saída de split_blocks()).
            source_file: Nome do arquivo de origem para rastreabilidade.

        Returns:
            Lista de InventoryEntry (pode ter duplicatas de tabela — normalizar depois).
        """
        job_name = Path(source_file).stem if source_file else "<unknown>"
        entries: list[InventoryEntry] = []
        seen: dict[tuple[str, str, str], InventoryEntry] = {}  # (libname, table_short, role) → entry

        for block in blocks:
            btype = _block_type(block)
            patterns = _PATTERNS.get(btype, _DEFAULT_PATTERNS)

            for pattern, role, context in patterns:
                for m in pattern.finditer(block.raw_code):
                    ref = m.group(1).strip()
                    self._add_ref(ref, role, context, job_name, seen, entries)

            # MERGE especial: pode ter N datasets na mesma cláusula
            if btype == "DATA":
                for ref in _extract_merge_tables(block.raw_code):
                    self._add_ref(ref, "SOURCE", "MERGE", job_name, seen, entries)

        logger.debug(
            "TableExtractor: %d referência(s) extraídas de %s (%d bloco(s))",
            len(entries), job_name, len(blocks),
        )
        return entries

    def _add_ref(
        self,
        ref: str,
        role: str,
        context: str,
        job_name: str,
        seen: dict,
        entries: list,
    ) -> None:
        """Adiciona uma referência de tabela à lista de entries, com deduplicação."""
        libname, table_short = _split_libname_table(ref)
        if libname in _IGNORE_LIBNAMES:
            return
        key = (libname, table_short, role)
        if key in seen:
            if job_name not in seen[key].referenced_in:
                seen[key].referenced_in.append(job_name)
        else:
            entry = InventoryEntry(
                table_short=table_short,
                libname=libname,
                role=role,
                sas_context=context,
                referenced_in=[job_name],
            )
            seen[key] = entry
            entries.append(entry)


def map_libnames(
    entries: list[InventoryEntry],
    libname_map: dict[str, dict],
) -> list[InventoryEntry]:
    """Resolve table_fqn para cada entry usando o mapa de LIBNAMEs.

    Entries cujo LIBNAME não está no mapa recebem role=UNRESOLVED.
    Entries sem LIBNAME (referências locais sem qualificação) também ficam UNRESOLVED.

    Args:
        entries: Lista de InventoryEntry (saída de TableExtractor.extract).
        libname_map: Dict LIBNAME_UPPER → {catalog, schema} (de load_libnames()).

    Returns:
        A mesma lista com table_fqn preenchido e role=UNRESOLVED onde não resolvível.
    """
    for entry in entries:
        if not entry.libname:
            entry.role = "UNRESOLVED"
            continue

        lib_upper = entry.libname.upper()
        if lib_upper not in libname_map:
            logger.debug(
                "TableExtractor: LIBNAME '%s' não encontrado no mapa → UNRESOLVED (%s)",
                entry.libname, entry.table_short,
            )
            entry.role = "UNRESOLVED"
            continue

        lib_info = libname_map[lib_upper]
        catalog = lib_info.get("catalog", "")
        schema = lib_info.get("schema", "")
        if catalog and schema:
            entry.table_fqn = f"{catalog}.{schema}.{entry.table_short}"
        else:
            entry.role = "UNRESOLVED"

    return entries


def classify_roles(entries: list[InventoryEntry]) -> list[InventoryEntry]:
    """Promove SOURCE+TARGET para INTERMEDIATE.

    Se uma tabela (mesmo table_fqn) aparece tanto como SOURCE quanto como TARGET,
    é intermediária — criada pela pipeline e consumida internamente. Todas as entries
    dessa tabela (tanto SOURCE quanto TARGET) são marcadas como INTERMEDIATE para
    indicar ao operador que ela não precisa existir antes da transpilação.

    Tabelas INTERMEDIATE não são verificadas no Databricks pelo checker.

    IMPORTANTE: deve ser chamado APÓS map_libnames() para que table_fqn esteja resolvido.
    """
    # Tabelas que aparecem como TARGET (serão criadas pela pipeline)
    target_fqns: set[str] = {
        e.table_fqn
        for e in entries
        if e.role == "TARGET" and e.table_fqn
    }
    # Tabelas que aparecem como SOURCE (precisariam existir antes)
    source_fqns: set[str] = {
        e.table_fqn
        for e in entries
        if e.role == "SOURCE" and e.table_fqn
    }
    # Interseção: tabelas que são tanto SOURCE quanto TARGET → INTERMEDIATE
    intermediate_fqns = target_fqns & source_fqns

    for entry in entries:
        if entry.table_fqn and entry.table_fqn in intermediate_fqns:
            entry.role = "INTERMEDIATE"

    return entries
