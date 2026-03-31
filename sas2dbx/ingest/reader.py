"""Reader SAS — lê arquivos .sas e divide em blocos lógicos.

Estratégia de divisão em blocos:
  - DATA step: DATA ... RUN;
  - PROC (não-SQL): PROC ... RUN;
  - PROC SQL: PROC SQL ... QUIT;
  - %MACRO: %MACRO ... %MEND ...;
  - LIBNAME: statement de linha única terminada em ;
  - Blocos incompletos (sem terminador) são preservados com aviso.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path

from sas2dbx.models.sas_ast import SASBlock

logger = logging.getLogger(__name__)

# Encodings tentados em ordem (SAS frequentemente usa latin-1/cp1252)
_ENCODINGS = ["utf-8-sig", "utf-8", "latin-1"]

# Patterns de início de bloco
_BLOCK_START = re.compile(
    r"^\s*(DATA|PROC|LIBNAME|%MACRO)\b",
    re.IGNORECASE,
)

# Terminadores de bloco (RUN; / QUIT; em linha própria ou inline)
_BLOCK_END_RUN_QUIT = re.compile(
    r"\b(RUN|QUIT)\s*;",
    re.IGNORECASE,
)

# Terminador de %MACRO
_MACRO_END = re.compile(
    r"^\s*%MEND\b[^;]*;?",
    re.IGNORECASE,
)

# LIBNAME completo em uma linha (termina com ;)
_LIBNAME_COMPLETE = re.compile(
    r"^\s*LIBNAME\s+\w+[^;]*;",
    re.IGNORECASE,
)


def read_sas_file(path: str | Path, encoding: str | None = None) -> tuple[str, str]:
    """Lê um arquivo SAS, retornando (code, encoding_usado).

    Tenta os encodings em ordem: utf-8-sig, utf-8, latin-1.
    latin-1 sempre terá sucesso pois aceita todos os bytes 0x00-0xFF.

    Args:
        path: Caminho para o arquivo .sas.
        encoding: Encoding forçado. Se None, detecta automaticamente.

    Returns:
        Tupla (code: str, encoding: str).

    Raises:
        FileNotFoundError: Se o arquivo não existe.
    """
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {p}")

    if encoding:
        code = p.read_text(encoding=encoding)
        logger.debug("Reader: leu %s com encoding=%s (%d chars)", p.name, encoding, len(code))
        return code, encoding

    for enc in _ENCODINGS:
        try:
            code = p.read_text(encoding=enc)
            logger.debug("Reader: leu %s com encoding=%s (%d chars)", p.name, enc, len(code))
            return code, enc
        except UnicodeDecodeError:
            continue

    # Fallback garantido: latin-1 nunca falha
    code = p.read_text(encoding="latin-1", errors="replace")
    logger.warning("Reader: encoding ambíguo em %s — usando latin-1 com replace", p.name)
    return code, "latin-1"


def split_blocks(code: str, source_file: Path | None = None) -> list[SASBlock]:
    """Divide código SAS em blocos lógicos.

    Um bloco começa em DATA/PROC/LIBNAME/%MACRO e termina em RUN;/QUIT;/%MEND.
    Blocos sem terminador explícito (fim de arquivo) são preservados com aviso.

    Args:
        code: Código SAS completo.
        source_file: Arquivo de origem para rastreabilidade.

    Returns:
        Lista de SASBlock na ordem em que aparecem no código.
    """
    lines = code.splitlines()
    blocks: list[SASBlock] = []
    current_lines: list[str] = []
    block_start_line = 0
    in_block = False
    in_macro = False

    for i, line in enumerate(lines, 1):
        stripped = line.strip()

        # Linhas em branco — comentários SAS (* ...; e /* ... */) não são filtrados
        # aqui intencionalmente: podem conter terminadores que afetam o estado.
        if not stripped:
            if in_block:
                current_lines.append(line)
            continue

        if not in_block:
            if _BLOCK_START.match(stripped):
                in_block = True
                in_macro = bool(re.match(r"^\s*%MACRO\b", stripped, re.IGNORECASE))
                block_start_line = i
                current_lines = [line]

                # LIBNAME de linha única termina imediatamente
                if _LIBNAME_COMPLETE.match(stripped):
                    blocks.append(_make_block(current_lines, block_start_line, i, source_file))
                    current_lines = []
                    in_block = False
                # DATA/PROC de linha única (ex: "DATA x; SET y; RUN;")
                elif not in_macro and _BLOCK_END_RUN_QUIT.search(stripped):
                    blocks.append(_make_block(current_lines, block_start_line, i, source_file))
                    current_lines = []
                    in_block = False
        else:
            current_lines.append(line)

            # Detecta fim de bloco
            if in_macro:
                if _MACRO_END.match(stripped):
                    blocks.append(_make_block(current_lines, block_start_line, i, source_file))
                    current_lines = []
                    in_block = False
                    in_macro = False
            else:
                if _BLOCK_END_RUN_QUIT.search(stripped):
                    blocks.append(_make_block(current_lines, block_start_line, i, source_file))
                    current_lines = []
                    in_block = False

    # Bloco incompleto no final do arquivo
    if current_lines:
        logger.warning(
            "Reader: bloco sem terminador (linha %d–%d) em %s — preservado como incompleto",
            block_start_line,
            len(lines),
            source_file or "<string>",
        )
        blocks.append(_make_block(current_lines, block_start_line, len(lines), source_file))

    logger.debug(
        "Reader: %d bloco(s) extraídos de %s",
        len(blocks),
        source_file or "<string>",
    )
    return blocks


def _make_block(
    lines: list[str],
    start: int,
    end: int,
    source_file: Path | None,
) -> SASBlock:
    return SASBlock(
        raw_code="\n".join(lines),
        start_line=start,
        end_line=end,
        source_file=source_file,
    )
