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

# Keywords SAS % que NÃO são invocações de macro do usuário
_MACRO_CALL_SAS_KEYWORDS = frozenset({
    "MACRO", "MEND", "IF", "THEN", "ELSE", "DO", "END", "LET", "PUT",
    "INCLUDE", "GLOBAL", "LOCAL", "SYSFUNC", "SYSEVALF", "SYSCALL",
    "STR", "NRSTR", "QUOTE", "NRQUOTE", "BQUOTE", "NRBQUOTE",
    "SUPERQ", "UNQUOTE", "EVAL", "NREVAL", "SCAN", "SUBSTR",
    "UPCASE", "LOWCASE", "TRIM", "LEFT", "RETURN", "GOTO",
    "ABORT", "STOP", "TO", "BY", "WHILE", "UNTIL",
})

# Detecta início de invocação de macro: %name( ou %name;
_MACRO_INVOCATION_LINE = re.compile(r"^\s*%(\w+)", re.IGNORECASE)

# Detecta %INCLUDE para gerar aviso
_INCLUDE_LINE = re.compile(r"^\s*%INCLUDE\b", re.IGNORECASE)


# Bytes característicos de latin-1/cp1252 em textos brasileiros (ç, ã, é, ô, etc.)
# Presença desses bytes em arquivo que "passou" como UTF-8 indica risco de corrupção.
_LATIN1_INDICATOR_BYTES = {0xE7, 0xE3, 0xF5, 0xE9, 0xF4, 0xFA, 0xED, 0xE1, 0xE2, 0xF3}


def check_encoding_risk(path: Path, encoding_used: str) -> str | None:
    """Retorna aviso de risco de encoding se o arquivo pode ter corrupção silenciosa.

    Um arquivo lido como UTF-8 mas com bytes típicos de latin-1 (ç, ã, é, etc.)
    pode ter sido gravado em cp1252 e decodificado incorretamente, causando:
    - Falhas silenciosas em joins por diferença de encoding
    - Corrupção de nomes/cidades com acentos

    Args:
        path: Arquivo lido.
        encoding_used: Encoding que o reader usou.

    Returns:
        String de aviso ou None se sem risco detectado.
    """
    if encoding_used in ("latin-1", "latin1", "cp1252"):
        # Confirma que há de fato caracteres não-ASCII (não é só ASCII puro)
        raw = path.read_bytes()
        high_bytes = sum(1 for b in raw if b > 0x7F)
        if high_bytes > 0:
            return (
                f"Encoding latin-1 detectado com {high_bytes} byte(s) não-ASCII — "
                "risco de corrupção em joins de campos com acentos (ç, ã, é, ô). "
                "Validar integridade de caracteres especiais pós-migração."
            )
    elif encoding_used in ("utf-8", "utf-8-sig"):
        # Verifica se os bytes brutos contêm padrões típicos de latin-1 mal-decodificados
        raw = path.read_bytes()
        suspicious = sum(1 for b in raw if b in _LATIN1_INDICATOR_BYTES)
        if suspicious > 5:
            return (
                f"Arquivo lido como UTF-8 mas contém {suspicious} byte(s) típicos de latin-1 — "
                "possível encoding incorreto; verificar se dados de entrada estão em cp1252/latin-1."
            )
    return None


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
        _emit_encoding_warning(p, encoding)
        return code, encoding

    for enc in _ENCODINGS:
        try:
            code = p.read_text(encoding=enc)
            logger.debug("Reader: leu %s com encoding=%s (%d chars)", p.name, enc, len(code))
            _emit_encoding_warning(p, enc)
            return code, enc
        except UnicodeDecodeError:
            continue

    # Fallback garantido: latin-1 nunca falha
    code = p.read_text(encoding="latin-1", errors="replace")
    logger.warning("Reader: encoding ambíguo em %s — usando latin-1 com replace", p.name)
    _emit_encoding_warning(p, "latin-1")
    return code, "latin-1"


def _emit_encoding_warning(path: Path, encoding: str) -> None:
    """Emite warning de encoding se risco detectado."""
    warning = check_encoding_risk(path, encoding)
    if warning:
        logger.warning("Reader [%s]: %s", path.name, warning)


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
    in_macro_call = False  # invocação de macro standalone (não %MACRO/%MEND)

    for i, line in enumerate(lines, 1):
        stripped = line.strip()

        # Linhas em branco — comentários SAS (* ...; e /* ... */) não são filtrados
        # aqui intencionalmente: podem conter terminadores que afetam o estado.
        if not stripped:
            if in_block:
                current_lines.append(line)
            continue

        if not in_block:
            # %INCLUDE: arquivo externo não processado — gera aviso para rastreabilidade
            if _INCLUDE_LINE.match(stripped):
                logger.warning(
                    "Reader: '%s' (linha %d) — %sINCLUDE não é processado recursivamente; "
                    "dependências do arquivo incluído ficam fora do grafo",
                    source_file.name if source_file else "<string>",
                    i,
                    "%",
                )

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
                # Invocação de macro standalone: %name(...); ou %name;
                m = _MACRO_INVOCATION_LINE.match(stripped)
                if m and m.group(1).upper() not in _MACRO_CALL_SAS_KEYWORDS:
                    in_block = True
                    in_macro_call = True
                    block_start_line = i
                    current_lines = [line]
                    # Linha única: termina com ;
                    if stripped.endswith(";"):
                        blocks.append(
                            _make_block(current_lines, block_start_line, i, source_file)
                        )
                        current_lines = []
                        in_block = False
                        in_macro_call = False
        else:
            current_lines.append(line)

            # Detecta fim de bloco
            if in_macro:
                if _MACRO_END.match(stripped):
                    blocks.append(_make_block(current_lines, block_start_line, i, source_file))
                    current_lines = []
                    in_block = False
                    in_macro = False
            elif in_macro_call:
                if stripped.endswith(";"):
                    blocks.append(_make_block(current_lines, block_start_line, i, source_file))
                    current_lines = []
                    in_block = False
                    in_macro_call = False
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

    # Pós-processamento: mescla %MACRO..%MEND com a(s) invocação(ões) consecutivas.
    # Sem isso, o LLM transpila a definição e a invocação como blocos independentes —
    # a definição não sabe os valores reais dos parâmetros (ex: ds_vendas=TELCO.vendas_raw)
    # e a invocação não sabe o corpo da macro, gerando duas transpilações inconsistentes.
    blocks = _merge_macro_with_invocations(blocks)

    logger.debug(
        "Reader: %d bloco(s) extraídos de %s",
        len(blocks),
        source_file or "<string>",
    )
    return blocks


_RE_MACRO_DEF_NAME = re.compile(r"^\s*%MACRO\s+(\w+)", re.IGNORECASE)


def _extract_macro_def_name(code: str) -> str | None:
    """Retorna o nome da macro definida em um bloco %MACRO...%MEND, ou None."""
    m = _RE_MACRO_DEF_NAME.search(code)
    return m.group(1).lower() if m else None


def _is_invocation_of(code: str, macro_name: str) -> bool:
    """Retorna True se o código é uma invocação de %macro_name."""
    pattern = re.compile(
        r"^\s*%" + re.escape(macro_name) + r"\s*[\(;]",
        re.IGNORECASE,
    )
    return bool(pattern.search(code))


def _merge_macro_with_invocations(blocks: list[SASBlock]) -> list[SASBlock]:
    """Mescla bloco %MACRO...%MEND com a(s) invocação(ões) consecutivas do mesmo nome.

    Problema que resolve: quando %MACRO rfm_score...%MEND e %rfm_score(...) são
    blocos separados, o LLM transpila cada um sem contexto do outro — a definição
    ignora os valores reais dos parâmetros e a invocação ignora o corpo da macro.

    A mesclagem entrega ao LLM definição + chamada como bloco único, permitindo
    resolução completa de &ds_vendas → TELCO.vendas_raw → colunas reais.

    Regras:
    - Mescla bloco[i] (%MACRO X...%MEND) com TODOS os block[i+1], [i+2]...
      consecutivos que sejam invocações de X (suporta múltiplas chamadas no mesmo job).
    - Blocos de tipo diferente (PROC, DATA, invocação de outra macro) interrompem a mesclagem.
    - Preserva start_line do bloco de definição e end_line do último invocado.
    """
    if len(blocks) < 2:
        return blocks

    result: list[SASBlock] = []
    i = 0
    while i < len(blocks):
        block = blocks[i]
        macro_name = _extract_macro_def_name(block.raw_code)

        if macro_name is None:
            result.append(block)
            i += 1
            continue

        # Coleta todas as invocações consecutivas do mesmo macro
        merged_code = block.raw_code
        end_line = block.end_line
        j = i + 1
        while j < len(blocks) and _is_invocation_of(blocks[j].raw_code, macro_name):
            merged_code = merged_code + "\n\n" + blocks[j].raw_code
            end_line = blocks[j].end_line
            j += 1

        if j > i + 1:
            # Pelo menos uma invocação foi mesclada
            logger.debug(
                "Reader: mesclando %%%s (%d bloco(s) de invocação) em bloco único",
                macro_name,
                j - i - 1,
            )

        result.append(SASBlock(
            raw_code=merged_code,
            start_line=block.start_line,
            end_line=end_line,
            source_file=block.source_file,
        ))
        i = j

    return result


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
