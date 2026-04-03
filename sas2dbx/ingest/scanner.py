"""Scanner de diretório SAS — encontra e cataloga arquivos .sas."""

from __future__ import annotations

import logging
from pathlib import Path

from sas2dbx.models.sas_ast import SASFile

logger = logging.getLogger(__name__)

# Extensões reconhecidas como código SAS
_SAS_EXTENSIONS = {".sas"}


def scan_directory(
    path: str | Path,
    recursive: bool = True,
    exclude_patterns: list[str] | None = None,
) -> list[SASFile]:
    """Escaneia um diretório e retorna lista de arquivos .sas encontrados.

    Args:
        path: Diretório ou arquivo .sas a escanear.
        recursive: Se True, busca recursivamente em subdiretórios.
        exclude_patterns: Padrões glob a excluir (ex: ["**/test/**", "**/macro_*"]).

    Returns:
        Lista de SASFile ordenada por path, vazia se nenhum arquivo encontrado.

    Raises:
        FileNotFoundError: Se o path não existe.
        NotADirectoryError: Se path não é diretório nem arquivo .sas.
    """
    root = Path(path).resolve()
    exclude_patterns = exclude_patterns or []

    if not root.exists():
        raise FileNotFoundError(f"Path não encontrado: {root}")

    # Aceita arquivo único .sas diretamente
    if root.is_file():
        if root.suffix.lower() not in _SAS_EXTENSIONS:
            raise ValueError(f"Arquivo não é .sas: {root}")
        return [_make_sas_file(root)]

    if not root.is_dir():
        raise NotADirectoryError(f"Path não é diretório: {root}")

    glob_pattern = "**/*.sas" if recursive else "*.sas"
    candidates = sorted(root.glob(glob_pattern))

    sas_files: list[SASFile] = []
    for candidate in candidates:
        if not candidate.is_file():
            continue
        # Ignora resource forks do macOS (._filename)
        if candidate.name.startswith("._"):
            logger.debug("Scanner: ignorando resource fork macOS %s", candidate.name)
            continue
        # Ignora arquivos autoexec* — são configuração de ambiente, não jobs executáveis
        if candidate.stem.lower().startswith("autoexec"):
            logger.info("Scanner: ignorando arquivo de configuração %s (autoexec)", candidate.name)
            continue
        if _is_excluded(candidate, root, exclude_patterns):
            logger.debug("Scanner: excluindo %s", candidate)
            continue
        sas_files.append(_make_sas_file(candidate))

    logger.info("Scanner: encontrados %d arquivo(s) .sas em %s", len(sas_files), root)

    if not sas_files:
        logger.warning("Scanner: nenhum arquivo .sas encontrado em %s", root)

    return sas_files


def _make_sas_file(path: Path) -> SASFile:
    return SASFile(
        path=path,
        size_bytes=path.stat().st_size,
    )


def _is_excluded(path: Path, root: Path, patterns: list[str]) -> bool:
    """Verifica se o path corresponde a algum padrão de exclusão."""
    relative = path.relative_to(root)
    for pattern in patterns:
        if relative.match(pattern):
            return True
    return False
