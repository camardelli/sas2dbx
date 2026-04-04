"""Scanner de diretório SAS — encontra e cataloga arquivos .sas."""

from __future__ import annotations

import logging
import zipfile
from pathlib import Path

from sas2dbx.models.sas_ast import SASFile

logger = logging.getLogger(__name__)

# Extensões reconhecidas como código SAS
_SAS_EXTENSIONS = {".sas"}

# Subdiretório onde ZIPs são extraídos dentro de output_dir
_ZIP_EXTRACT_SUBDIR = "_extracted"


def scan_directory(
    path: str | Path,
    recursive: bool = True,
    exclude_patterns: list[str] | None = None,
    extract_dir: Path | None = None,
) -> list[SASFile]:
    """Escaneia um diretório (ou arquivo ZIP) e retorna lista de arquivos .sas.

    Args:
        path: Diretório, arquivo .sas único, ou arquivo .zip com jobs SAS.
        recursive: Se True, busca recursivamente em subdiretórios.
        exclude_patterns: Padrões glob a excluir (ex: ["**/test/**", "**/macro_*"]).
        extract_dir: Diretório onde extrair ZIP. Se None, usa `path.parent/_extracted/`.
            Ignorado quando path não é ZIP.

    Returns:
        Lista de SASFile ordenada por path, vazia se nenhum arquivo encontrado.

    Raises:
        FileNotFoundError: Se o path não existe.
        NotADirectoryError: Se path não é diretório nem arquivo .sas/.zip.
    """
    root = Path(path).resolve()
    exclude_patterns = exclude_patterns or []

    if not root.exists():
        raise FileNotFoundError(f"Path não encontrado: {root}")

    # Aceita arquivo ZIP — extrai para extract_dir e escaneia
    if root.is_file() and root.suffix.lower() == ".zip":
        dest = extract_dir or (root.parent / _ZIP_EXTRACT_SUBDIR / root.stem)
        return _scan_zip(root, dest, recursive, exclude_patterns)

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


def _scan_zip(
    zip_path: Path,
    extract_dir: Path,
    recursive: bool,
    exclude_patterns: list[str],
) -> list[SASFile]:
    """Extrai ZIP para extract_dir (persistente) e escaneia os .sas contidos.

    A extração é idempotente: arquivos já extraídos não são sobrescritos,
    garantindo que re-runs após interrupção não re-extraiam desnecessariamente.

    Args:
        zip_path: Arquivo ZIP de entrada.
        extract_dir: Diretório destino da extração (criado se não existir).
        recursive: Passado para scan_directory após extração.
        exclude_patterns: Passado para scan_directory após extração.

    Returns:
        Lista de SASFile encontrados dentro do ZIP extraído.
    """
    extract_dir.mkdir(parents=True, exist_ok=True)

    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            members = [m for m in zf.infolist() if m.filename.lower().endswith(".sas")]
            for member in members:
                dest_file = extract_dir / member.filename
                if dest_file.exists():
                    logger.debug("Scanner: %s já extraído — pulando", member.filename)
                    continue
                dest_file.parent.mkdir(parents=True, exist_ok=True)
                dest_file.write_bytes(zf.read(member.filename))
                logger.debug("Scanner: extraído %s → %s", member.filename, dest_file)
    except zipfile.BadZipFile as exc:
        raise ValueError(f"Arquivo ZIP inválido: {zip_path}") from exc

    extracted_count = len(members)
    logger.info(
        "Scanner: ZIP '%s' → %d arquivo(s) .sas extraído(s) em %s",
        zip_path.name, extracted_count, extract_dir,
    )

    if not extracted_count:
        logger.warning("Scanner: nenhum arquivo .sas encontrado dentro de %s", zip_path.name)
        return []

    return scan_directory(extract_dir, recursive=recursive, exclude_patterns=exclude_patterns)


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
