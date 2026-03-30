"""KnowledgeHarvester — orquestrador offline-first de coleta de documentação.

Modo padrão: OFFLINE — lê HTMLs/PDFs locais de knowledge/raw_input/.
Modo online: opt-in explícito via --mode=online (frágil, pode quebrar sem aviso).
"""

from __future__ import annotations

import logging
import shutil
from enum import Enum
from pathlib import Path

logger = logging.getLogger(__name__)


class HarvestMode(str, Enum):
    """Modo de coleta de documentação."""

    OFFLINE = "offline"  # padrão — lê arquivos locais em raw_input/
    ONLINE = "online"    # opt-in — scraping externo (frágil)


class KnowledgeHarvester:
    """
    Orquestra coleta de documentação de todas as fontes.

    OFFLINE é sempre o default. ONLINE é explícito e frágil:
    sites como documentation.sas.com mudam estrutura sem aviso.

    Métodos de parsing específicos por fonte (_process_offline_docs,
    _harvest_sas_online, etc.) são stubs aqui — implementados em
    sas_docs.py, spark_docs.py, dbx_docs.py em stories futuras.
    """

    def __init__(self, base_path: str | Path = "./knowledge") -> None:
        self.base_path = Path(base_path)
        self.raw_input_dir = self.base_path / "raw_input"
        self.raw_dir = self.base_path / "raw"

    def harvest(
        self,
        source: str,
        version: str | None = None,
        mode: HarvestMode = HarvestMode.OFFLINE,
        topics: list[str] | None = None,
        custom_path: str | Path | None = None,
    ) -> None:
        """
        Dispatch harvest para o handler correto por fonte.

        Args:
            source: 'sas' | 'pyspark' | 'databricks' | 'custom'
            version: versão da fonte (ex: '9.4', '3.5')
            mode: OFFLINE (default) ou ONLINE (opt-in)
            topics: para databricks, lista de tópicos
            custom_path: para source='custom', path dos arquivos do cliente
        """
        match source:
            case "sas":
                self.harvest_sas(version=version or "9.4", mode=mode)
            case "pyspark":
                self.harvest_pyspark(version=version or "3.5", mode=mode)
            case "databricks":
                self.harvest_databricks(
                    topics=topics or ["unity-catalog", "workflows", "delta-lake", "sql-functions"],
                    mode=mode,
                )
            case "custom":
                if not custom_path:
                    raise ValueError("--path é obrigatório para --source custom")
                self.harvest_custom(input_path=custom_path)
            case _:
                raise ValueError(
                    f"Fonte desconhecida: {source!r}. Use: sas, pyspark, databricks, custom"
                )

    def harvest_sas(
        self, version: str = "9.4", mode: HarvestMode = HarvestMode.OFFLINE
    ) -> None:
        """
        Coleta documentação SAS.

        OFFLINE (default): lê HTML/PDFs locais de ./knowledge/raw_input/sas/
        ONLINE (explícito): scraping de documentation.sas.com — frágil.
        """
        output_dir = self.raw_dir / "sas"
        output_dir.mkdir(parents=True, exist_ok=True)

        if mode == HarvestMode.OFFLINE:
            input_dir = self.raw_input_dir / "sas"
            if not input_dir.exists() or not _has_files(input_dir):
                logger.warning(
                    "Modo offline: nenhum arquivo encontrado em %s. "
                    "Adicione HTML/PDFs de documentation.sas.com nesse diretório.",
                    input_dir,
                )
                return
            logger.info("Harvesting SAS %s — arquivos locais em %s", version, input_dir)
            self._process_offline_docs(input_dir, output_dir, source="sas", version=version)
        else:
            logger.info(
                "Harvesting SAS %s online (frágil — pode quebrar com mudanças no site)", version
            )
            self._harvest_sas_online(version=version, output_dir=output_dir)

    def harvest_pyspark(
        self, version: str = "3.5", mode: HarvestMode = HarvestMode.OFFLINE
    ) -> None:
        """
        Coleta documentação PySpark.

        OFFLINE (default): lê HTML locais de ./knowledge/raw_input/pyspark/
        ONLINE (explícito): scraping de spark.apache.org — frágil.
        """
        output_dir = self.raw_dir / "pyspark"
        output_dir.mkdir(parents=True, exist_ok=True)

        if mode == HarvestMode.OFFLINE:
            input_dir = self.raw_input_dir / "pyspark"
            if not input_dir.exists() or not _has_files(input_dir):
                logger.warning(
                    "Modo offline: nenhum arquivo encontrado em %s. "
                    "Adicione HTML de spark.apache.org nesse diretório.",
                    input_dir,
                )
                return
            logger.info("Harvesting PySpark %s — arquivos locais em %s", version, input_dir)
            self._process_offline_docs(input_dir, output_dir, source="pyspark", version=version)
        else:
            logger.info("Harvesting PySpark %s online", version)
            self._harvest_pyspark_online(version=version, output_dir=output_dir)

    def harvest_databricks(
        self,
        topics: list[str] | None = None,
        mode: HarvestMode = HarvestMode.OFFLINE,
    ) -> None:
        """
        Coleta documentação Databricks.

        OFFLINE (default): lê HTML locais de ./knowledge/raw_input/databricks/
        ONLINE (explícito): scraping de docs.databricks.com — frágil.
        """
        topics = topics or ["unity-catalog", "workflows", "delta-lake", "sql-functions"]
        output_dir = self.raw_dir / "databricks"
        output_dir.mkdir(parents=True, exist_ok=True)

        if mode == HarvestMode.OFFLINE:
            input_dir = self.raw_input_dir / "databricks"
            if not input_dir.exists() or not _has_files(input_dir):
                logger.warning(
                    "Modo offline: nenhum arquivo encontrado em %s. "
                    "Adicione HTML de docs.databricks.com nesse diretório.",
                    input_dir,
                )
                return
            logger.info(
                "Harvesting Databricks (topics: %s) — arquivos locais em %s", topics, input_dir
            )
            self._process_offline_docs(
                input_dir, output_dir, source="databricks", version=None
            )
        else:
            logger.info("Harvesting Databricks online (topics: %s)", topics)
            self._harvest_databricks_online(topics=topics, output_dir=output_dir)

    def harvest_custom(self, input_path: str | Path) -> None:
        """
        Processa arquivos do ambiente do cliente.

        Aceita:
          - libnames.yaml: LIBNAME → catalog.schema (+ depends_on_jobs)
          - macros.yaml: macros corporativas documentadas
          - conventions.yaml: naming conventions, exceções
          - *.sas: macros para auto-documentação

        Copia para knowledge/custom/ para consumo pelo KnowledgeStore.
        """
        input_path = Path(input_path)
        custom_dir = self.base_path / "custom"
        custom_dir.mkdir(parents=True, exist_ok=True)

        if not input_path.exists():
            raise FileNotFoundError(f"Custom path não encontrado: {input_path}")

        if input_path.is_file():
            shutil.copy2(input_path, custom_dir / input_path.name)
            logger.info("Copiado %s → %s", input_path, custom_dir)
        elif input_path.is_dir():
            copied = 0
            for yaml_file in input_path.glob("*.yaml"):
                shutil.copy2(yaml_file, custom_dir / yaml_file.name)
                logger.info("Copiado %s → %s", yaml_file, custom_dir)
                copied += 1
            if copied == 0:
                logger.warning("Nenhum arquivo .yaml encontrado em %s", input_path)

    # -------------------------------------------------------------------------
    # Stubs internos — implementados em sas_docs.py, spark_docs.py, dbx_docs.py
    # -------------------------------------------------------------------------

    def _process_offline_docs(
        self,
        input_dir: Path,
        output_dir: Path,
        source: str,
        version: str | None,
    ) -> None:
        """
        Processa documentos locais (HTML/PDF) em raw markdown por construto.
        STUB — implementação real em sas_docs.py / spark_docs.py / dbx_docs.py.
        """
        logger.info(
            "[STUB] _process_offline_docs: source=%s version=%s %s → %s",
            source,
            version,
            input_dir,
            output_dir,
        )

    def _harvest_sas_online(self, version: str, output_dir: Path) -> None:
        """STUB — scraping online SAS docs. Implementado em sas_docs.py."""
        logger.info("[STUB] _harvest_sas_online: version=%s output=%s", version, output_dir)

    def _harvest_pyspark_online(self, version: str, output_dir: Path) -> None:
        """STUB — scraping online PySpark docs. Implementado em spark_docs.py."""
        logger.info("[STUB] _harvest_pyspark_online: version=%s output=%s", version, output_dir)

    def _harvest_databricks_online(self, topics: list[str], output_dir: Path) -> None:
        """STUB — scraping online Databricks docs. Implementado em dbx_docs.py."""
        logger.info("[STUB] _harvest_databricks_online: topics=%s output=%s", topics, output_dir)


# -------------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------------

def _has_files(directory: Path) -> bool:
    """Returns True if directory exists and contains at least one file (non-recursive)."""
    return directory.is_dir() and any(f.is_file() for f in directory.iterdir())
