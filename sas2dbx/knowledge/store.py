"""KnowledgeStore — API de acesso ao knowledge store (lookup + retrieval).

Lê sempre de mappings/merged/ (curated > generated).
Fallback para mappings/generated/ se merged/ não existir ainda.

On-demand harvest (H.2):
  Quando llm_client é fornecido, lookups *_or_harvest() tentam gerar
  a entrada via LLM se não encontrada, gravam em generated/ e retornam.
  Sem llm_client, comportamento idêntico aos lookups simples.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)


class KnowledgeStore:
    """
    RAG file-based knowledge store for SAS→PySpark mappings and references.

    Priority for all lookups: mappings/merged/ → mappings/generated/
    (merged/ already contains curated > generated, but generated/ is the
    ultimate fallback when build-mappings hasn't run yet).

    Args:
        base_path: Raiz do knowledge store.
        llm_client: LLMClient opcional. Quando fornecido, habilita on-demand
            harvest: se uma chave não for encontrada nos YAMLs, chama o LLM
            para gerar o mapeamento, grava em generated/ e retorna.
            Sem llm_client, os métodos *_or_harvest() se comportam
            exatamente como os lookups simples (retornam None se não acham).
    """

    def __init__(
        self,
        base_path: str | Path = "./knowledge",
        llm_client: object | None = None,
    ) -> None:
        self.base_path = Path(base_path)
        self._mappings_merged = self.base_path / "mappings" / "merged"
        self._mappings_generated = self.base_path / "mappings" / "generated"
        self._sas_reference = self.base_path / "sas_reference"
        self._pyspark_reference = self.base_path / "pyspark_reference"
        self._custom = self.base_path / "custom"
        self._cache: dict[str, dict] = {}  # lazy cache: path_str → parsed YAML
        self._llm_client = llm_client
        self._harvest_attempted: set[str] = set()  # cache de negativas por sessão
        self._ref_cache: dict[str, str | None] = {}    # cache memória para get_reference()
        self._single_harvester: object | None = None   # instância cacheada de LLMSingleHarvester
        self._harvest_failures: set[str] | None = None # lazy-loaded do disco
        self._enriched_count: int = 0                  # contagem de enriquecimentos pós-transpilação

    # -------------------------------------------------------------------------
    # Lookup — deterministic, no LLM
    # -------------------------------------------------------------------------

    def lookup_function(self, func_name: str) -> dict[str, Any] | None:
        """
        Lookup a SAS function mapping.
        Reads from merged/ (curated > generated) with fallback to generated/.
        Returns None if not found.
        """
        return self._lookup_in_mapping("functions_map.yaml", func_name.upper())

    def lookup_proc(self, proc_name: str) -> dict[str, Any] | None:
        """
        Lookup a PROC mapping (approach: rule/llm/manual).
        Returns None if not found.
        """
        return self._lookup_in_mapping("proc_map.yaml", proc_name.upper())

    def lookup_sql_dialect(self, construct: str) -> dict[str, Any] | None:
        """
        Lookup a SQL dialect difference (SAS SQL → Spark SQL).
        Returns None if not found.
        """
        return self._lookup_in_mapping("sql_dialect_map.yaml", construct.upper())

    def lookup_format(self, format_name: str) -> dict[str, Any] | None:
        """Lookup a SAS format → Python/Spark format mapping."""
        return self._lookup_in_mapping("formats_map.yaml", format_name.upper())

    def lookup_informat(self, informat_name: str) -> dict[str, Any] | None:
        """Lookup a SAS informat → Python/Spark mapping."""
        return self._lookup_in_mapping("informats_map.yaml", informat_name.upper())

    # -------------------------------------------------------------------------
    # Lookup com fallback on-demand (H.2)
    # -------------------------------------------------------------------------

    def lookup_function_or_harvest(self, func_name: str) -> dict[str, Any] | None:
        """Lookup de função SAS com harvest on-demand se não encontrado.

        Fluxo: lookup simples → se None e llm_client configurado → harvest LLM
        → grava em generated/ → invalida cache → retorna.

        Args:
            func_name: Nome da função SAS (case-insensitive).

        Returns:
            Dict do mapeamento ou None se não encontrado e harvest falhou/indisponível.
        """
        result = self.lookup_function(func_name)
        if result is not None:
            return result
        return self._on_demand_harvest("function", func_name.upper(), "functions_map.yaml")

    def lookup_proc_or_harvest(self, proc_name: str) -> dict[str, Any] | None:
        """Lookup de PROC com harvest on-demand se não encontrado."""
        result = self.lookup_proc(proc_name)
        if result is not None:
            return result
        return self._on_demand_harvest("proc", proc_name.upper(), "proc_map.yaml")

    def lookup_sql_dialect_or_harvest(self, construct: str) -> dict[str, Any] | None:
        """Lookup de regra SQL dialect com harvest on-demand se não encontrado."""
        result = self.lookup_sql_dialect(construct)
        if result is not None:
            return result
        return self._on_demand_harvest("sql_dialect", construct.upper(), "sql_dialect_map.yaml")

    def lookup_format_or_harvest(self, format_name: str) -> dict[str, Any] | None:
        """Lookup de formato SAS com harvest on-demand se não encontrado."""
        result = self.lookup_format(format_name)
        if result is not None:
            return result
        return self._on_demand_harvest("format", format_name.upper(), "formats_map.yaml")

    def lookup_informat_or_harvest(self, informat_name: str) -> dict[str, Any] | None:
        """Lookup de informat SAS com harvest on-demand se não encontrado."""
        result = self.lookup_informat(informat_name)
        if result is not None:
            return result
        return self._on_demand_harvest("informat", informat_name.upper(), "informats_map.yaml")

    # -------------------------------------------------------------------------
    # Retrieval — markdown reference docs
    # -------------------------------------------------------------------------

    def get_reference(self, source: str, category: str, name: str) -> str | None:
        """
        Retrieve a markdown reference doc for context injection into LLM prompts.
        Cache em memória: cada arquivo .md é lido do disco apenas uma vez por sessão.

        Args:
            source: 'sas' | 'pyspark'
            category: e.g. 'procs', 'functions', 'dataframe_api', 'spark_sql'
            name: e.g. 'proc_means', 'months_between', 'groupby_agg'

        Returns:
            Markdown content as string, or None if file doesn't exist.
        """
        if source == "sas":
            ref_path = self._sas_reference / category / f"{name}.md"
        elif source == "pyspark":
            ref_path = self._pyspark_reference / category / f"{name}.md"
        else:
            logger.warning("Unknown reference source: %r. Use 'sas' or 'pyspark'.", source)
            return None

        cache_key = f"{source}/{category}/{name}"
        if cache_key not in self._ref_cache:
            self._ref_cache[cache_key] = (
                ref_path.read_text(encoding="utf-8") if ref_path.exists() else None
            )
            if self._ref_cache[cache_key] is None:
                logger.debug("Reference not found: %s", ref_path)
        return self._ref_cache[cache_key]

    # -------------------------------------------------------------------------
    # Custom environment
    # -------------------------------------------------------------------------

    def get_custom(self, name: str) -> dict[str, Any]:
        """
        Load a custom environment file (libnames, macros, conventions).

        Args:
            name: filename without extension (e.g. 'libnames', 'macros')

        Returns:
            Parsed YAML as dict, or empty dict if file doesn't exist.
        """
        custom_file = self._custom / f"{name}.yaml"
        if custom_file.exists():
            with open(custom_file, encoding="utf-8") as f:
                return yaml.safe_load(f) or {}
        return {}

    # -------------------------------------------------------------------------
    # Async-native lookups (A.1) — para uso em contextos async (FastAPI/uvicorn)
    # -------------------------------------------------------------------------

    async def alookup_function_or_harvest(self, func_name: str) -> dict[str, Any] | None:
        """Versão async de lookup_function_or_harvest — não usa _run_sync()."""
        result = self.lookup_function(func_name)
        if result is not None:
            return result
        return await self._on_demand_harvest_async(
            "function", func_name.upper(), "functions_map.yaml"
        )

    async def alookup_proc_or_harvest(self, proc_name: str) -> dict[str, Any] | None:
        """Versão async de lookup_proc_or_harvest."""
        result = self.lookup_proc(proc_name)
        if result is not None:
            return result
        return await self._on_demand_harvest_async(
            "proc", proc_name.upper(), "proc_map.yaml"
        )

    async def alookup_sql_dialect_or_harvest(self, construct: str) -> dict[str, Any] | None:
        """Versão async de lookup_sql_dialect_or_harvest."""
        result = self.lookup_sql_dialect(construct)
        if result is not None:
            return result
        return await self._on_demand_harvest_async(
            "sql_dialect", construct.upper(), "sql_dialect_map.yaml"
        )

    async def alookup_format_or_harvest(self, format_name: str) -> dict[str, Any] | None:
        """Versão async de lookup_format_or_harvest."""
        result = self.lookup_format(format_name)
        if result is not None:
            return result
        return await self._on_demand_harvest_async(
            "format", format_name.upper(), "formats_map.yaml"
        )

    async def alookup_informat_or_harvest(self, informat_name: str) -> dict[str, Any] | None:
        """Versão async de lookup_informat_or_harvest."""
        result = self.lookup_informat(informat_name)
        if result is not None:
            return result
        return await self._on_demand_harvest_async(
            "informat", informat_name.upper(), "informats_map.yaml"
        )

    # -------------------------------------------------------------------------
    # Internal
    # -------------------------------------------------------------------------

    def _load_mapping(self, directory: Path, filename: str) -> dict[str, Any]:
        """Carrega YAML com cache em memória. Lê do disco apenas na primeira chamada."""
        cache_key = str(directory / filename)
        if cache_key not in self._cache:
            mapping_file = directory / filename
            if mapping_file.exists():
                with open(mapping_file, encoding="utf-8") as f:
                    self._cache[cache_key] = yaml.safe_load(f) or {}
            else:
                self._cache[cache_key] = {}
        return self._cache[cache_key]

    def _lookup_in_mapping(self, filename: str, key: str) -> dict[str, Any] | None:
        """
        Lookup a key in a mapping YAML file (com cache em memória).

        Search order:
          1. mappings/merged/  — ground truth (curated > generated)
          2. mappings/generated/ — fallback if build-mappings hasn't run yet

        Returns the entry dict, or None if not found in either.
        """
        for directory in (self._mappings_merged, self._mappings_generated):
            data = self._load_mapping(directory, filename)
            if key in data:
                return data[key]
        return None

    def invalidate_cache(self) -> None:
        """Limpa o cache em memória. Usar após build-mappings ou edição manual dos YAMLs."""
        self._cache.clear()
        self._ref_cache.clear()

    # -------------------------------------------------------------------------
    # Internal — on-demand harvest (H.2)
    # -------------------------------------------------------------------------

    def _on_demand_harvest(
        self,
        category: str,
        key: str,
        filename: str,
    ) -> dict[str, Any] | None:
        """Tenta harvest on-demand de uma entrada individual via LLM.

        Guards:
          - LLM client não configurado → retorna None silenciosamente
          - Já tentou este key nesta sessão → retorna None (cache de negativas)
          - LLM falhou ou retornou YAML inválido → loga warning, retorna None

        Não levanta exceção — falha silenciosa é intencional para não
        interromper o pipeline de transpilação.
        """
        if self._llm_client is None:
            return None

        cache_key = f"{category}:{key}"
        if cache_key in self._harvest_attempted:
            return None

        self._harvest_attempted.add(cache_key)

        # Gap 2 — verifica se já foi tentado e confirmado sem equivalência
        if cache_key in self._load_harvest_failures():
            logger.debug(
                "KnowledgeStore: %s '%s' está em harvest_failures — pulando LLM call",
                category, key,
            )
            return None

        logger.info(
            "KnowledgeStore: on-demand harvest para %s '%s'...", category, key
        )

        try:
            harvester = self._get_single_harvester()
            entry = harvester.harvest_single_sync(category=category, key=key)

            if entry is None:
                logger.warning(
                    "KnowledgeStore: on-demand harvest retornou None para %s '%s' — registrando failure",
                    category, key,
                )
                self._record_harvest_failure(cache_key)
                return None

            self._append_to_generated(filename, key, entry)
            self.invalidate_cache()

            logger.info(
                "KnowledgeStore: on-demand harvest OK — %s '%s' (confidence=%.2f)",
                category, key, entry.get("confidence", 0),
            )
            return entry

        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "KnowledgeStore: on-demand harvest falhou para %s '%s': %s",
                category, key, exc,
            )
            return None

    def _append_to_generated(self, filename: str, key: str, entry: dict) -> None:
        """Adiciona ou atualiza uma entrada em generated/ sem sobrescrever outras.

        Thread-safe: usa read-modify-write com replace atômico via os.replace().

        Args:
            filename: Nome do arquivo YAML (ex: 'functions_map.yaml').
            key: Chave da entrada (ex: 'PRXMATCH').
            entry: Dict com o mapeamento validado.
        """
        path = self._mappings_generated / filename
        path.parent.mkdir(parents=True, exist_ok=True)

        existing: dict = {}
        if path.exists():
            try:
                with open(path, encoding="utf-8") as f:
                    loaded = yaml.safe_load(f)
                    if isinstance(loaded, dict):
                        existing = loaded
            except yaml.YAMLError:
                logger.warning(
                    "KnowledgeStore: falha ao ler %s — sobrescrevendo", path
                )

        existing[key] = entry

        tmp = path.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            yaml.dump(
                existing,
                f,
                default_flow_style=False,
                allow_unicode=True,
                sort_keys=True,
            )
        os.replace(tmp, path)

    def _get_single_harvester(self) -> object:
        """Retorna instância cacheada de LLMSingleHarvester (criada uma vez por sessão)."""
        if self._single_harvester is None:
            from sas2dbx.knowledge.populate.llm_harvester import LLMSingleHarvester
            self._single_harvester = LLMSingleHarvester(self._llm_client)
        return self._single_harvester

    # ------------------------------------------------------------------
    # Gap 2 — Harvest failure persistence
    # ------------------------------------------------------------------

    _FAILURES_FILENAME = "harvest_no_mapping.yaml"

    def _load_harvest_failures(self) -> set[str]:
        """Carrega (lazy) o conjunto de chaves sem equivalência conhecida.

        Retorna set de strings no formato "category:KEY" (ex: "function:PRXMATCH").
        Falhas são persistidas em generated/harvest_no_mapping.yaml.
        """
        if self._harvest_failures is not None:
            return self._harvest_failures

        failures_path = self._mappings_generated / self._FAILURES_FILENAME
        if not failures_path.exists():
            self._harvest_failures = set()
            return self._harvest_failures

        try:
            import yaml
            data = yaml.safe_load(failures_path.read_text(encoding="utf-8")) or {}
            self._harvest_failures = set(data.keys())
        except Exception:  # noqa: BLE001
            self._harvest_failures = set()

        logger.debug(
            "KnowledgeStore: %d chave(s) em harvest_failures carregadas",
            len(self._harvest_failures),
        )
        return self._harvest_failures

    def _record_harvest_failure(self, cache_key: str) -> None:
        """Persiste uma chave sem equivalência em generated/harvest_no_mapping.yaml.

        Próximas sessões vão pular o LLM call para esta chave imediatamente.
        """
        import datetime as _dt
        import yaml

        self._load_harvest_failures()  # garante que self._harvest_failures está carregado
        if cache_key in self._harvest_failures:
            return  # já registrado

        self._harvest_failures.add(cache_key)

        failures_path = self._mappings_generated / self._FAILURES_FILENAME
        failures_path.parent.mkdir(parents=True, exist_ok=True)

        existing: dict = {}
        if failures_path.exists():
            try:
                loaded = yaml.safe_load(failures_path.read_text(encoding="utf-8"))
                if isinstance(loaded, dict):
                    existing = loaded
            except Exception:  # noqa: BLE001
                pass

        existing[cache_key] = {"recorded_at": _dt.datetime.now(tz=_dt.timezone.utc).isoformat()}

        tmp = failures_path.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            yaml.dump(existing, f, default_flow_style=False, allow_unicode=True, sort_keys=True)
        import os
        os.replace(tmp, failures_path)

        logger.info(
            "KnowledgeStore: failure registrado para '%s' em %s",
            cache_key, failures_path.name,
        )

    # ------------------------------------------------------------------
    # Gap 3 — Enriquecimento pós-transpilação
    # ------------------------------------------------------------------

    def enrich_from_transpilation(
        self,
        unknown_funcs: list[str],
        sas_code: str,
        pyspark_code: str,
    ) -> int:
        """Enriquece a KB com funções SAS desconhecidas usando contexto da transpilação.

        Diferente do on-demand harvest (uma função por vez, sem contexto), este método
        envia UMA chamada LLM em batch com o código SAS original + PySpark gerado,
        permitindo ao LLM inferir mapeamentos com contexto real de uso.

        Args:
            unknown_funcs: Funções SAS não encontradas na KB antes da transpilação.
            sas_code: Bloco SAS original.
            pyspark_code: Código PySpark gerado pela transpilação.

        Returns:
            Número de entradas adicionadas à KB.
        """
        if self._llm_client is None or not unknown_funcs:
            return 0

        failures = self._load_harvest_failures()
        # Filtra funções já tentadas (nesta sessão ou em sessões anteriores)
        to_enrich = [
            f for f in unknown_funcs
            if f"function:{f}" not in self._harvest_attempted
            and f"function:{f}" not in failures
        ]
        if not to_enrich:
            return 0

        logger.info(
            "KnowledgeStore: enriquecimento pós-transpilação para %d função(ões): %s",
            len(to_enrich), ", ".join(to_enrich[:8]),
        )

        try:
            harvester = self._get_single_harvester()
            entries = harvester.harvest_from_context_sync(
                func_names=to_enrich,
                sas_code=sas_code,
                pyspark_code=pyspark_code,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("KnowledgeStore: enriquecimento falhou: %s", exc)
            return 0

        added = 0
        for func_name, entry in (entries or {}).items():
            cache_key = f"function:{func_name.upper()}"
            self._harvest_attempted.add(cache_key)
            if entry is not None:
                self._append_to_generated("functions_map.yaml", func_name.upper(), entry)
                added += 1
                logger.debug("KnowledgeStore: enriquecimento OK — %s", func_name)
            else:
                self._record_harvest_failure(cache_key)

        # Funções em to_enrich que não apareceram na resposta do LLM
        returned_keys = {k.upper() for k in (entries or {})}
        for func_name in to_enrich:
            if func_name.upper() not in returned_keys:
                self._record_harvest_failure(f"function:{func_name.upper()}")

        if added > 0:
            self.invalidate_cache()
            self._enriched_count += added
            logger.info(
                "KnowledgeStore: %d função(ões) adicionada(s) à KB via enriquecimento", added
            )

        return added

    async def _on_demand_harvest_async(
        self,
        category: str,
        key: str,
        filename: str,
    ) -> dict[str, Any] | None:
        """Versão async de _on_demand_harvest — chama harvest_single() diretamente.

        Evita _run_sync() / ThreadPoolExecutor — adequado para contextos async nativos.
        """
        if self._llm_client is None:
            return None

        cache_key = f"{category}:{key}"
        if cache_key in self._harvest_attempted:
            return None

        self._harvest_attempted.add(cache_key)

        # Gap 2 — verifica failures persistidos
        if cache_key in self._load_harvest_failures():
            logger.debug(
                "KnowledgeStore: %s '%s' está em harvest_failures — pulando LLM call",
                category, key,
            )
            return None

        logger.info(
            "KnowledgeStore: async harvest para %s '%s'...", category, key
        )

        try:
            harvester = self._get_single_harvester()
            entry = await harvester.harvest_single(category=category, key=key)

            if entry is None:
                logger.warning(
                    "KnowledgeStore: async harvest retornou None para %s '%s' — registrando failure",
                    category, key,
                )
                self._record_harvest_failure(cache_key)
                return None

            self._append_to_generated(filename, key, entry)
            self.invalidate_cache()

            logger.info(
                "KnowledgeStore: async harvest OK — %s '%s' (confidence=%.2f)",
                category, key, entry.get("confidence", 0),
            )
            return entry

        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "KnowledgeStore: async harvest falhou para %s '%s': %s",
                category, key, exc,
            )
            return None
