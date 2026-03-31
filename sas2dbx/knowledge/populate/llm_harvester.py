"""LLMHarvester — popula mappings/generated/ usando o conhecimento embutido do LLM.

Não requer arquivos locais de documentação. Usa Claude para gerar mapeamentos
SAS → PySpark diretamente, com validação de schema antes de gravar.

Arquitectura aprovada em revisão @architect:
  C1 — Validação de schema antes de gravar (descartar entradas não-conformes)
  C2 — Fallback de parse YAML: direto → extrair bloco ```yaml → retornar {}
  C3 — harvest_all_sync() via asyncio.run() para compatibilidade com CLI
  C4 — options_map.yaml excluído (muito específico de ambiente)
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)

# Arquivos que o LLM harvest popula (C4: options_map.yaml excluído)
_LLM_TARGET_FILES = [
    "functions_map.yaml",
    "formats_map.yaml",
    "informats_map.yaml",
    "proc_map.yaml",
    "sql_dialect_map.yaml",
]

# Campos obrigatórios e opcionais por entrada de mapeamento
_REQUIRED_FIELDS = {"pyspark", "notes", "confidence"}
_OPTIONAL_FIELDS = {"variants"}

# Prompts por arquivo de mapeamento
_PROMPTS: dict[str, str] = {
    "functions_map.yaml": """\
Gere um mapeamento YAML de funções SAS para equivalentes PySpark/Spark SQL.
Inclua as 60 funções SAS mais usadas em migrações para Databricks.

Formato YAML obrigatório para cada entrada:
```yaml
NOME_FUNCAO_SAS:
  pyspark: "expressão PySpark equivalente (use {arg1}, {arg2}... para args)"
  notes: "observações críticas de conversão (diferenças de comportamento, edge cases)"
  confidence: 0.95  # 0.0–1.0 — quão confiável é esta equivalência
  variants:  # opcional — variações por argumento/modo
    MODO: "expressão alternativa"
```

Priorize: INTCK, INTNX, PUT, INPUT, CATX, COMPRESS, SUBSTR, SCAN, TRIM, LEFT,
RIGHT, UPCASE, LOWCASE, PROPCASE, STRIP, CATS, CATT, CATQ, INDEX, FIND,
TRANWRD, TRANSLATE, REPEAT, REVERSE, LENGTH, LENGTHN, ANYALPHA, ANYDIGIT,
ABS, CEIL, FLOOR, ROUND, MOD, INT, SIGN, MIN, MAX, SUM, MEAN, STD,
DATEPART, TIMEPART, TODAY, DATE, TIME, DATETIME, MDY, YMD, HMS,
YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, WEEKDAY, QTR,
INTNX (com variantes B/M/E), LAG, DIF, FIRST., LAST.,
PUT (formatos numéricos e de data), INPUT (informats numéricos e de data),
IFC, IFN, COALESCE, MISSING, N, NMISS, RANGE, USS, CSS.

Responda APENAS com o bloco YAML. Sem explicações fora do YAML.""",

    "formats_map.yaml": """\
Gere um mapeamento YAML de formatos SAS para equivalentes Spark SQL / PySpark.
Inclua os 40 formatos SAS mais usados.

Formato YAML obrigatório para cada entrada:
```yaml
NOME_FORMATO_SAS:
  pyspark: "função ou pattern PySpark/Spark equivalente"
  notes: "observações sobre diferenças de comportamento"
  confidence: 0.9  # 0.0–1.0
```

Priorize: DATE9., DATE7., DATETIME19., DATETIME20., TIME8., MMDDYY10.,
DDMMYY10., YYMMDD10., YYMMDDN8., MONYY7., MONNAME., MONTH., YEAR4.,
QTR., WEEKDAY., JULDAY., JULIAN7., WORDDATX., WEEKDATE.,
BEST., BEST12., BEST16., COMMA., COMMAX., DOLLAR., DOLLARX.,
PERCENT., PVALUE., E., F., FRACT., Z., BINARY., HEX.,
CHAR., $CHAR., $UPCASE., $QUOTE., $VARYING.,
NLDATE., NLDATM., NLNUM., EUROX., NLMNY.

Responda APENAS com o bloco YAML. Sem explicações fora do YAML.""",

    "informats_map.yaml": """\
Gere um mapeamento YAML de informats SAS para equivalentes PySpark/Spark SQL.
Inclua os 35 informats SAS mais usados.

Formato YAML obrigatório para cada entrada:
```yaml
NOME_INFORMAT_SAS:
  pyspark: "cast ou to_date/to_timestamp equivalente"
  notes: "observações sobre diferenças de comportamento"
  confidence: 0.9  # 0.0–1.0
```

Priorize: DATE9., DATE7., DATETIME19., DATETIME20., TIME8., MMDDYY10.,
DDMMYY10., YYMMDD10., MONYY7., JULIAN7., ANYDTDTE., ANYDTDTM.,
BEST., F., COMMA., DOLLAR., PERCENT., HEX.,
$CHAR., $VARYING., $ASCII., $EBCDIC.,
NLDATE., NLDATM., EUROX.,
E8601DA., E8601DT., E8601TM., IS8601DA., IS8601DT.,
YYMMDD10., B8601DA., B8601DT.

Responda APENAS com o bloco YAML. Sem explicações fora do YAML.""",

    "proc_map.yaml": """\
Gere um mapeamento YAML de PROCs SAS para equivalentes PySpark/Spark SQL.
Inclua os 30 PROCs SAS mais comuns em migrações para Databricks.

Formato YAML obrigatório para cada entrada:
```yaml
NOME_PROC_SAS:
  pyspark: "abordagem PySpark/SparkSQL recomendada (resumida)"
  notes: "observações críticas: o que não tem equivalente direto, gotchas"
  confidence: 0.85  # 0.0–1.0 — considerando complexidade de conversão
  variants:  # opcional — variações por opção/uso
    OPCAO: "abordagem alternativa"
```

Priorize: PROC SQL, PROC SORT, PROC MEANS, PROC SUMMARY, PROC FREQ,
PROC UNIVARIATE, PROC CONTENTS, PROC PRINT, PROC EXPORT, PROC IMPORT,
PROC TRANSPOSE, PROC RANK, PROC STANDARD, PROC APPEND,
PROC DATASETS, PROC COPY, PROC DELETE, PROC FORMAT,
PROC TABULATE, PROC REPORT, PROC SGPLOT, PROC SGPANEL,
PROC REG, PROC GLM, PROC LOGISTIC, PROC PHREG,
PROC CLUSTER, PROC FACTOR, PROC CORRESP,
PROC IML, PROC FCMP, PROC OPTMODEL.

Responda APENAS com o bloco YAML. Sem explicações fora do YAML.""",

    "sql_dialect_map.yaml": """\
Gere um mapeamento YAML de diferenças de dialeto SQL SAS para Spark SQL.
Inclua os 25 casos mais frequentes de divergência de sintaxe/comportamento.

Formato YAML obrigatório para cada entrada (chave = nome do construto SAS):
```yaml
NOME_CONSTRUTO:
  sas: "sintaxe SAS com exemplo"
  spark: "sintaxe Spark SQL equivalente com exemplo"
  notes: "explicação da diferença e como converter"
  confidence: 0.9  # 0.0–1.0
```

Priorize: CALCULATED, MONOTONIC, COALESCE (diferenças), CASE WHEN (diferenças),
CREATE TABLE AS SELECT, INSERT INTO (diferenças), UPDATE (ausente no Spark),
DELETE (ausente no Spark), OUTER UNION CORR, INTERSECT, EXCEPT,
DATE literals ('01JAN2025'd), DATETIME literals ('01JAN2025:00:00:00'dt),
DATEPART(), TIMEPART(), TODAY(), DATE(), DATETIME(),
INTNX() em SQL, INTCK() em SQL, FORMAT em SELECT,
HAVING sem GROUP BY, REMERGE (SAS-specific),
PROC SQL NODUP vs DISTINCT, OUTOBS=, INOBS=,
GROUP BY com aliases, ORDER BY com números de coluna,
NULL handling (MISSING vs NULL), LIKE com wildcards SAS,
CONNECT TO (pass-through), %str() em SQL, macro vars em SQL.

Responda APENAS com o bloco YAML. Sem explicações fora do YAML.""",
}


# ---------------------------------------------------------------------------
# HarvestReport
# ---------------------------------------------------------------------------


@dataclass
class HarvestReport:
    """Resultado do harvest LLM de todos os arquivos de mapeamento.

    Attributes:
        sources_processed: Nomes dos arquivos processados com sucesso.
        entries_per_file: Quantidade de entradas válidas gravadas por arquivo.
        tokens_used: Total de tokens consumidos em todas as chamadas.
        errors: Mensagens de erro encontradas durante o harvest.
        skipped_entries: Total de entradas descartadas por falha de schema (C1).
    """

    sources_processed: list[str] = field(default_factory=list)
    entries_per_file: dict[str, int] = field(default_factory=dict)
    tokens_used: int = 0
    errors: list[str] = field(default_factory=list)
    skipped_entries: int = 0


# ---------------------------------------------------------------------------
# LLMHarvester
# ---------------------------------------------------------------------------


class LLMHarvester:
    """Popula mappings/generated/ usando o conhecimento interno do LLM.

    Não depende de arquivos locais de documentação. Cada chamada ao LLM
    gera YAML para um arquivo de mapeamento, que é validado (C1) e gravado.

    Args:
        base_path: Raiz do knowledge store (contém mappings/).
        llm_config: Configuração do LLMClient (provider, api_key, model…).
    """

    def __init__(self, base_path: str | Path, llm_config: object) -> None:
        self.base_path = Path(base_path)
        self._generated_dir = self.base_path / "mappings" / "generated"
        self._llm_config = llm_config

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def harvest_all_sync(self) -> HarvestReport:
        """Versão síncrona de harvest_all() — compatível com CLI (C3).

        Returns:
            HarvestReport com estatísticas de todos os arquivos processados.
        """
        import asyncio

        return asyncio.run(self.harvest_all())

    async def harvest_all(self) -> HarvestReport:
        """Processa todos os arquivos de mapeamento LLM em sequência.

        Returns:
            HarvestReport com estatísticas agregadas.
        """
        from sas2dbx.transpile.llm.client import LLMClient

        client = LLMClient(self._llm_config)
        report = HarvestReport()

        self._generated_dir.mkdir(parents=True, exist_ok=True)

        for filename in _LLM_TARGET_FILES:
            logger.info("LLMHarvester: processando %s", filename)
            try:
                entries, tokens = await self._harvest_file(client, filename)
                self._write_generated(filename, entries)
                report.sources_processed.append(filename)
                report.entries_per_file[filename] = len(entries)
                report.tokens_used += tokens
                logger.info(
                    "LLMHarvester: %s → %d entradas válidas (%d tokens)",
                    filename,
                    len(entries),
                    tokens,
                )
            except Exception as exc:  # noqa: BLE001
                msg = f"{filename}: {exc}"
                report.errors.append(msg)
                logger.error("LLMHarvester: erro em %s — %s", filename, exc)

        return report

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _harvest_file(
        self,
        client: object,
        filename: str,
    ) -> tuple[dict, int]:
        """Chama o LLM para um arquivo de mapeamento e retorna entradas válidas.

        Returns:
            Tupla (entradas_válidas, tokens_consumidos).
        """
        prompt = _PROMPTS[filename]
        response = await client.complete(prompt)  # type: ignore[attr-defined]

        raw = self._parse_yaml(response.content)  # C2
        entries, skipped = self._validate_entries(raw, filename)

        if skipped:
            logger.warning(
                "LLMHarvester: %s — %d entradas descartadas por schema inválido",
                filename,
                skipped,
            )

        return entries, response.tokens_used

    def _parse_yaml(self, content: str) -> dict:
        """Parse YAML com fallback em 3 níveis (C2).

        1. Tenta parse direto do content.
        2. Extrai bloco ```yaml ... ``` e tenta parse.
        3. Retorna {} e loga warning.

        Args:
            content: String retornada pelo LLM.

        Returns:
            Dict com o conteúdo parseado, ou {} em falha total.
        """
        # Nível 1: parse direto
        try:
            result = yaml.safe_load(content)
            if isinstance(result, dict):
                return result
        except yaml.YAMLError:
            pass

        # Nível 2: extrai bloco ```yaml ... ```
        match = re.search(r"```(?:yaml)?\s*\n(.*?)```", content, re.DOTALL)
        if match:
            try:
                result = yaml.safe_load(match.group(1))
                if isinstance(result, dict):
                    logger.debug("LLMHarvester: YAML extraído de bloco ```yaml")
                    return result
            except yaml.YAMLError:
                pass

        # Nível 3: falha total
        logger.warning(
            "LLMHarvester: não foi possível parsear YAML — retornando dict vazio"
        )
        return {}

    def _validate_entries(
        self, raw: dict, filename: str
    ) -> tuple[dict, int]:
        """Valida cada entrada contra o schema esperado (C1).

        Descarta entradas que não possuem os campos obrigatórios ou que
        têm `confidence` fora do intervalo [0.0, 1.0].

        Args:
            raw: Dict raw do YAML parseado.
            filename: Nome do arquivo (para logging).

        Returns:
            Tupla (entradas_válidas, n_descartadas).
        """
        is_sql_dialect = filename == "sql_dialect_map.yaml"
        valid: dict = {}
        skipped = 0

        for key, value in raw.items():
            if not isinstance(value, dict):
                logger.debug(
                    "LLMHarvester: [%s] entrada '%s' ignorada — valor não é dict",
                    filename, key,
                )
                skipped += 1
                continue

            # sql_dialect_map usa 'spark' em vez de 'pyspark'
            if is_sql_dialect:
                required = {"spark", "notes", "confidence"}
            else:
                required = _REQUIRED_FIELDS

            missing = required - value.keys()
            if missing:
                logger.debug(
                    "LLMHarvester: [%s] entrada '%s' ignorada — campos ausentes: %s",
                    filename, key, missing,
                )
                skipped += 1
                continue

            confidence = value.get("confidence")
            if not isinstance(confidence, (int, float)) or not (0.0 <= confidence <= 1.0):
                logger.debug(
                    "LLMHarvester: [%s] entrada '%s' ignorada — confidence inválido: %s",
                    filename, key, confidence,
                )
                skipped += 1
                continue

            valid[key] = value

        return valid, skipped

    def _write_generated(self, filename: str, data: dict) -> None:
        """Grava entradas validadas em mappings/generated/<filename>.

        Faz merge com conteúdo existente: novas entradas sobrescrevem por chave,
        entradas existentes não presentes no novo harvest são preservadas.

        Args:
            filename: Nome do arquivo YAML (ex: 'functions_map.yaml').
            data: Dict com entradas validadas.
        """
        target = self._generated_dir / filename
        target.parent.mkdir(parents=True, exist_ok=True)

        existing: dict = {}
        if target.exists():
            try:
                loaded = yaml.safe_load(target.read_text(encoding="utf-8"))
                if isinstance(loaded, dict):
                    existing = loaded
            except yaml.YAMLError:
                logger.warning(
                    "LLMHarvester: falha ao ler %s existente — sobrescrevendo", target
                )

        merged = {**existing, **data}

        target.write_text(
            yaml.dump(merged, allow_unicode=True, default_flow_style=False, sort_keys=True),
            encoding="utf-8",
        )
        logger.debug("LLMHarvester: gravado %s (%d entradas)", target, len(merged))
