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
import os
import re
from dataclasses import dataclass, field
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helper — execução síncrona segura em qualquer contexto (M3)
# ---------------------------------------------------------------------------

def _run_sync(coro: object) -> object:
    """Executa uma coroutine de forma síncrona, seguro dentro ou fora de loop async.

    - Sem loop em execução: usa asyncio.run() diretamente.
    - Com loop em execução (FastAPI/uvicorn): delega para ThreadPoolExecutor
      isolado para evitar RuntimeError de loop aninhado.

    Args:
        coro: Coroutine a executar.

    Returns:
        Resultado da coroutine.
    """
    import asyncio
    import concurrent.futures

    try:
        asyncio.get_running_loop()
        # Já dentro de um loop — executa em thread separada com seu próprio loop
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            return pool.submit(asyncio.run, coro).result()
    except RuntimeError:
        # Sem loop em execução — safe to use asyncio.run()
        return asyncio.run(coro)


# Arquivos que o LLM harvest popula (C4: options_map.yaml excluído)
_LLM_TARGET_FILES = [
    "functions_map.yaml",
    "formats_map.yaml",
    "informats_map.yaml",
    "proc_map.yaml",
    "sql_dialect_map.yaml",
]

# Campos obrigatórios por entrada de mapeamento
_REQUIRED_FIELDS = {"pyspark", "notes", "confidence"}

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

        Seguro em contexto async (FastAPI/uvicorn): detecta loop em execução
        e usa ThreadPoolExecutor para evitar RuntimeError de loop aninhado.

        Returns:
            HarvestReport com estatísticas de todos os arquivos processados.
        """
        return _run_sync(self.harvest_all())

    async def harvest_all(self) -> HarvestReport:
        """PP2-08: Processa todos os arquivos de mapeamento LLM em paralelo via asyncio.gather.

        Cada arquivo é independente — rodar em paralelo reduz o tempo total de harvest
        de N × latência_média para ≈ 1 × latência_máxima.

        Returns:
            HarvestReport com estatísticas agregadas.
        """
        import asyncio

        from sas2dbx.transpile.llm.client import LLMClient

        client = LLMClient(self._llm_config)
        report = HarvestReport()

        self._generated_dir.mkdir(parents=True, exist_ok=True)

        async def _process_file(filename: str) -> tuple[str, dict, int, int, Exception | None]:
            logger.info("LLMHarvester: processando %s", filename)
            try:
                entries, tokens, skipped = await self._harvest_file(client, filename)
                return filename, entries, tokens, skipped, None
            except Exception as exc:  # noqa: BLE001
                logger.error("LLMHarvester: erro em %s — %s", filename, exc)
                return filename, {}, 0, 0, exc

        results = await asyncio.gather(*[_process_file(fn) for fn in _LLM_TARGET_FILES])

        for filename, entries, tokens, skipped, exc in results:
            if exc is not None:
                report.errors.append(f"{filename}: {exc}")
            else:
                self._write_generated(filename, entries)
                report.sources_processed.append(filename)
                report.entries_per_file[filename] = len(entries)
                report.tokens_used += tokens
                report.skipped_entries += skipped
                logger.info(
                    "LLMHarvester: %s → %d entradas válidas, %d descartadas (%d tokens)",
                    filename,
                    len(entries),
                    skipped,
                    tokens,
                )

        return report

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _harvest_file(
        self,
        client: object,
        filename: str,
    ) -> tuple[dict, int, int]:
        """Chama o LLM para um arquivo de mapeamento e retorna entradas válidas.

        Returns:
            Tupla (entradas_válidas, tokens_consumidos, n_descartadas).
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

        return entries, response.tokens_used, skipped

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

        tmp = target.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            yaml.dump(merged, f, allow_unicode=True, default_flow_style=False, sort_keys=True)
        os.replace(tmp, target)
        logger.debug("LLMHarvester: gravado %s (%d entradas)", target, len(merged))


# ---------------------------------------------------------------------------
# Prompts para harvest individual (um item por vez)
# ---------------------------------------------------------------------------

_SINGLE_PROMPTS: dict[str, str] = {
    "function": """\
Você é um especialista em migração SAS para PySpark.

Gere o mapeamento YAML para a função SAS "{key}" para PySpark.

Responda APENAS com YAML neste schema exato:
```yaml
{key}:
  pyspark: "expressão PySpark equivalente com placeholders {{col}}, {{arg1}}"
  notes: "diferenças de comportamento, ordem de args, NULL handling"
  confidence: 0.0
```

Se não há equivalente direto, use confidence < 0.5 e explique alternativa em notes.
Responda APENAS com o YAML, sem texto adicional.""",

    "proc": """\
Você é um especialista em migração SAS para PySpark/Databricks.

Gere o mapeamento YAML para o PROC SAS "{key}" para PySpark.

Schema exato:
```yaml
{key}:
  approach: "rule"
  tier: "Tier.RULE"
  notes: "abordagem de transpilação, opções principais, gotchas"
  confidence: 0.0
```

approach deve ser: "rule" | "llm" | "manual"
tier deve ser: "Tier.RULE" | "Tier.LLM" | "Tier.MANUAL"
Responda APENAS com o YAML.""",

    "sql_dialect": """\
Você é um especialista em diferenças entre SAS PROC SQL e Spark SQL.

Gere a regra de diferença de dialeto para "{key}".

Schema exato:
```yaml
{key}:
  sas: "exemplo de sintaxe SAS SQL"
  spark: "equivalente Spark SQL"
  notes: "explicação da diferença e como converter"
  confidence: 0.0
```

Responda APENAS com o YAML.""",

    "format": """\
Você é um especialista em formatos SAS e conversão para PySpark.

Gere o mapeamento para o formato SAS "{key}".

Schema exato:
```yaml
{key}:
  pyspark: "expressão PySpark para aplicar o formato"
  notes: "diferenças e gotchas"
  confidence: 0.0
```

Responda APENAS com o YAML.""",

    "informat": """\
Você é um especialista em informats SAS e conversão para PySpark.

Gere o mapeamento para o informat SAS "{key}".

Schema exato:
```yaml
{key}:
  pyspark: "expressão PySpark para ler/converter"
  notes: "diferenças e gotchas"
  confidence: 0.0
```

Responda APENAS com o YAML.""",
}


# Prompt para harvest em batch com contexto de transpilação real (Gap 3)
_CONTEXT_HARVEST_PROMPT = """\
Você é um especialista em migração SAS para PySpark/Databricks.

O bloco SAS abaixo foi transpilado para PySpark. Usando essa transpilação como \
contexto, gere mapeamentos YAML para as funções SAS listadas que aparecem no código.

FUNÇÕES A MAPEAR: {func_list}

CÓDIGO SAS ORIGINAL:
```sas
{sas_code}
```

CÓDIGO PYSPARK GERADO:
```python
{pyspark_code}
```

Para CADA função listada, gere uma entrada no formato exato:
```yaml
NOME_FUNCAO:
  pyspark: "expressão PySpark equivalente (use {{col}}, {{arg1}} como placeholders)"
  notes: "diferenças de comportamento, ordem de args, NULL handling"
  confidence: 0.85
```

Regras:
- Inclua APENAS as funções listadas em FUNÇÕES A MAPEAR
- Se a função não tem equivalente direto, use confidence < 0.5 e explique em notes
- Base a equivalência no que você vê no código PySpark gerado
- Responda APENAS com o bloco YAML, sem texto adicional"""


# ---------------------------------------------------------------------------
# LLMSingleHarvester — harvest de uma entrada por vez (on-demand)
# ---------------------------------------------------------------------------


class LLMSingleHarvester:
    """Harvesta uma única entrada via LLM (para uso on-demand pelo KnowledgeStore).

    Diferente do LLMHarvester (batch), este gera uma entrada por vez
    com prompt focado e response parsing mais restrito.

    Args:
        llm_client: LLMClient configurado (ou qualquer objeto com método complete()).
    """

    def __init__(self, llm_client: object) -> None:
        self._llm = llm_client

    def harvest_single_sync(self, category: str, key: str) -> dict | None:
        """Versão síncrona de harvest_single() — compatível com código não-async.

        Seguro em contexto async (FastAPI/uvicorn): detecta loop em execução
        e usa ThreadPoolExecutor para evitar RuntimeError de loop aninhado.

        Args:
            category: "function" | "proc" | "sql_dialect" | "format" | "informat"
            key: Nome da entrada (ex: "PRXMATCH", "TRANSPOSE", "WEEKDATE.")

        Returns:
            Dict com o mapeamento ou None se LLM não conseguiu.
        """
        return _run_sync(self.harvest_single(category=category, key=key))

    async def harvest_single(self, category: str, key: str) -> dict | None:
        """Gera mapeamento para uma única entrada via LLM.

        Args:
            category: "function" | "proc" | "sql_dialect" | "format" | "informat"
            key: Nome da entrada (ex: "PRXMATCH", "TRANSPOSE", "WEEKDATE.")

        Returns:
            Dict com o mapeamento ou None se LLM retornou YAML inválido/vazio.
        """
        prompt_template = _SINGLE_PROMPTS.get(category)
        if prompt_template is None:
            logger.warning(
                "LLMSingleHarvester: categoria desconhecida '%s'", category
            )
            return None

        prompt = prompt_template.format(key=key)
        response = await self._llm.complete(prompt)  # type: ignore[attr-defined]

        return self._parse_single_response(response.content, key, category)

    def harvest_from_context_sync(
        self,
        func_names: list[str],
        sas_code: str,
        pyspark_code: str,
    ) -> dict[str, dict | None]:
        """Versão síncrona de harvest_from_context().

        Usa o contexto real de uma transpilação (código SAS + PySpark gerado)
        para inferir mapeamentos de múltiplas funções em uma única chamada LLM.

        Args:
            func_names: Funções SAS a mapear (desconhecidas na KB).
            sas_code: Código SAS original do bloco transpilado.
            pyspark_code: Código PySpark gerado pela transpilação.

        Returns:
            Dict {func_name → entry_dict} — entry_dict pode ser None se não mapeado.
        """
        return _run_sync(
            self.harvest_from_context(
                func_names=func_names,
                sas_code=sas_code,
                pyspark_code=pyspark_code,
            )
        )

    async def harvest_from_context(
        self,
        func_names: list[str],
        sas_code: str,
        pyspark_code: str,
    ) -> dict[str, dict | None]:
        """Extrai mapeamentos de funções SAS usando contexto de transpilação real.

        Uma única chamada LLM para múltiplas funções, com contexto rico:
        o LLM vê como as funções foram efetivamente usadas no código SAS e
        qual foi o equivalente PySpark gerado — resultado mais preciso que
        o harvest standalone (que não tem contexto de uso).

        Args:
            func_names: Funções SAS a mapear.
            sas_code: Bloco SAS original (contexto de uso).
            pyspark_code: PySpark gerado (contexto do equivalente).

        Returns:
            Dict {func_name_upper → entry_dict | None}.
        """
        if not func_names:
            return {}

        # Trunca código para não exceder context window do LLM
        sas_snippet = sas_code[:1500] if len(sas_code) > 1500 else sas_code
        py_snippet = pyspark_code[:1500] if len(pyspark_code) > 1500 else pyspark_code
        func_list_str = ", ".join(func_names[:20])  # máx 20 funções por chamada

        prompt = _CONTEXT_HARVEST_PROMPT.format(
            func_list=func_list_str,
            sas_code=sas_snippet,
            pyspark_code=py_snippet,
        )

        try:
            response = await self._llm.complete(prompt)  # type: ignore[attr-defined]
        except Exception as exc:  # noqa: BLE001
            logger.warning("LLMSingleHarvester.harvest_from_context: LLM call falhou: %s", exc)
            return {}

        raw = self._try_parse_yaml(self._strip_fences(response.content))
        if not raw:
            logger.warning(
                "LLMSingleHarvester.harvest_from_context: YAML inválido para %s", func_names
            )
            return {}

        results: dict[str, dict | None] = {}
        for func_name in func_names:
            key = func_name.upper()
            entry = raw.get(key) or raw.get(func_name)
            if isinstance(entry, dict) and {"pyspark", "notes", "confidence"} <= entry.keys():
                conf = entry.get("confidence")
                if isinstance(conf, (int, float)) and 0.0 <= conf <= 1.0:
                    results[key] = entry
                    continue
            results[key] = None  # inválido ou ausente — será registrado como failure

        logger.debug(
            "LLMSingleHarvester.harvest_from_context: %d/%d funções mapeadas",
            sum(1 for v in results.values() if v is not None),
            len(func_names),
        )
        return results

    def _parse_single_response(
        self, content: str, key: str, category: str
    ) -> dict | None:
        """Extrai e valida o dict de mapeamento da resposta do LLM.

        Tenta 3 formas de extrair:
          1. {key: {...}} → retorna o value
          2. Dict direto com campos do schema (sem wrapper de chave)
          3. Extrai bloco ```yaml e repete

        Returns:
            Dict do mapeamento validado ou None.
        """
        content = content.strip()

        # Remove fences markdown (```yaml ... ``` ou ``` ... ```)
        clean = self._strip_fences(content)

        data = self._try_parse_yaml(clean)
        if data is None:
            logger.warning(
                "LLMSingleHarvester: YAML inválido para %s '%s'", category, key
            )
            return None

        # Caso 1: {KEY: {...}}
        if key in data and isinstance(data[key], dict):
            return data[key]

        # Caso 2: dict direto com campos do schema
        schema_fields = {"pyspark", "notes", "confidence", "approach", "sas", "spark"}
        if isinstance(data, dict) and schema_fields & data.keys():
            return data

        logger.warning(
            "LLMSingleHarvester: formato inesperado para %s '%s': keys=%s",
            category, key, list(data.keys())[:5],
        )
        return None

    @staticmethod
    def _strip_fences(content: str) -> str:
        """Remove delimitadores de bloco de código markdown."""
        lines = content.splitlines()
        # Remove primeira linha se for ``` ou ```yaml
        if lines and lines[0].strip().startswith("```"):
            lines = lines[1:]
        # Remove última linha se for ```
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        return "\n".join(lines).strip()

    @staticmethod
    def _try_parse_yaml(content: str) -> dict | None:
        """Tenta parsear YAML; retorna None em falha."""
        try:
            result = yaml.safe_load(content)
            if isinstance(result, dict):
                return result
        except yaml.YAMLError:
            pass
        return None
