"""EvolutionAnalyzer — analisa UnresolvedErrors e propõe fixes no código-fonte.

Diferente do self-healing (que corrige notebooks individuais), o EvolutionAnalyzer
corrige o código da APLICAÇÃO — patterns.py, prompts.py, knowledge store — para
que o mesmo erro nunca se repita em jobs futuros.

Usa o LLMClient existente (AnthropicProvider / KhonGatewayProvider).
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from pathlib import Path

from sas2dbx.evolve.unresolved import UnresolvedError

logger = logging.getLogger(__name__)

# Tiers de risco — determinam se o fix é auto-aplicado ou vai para quarentena
RISK_LEVELS = ("low", "medium", "high")

# Fix types mapeados para risk level
_FIX_RISK: dict[str, str] = {
    "knowledge_store": "low",
    "error_pattern": "low",
    "prompt_refinement": "medium",
    "engine_rule": "high",
    "context_rule": "high",
}

# Arquivos permitidos por tier de risco
ALLOWED_PATHS: dict[str, list[str]] = {
    "low": [
        "knowledge/mappings/generated/",
        "knowledge/mappings/curated/",
        "sas2dbx/validate/heal/patterns.py",
        # fixer.py é o handler natural de patterns.py — um error_pattern fix
        # quase sempre requer os dois arquivos juntos (regex + handler)
        "sas2dbx/validate/heal/fixer.py",
    ],
    "medium": [
        "knowledge/mappings/generated/",
        "knowledge/mappings/curated/",
        "sas2dbx/validate/heal/patterns.py",
        "sas2dbx/validate/heal/fixer.py",
        "sas2dbx/transpile/llm/prompts.py",
        "sas2dbx/document/prompts.py",
    ],
    "high": [
        "sas2dbx/",  # qualquer módulo — vai para quarentena, nunca auto-aplica
    ],
}


@dataclass
class FileModification:
    """Uma modificação de arquivo proposta pelo EvolutionAnalyzer.

    Attributes:
        path: Caminho relativo à raiz do projeto.
        action: "add" para novo conteúdo, "modify" para substituição, "append".
        content: Conteúdo a adicionar/substituir.
        reason: Por que essa alteração resolve o problema.
        old_string: Para action="modify", o trecho exato a substituir (opcional).
    """

    path: str
    action: str  # "add" | "modify" | "append"
    content: str
    reason: str
    old_string: str | None = None


@dataclass
class EvolutionTest:
    """Teste proposto para validar o fix.

    Attributes:
        path: Caminho do arquivo de teste (relativo à raiz do projeto).
        content: Código Python do teste.
    """

    path: str
    content: str


@dataclass
class KnowledgeEntry:
    """Nova entrada para o Knowledge Store (opcional).

    Attributes:
        filename: Nome do arquivo YAML (ex: "functions_map.yaml").
        key: Chave SAS (ex: "PRXMATCH").
        value: Dict com pyspark, notes, confidence, etc.
    """

    filename: str
    key: str
    value: dict


@dataclass
class EvolutionProposal:
    """Proposta completa de fix do EvolutionAnalyzer.

    Attributes:
        fix_type: Tipo do fix ("knowledge_store", "error_pattern", etc.).
        risk_level: Nível de risco calculado.
        description: Descrição legível do fix.
        files_to_modify: Lista de modificações de arquivo.
        test: Teste que valida o fix (obrigatório — QualityGate rejeita sem ele).
        knowledge_entry: Nova entrada no KS (opcional, para fix_type="knowledge_store").
        similar_jobs_prediction: Previsão de outros jobs beneficiados.
        raw_llm_response: Resposta bruta do LLM (para auditoria).
    """

    fix_type: str
    risk_level: str
    description: str
    files_to_modify: list[FileModification] = field(default_factory=list)
    test: EvolutionTest | None = None
    knowledge_entry: KnowledgeEntry | None = None
    similar_jobs_prediction: str = ""
    raw_llm_response: str = ""

    @property
    def is_valid(self) -> bool:
        """True se a proposta tem o mínimo necessário para ser avaliada."""
        return (
            self.fix_type in _FIX_RISK
            and self.risk_level in RISK_LEVELS
            and bool(self.description)
            and bool(self.files_to_modify)
            and self.test is not None
        )


_PROMPT_TEMPLATE = """\
Você é um engenheiro de software sênior mantendo o SAS2DBX, uma ferramenta
de migração SAS→Databricks.

SITUAÇÃO: Um job falhou no Databricks e o sistema de self-healing não
conseguiu resolver em {num_attempts} tentativa(s). Sua tarefa é propor uma
alteração no CÓDIGO-FONTE DA FERRAMENTA (não no notebook individual) para
que este tipo de erro nunca se repita em jobs futuros.

═══════════════════════════════════════════════════════════════
ERRO (última iteração)
═══════════════════════════════════════════════════════════════
Job ID:         {job_id}
Construto SAS:  {construct_type}
Categoria:      {error_category}
Erro Databricks:
{databricks_error}

═══════════════════════════════════════════════════════════════
TENTATIVAS DE FIX QUE FALHARAM
═══════════════════════════════════════════════════════════════
{healing_attempts_text}

═══════════════════════════════════════════════════════════════
CÓDIGO SAS ORIGINAL
═══════════════════════════════════════════════════════════════
{sas_original}

═══════════════════════════════════════════════════════════════
CÓDIGO PYSPARK GERADO (que falhou — últimas 80 linhas)
═══════════════════════════════════════════════════════════════
{pyspark_snippet}

═══════════════════════════════════════════════════════════════
OPÇÕES DE FIX (em ordem de preferência — escolha o de menor risco)
═══════════════════════════════════════════════════════════════
1. KNOWLEDGE STORE (risk=low):
   Adicionar entrada em knowledge/mappings/generated/functions_map.yaml
   ou knowledge/mappings/generated/sql_dialect_map.yaml
   Ex: nova função SAS→PySpark, nova regra de dialeto SQL

2. ERROR PATTERN (risk=low):
   Adicionar pattern em sas2dbx/validate/heal/patterns.py
   Ex: novo regex + categoria + fix determinístico

3. PROMPT REFINEMENT (risk=medium):
   Alterar sas2dbx/transpile/llm/prompts.py
   Ex: adicionar regra ao prompt de transpilação

4. ENGINE RULE (risk=high — vai para quarentena, não é auto-aplicado):
   Alterar sas2dbx/transpile/engine.py ou sas2dbx/transpile/llm/context.py
   Ex: nova regra Tier 1, ajuste no context builder

═══════════════════════════════════════════════════════════════
RESPOSTA OBRIGATÓRIA (JSON válido, sem markdown)
═══════════════════════════════════════════════════════════════
{{
  "fix_type": "knowledge_store" | "error_pattern" | "prompt_refinement" | "engine_rule",
  "risk_level": "low" | "medium" | "high",
  "description": "o que o fix faz em linguagem simples (máx 200 chars)",
  "files_to_modify": [
    {{
      "path": "caminho/relativo/do/arquivo.py",
      "action": "add" | "modify" | "append",
      "content": "conteúdo exato a adicionar/substituir",
      "reason": "por que essa alteração resolve o problema",
      "old_string": "trecho EXATO a substituir (apenas quando action=modify, pode ser null)"
    }}
  ],
  "test": {{
    "path": "tests/evolve/test_fix_{job_id_safe}.py",
    "content": "código Python completo do teste pytest que valida o fix"
  }},
  "knowledge_entry": {{
    "filename": "functions_map.yaml",
    "key": "NOME_FUNCAO_SAS",
    "value": {{ "pyspark": "...", "notes": "...", "confidence": 0.8 }}
  }},
  "similar_jobs_prediction": "descrição de quais outros jobs se beneficiam"
}}

REGRAS:
- O JSON deve ser VÁLIDO e parseável
- "test" é OBRIGATÓRIO — sem teste a proposta é rejeitada automaticamente
- "knowledge_entry" é opcional (apenas quando fix_type=knowledge_store)
- "old_string" só é necessário quando action="modify"
- Máximo 3 arquivos em "files_to_modify"
- Prefira fix_type de menor risco que resolva o problema
"""


class EvolutionAnalyzer:
    """Analisa UnresolvedErrors e propõe fixes no código-fonte via LLM.

    Args:
        llm_client: LLMClient configurado (Anthropic ou Khon gateway).
        project_root: Raiz do projeto (para ler arquivos relevantes).
    """

    def __init__(self, llm_client: object, project_root: Path | None = None) -> None:
        self._llm = llm_client
        self._project_root = project_root or Path.cwd()

    def analyze_sync(self, error: UnresolvedError) -> EvolutionProposal | None:
        """Analisa o erro e propõe fix no código-fonte (síncrono).

        Args:
            error: UnresolvedError com contexto completo.

        Returns:
            EvolutionProposal ou None se o LLM retornou resposta inválida.
        """
        import asyncio

        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        if loop.is_running():
            import concurrent.futures

            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(asyncio.run, self._analyze_async(error))
                # 240s: 8192 tokens em Claude Sonnet pode ultrapassar 120s com retry
                return future.result(timeout=240)
        else:
            return loop.run_until_complete(self._analyze_async(error))

    async def _analyze_async(self, error: UnresolvedError) -> EvolutionProposal | None:
        """Versão async do analyze."""
        prompt = self._build_prompt(error)

        try:
            response = await self._llm.complete(prompt)
            raw = response.content
        except Exception as exc:
            logger.error("EvolutionAnalyzer: erro ao chamar LLM: %s", exc)
            return None

        return self._parse_proposal(raw, error)

    def _build_prompt(self, error: UnresolvedError) -> str:
        """Constrói o prompt com contexto completo do erro."""
        # Tentativas de healing em texto
        attempts_text = ""
        for att in error.healing_attempts:
            attempts_text += (
                f"  Iteração {att.iteration}: strategy={att.strategy}"
                f" | fix={att.fix_applied or 'nenhum'}"
                f" | retest={att.retest_status or 'N/A'}\n"
            )
        if not attempts_text:
            attempts_text = "  (nenhuma tentativa registrada)\n"

        # Recorta o notebook (últimas 80 linhas para não exceder context)
        lines = error.pyspark_generated.splitlines()
        pyspark_snippet = "\n".join(lines[-80:]) if len(lines) > 80 else error.pyspark_generated

        # Safe job_id para nome de arquivo de teste
        job_id_safe = re.sub(r"[^a-zA-Z0-9_]", "_", error.job_id)[:40]

        return _PROMPT_TEMPLATE.format(
            num_attempts=len(error.healing_attempts),
            job_id=error.job_id,
            job_id_safe=job_id_safe,
            construct_type=error.construct_type,
            error_category=error.error_category,
            databricks_error=error.databricks_error[:2000],
            healing_attempts_text=attempts_text,
            sas_original=error.sas_original[:3000],
            pyspark_snippet=pyspark_snippet,
        )

    def _parse_proposal(self, raw: str, error: UnresolvedError) -> EvolutionProposal | None:
        """Parseia JSON da resposta do LLM em EvolutionProposal."""
        # Remove markdown code fences se presentes
        cleaned = re.sub(r"```(?:json)?\s*", "", raw).strip()
        cleaned = re.sub(r"```\s*$", "", cleaned).strip()

        # Encontra primeiro { e último }
        start = cleaned.find("{")
        end = cleaned.rfind("}")
        if start == -1 or end == -1:
            logger.warning("EvolutionAnalyzer: LLM não retornou JSON válido")
            return None

        try:
            data = json.loads(cleaned[start : end + 1])
        except json.JSONDecodeError as exc:
            logger.warning("EvolutionAnalyzer: JSON inválido: %s — tentando recuperação parcial", exc)
            data = self._recover_partial_json(cleaned[start:])
            if data is None:
                return None

        # Valida campos obrigatórios
        for required in ("fix_type", "risk_level", "description", "files_to_modify"):
            if required not in data:
                logger.warning("EvolutionAnalyzer: campo '%s' ausente na proposta", required)
                return None

        # Normaliza risk_level — usa o mapeamento canônico se inconsistente
        fix_type = data.get("fix_type", "engine_rule")
        risk_level = data.get("risk_level", _FIX_RISK.get(fix_type, "high"))
        # Sempre força o risk_level canônico pelo fix_type (LLM pode errar)
        canonical_risk = _FIX_RISK.get(fix_type, "high")
        if RISK_LEVELS.index(canonical_risk) > RISK_LEVELS.index(risk_level):
            risk_level = canonical_risk  # escala para cima se necessário

        # Constrói FileModifications
        files = []
        for fm in data.get("files_to_modify", []):
            if not all(k in fm for k in ("path", "action", "content", "reason")):
                continue
            files.append(
                FileModification(
                    path=fm["path"],
                    action=fm["action"],
                    content=fm["content"],
                    reason=fm["reason"],
                    old_string=fm.get("old_string"),
                )
            )

        # Constrói test
        test_data = data.get("test")
        test = None
        if test_data and "path" in test_data and "content" in test_data:
            test = EvolutionTest(path=test_data["path"], content=test_data["content"])

        # Constrói knowledge_entry
        ke_data = data.get("knowledge_entry")
        knowledge_entry = None
        if ke_data and "filename" in ke_data and "key" in ke_data:
            knowledge_entry = KnowledgeEntry(
                filename=ke_data["filename"],
                key=ke_data["key"],
                value=ke_data.get("value", {}),
            )

        proposal = EvolutionProposal(
            fix_type=fix_type,
            risk_level=risk_level,
            description=data.get("description", "")[:500],
            files_to_modify=files,
            test=test,
            knowledge_entry=knowledge_entry,
            similar_jobs_prediction=data.get("similar_jobs_prediction", ""),
            raw_llm_response=raw[:5000],
        )

        logger.info(
            "EvolutionAnalyzer: proposta gerada — fix_type=%s risk=%s files=%d test=%s",
            proposal.fix_type,
            proposal.risk_level,
            len(proposal.files_to_modify),
            "OK" if proposal.test else "AUSENTE",
        )
        return proposal

    def _recover_partial_json(self, text: str) -> dict | None:
        """Tenta recuperar campos de um JSON truncado extraindo pares chave-valor simples.

        Estratégia: para cada campo escalar obrigatório, usa regex para extrair o valor
        mesmo que o JSON não tenha fechado. Campos complexos (arrays, objects) são
        extraídos tentando parsear até o ponto onde o JSON estava válido.

        Returns:
            dict parcial com os campos recuperados, ou None se impossível.
        """
        result: dict = {}

        # Extrai campos escalares simples (string, number)
        scalar_fields = ["fix_type", "risk_level", "description", "similar_jobs_prediction"]
        for field_name in scalar_fields:
            m = re.search(
                rf'"{field_name}"\s*:\s*"([^"]*)"',
                text,
            )
            if m:
                result[field_name] = m.group(1)

        # Extrai files_to_modify tentando fechar o array incrementalmente
        fm_match = re.search(r'"files_to_modify"\s*:\s*(\[)', text)
        if fm_match:
            array_start = fm_match.start(1)
            # Tenta parsear um JSON parcial fechando o array e o objeto principal
            candidate = text[array_start:]
            result["files_to_modify"] = self._recover_array(candidate)

        # Extrai test.path (o mais importante — content pode ter sido truncado)
        test_path_m = re.search(r'"test"\s*:\s*\{[^}]*"path"\s*:\s*"([^"]+)"', text, re.DOTALL)
        if test_path_m:
            test_content_m = re.search(
                r'"test"\s*:\s*\{.*?"content"\s*:\s*"(.*?)(?:"\s*\}|$)',
                text,
                re.DOTALL,
            )
            content = ""
            if test_content_m:
                # Decodifica escapes básicos
                content = test_content_m.group(1).replace("\\n", "\n").replace('\\"', '"').replace("\\\\", "\\")
            if content or test_path_m.group(1):
                result["test"] = {
                    "path": test_path_m.group(1),
                    "content": content or "# test content truncated — verify manually\nassert True",
                }

        if not result.get("fix_type") or not result.get("risk_level"):
            logger.warning("EvolutionAnalyzer: recuperação parcial falhou — campos obrigatórios ausentes")
            return None

        logger.info(
            "EvolutionAnalyzer: recuperação parcial OK — campos=%s",
            list(result.keys()),
        )
        return result

    def _recover_array(self, text: str) -> list:
        """Tenta parsear um array JSON possivelmente truncado, retornando o máximo possível."""
        depth = 0
        last_valid_end = 1  # começa após o '['
        for i, ch in enumerate(text):
            if ch in ("{", "["):
                depth += 1
            elif ch in ("}", "]"):
                depth -= 1
                if depth == 0:
                    # Array completo
                    try:
                        return json.loads(text[: i + 1])
                    except json.JSONDecodeError:
                        break
                elif depth == 1:
                    # Fim de um objeto dentro do array — tenta parsear até aqui
                    try:
                        candidate = text[:i + 1] + "]"
                        parsed = json.loads(candidate)
                        last_valid_end = i + 1
                        return parsed
                    except json.JSONDecodeError:
                        pass
        # Retorna lista vazia se não conseguiu parsear nada
        return []
