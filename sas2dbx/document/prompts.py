"""Templates de prompt para documentação de jobs SAS via LLM.

Funções puras — sem estado, sem dependências externas além dos modelos.
"""

from __future__ import annotations

from sas2dbx.models.sas_ast import ClassificationResult


def build_job_doc_prompt(
    job_name: str,
    sas_code: str,
    inputs: list[str],
    outputs: list[str],
    libnames: list[str],
    macros_called: list[str],
    macros_defined: list[str],
    constructs: list[ClassificationResult],
    prereqs: list[str],
    dependents: list[str],
    knowledge_context: str = "",
) -> str:
    """Monta o prompt para geração de README.md de um job SAS.

    Args:
        job_name: Nome do job (filename sem extensão).
        sas_code: Código SAS completo do job.
        inputs: Datasets lidos pelo job.
        outputs: Datasets criados/sobrescritos pelo job.
        libnames: Bibliotecas declaradas no job.
        macros_called: Macros invocadas.
        macros_defined: Macros definidas neste job.
        constructs: Lista de ClassificationResult por bloco.
        prereqs: Jobs que devem executar antes.
        dependents: Jobs que dependem deste.
        knowledge_context: Contexto técnico do Knowledge Store (vazio se indisponível).

    Returns:
        Prompt completo pronto para envio ao LLM.
    """
    constructs_text = _format_constructs(constructs)
    prereqs_text = ", ".join(prereqs) if prereqs else "nenhum (job de carga primária)"
    dependents_text = ", ".join(dependents) if dependents else "nenhum"
    inputs_text = ", ".join(inputs) if inputs else "não detectados"
    outputs_text = ", ".join(outputs) if outputs else "não detectados"
    libnames_text = ", ".join(libnames) if libnames else "nenhuma"
    macros_called_text = ", ".join(macros_called) if macros_called else "nenhuma"
    macros_defined_text = ", ".join(macros_defined) if macros_defined else "nenhuma"

    ks_section = ""
    if knowledge_context.strip():
        ks_section = f"\nCONTEXTO TÉCNICO (Knowledge Store):\n{knowledge_context}\n"

    return f"""Você é um documentador técnico de jobs SAS legados.

CONTEXTO: Este job faz parte de um ambiente SAS em migração para Databricks.
A documentação original foi perdida ou nunca existiu. Seu objetivo é gerar
documentação técnica clara que permita a qualquer desenvolvedor entender
o que o job faz sem ler o código SAS.

METADATA EXTRAÍDAS AUTOMATICAMENTE:
- Job: {job_name}
- Datasets de entrada: {inputs_text}
- Datasets de saída: {outputs_text}
- Bibliotecas usadas: {libnames_text}
- Macros chamadas: {macros_called_text}
- Macros definidas: {macros_defined_text}
- Construtos detectados:
{constructs_text}
- Dependências: prerequisitos={prereqs_text}
- Dependentes: {dependents_text}
{ks_section}
GERAR (em markdown, exatamente nesta ordem):

## Objetivo
(2-3 frases em linguagem de negócio — o que esse job faz, não como.
Se não for possível inferir o objetivo de negócio, escreva:
"Objetivo não inferível a partir do código — requer input do time de negócio")

## Diagrama de fluxo
(mermaid graph LR: inputs → transformações nomeadas → outputs.
Usar nomes curtos nos nodes. Deve ser renderizável.)

## Datasets
(tabela markdown: Dataset | Tipo | Descrição
Tipo = "Input" ou "Output". Descrição = inferida do contexto do código.)

## Transformações
(lista numerada na ordem de execução — o que cada bloco faz em 1 linha)

## Dependências
- **Prerequisitos:** {prereqs_text}
- **Dependentes:** {dependents_text}

## Construtos SAS detectados
(tabela: Construto | Tier | Confidence)

## Riscos e observações
(construtos Tier 3, lógica ambígua, macros não documentadas, gotchas de migração.
Cada risco deve ser acionável: dizer o que fazer, não apenas o que está errado.
Se não houver riscos, escrever "Nenhum construto Tier 3 detectado.")

IMPORTANTE:
- O objetivo deve ser em linguagem de negócio, não técnica
- O diagrama mermaid deve ser funcional (renderizável)
- NÃO incluir cabeçalho de nível 1 (# nome-do-job) — será adicionado externamente
- Responder APENAS o markdown solicitado, sem texto fora das seções

CÓDIGO SAS COMPLETO:
```sas
{sas_code}
```"""


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _format_constructs(constructs: list[ClassificationResult]) -> str:
    if not constructs:
        return "  (nenhum detectado)"
    lines = []
    for cr in constructs:
        tier_label = cr.tier.value.capitalize()
        confidence_pct = f"{cr.confidence:.0%}"
        lines.append(f"  - {cr.construct_type} | Tier {tier_label} | {confidence_pct}")
    return "\n".join(lines)
