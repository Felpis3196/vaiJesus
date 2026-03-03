"""
Resolvedor Hierárquico de Fontes Financeiras

Implementa a hierarquia obrigatória de fontes para resolver conflitos entre
valores extraídos do texto e valores calculados do DataFrame.

HIERARQUIA (nesta ordem):
  1. Valores mensais explícitos (linhas/colunas de receitas e despesas do mês)
  2. Totais mensais calculáveis (soma de receitas/despesas individuais do mês)
  3. Valores acumulados identificados (usar apenas como referência, nunca como resultado)
  4. Totais históricos / saldos globais (NUNCA usar como valor mensal)

REGRA DE OURO: Detectar erro não basta. A IA deve saber QUAL valor usar no lugar.
"""
import logging
from typing import Dict, Any, Optional, List

logger = logging.getLogger(__name__)

# Frases obrigatórias quando substituição ocorre
FRASE_ACUMULADO_DESCARTADO = "Valor acumulado identificado e descartado."
FRASE_RESUMO_MENSAL = "Resumo financeiro baseado na movimentação mensal plausível."
FRASE_MENSAL_PRIORIZADO = "Valores mensais priorizados sobre totais históricos."
FRASE_BASE_MENSAL_LIMITACOES = "Base mensal identificada com limitações documentais."


def _is_plausible_monthly_pair(receitas: Optional[float], despesas: Optional[float]) -> bool:
    """
    REGRA 3 — Plausibilidade condominial.
    Verifica se receitas e despesas são plausíveis como valores mensais de condomínio.
    """
    if (receitas is None or receitas <= 0) and (despesas is None or despesas <= 0):
        return False

    # Se ambos existem, checar proporção
    if receitas is not None and receitas > 0 and despesas is not None and despesas > 0:
        ratio = receitas / despesas if despesas > 0 else float('inf')
        # Razão entre 0.3 e 3.0 é plausível para condomínio (receita ~ despesa)
        if ratio > 10 or ratio < 0.1:
            return False

    return True


def _detect_scale_error(extracted_val: Optional[float], calculated_val: Optional[float], field: str) -> Optional[Dict[str, Any]]:
    """
    REGRA 2 e 3 — Detecta se valor extraído tem erro de escala comparado ao calculado.
    Retorna None se OK, ou Dict com detalhes do erro.
    """
    if extracted_val is None or calculated_val is None:
        return None
    if extracted_val <= 0 or calculated_val <= 0:
        return None

    ratio = extracted_val / calculated_val
    # REGRA 3: Se extraído > 100x calculado, é claramente acumulado/total histórico
    if ratio > 100:
        return {
            "field": field,
            "original": extracted_val,
            "replaced_by": calculated_val,
            "ratio": ratio,
            "reason": f"Valor extraído (R$ {extracted_val:,.2f}) é {ratio:.0f}x maior que valor mensal calculado (R$ {calculated_val:,.2f}). Provável total acumulado/histórico."
        }
    # Se extraído > 10x calculado, é provável erro de escala
    if ratio > 10:
        return {
            "field": field,
            "original": extracted_val,
            "replaced_by": calculated_val,
            "ratio": ratio,
            "reason": f"Valor extraído (R$ {extracted_val:,.2f}) é {ratio:.1f}x maior que valor mensal calculado (R$ {calculated_val:,.2f}). Possível total acumulado."
        }
    # Ordem de grandeza muito diferente (>2 dígitos de diferença)
    ordem_ext = len(str(int(abs(extracted_val)))) if extracted_val > 0 else 0
    ordem_calc = len(str(int(abs(calculated_val)))) if calculated_val > 0 else 0
    if abs(ordem_ext - ordem_calc) > 2:
        return {
            "field": field,
            "original": extracted_val,
            "replaced_by": calculated_val,
            "ratio": ratio,
            "reason": f"Valor extraído ({ordem_ext} dígitos) e calculado ({ordem_calc} dígitos) em ordens de grandeza muito diferentes."
        }

    return None


def resolve_financial_values(
    extracted_totals: Dict[str, Any],
    calculated_totals: Dict[str, Any],
    saldo_anterior: Optional[float] = None
) -> Dict[str, Any]:
    """
    REGRA 1: Hierarquia de fontes. Resolve conflitos entre múltiplas bases numéricas.

    Quando múltiplos valores existirem para o mesmo conceito (crédito, débito, saldo):
      1. Valores mensais explícitos (extraídos do texto, se plausíveis)
      2. Totais mensais calculáveis (calculados do DataFrame)
      3. Valores acumulados identificados (referência, nunca como resultado)

    REGRA 2: Se valor marcado como "erro de escala" ou "possível total acumulado",
    substitui automaticamente pela melhor alternativa válida.

    REGRA 3: Plausibilidade condominial — descartar automaticamente valores 100x maiores.

    REGRA 5: Saldo final = saldo_anterior + (creditos - debitos), nunca zero.

    REGRA 6: Relatório não pode ser contraditório — valor descartado não aparece no resumo.

    Args:
        extracted_totals: Valores do texto (totals_extracted) — pode ser plano ou com "values"
        calculated_totals: Valores do DataFrame (calculate_financial_totals_correct)
        saldo_anterior: Saldo anterior do período (se disponível)

    Returns:
        Dict com valores resolvidos, fonte de cada valor, substituições realizadas e observações.
    """
    substitutions: List[Dict[str, Any]] = []
    observacoes: List[str] = []
    source = "calculated"  # default

    # --- Extrair valores de ambas as fontes ---

    # Calculados (podem ser None = NÃO APURADO)
    calc_receitas_raw = calculated_totals.get("total_receitas")
    calc_despesas_raw = calculated_totals.get("total_despesas")
    calc_saldo_raw = calculated_totals.get("saldo")
    
    # REGRA 4.1: Tratar None como NÃO APURADO, não como 0
    calc_receitas = float(calc_receitas_raw) if calc_receitas_raw is not None else None
    calc_despesas = float(calc_despesas_raw) if calc_despesas_raw is not None else None
    
    # Calcular saldo só se ambos existirem
    if calc_receitas is not None and calc_despesas is not None:
        calc_saldo = calc_receitas - calc_despesas
    elif calc_saldo_raw is not None:
        calc_saldo = float(calc_saldo_raw)
    else:
        calc_saldo = None

    # Extraídos (podem ser None / inexistentes)
    extracted_values = {}
    if extracted_totals:
        if isinstance(extracted_totals, dict) and "values" in extracted_totals:
            extracted_values = extracted_totals.get("values", {}) or {}
        else:
            extracted_values = extracted_totals

    ext_receitas_raw = extracted_values.get("total_receitas")
    ext_despesas_raw = extracted_values.get("total_despesas")
    ext_deficit_raw = extracted_values.get("deficit")
    ext_saldo_anterior = extracted_values.get("saldo_anterior")
    ext_saldo_final = extracted_values.get("saldo_final")

    ext_receitas = abs(float(ext_receitas_raw)) if ext_receitas_raw is not None else None
    ext_despesas = abs(float(ext_despesas_raw)) if ext_despesas_raw is not None else None

    # Verificar flags de validação do extractor
    ext_validation = {}
    if isinstance(extracted_totals, dict):
        ext_validation = extracted_totals.get("validation", {}) or {}
    ext_has_scale_error = ext_validation.get("scale_error", False)
    
    # REGRA CRÍTICA: Detectar valores acumulados extraídos do texto
    # Valores > 1.000.000 são quase sempre acumulados (ex: 58.383.445,04)
    # Valores mensais são tipicamente < 500.000 (ex: 65.395,04, 70.095,37)
    if ext_receitas is not None and ext_receitas > 1_000_000:
        ext_has_scale_error = True
        logger.warning(f"[RESOLVER] Receita extraída do texto é muito grande (R$ {ext_receitas:,.2f}). Provavelmente acumulado. Marcando como erro de escala.")
    if ext_despesas is not None and ext_despesas > 1_000_000:
        ext_has_scale_error = True
        logger.warning(f"[RESOLVER] Despesa extraída do texto é muito grande (R$ {ext_despesas:,.2f}). Provavelmente acumulado. Marcando como erro de escala.")

    # --- RESOLUÇÃO HIERÁRQUICA ---

    # REGRA 4.1: Inicializar com valores calculados (podem ser None)
    final_receitas = calc_receitas
    final_despesas = calc_despesas
    receitas_source = "calculated"
    despesas_source = "calculated"

    # RECEITAS: Hierarquia de fontes
    if ext_receitas is not None:
        if ext_has_scale_error:
            # REGRA 2: Valor extraído marcado com erro de escala — usar calculado (se disponível)
            if calc_receitas is not None:
                sub = {
                    "field": "total_receitas",
                    "original": ext_receitas,
                    "replaced_by": calc_receitas,
                    "reason": f"{FRASE_ACUMULADO_DESCARTADO} Valor extraído (R$ {ext_receitas:,.2f}) descartado por erro de escala. Usando valor mensal calculado (R$ {calc_receitas:,.2f})."
                }
                substitutions.append(sub)
                final_receitas = calc_receitas
                receitas_source = "calculated"
                logger.info(f"[RESOLVER] Receitas: substituição por erro de escala. Extraído R$ {ext_receitas:,.2f} -> Calculado R$ {calc_receitas:,.2f}")
            else:
                # Calculado não disponível - manter extraído mas marcar incerteza
                final_receitas = ext_receitas
                receitas_source = "extracted"
        elif calc_receitas is not None and calc_receitas > 0:
            # REGRA 3: Verificar plausibilidade comparativa
            # REGRA CRÍTICA: Se valor extraído > 1.000.000, sempre usar calculado (mensal)
            if ext_receitas > 1_000_000:
                substitutions.append({
                    "field": "total_receitas",
                    "original": ext_receitas,
                    "replaced_by": calc_receitas,
                    "reason": f"Valor extraído do texto (R$ {ext_receitas:,.2f}) é acumulado. Usando valor mensal calculado do DataFrame (R$ {calc_receitas:,.2f})."
                })
                final_receitas = calc_receitas
                receitas_source = "calculated"
                logger.info(f"[RESOLVER] Receitas: substituição por valor acumulado detectado. Extraído R$ {ext_receitas:,.2f} -> Calculado R$ {calc_receitas:,.2f}")
            else:
                scale_issue = _detect_scale_error(ext_receitas, calc_receitas, "total_receitas")
                if scale_issue:
                    substitutions.append(scale_issue)
                    final_receitas = calc_receitas
                    receitas_source = "calculated"
                    logger.info(f"[RESOLVER] Receitas: substituição por plausibilidade. Extraído R$ {ext_receitas:,.2f} -> Calculado R$ {calc_receitas:,.2f}")
                else:
                    # Valor extraído é plausível — usar (nível 1 na hierarquia)
                    final_receitas = ext_receitas
                    receitas_source = "extracted"
        else:
            # Sem valor calculado para comparar — usar extraído se razoável
            final_receitas = ext_receitas
            receitas_source = "extracted"

    # DESPESAS: Hierarquia de fontes
    if ext_despesas is not None:
        if calc_despesas is not None and calc_despesas > 0:
            # REGRA CRÍTICA: Se valor extraído > 1.000.000, sempre usar calculado (mensal)
            if ext_despesas > 1_000_000:
                substitutions.append({
                    "field": "total_despesas",
                    "original": ext_despesas,
                    "replaced_by": calc_despesas,
                    "reason": f"Valor extraído do texto (R$ {ext_despesas:,.2f}) é acumulado. Usando valor mensal calculado do DataFrame (R$ {calc_despesas:,.2f})."
                })
                final_despesas = calc_despesas
                despesas_source = "calculated"
                logger.info(f"[RESOLVER] Despesas: substituição por valor acumulado detectado. Extraído R$ {ext_despesas:,.2f} -> Calculado R$ {calc_despesas:,.2f}")
            else:
                scale_issue = _detect_scale_error(ext_despesas, calc_despesas, "total_despesas")
                if scale_issue:
                    substitutions.append(scale_issue)
                    final_despesas = calc_despesas
                    despesas_source = "calculated"
                    logger.info(f"[RESOLVER] Despesas: substituição por plausibilidade. Extraído R$ {ext_despesas:,.2f} -> Calculado R$ {calc_despesas:,.2f}")
                else:
                    final_despesas = ext_despesas
                    despesas_source = "extracted"
        else:
            # Usar extraído se calculado não disponível ou zero
            # Mas ainda verificar se não é acumulado
            if ext_despesas > 1_000_000:
                logger.warning(f"[RESOLVER] Despesa extraída do texto é muito grande (R$ {ext_despesas:,.2f}) mas não há valor calculado disponível. Usando mesmo assim.")
            final_despesas = ext_despesas
            despesas_source = "extracted"

    # Determinar fonte geral
    if receitas_source == "extracted" and despesas_source == "extracted":
        source = "extracted"
    elif receitas_source == "calculated" and despesas_source == "calculated":
        source = "calculated"
    else:
        source = "mixed"

    # --- SALDO (REGRA 5 / ETAPA 5) ---
    # ETAPA 5: Só calcular se ambos existirem
    final_saldo = None
    if final_receitas is not None and final_despesas is not None:
        final_saldo = final_receitas - final_despesas
    elif calc_saldo is not None:
        final_saldo = calc_saldo

    # Deficit extraído: usar se a fonte dos valores finais é "extracted" e deficit está disponível
    if ext_deficit_raw is not None and source == "extracted":
        final_saldo = -abs(float(ext_deficit_raw))

    # Saldo anterior
    final_saldo_anterior = saldo_anterior
    if ext_saldo_anterior is not None:
        final_saldo_anterior = float(ext_saldo_anterior)

    # REGRA 5 / ETAPA 5: Saldo final = saldo_anterior + (creditos - debitos), só se ambos existirem
    final_saldo_final = None
    if final_saldo_anterior is not None and final_saldo is not None:
        final_saldo_final = final_saldo_anterior + final_saldo
    elif final_saldo is not None:
        final_saldo_final = final_saldo

    # Saldo final demonstrado (do documento)
    final_saldo_final_demonstrado = None
    if ext_saldo_final is not None:
        final_saldo_final_demonstrado = float(ext_saldo_final)

    # --- OBSERVAÇÕES OBRIGATÓRIAS (REGRA 6) ---
    if substitutions:
        observacoes.append(FRASE_ACUMULADO_DESCARTADO)
        observacoes.append(FRASE_RESUMO_MENSAL)
        observacoes.append(FRASE_MENSAL_PRIORIZADO)
        for sub in substitutions:
            observacoes.append(
                f"Campo '{sub['field']}': valor original R$ {sub['original']:,.2f} substituído por R$ {sub['replaced_by']:,.2f}. Razão: {sub.get('reason', 'erro de escala')}"
            )

    # Verificar se base mensal existe (REGRA 4)
    has_monthly_base = (final_receitas is not None and final_receitas > 0) or (final_despesas is not None and final_despesas > 0)

    logger.info(
        f"[RESOLVER] Resultado: receitas={_format_value_for_log(final_receitas)} ({receitas_source}), "
        f"despesas={_format_value_for_log(final_despesas)} ({despesas_source}), "
        f"saldo={_format_value_for_log(final_saldo)}, "
        f"substituições={len(substitutions)}, "
        f"has_monthly_base={has_monthly_base}"
    )

    return {
        "total_receitas": round(final_receitas, 2) if final_receitas is not None else None,
        "total_despesas": round(final_despesas, 2) if final_despesas is not None else None,
        "saldo": round(final_saldo, 2) if final_saldo is not None else None,
        "saldo_final": round(final_saldo_final, 2) if final_saldo_final is not None else None,
        "saldo_anterior": final_saldo_anterior,
        "saldo_final_demonstrado": final_saldo_final_demonstrado,
        "source": source,
        "receitas_source": receitas_source,
        "despesas_source": despesas_source,
        "substitutions": substitutions,
        "observacoes": observacoes,
        "has_monthly_base": has_monthly_base,
    }


def _format_value_for_log(valor: Optional[float]) -> str:
    """Formata valor para log (None = NÃO APURADO)."""
    if valor is None:
        return "NÃO APURADO"
    return f"R$ {valor:,.2f}"
