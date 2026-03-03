"""
ETAPA 4: Fallback Numérico (Anti-Zero)
REGRA 4.1: Zero é proibido se existe qualquer valor > 0 no arquivo.
REGRA 4.2: Se não há receita mas há despesas, créditos = NÃO APURADO.
REGRA 4.3: Uso controlado de TOTAIS como débito.
"""
import logging
import pandas as pd
from typing import Dict, Any, Optional, List

logger = logging.getLogger(__name__)

# Representa "NÃO APURADO" (não é zero!)
NAO_APURADO = None


def apply_numerical_fallback(
    extracted_data: Dict[str, Any],
    df: pd.DataFrame
) -> Dict[str, Any]:
    """
    ETAPA 4: Aplica fallback numérico anti-zero.
    
    REGRA 4.1: Zero é proibido se existe qualquer valor > 0 no arquivo.
    REGRA 4.2: Se não há receita mas há despesas, créditos = NÃO APURADO.
    REGRA 4.3: Uso controlado de TOTAIS como débito quando não há despesas individuais.
    
    Args:
        extracted_data: Resultado de extract_monthly_financial_data()
        df: DataFrame original
        
    Returns:
        Dict com valores finais após fallback:
        {
            "creditos_mes": float | None,  # None = NÃO APURADO
            "debitos_mes": float | None,
            "resultado_mes": float | None,  # None = NÃO CALCULADO
            "saldo_final": float | None,  # None = NÃO APURADO
            "fallback_applied": bool,
            "fallback_reason": str
        }
    """
    if df is None or df.empty:
        return {
            "creditos_mes": None,  # NÃO APURADO
            "debitos_mes": None,  # NÃO APURADO
            "resultado_mes": None,  # NÃO CALCULADO
            "saldo_final": None,  # NÃO APURADO
            "fallback_applied": False,
            "fallback_reason": "DataFrame vazio"
        }
    
    receitas = extracted_data.get("receitas_mensais_extraidas", [])
    despesas = extracted_data.get("despesas_mensais_extraidas", [])
    
    # Verificar se há valores no arquivo (REGRA 4.1)
    has_any_value = False
    for col in df.columns:
        try:
            if pd.api.types.is_numeric_dtype(df[col]):
                if df[col].abs().sum() > 0:
                    has_any_value = True
                    break
        except Exception:
            continue
    
    # REGRA 4.1: Se há valores no arquivo, zero é PROIBIDO
    if not has_any_value:
        # Arquivo realmente vazio - zero é permitido
        return {
            "creditos_mes": 0.0,
            "debitos_mes": 0.0,
            "resultado_mes": 0.0,
            "saldo_final": None,
            "fallback_applied": False,
            "fallback_reason": "Arquivo vazio - zero permitido"
        }
    
    # Calcular totais das extrações
    total_receitas = sum(r["valor"] for r in receitas if r.get("valor") is not None) if receitas else 0.0
    total_despesas = sum(d["valor"] for d in despesas if d.get("valor") is not None) if despesas else 0.0
    
    # Rejeitar totais em milhões (valor mensal plausível para condomínio < 1M)
    LIMITE_MENSAL_PLAUSIVEL = 1_000_000.0
    if total_receitas >= LIMITE_MENSAL_PLAUSIVEL:
        logger.warning(f"[FALLBACK] Total de receitas rejeitado (valor em milhões): R$ {total_receitas:,.2f}. Usando NÃO APURADO.")
        total_receitas = 0.0
    if total_despesas >= LIMITE_MENSAL_PLAUSIVEL:
        logger.warning(f"[FALLBACK] Total de despesas rejeitado (valor em milhões): R$ {total_despesas:,.2f}. Usando NÃO APURADO.")
        total_despesas = 0.0
    
    logger.info(f"[FALLBACK] Totais calculados: receitas={total_receitas:,.2f} ({len(receitas)} itens), despesas={total_despesas:,.2f} ({len(despesas)} itens)")
    
    # Coletar despesas individuais para REGRA 4.3
    despesas_individuals = [d["valor"] for d in despesas] if despesas else []
    
    # REGRA 4.3: Fallback de débito usando TOTAIS
    if total_receitas == 0.0 and total_despesas == 0.0:
        # Tentar usar TOTAIS como débito (REGRA 4.3)
        from app.extraction.legacy.financial_extractor import _extract_totais_as_debito_fallback
        total_despesas_fallback = _extract_totais_as_debito_fallback(df, despesas_individuals)
        
        if total_despesas_fallback:
            logger.info(f"[FALLBACK] Usando linha TOTAIS como débito: R$ {total_despesas_fallback:,.2f}")
            return {
                "creditos_mes": None,  # NÃO APURADO
                "debitos_mes": total_despesas_fallback,
                "resultado_mes": None,  # NÃO CALCULADO
                "saldo_final": None,  # NÃO APURADO
                "fallback_applied": True,
                "fallback_reason": "Valores extraídos por fallback mínimo para evitar zeramento artificial"
            }
    
    # REGRA 4.2: Se não há receita mas há despesas
    if total_receitas == 0.0 and total_despesas > 0.0:
        logger.info(f"[FALLBACK] Preservando despesas (R$ {total_despesas:,.2f}) mesmo sem receita explícita")
        return {
            "creditos_mes": None,  # NÃO APURADO
            "debitos_mes": total_despesas,
            "resultado_mes": None,  # NÃO CALCULADO
            "saldo_final": None,  # NÃO APURADO
            "fallback_applied": True,
            "fallback_reason": "Despesa mensal preservada mesmo sem receita explícita"
        }
    
    # Se ambos existem, usar valores extraídos
    # REGRA 4.1: Nunca retornar 0.0 se há valores no arquivo
    creditos_final = total_receitas if total_receitas > 0 else None
    debitos_final = total_despesas if total_despesas > 0 else None
    
    return {
        "creditos_mes": creditos_final,
        "debitos_mes": debitos_final,
        "resultado_mes": None,  # Será calculado na ETAPA 5
        "saldo_final": None,  # Será calculado na ETAPA 5
        "fallback_applied": False,
        "fallback_reason": ""
    }
