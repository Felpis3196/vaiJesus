"""
Validador de Base Financeira e Escala
REGRA 1-5: Validação crítica antes de cálculos financeiros.
Evita uso de totais consolidados como valores mensais.
"""
import logging
from typing import Dict, Any, Optional, List, Tuple
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

# Frases obrigatórias
FRASE_ERRO_ESCALA = "Erro de escala identificado."
FRASE_TOTAL_ACUMULADO = "Possível uso de total acumulado como valor mensal."
FRASE_BASE_INDEFINIDA = "Base financeira indefinida."
FRASE_BASE_INCORRETA = "Base de cálculo incorreta invalida análise financeira."
FRASE_NAO_CONFIAVEL = "Não confiável para cálculo financeiro."

# Frases proibidas (não usar quando base é incerta)
FRASES_PROIBIDAS_BASE_INCERTA = [
    "A conta fecha corretamente",
    "Créditos elevados são aceitáveis",
    "Valores consistentes",
]


def identify_column_purpose(df: pd.DataFrame, column_name: str, sample_values: List[float]) -> Dict[str, Any]:
    """
    REGRA 1: Identifica explicitamente o propósito da coluna.
    Retorna: {"purpose": "mensal"|"acumulado"|"total_historico"|"auxiliar"|"indefinido", "confidence": float}
    
    Heurísticas adicionais:
    - Verifica nomes de colunas (Total, Acumulado, Histórico)
    - Detecta valores múltiplos de 1000 (possível erro de escala)
    - Detecta crescimento rápido (10x entre linhas consecutivas)
    """
    if df is None or df.empty or column_name not in df.columns:
        return {"purpose": "indefinido", "confidence": 0.0, "reason": "Coluna não encontrada"}
    
    col_data = df[column_name].dropna()
    if len(col_data) == 0:
        return {"purpose": "indefinido", "confidence": 0.0, "reason": "Coluna vazia"}
    
    # HEURÍSTICA 1: Verificar nome da coluna
    column_name_lower = str(column_name).lower()
    name_indicators = {
        "total": ("total_historico", 0.8),
        "acumulado": ("acumulado", 0.8),
        "acumulad": ("acumulado", 0.8),
        "historico": ("total_historico", 0.7),
        "histórico": ("total_historico", 0.7),
        "histor": ("total_historico", 0.7),
    }
    for keyword, (purpose, conf) in name_indicators.items():
        if keyword in column_name_lower:
            logger.debug(f"Coluna '{column_name}' identificada como {purpose} por nome (contém '{keyword}')")
            return {
                "purpose": purpose,
                "confidence": conf,
                "reason": f"Nome da coluna contém '{keyword}' indicando {purpose}"
            }
    
    # Converter para numérico
    try:
        numeric_values = pd.to_numeric(col_data, errors="coerce").dropna()
        if len(numeric_values) == 0:
            return {"purpose": "auxiliar", "confidence": 0.5, "reason": "Valores não numéricos"}
    except Exception:
        return {"purpose": "indefinido", "confidence": 0.0, "reason": "Erro na conversão"}
    
    # Análise de padrão
    values_array = numeric_values.values
    mean_val = float(np.mean(values_array))
    std_val = float(np.std(values_array))
    max_val = float(np.max(values_array))
    min_val = float(np.min(values_array))
    
    # HEURÍSTICA 2: Verificar se valores são múltiplos de 1000 (possível erro de escala)
    # Se muitos valores são múltiplos exatos de 1000, pode indicar erro de escala
    if len(values_array) > 0 and mean_val > 1000:
        multiples_of_1000 = sum(1 for v in values_array if abs(v) > 0 and abs(v) % 1000 < 0.01)
        if multiples_of_1000 / len(values_array) > 0.5:  # Mais de 50% são múltiplos de 1000
            logger.warning(f"Coluna '{column_name}': {multiples_of_1000}/{len(values_array)} valores são múltiplos de 1000. Possível erro de escala.")
    
    # Padrão de acumulado: valores sempre crescentes ou muito grandes
    is_monotonic_increasing = len(values_array) > 1 and all(
        values_array[i] <= values_array[i+1] for i in range(len(values_array)-1)
    )
    
    # HEURÍSTICA 3: Detectar crescimento rápido (multiplicação por 10x entre linhas consecutivas)
    rapid_growth_detected = False
    if len(values_array) > 1:
        growth_factors = []
        for i in range(len(values_array) - 1):
            if values_array[i] > 0:
                growth_factor = values_array[i+1] / values_array[i]
                if growth_factor > 0:
                    growth_factors.append(growth_factor)
        
        if growth_factors:
            avg_growth = float(np.mean(growth_factors))
            max_growth = float(np.max(growth_factors))
            # Se crescimento médio > 5x ou algum crescimento > 10x, é suspeito
            if avg_growth > 5.0 or max_growth > 10.0:
                rapid_growth_detected = True
                logger.warning(f"Coluna '{column_name}': Crescimento rápido detectado (média={avg_growth:.2f}x, máximo={max_growth:.2f}x). Possível acumulado.")
    
    # Padrão de total histórico: valor final muito maior que médias
    ratio_max_mean = max_val / mean_val if mean_val > 0 else 0
    
    # Padrão de mensal: variação razoável, não monotônico crescente
    cv = std_val / mean_val if mean_val > 0 else 0  # coeficiente de variação
    
    # Decisão com heurísticas melhoradas
    if rapid_growth_detected:
        return {
            "purpose": "acumulado",
            "confidence": 0.85,
            "reason": f"Crescimento rápido detectado (possível acumulado). Razão max/média = {ratio_max_mean:.2f}"
        }
    elif is_monotonic_increasing and ratio_max_mean > 5:
        return {
            "purpose": "acumulado",
            "confidence": 0.8,
            "reason": f"Padrão monotônico crescente com razão max/média = {ratio_max_mean:.2f}"
        }
    elif ratio_max_mean > 10 and max_val > 1_000_000:
        return {
            "purpose": "total_historico",
            "confidence": 0.7,
            "reason": f"Valor máximo muito alto ({max_val:,.2f}) comparado à média ({mean_val:,.2f})"
        }
    elif 0.1 < cv < 2.0 and not is_monotonic_increasing and not rapid_growth_detected:
        return {
            "purpose": "mensal",
            "confidence": 0.7,
            "reason": f"Variação razoável (CV={cv:.2f}), não monotônico, sem crescimento rápido"
        }
    elif std_val < mean_val * 0.05:  # Muito constante
        return {
            "purpose": "auxiliar",
            "confidence": 0.6,
            "reason": "Valores muito constantes (possível campo auxiliar)"
        }
    else:
        return {
            "purpose": "indefinido",
            "confidence": 0.3,
            "reason": f"Padrão não identificável (CV={cv:.2f}, ratio={ratio_max_mean:.2f})"
        }


def validate_monthly_scale(
    creditos: float,
    debitos: float,
    debitos_individuals: Optional[List[float]] = None
) -> Dict[str, Any]:
    """
    REGRA 2 e 3: Validação de plausibilidade mensal.
    Retorna: {"valid": bool, "error_type": str, "message": str, "scale_factor": float}
    """
    if creditos <= 0 and debitos <= 0:
        return {
            "valid": False,
            "error_type": "sem_dados",
            "message": FRASE_BASE_INDEFINIDA,
            "scale_factor": 1.0
        }
    
    # Calcular média de débitos individuais se disponível
    if debitos_individuals and len(debitos_individuals) > 0:
        debitos_array = np.array([d for d in debitos_individuals if d > 0])
        if len(debitos_array) > 0:
            debito_medio = float(np.mean(debitos_array))
        else:
            debito_medio = debitos
    else:
        debito_medio = debitos
    
    # REGRA 2: Crédito > 10x média de débitos = erro de escala
    if debito_medio > 0:
        scale_factor = creditos / debito_medio
        if scale_factor > 10:
            return {
                "valid": False,
                "error_type": "erro_escala",
                "message": f"{FRASE_ERRO_ESCALA} {FRASE_TOTAL_ACUMULADO} Crédito ({creditos:,.2f}) é {scale_factor:.1f}x maior que débito médio ({debito_medio:,.2f}).",
                "scale_factor": scale_factor
            }
    
    # REGRA 3: Ordem de grandeza muito diferente
    if debitos > 0:
        ordem_credito = len(str(int(abs(creditos))))
        ordem_debito = len(str(int(abs(debitos))))
        if abs(ordem_credito - ordem_debito) > 2:  # Mais de 2 ordens de grandeza
            return {
                "valid": False,
                "error_type": "ordem_grandeza",
                "message": f"{FRASE_ERRO_ESCALA} Crédito e débito em ordens de grandeza muito diferentes (crédito: {ordem_credito} dígitos, débito: {ordem_debito} dígitos).",
                "scale_factor": 10 ** abs(ordem_credito - ordem_debito)
            }
    
    # REGRA 3: Variação razoável (±30-50%)
    if debitos > 0:
        ratio = creditos / debitos
        if ratio > 3.0:  # Crédito > 3x débito (muito alto para condomínio)
            return {
                "valid": False,
                "error_type": "ratio_incompativel",
                "message": f"{FRASE_TOTAL_ACUMULADO} Crédito ({creditos:,.2f}) é {ratio:.1f}x maior que débito ({debitos:,.2f}). Suspeita de coluna incorreta ou total histórico.",
                "scale_factor": ratio
            }
    
    return {
        "valid": True,
        "error_type": None,
        "message": "Escala validada",
        "scale_factor": 1.0
    }


def validate_financial_base(
    df: pd.DataFrame,
    creditos_col: Optional[str] = None,
    debitos_col: Optional[str] = None,
    valor_col: Optional[str] = None
) -> Dict[str, Any]:
    """
    REGRA 1-5: Validação completa da base financeira antes de cálculos.
    Retorna: {"base_valid": bool, "warnings": List[str], "errors": List[str], "column_analysis": Dict}
    """
    warnings = []
    errors = []
    column_analysis = {}
    
    if df is None or df.empty:
        return {
            "base_valid": False,
            "warnings": [FRASE_BASE_INDEFINIDA],
            "errors": ["DataFrame vazio"],
            "column_analysis": {}
        }
    
    # Identificar propósito das colunas (REGRA 1)
    colunas_analisadas = []
    if creditos_col and creditos_col in df.columns:
        creditos_sample = df[creditos_col].dropna().head(50).tolist()
        col_creditos = identify_column_purpose(df, creditos_col, creditos_sample)
        column_analysis[creditos_col] = col_creditos
        colunas_analisadas.append(creditos_col)
        if col_creditos["purpose"] in ("acumulado", "total_historico"):
            errors.append(f"Coluna '{creditos_col}' identificada como {col_creditos['purpose']} (não mensal). {col_creditos['reason']}")
        elif col_creditos["purpose"] == "indefinido" and col_creditos["confidence"] < 0.5:
            warnings.append(f"Coluna '{creditos_col}': propósito indefinido. {FRASE_NAO_CONFIAVEL}")
    
    if debitos_col and debitos_col in df.columns:
        debitos_sample = df[debitos_col].dropna().head(50).tolist()
        col_debitos = identify_column_purpose(df, debitos_col, debitos_sample)
        column_analysis[debitos_col] = col_debitos
        colunas_analisadas.append(debitos_col)
        if col_debitos["purpose"] in ("acumulado", "total_historico"):
            errors.append(f"Coluna '{debitos_col}' identificada como {col_debitos['purpose']} (não mensal). {col_debitos['reason']}")
        elif col_debitos["purpose"] == "indefinido" and col_debitos["confidence"] < 0.5:
            warnings.append(f"Coluna '{debitos_col}': propósito indefinido. {FRASE_NAO_CONFIAVEL}")
    
    if valor_col and valor_col in df.columns:
        valor_sample = df[valor_col].dropna().head(50).tolist()
        col_valor = identify_column_purpose(df, valor_col, valor_sample)
        column_analysis[valor_col] = col_valor
        colunas_analisadas.append(valor_col)
        if col_valor["purpose"] in ("acumulado", "total_historico"):
            warnings.append(f"Coluna '{valor_col}' pode conter valores acumulados. Verificar antes de somar.")
    
    # Se não há colunas identificadas como mensais com confiança suficiente
    colunas_mensais = [
        c for c, info in column_analysis.items()
        if info.get("purpose") == "mensal" and info.get("confidence", 0) >= 0.6
    ]
    if not colunas_mensais:
        errors.append(f"{FRASE_BASE_INDEFINIDA} Nenhuma coluna identificada como movimentação mensal com confiança suficiente.")
    
    # Calcular totais para validação de escala (REGRA 2 e 3)
    creditos_total = 0
    debitos_total = 0
    debitos_individuals = []
    
    try:
        if creditos_col and creditos_col in df.columns:
            creditos_total = float(pd.to_numeric(df[creditos_col], errors="coerce").sum())
        elif valor_col and valor_col in df.columns:
            # Tentar inferir créditos de valor positivo
            df_positivo = df[df[valor_col] > 0]
            creditos_total = float(pd.to_numeric(df_positivo[valor_col], errors="coerce").sum()) if not df_positivo.empty else 0.0
        
        if debitos_col and debitos_col in df.columns:
            debitos_total = float(pd.to_numeric(df[debitos_col], errors="coerce").sum())
            debitos_individuals = pd.to_numeric(df[debitos_col], errors="coerce").dropna().tolist()
        elif valor_col and valor_col in df.columns:
            # Tentar inferir débitos de valor negativo
            df_negativo = df[df[valor_col] < 0]
            if not df_negativo.empty:
                debitos_total = abs(float(pd.to_numeric(df_negativo[valor_col], errors="coerce").sum()))
                debitos_individuals = abs(pd.to_numeric(df_negativo[valor_col], errors="coerce").dropna()).tolist()
        
        # Validar escala (REGRA 2 e 3)
        if creditos_total > 0 or debitos_total > 0:
            scale_validation = validate_monthly_scale(creditos_total, debitos_total, debitos_individuals)
            if not scale_validation["valid"]:
                errors.append(scale_validation["message"])
                logger.warning(f"[VALIDATION] Erro de escala detectado: {scale_validation.get('message')}. Crédito={creditos_total:,.2f}, Débito={debitos_total:,.2f}")
            else:
                logger.debug(f"[VALIDATION] Escala validada: Crédito={creditos_total:,.2f}, Débito={debitos_total:,.2f}")
    except Exception as e:
        warnings.append(f"Erro ao calcular totais para validação: {e}")
    
    base_valid = len(errors) == 0
    
    return {
        "base_valid": base_valid,
        "warnings": warnings,
        "errors": errors,
        "column_analysis": column_analysis,
        "creditos_total": creditos_total,
        "debitos_total": debitos_total,
    }


def get_validation_status_message(validation_result: Dict[str, Any]) -> str:
    """Retorna mensagem de status da validação para exibição no relatório."""
    if validation_result.get("base_valid"):
        return "Base financeira validada."
    
    errors = validation_result.get("errors", [])
    warnings = validation_result.get("warnings", [])
    
    if errors:
        return f"{FRASE_BASE_INCORRETA} {' '.join(errors[:2])}"
    elif warnings:
        return f"{FRASE_NAO_CONFIAVEL} {' '.join(warnings[:2])}"
    else:
        return FRASE_BASE_INDEFINIDA


def calculate_value_confidence(
    base_validation: Dict[str, Any],
    scale_validation: Dict[str, Any],
    extracted_totals_validation: Optional[Dict[str, Any]] = None,
    cross_validation: Optional[Dict[str, Any]] = None,
    column_analysis: Optional[Dict[str, Dict[str, Any]]] = None
) -> float:
    """
    Calcula score de confiança (0-1) para valores financeiros baseado em múltiplas validações.
    
    Fatores de confiança:
    - Validação de escala passou: +0.4
    - Valores extraídos vs calculados coincidem (±5%): +0.3
    - Coluna identificada como mensal com alta confiança: +0.2
    - Sem warnings de base: +0.1
    
    Valores com confiança < 0.5 não devem ser usados para cálculos críticos.
    
    Args:
        base_validation: Resultado de validate_financial_base
        scale_validation: Resultado de validate_monthly_scale
        extracted_totals_validation: Resultado de validate_extracted_totals (opcional)
        cross_validation: Resultado de cross_validate_totals (opcional)
        column_analysis: Análise de colunas de validate_financial_base (opcional)
    
    Returns:
        float: Score de confiança entre 0.0 e 1.0
    """
    confidence = 0.0
    
    # Fator 1: Validação de escala passou (+0.4)
    if scale_validation.get("valid", False):
        confidence += 0.4
        logger.debug("Confiança +0.4: Validação de escala passou")
    else:
        logger.debug(f"Confiança não aumentada: Validação de escala falhou ({scale_validation.get('message', 'erro desconhecido')})")
    
    # Fator 2: Valores extraídos vs calculados coincidem (±5%) (+0.3)
    if cross_validation and cross_validation.get("match", False):
        confidence += 0.3
        logger.debug("Confiança +0.3: Valores extraídos vs calculados coincidem")
    elif cross_validation and cross_validation.get("confidence", 0) > 0.8:
        # Confiança parcial se diferença é pequena mas não perfeita
        confidence += 0.15
        logger.debug(f"Confiança +0.15: Validação cruzada com confiança parcial ({cross_validation.get('confidence', 0):.2f})")
    
    # Fator 3: Coluna identificada como mensal com alta confiança (+0.2)
    if column_analysis:
        mensal_columns = [
            col for col, info in column_analysis.items()
            if info.get("purpose") == "mensal" and info.get("confidence", 0) >= 0.6
        ]
        if mensal_columns:
            confidence += 0.2
            logger.debug(f"Confiança +0.2: {len(mensal_columns)} coluna(s) identificada(s) como mensal com alta confiança")
    
    # Fator 4: Sem warnings de base (+0.1)
    if base_validation.get("base_valid", False) and not base_validation.get("warnings"):
        confidence += 0.1
        logger.debug("Confiança +0.1: Base válida sem warnings")
    elif base_validation.get("base_valid", False):
        # Base válida mas com warnings - confiança parcial
        confidence += 0.05
        logger.debug("Confiança +0.05: Base válida mas com warnings")
    
    # Penalidades: reduzir confiança se há erros críticos
    if base_validation.get("base_invalid", False):
        confidence *= 0.3  # Reduzir drasticamente se base inválida
        logger.warning("Confiança reduzida: Base financeira inválida")
    
    if scale_validation.get("error_type") in ("erro_escala", "ordem_grandeza"):
        confidence *= 0.4  # Reduzir se erro de escala crítico
        logger.warning("Confiança reduzida: Erro de escala crítico detectado")
    
    # Garantir que confiança está entre 0 e 1
    confidence = max(0.0, min(1.0, confidence))
    
    logger.info(f"Confiança calculada: {confidence:.2f} (base_valid={base_validation.get('base_valid')}, scale_valid={scale_validation.get('valid')}, cross_match={cross_validation.get('match') if cross_validation else None})")
    
    return confidence


# ETAPA 2: Validação matemática por conta (saldo_atual = saldo_anterior + receitas - despesas)
def _safe_numeric(val: Any) -> float:
    """Converte valor para float; retorna 0.0 se inválido."""
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return 0.0
    try:
        return float(pd.to_numeric(val, errors="coerce"))
    except (TypeError, ValueError):
        return 0.0


def validate_balance_per_conta(
    df: pd.DataFrame,
    tolerance: float = 0.02,
) -> Dict[str, Any]:
    """
    ETAPA 2: Valida por conta a fórmula saldo_atual = saldo_anterior + receitas - despesas.
    Agrupa por coluna 'conta' ou 'categoria'; quando há saldo_anterior/saldo_atual extraídos
    do bloco, verifica se a conta fecha dentro da tolerância.

    Args:
        df: DataFrame com colunas de valor (credito/debito ou valor + tipo) e opcionalmente conta/categoria.
        tolerance: Tolerância absoluta para considerar saldo fechado (ex.: 0.02).

    Returns:
        Dict com:
            - per_conta: List[Dict] com conta, receitas, despesas, saldo_anterior, saldo_atual, formula_ok;
            - total_geral_ok: bool (mantido para compatibilidade; validação do total geral é feita no consolidator);
            - has_conta_column: bool.
    """
    result: Dict[str, Any] = {
        "per_conta": [],
        "total_geral_ok": True,
        "has_conta_column": False,
    }
    if df is None or df.empty:
        return result

    col_conta = None
    for c in ("conta", "categoria"):
        if c in df.columns and df[c].notna().any():
            col_conta = c
            break
    if not col_conta:
        return result

    result["has_conta_column"] = True
    desc_col = "descricao" if "descricao" in df.columns else ("historico" if "historico" in df.columns else None)
    valor_col = None
    for v in ("valor_para_calculo", "valor_normalizado", "valor"):
        if v in df.columns:
            valor_col = v
            break
    credito_col = "credito" if "credito" in df.columns else None
    debito_col = "debito" if "debito" in df.columns else None
    tipo_col = "tipo" if "tipo" in df.columns else None

    if not valor_col and not (credito_col and debito_col):
        return result

    # Contas únicas (excluir vazios e rótulos que parecem totais)
    contas = df[col_conta].dropna().astype(str).str.strip()
    contas = contas[contas != ""]
    skip = {"total", "totais", "geral", "resumo", "soma", "subtotal"}
    contas = contas[~contas.str.lower().isin(skip)].unique().tolist()

    for conta in contas:
        grp = df[df[col_conta].astype(str).str.strip() == conta]
        receitas = 0.0
        despesas = 0.0

        if credito_col and debito_col:
            receitas = _safe_numeric(grp[credito_col].sum())
            despesas = _safe_numeric(grp[debito_col].sum())
        elif valor_col and tipo_col:
            tipos = grp[tipo_col].astype(str).str.strip().str.lower()
            mask_rec = tipos.isin(("receita", "credito", "crédito"))
            mask_des = tipos.isin(("despesa", "debito", "débito"))
            rec_vals = grp.loc[mask_rec, valor_col] if mask_rec.any() else pd.Series(dtype=float)
            des_vals = grp.loc[mask_des, valor_col] if mask_des.any() else pd.Series(dtype=float)
            receitas = _safe_numeric(rec_vals.sum())
            despesas = _safe_numeric(des_vals.sum())
        elif valor_col:
            receitas = _safe_numeric(grp[valor_col].clip(lower=0).sum())
            despesas = abs(_safe_numeric(grp[valor_col].clip(upper=0).sum()))

        saldo_anterior = None
        saldo_atual = None
        if desc_col:
            desc_lower = grp[desc_col].astype(str).str.strip().str.lower()
            for idx, row in grp.iterrows():
                d = (row.get(desc_col) or "")
                if not isinstance(d, str):
                    d = str(d)
                d = d.strip().lower()
                val = _safe_numeric(row.get(valor_col) or row.get(credito_col) or row.get(debito_col))
                if not val:
                    continue
                if "saldo anterior" in d or "saldo inicial" in d:
                    saldo_anterior = val
                if "saldo atual" in d or "saldo final" in d or "total da conta" in d:
                    saldo_atual = val

        formula_ok: Optional[bool] = None
        if saldo_atual is not None:
            esperado = (saldo_anterior or 0.0) + receitas - despesas
            formula_ok = abs(float(saldo_atual) - esperado) <= tolerance

        result["per_conta"].append({
            "conta": conta,
            "receitas": round(receitas, 2),
            "despesas": round(despesas, 2),
            "saldo_anterior": round(saldo_anterior, 2) if saldo_anterior is not None else None,
            "saldo_atual": round(saldo_atual, 2) if saldo_atual is not None else None,
            "formula_ok": formula_ok,
        })

    return result
