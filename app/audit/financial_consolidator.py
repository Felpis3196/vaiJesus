"""
Consolidador Financeiro Correto
Corrige erros de cálculo matemático identificados:
1. Deduplicação de lançamentos repetidos
2. Filtragem de transferências internas e fundos
3. Normalização monetária (centavos -> reais)
4. Cálculo correto de saldo
5. Evita duplicação de holerites
6. Regra de gasto: >5% acima do mês passado e acima da inflação do mês
7. Validação de escala e base financeira (REGRA 1-5)
"""
import os
import re
import logging
import pandas as pd
from typing import Dict, Any, Optional, Tuple
from datetime import datetime
from .financial_base_validator import (
    validate_financial_base,
    validate_monthly_scale,
    get_validation_status_message,
    validate_balance_per_conta,
    FRASE_BASE_INDEFINIDA,
    FRASE_BASE_INCORRETA,
    FRASE_NAO_CONFIAVEL,
)
from .value_resolver import _format_value_for_log

logger = logging.getLogger(__name__)


def cross_validate_totals(
    calculated_totals: Dict[str, float],
    extracted_totals: Optional[Dict[str, Any]] = None,
    tolerance_percent: float = 20.0
) -> Dict[str, Any]:
    """
    Validação cruzada entre valores calculados e valores extraídos do texto.
    
    Compara valores e detecta discrepâncias significativas (>tolerance_percent).
    Se valores calculados são válidos e há discrepância, prefere valores calculados.
    
    Args:
        calculated_totals: Dict com valores calculados (total_receitas, total_despesas, saldo)
        extracted_totals: Dict com valores extraídos (pode ser estrutura plana ou com "values")
        tolerance_percent: Tolerância percentual para considerar valores como coincidentes (padrão: 20%)
    
    Returns:
        Dict com:
            - match: bool - Se valores coincidem dentro da tolerância
            - mismatches: List[Dict] - Lista de discrepâncias encontradas
            - prefer_calculated: bool - Se deve preferir valores calculados
            - confidence: float - Score de confiança (0-1)
    """
    if not extracted_totals:
        return {
            "match": True,
            "mismatches": [],
            "prefer_calculated": True,
            "confidence": 1.0
        }
    
    # Extrair estrutura (pode ser plana ou com "values")
    extracted_values = extracted_totals.get("values", extracted_totals) if isinstance(extracted_totals, dict) and "values" in extracted_totals else extracted_totals
    
    mismatches = []
    total_mismatches = 0
    
    # Comparar receitas
    if "total_receitas" in extracted_values and extracted_values["total_receitas"] is not None:
        calc_receitas = abs(float(calculated_totals.get("total_receitas", 0)))
        extr_receitas = abs(float(extracted_values["total_receitas"]))
        if calc_receitas > 0:
            diff_pct = abs(calc_receitas - extr_receitas) / calc_receitas * 100
            if diff_pct > tolerance_percent:
                mismatches.append({
                    "field": "total_receitas",
                    "calculated": calc_receitas,
                    "extracted": extr_receitas,
                    "difference_percent": diff_pct
                })
                total_mismatches += 1
                logger.warning(f"Discrepância em receitas: calculado={calc_receitas:,.2f}, extraído={extr_receitas:,.2f}, diferença={diff_pct:.1f}%")
    
    # Comparar despesas
    if "total_despesas" in extracted_values and extracted_values["total_despesas"] is not None:
        calc_despesas = abs(float(calculated_totals.get("total_despesas", 0)))
        extr_despesas = abs(float(extracted_values["total_despesas"]))
        if calc_despesas > 0:
            diff_pct = abs(calc_despesas - extr_despesas) / calc_despesas * 100
            if diff_pct > tolerance_percent:
                mismatches.append({
                    "field": "total_despesas",
                    "calculated": calc_despesas,
                    "extracted": extr_despesas,
                    "difference_percent": diff_pct
                })
                total_mismatches += 1
                logger.warning(f"Discrepância em despesas: calculado={calc_despesas:,.2f}, extraído={extr_despesas:,.2f}, diferença={diff_pct:.1f}%")
    
    # Comparar saldo (se disponível)
    if "deficit" in extracted_values and extracted_values["deficit"] is not None:
        calc_saldo = float(calculated_totals.get("saldo", 0))
        extr_saldo = -abs(float(extracted_values["deficit"]))  # deficit é negativo
        if abs(calc_saldo) > 0.01:  # Evitar divisão por zero
            diff_pct = abs(calc_saldo - extr_saldo) / abs(calc_saldo) * 100
            if diff_pct > tolerance_percent:
                mismatches.append({
                    "field": "saldo",
                    "calculated": calc_saldo,
                    "extracted": extr_saldo,
                    "difference_percent": diff_pct
                })
                total_mismatches += 1
                logger.warning(f"Discrepância em saldo: calculado={calc_saldo:,.2f}, extraído={extr_saldo:,.2f}, diferença={diff_pct:.1f}%")
    
    # Determinar se deve preferir valores calculados
    # Preferir calculados se há discrepâncias E valores calculados são válidos (sem base_invalid/scale_error)
    prefer_calculated = (
        total_mismatches > 0 and
        not calculated_totals.get("base_invalid", False) and
        not calculated_totals.get("scale_error", False)
    )
    
    # Calcular confiança: 1.0 se match, reduzido por cada mismatch
    confidence = max(0.0, 1.0 - (total_mismatches * 0.2))
    
    return {
        "match": total_mismatches == 0,
        "mismatches": mismatches,
        "prefer_calculated": prefer_calculated,
        "confidence": confidence,
        "total_mismatches": total_mismatches
    }


# Inflação mensal padrão (ex.: 0.5% = 0.005). Pode ser sobrescrita por INFLATION_MONTHLY_PCT (em %)
def _get_inflation_monthly_decimal() -> float:
    """Retorna inflação mensal em decimal (ex.: 0.005 para 0,5%). Fonte: variável de ambiente INFLATION_MONTHLY_PCT (em %)."""
    try:
        pct = os.environ.get("INFLATION_MONTHLY_PCT", "0.5")
        return float(pct) / 100.0
    except (ValueError, TypeError):
        return 0.005  # 0,5% padrão

# Keywords para identificar transferências internas (não devem ser contadas como receita/despesa)
KEYWORDS_TRANSFERENCIAS_INTERNAS = [
    r'\btransferencia\b', r'\btransferência\b', r'\btransfer\b',
    r'\baplicacao\b', r'\baplicação\b', r'\baplic\b',
    r'\bresgate\b', r'\bresg\b',
    r'\bfundo\s+de\s+reserva\b', r'\bfundo\s+reserva\b', r'\breserva\s+geral\b',
    r'\bestorno\b', r'\breversao\b', r'\breversão\b',
    r'\bcompensacao\b', r'\bcompensação\b',
    r'\bconta\s+corrente\b', r'\bcc\b',
    r'\bconta\s+poupanca\b', r'\bconta\s+poupança\b',
    r'\bentrada\s+para\s+saida\b', r'\bsaida\s+para\s+entrada\b',
]

# Keywords para identificar totais e subtotais (não devem ser somados novamente)
KEYWORDS_TOTAIS = [
    r'^total\b', r'^totais\b', r'\btotal\s+geral\b', r'\btotal\s+das?\b',
    r'\bsubtotal\b', r'\bsoma\b', r'\bsomatorio\b', r'\bsomatório\b',
    r'\bresumo\b',  # REGRA 2: adicionar RESUMO
    r'\btotal\s+de\s+receitas?\b', r'\btotal\s+de\s+despesas?\b',
    r'\btotal\s+receitas?\b', r'\btotal\s+despesas?\b',
    r'\btotal\s+dos\s+recebimentos\b', r'\btotal\s+das\s+despesas\b',
]

# Keywords para identificar holerites (para evitar duplicação)
KEYWORDS_HOLERITE = [
    r'\bholerite\b', r'\bcontracheque\b', r'\bfolha\s+de\s+pagamento\b',
    r'\bfolha\s+pagamento\b', r'\bsalario\s+bruto\b', r'\bsalário\s+bruto\b',
    r'\bsalario\s+liquido\b', r'\bsalário\s+líquido\b',
]


def _normalize_text(text: Any) -> str:
    """Normaliza texto para comparação."""
    if text is None or (isinstance(text, float) and pd.isna(text)):
        return ""
    return str(text).lower().strip()


def _matches_keywords(text: str, patterns: list) -> bool:
    """Verifica se texto corresponde a algum padrão."""
    text_norm = _normalize_text(text)
    for pattern in patterns:
        try:
            if re.search(pattern, text_norm, re.IGNORECASE):
                return True
        except re.error:
            if pattern.lower().replace(r'\b', '') in text_norm:
                return True
    return False


def _normalize_monetary_value(valor: Any) -> Optional[float]:
    """
    Extrai valor monetário com regra única: formato BR (ponto = milhar, vírgula = decimal).
    REGRA 7: Nunca zerar valores. Retorna None se não conseguir parsear (NÃO APURADO).
    """
    if valor is None or (isinstance(valor, float) and pd.isna(valor)):
        return None  # NÃO APURADO ao invés de 0.0
    
    try:
        if isinstance(valor, str):
            valor_clean = valor.replace('R$', '').replace('$', '').strip()
            if not valor_clean:  # String vazia após limpeza
                return None
            valor_clean = valor_clean.replace('.', '').replace(',', '.')
            valor_float = float(valor_clean)
        else:
            valor_float = float(valor)
        return round(valor_float, 2)
    except (ValueError, TypeError):
        return None  # NÃO APURADO ao invés de 0.0


def _is_duplicate_transaction(row: pd.Series, seen: set) -> bool:
    """
    Verifica se transação é duplicada: mesma data, mesmo valor, mesma descrição e mesma conta.
    Não considerar duplicado: encargos com mesma base e naturezas diferentes, parcelas, provisões.
    """
    desc = _normalize_text(row.get("descricao", ""))
    valor_raw = _normalize_monetary_value(row.get("valor", 0))
    valor = valor_raw if valor_raw is not None else 0.0
    data = str(row.get("data", ""))[:10]
    conta = _normalize_text(row.get("conta", "") or row.get("categoria", "")) if "conta" in row.index or "categoria" in row.index else ""

    key = (desc[:100], round(valor, 2), data, conta)
    if key in seen:
        return True
    seen.add(key)
    return False


def _is_internal_transfer(row: pd.Series) -> bool:
    """Verifica se é transferência interna (não deve ser contada)."""
    desc = _normalize_text(row.get("descricao", ""))
    categoria = _normalize_text(row.get("categoria", ""))
    
    # Verificar descrição
    if _matches_keywords(desc, KEYWORDS_TRANSFERENCIAS_INTERNAS):
        return True
    
    # Verificar categoria
    if _matches_keywords(categoria, KEYWORDS_TRANSFERENCIAS_INTERNAS):
        return True
    
    return False


def _is_total_line(row: pd.Series) -> bool:
    """Verifica se linha é um total/subtotal (não deve ser somada)."""
    desc = _normalize_text(row.get("descricao", ""))
    return _matches_keywords(desc, KEYWORDS_TOTAIS)


def _is_holerite_duplicate(row: pd.Series, holerite_seen: set) -> bool:
    """Verifica se holerite já foi contabilizado (evita somar bruto+líquido+encargos)."""
    desc = _normalize_text(row.get("descricao", ""))
    
    if not _matches_keywords(desc, KEYWORDS_HOLERITE):
        return False
    
    # Extrair período e funcionário se possível
    periodo = str(row.get("data", ""))[:7]  # YYYY-MM
    valor_raw = _normalize_monetary_value(row.get("valor", 0))
    # Tratar None como 0 para comparação de duplicatas
    valor = valor_raw if valor_raw is not None else 0.0
    
    # Criar chave baseada em período e valor aproximado
    # Se já vimos um holerite com mesmo período e valor similar (±10%), é duplicata
    for (p, v_range) in holerite_seen:
        if p == periodo and v_range[0] <= valor <= v_range[1]:
            return True
    
    # Adicionar à lista de holerites vistos (tolerância de ±10%)
    holerite_seen.add((periodo, (valor * 0.9, valor * 1.1)))
    return False


def calculate_financial_totals_correct(
    df: pd.DataFrame,
    saldo_inicial: Optional[float] = None,
    extracted_totals: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Calcula totais financeiros corretamente, evitando erros identificados:
    
    1. Deduplica lançamentos repetidos
    2. Filtra transferências internas
    3. Normaliza valores monetários
    4. Calcula saldo corretamente
    5. Evita duplicação de holerites
    
    Args:
        df: DataFrame com transações
        saldo_inicial: Saldo inicial do período (opcional)
        
    Returns:
        Dict com totais calculados corretamente
    """
    if df is None or df.empty:
        return {
            "total_receitas": None,  # REGRA 4.1: NÃO APURADO ao invés de 0.0
            "total_despesas": None,  # REGRA 4.1: NÃO APURADO ao invés de 0.0
            "saldo": None,  # REGRA 4.1: NÃO CALCULADO ao invés de 0.0
            "saldo_final": None,  # REGRA 4.1: NÃO APURADO ao invés de saldo_inicial or 0.0
            "transacoes_filtradas": 0,
            "transferencias_excluidas": 0,
            "duplicatas_removidas": 0,
            "totais_removidos": 0,
            "extracted_data": {},
            "fallback_result": {}
        }
    
    # Criar cópia para não modificar original
    df_work = df.copy()
    
    # Normalizar valores monetários (REGRA 7: não zerar, usar None para não apurado)
    if "valor" in df_work.columns:
        df_work["valor_normalizado"] = df_work["valor"].apply(_normalize_monetary_value)
        # Para cálculos, tratar None como 0, mas manter None na coluna para rastreamento
        df_work["valor_para_calculo"] = df_work["valor_normalizado"].fillna(0.0)
    else:
        df_work["valor_normalizado"] = None
        df_work["valor_para_calculo"] = 0.0
    
    # Totais do balancete (soma por tipo) — excluir linhas de tabela explicativa (encargos)
    # Débitos = apenas lançamentos reais; não usar tabela de encargos como fonte primária
    soma_balancete_receita: Optional[float] = None
    soma_balancete_despesa: Optional[float] = None
    if "tipo" in df_work.columns:
        col_v = "valor_para_calculo" if "valor_para_calculo" in df_work.columns else ("valor" if "valor" in df_work.columns else None)
        if col_v:
            mask_explicativo = (
                df_work["_apenas_explicativo"] == True
                if "_apenas_explicativo" in df_work.columns
                else pd.Series(False, index=df_work.index)
            )
            df_transacoes = df_work[~mask_explicativo]
            tipos = df_transacoes["tipo"].astype(str).str.strip().str.lower()
            mask_rec = tipos.isin(["receita", "credito", "crédito"])
            mask_des = tipos.isin(["despesa", "debito", "débito"])
            sr = float(df_transacoes.loc[mask_rec, col_v].sum())
            sd = float(df_transacoes.loc[mask_des, col_v].sum())
            if sr > 0 or sd > 0:
                soma_balancete_receita = sr if sr > 0 else None
                soma_balancete_despesa = sd if sd > 0 else None
                logger.info(f"[CONSOLIDATOR] Totais do balancete (transações, sem explicativos): receita={soma_balancete_receita}, despesa={soma_balancete_despesa}")
    
    # Estatísticas de filtragem
    stats = {
        "transacoes_filtradas": 0,
        "transferencias_excluidas": 0,
        "duplicatas_removidas": 0,
        "totais_removidos": 0,
    }
    
    # ETAPA 1: EXTRAÇÃO (nunca falha) - FAZER ANTES de remover linhas TOTAL
    # Isso garante que linhas TOTAL sejam coletadas e priorizadas
    from app.extraction.legacy.financial_extractor import extract_monthly_financial_data
    logger.info(f"[CONSOLIDATOR] Iniciando extração mensal de dados financeiros (DataFrame com {len(df_work)} linhas)")
    extracted_data = extract_monthly_financial_data(df_work)
    
    # 1. Remover linhas de totais/subtotais (após extração, para não perder dados)
    mask_totais = df_work.apply(_is_total_line, axis=1)
    stats["totais_removidos"] = int(mask_totais.sum())
    df_work = df_work[~mask_totais].copy()
    
    # 2. Remover transferências internas
    mask_transferencias = df_work.apply(_is_internal_transfer, axis=1)
    stats["transferencias_excluidas"] = int(mask_transferencias.sum())
    df_work = df_work[~mask_transferencias].copy()
    
    # 3. Deduplicar transações
    seen_transactions = set()
    holerite_seen = set()
    
    mask_duplicatas = df_work.apply(
        lambda row: _is_duplicate_transaction(row, seen_transactions) or 
                     _is_holerite_duplicate(row, holerite_seen),
        axis=1
    )
    stats["duplicatas_removidas"] = int(mask_duplicatas.sum())
    df_work = df_work[~mask_duplicatas].copy()
    
    stats["transacoes_filtradas"] = len(df_work)
    logger.info(f"[CONSOLIDATOR] Extração concluída: {len(extracted_data.get('receitas_mensais_extraidas', []))} receitas, {len(extracted_data.get('despesas_mensais_extraidas', []))} despesas")
    
    # Verificar se valores TOTAL foram extraídos e priorizados
    receitas_extraidas = extracted_data.get('receitas_mensais_extraidas', [])
    despesas_extraidas = extracted_data.get('despesas_mensais_extraidas', [])
    if receitas_extraidas:
        total_receitas_extracted = sum(r.get('valor', 0) for r in receitas_extraidas)
        logger.info(f"[CONSOLIDATOR] Total de receitas extraídas: R$ {total_receitas_extracted:,.2f} ({len(receitas_extraidas)} itens)")
    if despesas_extraidas:
        total_despesas_extracted = sum(d.get('valor', 0) for d in despesas_extraidas)
        logger.info(f"[CONSOLIDATOR] Total de despesas extraídas: R$ {total_despesas_extracted:,.2f} ({len(despesas_extraidas)} itens)")
    
    # PRIORIDADE: Se temos valores extraídos do texto, usar eles como primeira opção
    if extracted_totals:
        extracted_values = {}
        if isinstance(extracted_totals, dict) and "values" in extracted_totals:
            extracted_values = extracted_totals.get("values", {}) or {}
        else:
            extracted_values = extracted_totals
        
        # Se valores do texto são válidos (não são acumulados), usar eles
        texto_receitas = extracted_values.get("total_receitas")
        texto_despesas = extracted_values.get("total_despesas")
        
        if texto_receitas is not None and texto_receitas < 1_000_000:
            logger.info(f"[CONSOLIDATOR] Usando receitas extraídas do texto: R$ {texto_receitas:,.2f}")
            # Sobrescrever com totais do texto para que o fallback e o resultado final usem esse valor (evita 13,37 do DataFrame quando o texto tem 64k)
            extracted_data["receitas_mensais_extraidas"] = [{
                "descricao": "Receitas extraídas do texto do documento",
                "valor": texto_receitas,
                "origem": {"linha": "texto", "descricao": "extraído do texto", "coluna_valor": "texto", "aba": "texto", "tipo_preliminar": "receita"}
            }]
        
        if texto_despesas is not None and texto_despesas < 1_000_000:
            logger.info(f"[CONSOLIDATOR] Usando despesas extraídas do texto: R$ {texto_despesas:,.2f}")
            # Sobrescrever com totais do texto para que o fallback e o resultado final usem esse valor
            extracted_data["despesas_mensais_extraidas"] = [{
                "descricao": "Despesas extraídas do texto do documento",
                "valor": texto_despesas,
                "origem": {"linha": "texto", "descricao": "extraído do texto", "coluna_valor": "texto", "aba": "texto", "tipo_preliminar": "despesa"}
            }]
    
    # ETAPA 4: FALLBACK NUMÉRICO (anti-zero)
    from .financial_fallback import apply_numerical_fallback
    fallback_result = apply_numerical_fallback(extracted_data, df_work)
    
    # ETAPA 5: CÁLCULO (só se permitido)
    creditos_mes = fallback_result.get("creditos_mes")
    debitos_mes = fallback_result.get("debitos_mes")
    
    # Resolver totais do texto para prioridade e para balancete
    LIMITE_MENSAL_PLAUSIVEL = 1_000_000.0
    texto_receitas = None
    texto_despesas = None
    if extracted_totals:
        ext_vals = extracted_totals.get("values", extracted_totals) if isinstance(extracted_totals, dict) and "values" in extracted_totals else (extracted_totals if isinstance(extracted_totals, dict) else {})
        texto_receitas = ext_vals.get("total_receitas")
        texto_despesas = ext_vals.get("total_despesas")
    tem_par_texto_plausivel = (
        texto_receitas is not None and texto_despesas is not None
        and texto_receitas < LIMITE_MENSAL_PLAUSIVEL and texto_despesas < LIMITE_MENSAL_PLAUSIVEL
    )
    if tem_par_texto_plausivel:
        creditos_mes = texto_receitas
        debitos_mes = texto_despesas
        logger.info(f"[CONSOLIDATOR] Prioridade texto: créditos={creditos_mes}, débitos={debitos_mes}")
    elif (soma_balancete_receita is not None or soma_balancete_despesa is not None):
        rec_ok = soma_balancete_receita is None or soma_balancete_receita < LIMITE_MENSAL_PLAUSIVEL
        des_ok = soma_balancete_despesa is None or soma_balancete_despesa < LIMITE_MENSAL_PLAUSIVEL
        algum_positivo = (soma_balancete_receita is not None and soma_balancete_receita > 0) or (soma_balancete_despesa is not None and soma_balancete_despesa > 0)
        if rec_ok and des_ok and algum_positivo:
            creditos_mes = soma_balancete_receita
            debitos_mes = soma_balancete_despesa
            logger.info(f"[CONSOLIDATOR] Usando totais do balancete (soma por tipo): créditos={creditos_mes}, débitos={debitos_mes}")
    
    # REGRA 4.1: Nunca retornar 0.0 se há valores no arquivo
    has_values = False
    if df is not None and not df.empty:
        # Verificar se há valores no arquivo
        for col in df.columns:
            try:
                if pd.api.types.is_numeric_dtype(df[col]):
                    if df[col].abs().sum() > 0:
                        has_values = True
                        break
            except Exception:
                continue
        
        if has_values:
            # REGRA 4.1: Zero é proibido - usar None (NÃO APURADO)
            if creditos_mes == 0.0:
                creditos_mes = None
            if debitos_mes == 0.0:
                debitos_mes = None

    # ETAPA 5: Calcular resultado e saldo final (só se ambos existirem)
    resultado_mes = None
    if creditos_mes is not None and debitos_mes is not None:
        resultado_mes = creditos_mes - debitos_mes
    elif fallback_result.get("resultado_mes") is not None:
        resultado_mes = fallback_result["resultado_mes"]
    
    saldo_final = None
    if saldo_inicial is not None and resultado_mes is not None:
        saldo_final = saldo_inicial + resultado_mes
    elif extracted_data.get("saldo_final_extraido") is not None:
        saldo_final = extracted_data["saldo_final_extraido"]
    elif fallback_result.get("saldo_final") is not None:
        saldo_final = fallback_result["saldo_final"]
    
    # Usar valores do fallback (já aplicam REGRA 4.1-4.3)
    total_receitas = creditos_mes
    total_despesas = debitos_mes
    
    # 4. REGRA 1-5: Validar base financeira ANTES de calcular totais
    # Identificar colunas disponíveis
    creditos_col = "credito" if "credito" in df_work.columns else None
    debitos_col = "debito" if "debito" in df_work.columns else None
    valor_col = "valor_normalizado" if "valor_normalizado" in df_work.columns else ("valor" if "valor" in df_work.columns else None)
    
    base_validation = {}
    if valor_col:
        base_validation = validate_financial_base(
            df_work,
            creditos_col=creditos_col,
            debitos_col=debitos_col,
            valor_col=valor_col
        )
    else:
        base_validation = {
            "base_valid": False,
            "errors": [FRASE_BASE_INDEFINIDA + " Coluna de valores não encontrada."],
            "warnings": [],
            "column_analysis": {}
        }
    
    # REGRA FUNDAMENTAL: Extrair ≠ Validar. Sempre calcular, mesmo com base incerta.
    # Marcar incertezas mas não bloquear extração.
    extraction_metadata = {
        "uncertainty_reasons": [],
        "confidence_level": "ALTO",
        "items_excluded": [],
        "items_with_uncertainty": [],
        "column_classifications": {}
    }
    
    if not base_validation.get("base_valid", True):
        errors = base_validation.get("errors", [])
        warnings = base_validation.get("warnings", [])
        extraction_metadata["uncertainty_reasons"].extend(errors)
        extraction_metadata["uncertainty_reasons"].extend(warnings)
        extraction_metadata["confidence_level"] = "BAIXO"
        logger.info(f"[EXTRACTION] Base financeira com classificação incerta. Continuando extração. Erros: {errors}. Warnings: {warnings}.")
        # NÃO retornar early - continuar calculando
    
    # Totais já calculados acima usando fallback (ETAPA 4)
    # Não usar método antigo - fallback já aplica todas as regras
    
    # ETAPA 3: DETECÇÃO DE ESCALA (não bloqueia, apenas marca incerteza)
    debitos_individuals = []
    if extracted_data.get("despesas_mensais_extraidas"):
        debitos_individuals = [d["valor"] for d in extracted_data["despesas_mensais_extraidas"]]
    
    # Usar valores numéricos para validação (None tratado como 0 para validação)
    receitas_para_validacao = total_receitas if total_receitas is not None else 0.0
    despesas_para_validacao = total_despesas if total_despesas is not None else 0.0
    scale_validation = validate_monthly_scale(receitas_para_validacao, despesas_para_validacao, debitos_individuals)
    
    if not scale_validation.get("valid", True):
        extraction_metadata["uncertainty_reasons"].append(scale_validation.get("message", "Possível erro de escala detectado"))
        extraction_metadata["confidence_level"] = "MÉDIO" if extraction_metadata["confidence_level"] == "ALTO" else "BAIXO"
        logger.info(f"[EXTRACTION] Possível erro de escala detectado: {scale_validation.get('message')}. Receitas={_format_value_for_log(total_receitas)}, Despesas={_format_value_for_log(total_despesas)}. Continuando extração com incerteza marcada.")
        # NÃO retornar early - continuar e retornar valores com incerteza marcada
    
    # Saldo já calculado acima na ETAPA 5
    saldo = resultado_mes
    
    # Classificar colunas para metadados
    column_classifications = {}
    if base_validation.get("column_analysis"):
        for col_name, col_info in base_validation["column_analysis"].items():
            purpose = col_info.get("purpose", "DESCONHECIDA")
            confidence = col_info.get("confidence", 0.0)
            column_classifications[col_name] = {
                "type": purpose.upper() if purpose else "DESCONHECIDA",
                "confidence": "ALTO" if confidence >= 0.7 else "MÉDIO" if confidence >= 0.4 else "BAIXO",
                "reason": col_info.get("reason", "")
            }
    
    extraction_metadata["column_classifications"] = column_classifications
    
    # Registrar itens excluídos (totais, transferências, duplicatas)
    items_excluded = []
    if stats.get("totais_removidos", 0) > 0:
        items_excluded.append({
            "tipo": "totais",
            "quantidade": stats["totais_removidos"],
            "razao": "Linhas de totais/subtotais excluídas para evitar duplicação"
        })
    if stats.get("transferencias_excluidas", 0) > 0:
        items_excluded.append({
            "tipo": "transferencias",
            "quantidade": stats["transferencias_excluidas"],
            "razao": "Transferências internas excluídas"
        })
    if stats.get("duplicatas_removidas", 0) > 0:
        items_excluded.append({
            "tipo": "duplicatas",
            "quantidade": stats["duplicatas_removidas"],
            "razao": "Lançamentos duplicados removidos"
        })
    extraction_metadata["items_excluded"] = items_excluded
    
    # ETAPA 2: Validação matemática por conta (quando há coluna conta/categoria)
    validation_per_conta = {}
    try:
        validation_per_conta = validate_balance_per_conta(df_work, tolerance=0.02)
    except Exception as e:
        logger.warning(f"[CONSOLIDATOR] Validação por conta não realizada: {e}")
    
    # Formatar valores (None permanece None, valores numéricos são arredondados)
    total_receitas_formatted = round(total_receitas, 2) if total_receitas is not None else None
    total_despesas_formatted = round(total_despesas, 2) if total_despesas is not None else None
    saldo_formatted = round(saldo, 2) if saldo is not None else None
    saldo_final_formatted = round(saldo_final, 2) if saldo_final is not None else None
    
    result = {
        "total_receitas": total_receitas_formatted,  # Pode ser None (NÃO APURADO)
        "total_despesas": total_despesas_formatted,  # Pode ser None (NÃO APURADO)
        "saldo": saldo_formatted,  # Pode ser None (NÃO CALCULADO)
        "saldo_final": saldo_final_formatted,  # Pode ser None (NÃO APURADO)
        "saldo_inicial": saldo_inicial,
        "extraction_metadata": extraction_metadata,
        "base_validation": base_validation,
        "scale_validation": scale_validation,
        # Adicionar extracted_data e fallback_result ao resultado
        "extracted_data": extracted_data,
        "fallback_result": fallback_result,
        # Manter flags para compatibilidade, mas não bloquear uso
        "base_invalid": not base_validation.get("base_valid", True),
        "scale_error": not scale_validation.get("valid", True),
        "base_error_message": get_validation_status_message(base_validation) if not base_validation.get("base_valid", True) else None,
        "scale_error_message": scale_validation.get("message") if not scale_validation.get("valid", True) else None,
        "validation_per_conta": validation_per_conta,
        **stats,
    }
    
    return result


def check_gasto_acima_5pct_e_inflacao(
    total_despesas_atual: float,
    total_despesas_mes_anterior: Optional[float] = None,
    inflacao_pct_mes: Optional[float] = None,
    mes_referencia: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Verifica se o gasto do mês atual está mais de 5% acima do mês passado
    e acima da inflação do mês (regra matemática de especificação).

    Condição de alerta: gasto_atual > gasto_mes_passado * 1,05
                        E
                        gasto_atual > gasto_mes_passado * (1 + inflação_mês)

    Args:
        total_despesas_atual: Total de despesas do mês atual
        total_despesas_mes_anterior: Total de despesas do mês anterior (se None, não há comparação)
        inflacao_pct_mes: Inflação do mês em decimal (ex.: 0.005 = 0,5%). Se None, usa INFLATION_MONTHLY_PCT ou 0,5%
        mes_referencia: Label do mês (ex.: "2026-01") para mensagens

    Returns:
        Dict com: exceeded_5pct (bool), exceeded_inflation (bool), alert (bool), message (str), details (dict)
    """
    result = {
        "exceeded_5pct": False,
        "exceeded_inflation": False,
        "alert": False,
        "message": "",
        "details": {},
    }
    if total_despesas_mes_anterior is None or total_despesas_mes_anterior <= 0:
        return result
    if total_despesas_atual <= 0:
        return result

    inflacao = inflacao_pct_mes if inflacao_pct_mes is not None else _get_inflation_monthly_decimal()
    ratio_atual = total_despesas_atual / total_despesas_mes_anterior
    limite_5pct = 1.05
    limite_inflacao = 1.0 + inflacao

    result["exceeded_5pct"] = ratio_atual > limite_5pct
    result["exceeded_inflation"] = ratio_atual > limite_inflacao
    result["alert"] = result["exceeded_5pct"] and result["exceeded_inflation"]
    result["details"] = {
        "total_atual": round(total_despesas_atual, 2),
        "total_mes_anterior": round(total_despesas_mes_anterior, 2),
        "variacao_pct": round((ratio_atual - 1.0) * 100.0, 2),
        "inflacao_mes_pct": round(inflacao * 100.0, 2),
        "mes_referencia": mes_referencia,
    }

    if result["alert"]:
        variacao_pct = (ratio_atual - 1.0) * 100.0
        result["message"] = (
            f"Gasto do mês está {variacao_pct:.1f}% acima do mês anterior e acima da inflação do mês ({result['details']['inflacao_mes_pct']}%). "
            "Especificação: alerta quando gasto > 5% acima do mês passado e acima da inflação."
        )
        if mes_referencia:
            result["message"] = f"[{mes_referencia}] " + result["message"]
    return result
