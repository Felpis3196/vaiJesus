"""
Analisador de Encargos Trabalhistas
Extrai e calcula FGTS, INSS, IRRF, PIS, ISS a partir do DataFrame de transações.
REGRA 3: Ausência de documento ≠ ausência de tributo. Encargo sem doc = NÃO AUDITÁVEL.
REGRA 5: Holerite inválido (líquido=0, descontos=0, rubricas genéricas, sem CPF/cargo) invalida análise trabalhista.
"""
import re
import logging
from collections import Counter
from services.audit_rules import FRASE_ENCARGO_NAO_AUDITAVEL
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
import pandas as pd

logger = logging.getLogger(__name__)

# Keywords para identificar tipos de encargos
KEYWORDS_FGTS = [
    r'\bfgts\b', r'\bfundo\s+de\s+garantia\b', r'\bgrf\b', r'\bguia\s+fgts\b'
]
KEYWORDS_INSS = [
    r'\binss\b', r'\bprevidencia\b', r'\bprevidência\b', r'\bcontribuicao\s+previdenciaria\b',
    r'\bgps\b', r'\bguia\s+da\s+previdencia\b', r'\bguia\s+inss\b', r'\bgfip\b', r'\besocial\b', r'\be-social\b'
]
KEYWORDS_IRRF = [
    r'\birrf\b', r'\bir\s+retido\b', r'\bimposto\s+de\s+renda\s+retido\b',
    r'\bretencao\s+ir\b', r'\bimposto\s+renda\b'
]
KEYWORDS_PIS = [
    r'\bpis\b', r'\bpasep\b', r'\bpis\s*/\s*pasep\b', r'\b8301\b'
]
KEYWORDS_ISS = [
    r'\biss\b', r'\bimposto\s+sobre\s+servico\b', r'\bimposto\s+sobre\s+serviços\b'
]
KEYWORDS_CONTRIB_SINDICAL = [
    r'\bcontribuicao\s+sindical\b', r'\bcontribuição\s+sindical\b', r'\bcontrib\s+sindical\b',
    r'\bcontrib\.\s*sindical\b', r'\bsindical\b',
]
KEYWORDS_SAT_RAT = [
    r'\bsat\b', r'\brat\b', r'\bacidente\s+de\s+trabalho\b', r'\bseguro\s+acidentes\b',
    r'\brisco\s+acidente\b', r'\bcontribuicao\s+acidente\b'
]
KEYWORDS_SALARIO = [
    r'\bsalario\b', r'\bsalário\b', r'\bsalarios\b', r'\bsalários\b',
    r'\bfolha\s+de\s+pagamento\b', r'\bfolha\s+pagamento\b', r'\bfolha\s+salarial\b',
    r'\bfolha\b',  # "Folha" isolada ou "folha dezembro" (interligação meses)
    r'\bproventos\b', r'\bdescontos\b', r'\bcontracheque\b', r'\bholerite\b',
    r'\bvencimentos\b', r'\bremuneracao\b', r'\bremuneração\b',
    r'\bzelador\b', r'\bporteiro\b', r'\bfuncionario\b', r'\bfuncionário\b'
]
KEYWORDS_ADIANTAMENTO = [
    r'\badiantamento\b', r'\bvale\b', r'\bantecipacao\b', r'\bantecipação\b'
]
KEYWORDS_FERIAS = [
    r'\bferias\b', r'\bférias\b', r'\bferias\s+proporcionais\b'
]
KEYWORDS_DECIMO_TERCEIRO = [
    # Padrões específicos de 13º salário (mais restritivos)
    r'\b13[°ºº]\s*salario\b', r'\b13\s*salario\b', r'\b13[°ºº]\s*salário\b',
    r'\bdecimo\s+terceiro\b', r'\bdécimo\s+terceiro\b', r'\bdecimo\s+terceiro\s+salario\b',
    r'\b13o\s*salario\b', r'\b13º\s*salário\b', r'\b13[°ºº]\s*sal\b',
    r'\bgratificacao\s+natalina\b', r'\bgratificação\s+natalina\b',
    # Padrões com contexto de pagamento/provisão
    r'\bpagamento\s+13[°ºº]\s*salario\b', r'\bpagamento\s+decimo\s+terceiro\b',
    r'\bprovisao\s+13[°ºº]\s*salario\b', r'\bprovisão\s+13[°ºº]\s*salário\b',
    r'\bconta\s+48.*13[°ºº]', r'\b48.*13[°ºº]\s*salario',  # Conta 48 com contexto de 13º
]
KEYWORDS_PROVISAO = [
    # Provisões específicas de férias e 13º (mais restritivas)
    r'provisao\s*13[°ºº]', r'provisão\s*13[°ºº]', r'provisao\s*decimo\s*terceiro',
    r'13[°ºº]\s*ferias', r'13[°ºº]\s*férias', r'ferias\s*e?\s*13[°ºº]', r'férias\s*e?\s*13[°ºº]',
    r'decimo\s*terceiro\s*ferias', r'décimo\s*terceiro\s*férias',
    r'provisao\s*ferias', r'provisão\s*férias',
    # Conta 48 com contexto específico
    r'conta\s*48.*provisao', r'conta\s*48.*13[°ºº]', r'conta\s*48.*ferias',
]

# Palavras-chave de EXCLUSÃO para evitar falsos positivos
# Linhas que contêm essas palavras NÃO devem ser consideradas encargos trabalhistas
EXCLUSION_KEYWORDS_ENCARGOS = [
    r'\b01[-_]?ordinaria\b', r'\b01[-_]?ordinária\b', r'\bordinaria\b', r'\bordinária\b',
    r'\bconta\s+ordinaria\b', r'\bconta\s+ordinária\b', r'\bconta\s+01\b',
    r'\bextrato\b', r'\bextratos\b', r'\bbancario\b', r'\bbancário\b',
    r'\bconta\s+corrente\b', r'\bconta\s+bancaria\b', r'\bconta\s+bancária\b',
    r'\bagua\b', r'\bágua\b', r'\benergia\b', r'\bluz\b', r'\beletricidade\b',
    r'\badministracao\b', r'\badministração\b', r'\bconservacao\b', r'\bconservação\b',
    r'\bresumo\b', r'\bsaldo\b', r'\bsaldos\b',
    r'\btotal\s+geral\b', r'\btotais\s+gerais\b', r'\btotal\s+do\s+mes\b', r'\btotal\s+do\s+mês\b',
    r'\bdevedores\b', r'\breceita\s+prevista\b', r'\bprevisao\b', r'\bprevisão\b',
    r'\bfundo\s+obras\b', r'\bfundo\s+reserva\b', r'\bprovisao\s+ordinaria\b',
]

# Percentuais baseline condomínio (FGTS 8%, INSS patronal 27,8%, PIS 1%)
FGTS_PERCENT = 0.08
INSS_PATRONAL_CONDOMINIO_PERCENT = 0.278  # 20% + 2% RAT + 5,8% terceiros
PIS_CONDOMINIO_PERCENT = 0.01  # 1% sobre folha (condomínio, sem fins lucrativos)

# Tabela IRRF 2025 – vigente a partir de 2025 (base = salário bruto - INSS; aqui usamos bruto como proxy)
# O IRRF é recolhido por salário individual, não pelo total da folha.
TABELA_IRRF_2025 = [
    (5000.00, 0.0, 0.0),       # Até 5.000: isento
    (7500.00, 0.075, 375.00),   # 5.000,01 a 7.500: 7,5%, deduzir 375
    (10000.00, 0.15, 1125.00),  # 7.500,01 a 10.000: 15%, deduzir 1.125
    (15000.00, 0.225, 2250.00), # 10.000,01 a 15.000: 22,5%, deduzir 2.250
    (float("inf"), 0.275, 3375.00),  # Acima 15.000: 27,5%, deduzir 3.375
]

# Mensagem quando IRRF não pode ser validado sem holerites individuais
FRASE_IRRF_NAO_AUDITAVEL_SEM_HOLERITES = (
    "O IRRF é recolhido por salário individual, não pelo total da folha. "
    "A prestação de contas não traz holerites de cada funcionário. "
    "Para validar o IRRF, é necessária a pasta com os holerites individuais. "
    "Conferir comprovantes (DARFs) no demonstrativo."
)

# Baseline de percentuais para cálculo e validação (base = valor base da folha)
BASELINE_PERCENTUAIS = {
    "fgts": {"percentual": FGTS_PERCENT, "base_descricao": "Salário bruto"},
    "inss_patronal": {"percentual": INSS_PATRONAL_CONDOMINIO_PERCENT, "base_descricao": "Salário bruto"},
    "pis": {"percentual": PIS_CONDOMINIO_PERCENT, "base_descricao": "Folha de pagamento"},
}
# Tolerância para considerar valor encontrado correto: 5% do esperado ou R$ 0,02
TOLERANCIA_PERCENT_VALIDACAO = 0.05
TOLERANCIA_ABSOLUTA_VALIDACAO = 0.02

# Faixa típica de folha mensal (condomínio) para priorizar quando há múltiplos candidatos (base mês anterior)
FOLHA_MENSAL_MIN = 25_000
FOLHA_MENSAL_MAX = 60_000

# Tabela oficial de encargos: percentual, base de cálculo, quem paga (para PDF/JSON)
TABELA_ENCARGOS = {
    "inss": {
        "percentual": "7,5% a 14% (progressivo); patronal condomínio 27,8%",
        "base_calculo": "Salário até o teto",
        "quem_paga": "Trabalhador (desconto no salário); condomínio recolhe e repassa",
    },
    "irrf": {
        "percentual": "Isento até R$ 5.000; 7,5% a 27,5% (progressivo, tabela 2025)",
        "base_calculo": "Salário bruto – INSS – deduções (por funcionário)",
        "quem_paga": "Trabalhador (desconto no salário); condomínio recolhe e repassa",
    },
    "fgts": {
        "percentual": "8%",
        "base_calculo": "Salário bruto",
        "quem_paga": "Condomínio (encargo patronal)",
    },
    "pis": {
        "percentual": "1% (condomínio, entidade sem fins lucrativos)",
        "base_calculo": "Folha de pagamento (não incide sobre 13º quando aplicável)",
        "quem_paga": "Condomínio (encargo patronal)",
    },
    "iss": {
        "percentual": "Conforme legislação municipal",
        "base_calculo": "Base de cálculo do serviço",
        "quem_paga": "Condomínio (quando devido)",
    },
    "contrib_sindical": {
        "percentual": "-",
        "base_calculo": "-",
        "quem_paga": "Condomínio (encargo patronal)",
    },
    "sat_rat": {
        "percentual": "1%",
        "base_calculo": "Pagamento",
        "quem_paga": "Condomínio (encargo patronal)",
    },
}

# Palavras-chave de EXCLUSÃO específicas para 13º salário
# Linhas que contêm essas palavras NÃO devem ser consideradas 13º salário
EXCLUSION_KEYWORDS_13_SALARIO = EXCLUSION_KEYWORDS_ENCARGOS + [
    r'\bprovisao\s+ordinaria\b', r'\bprovisão\s+ordinária\b',
    r'\bfundo\s+reserva\b', r'\breserva\s+geral\b',
    r'\bconta\s+48\b', r'\b48[-_]?13',  # Conta 48 pode ser 13º/Férias, mas precisa contexto
    r'\bconta\s+provisao\b', r'\bconta\s+provisão\b',  # Provisão genérica, não específica de 13º
]


def _fmt_brl(val: float) -> str:
    """Formata valor em reais (padrão BR: 1.234,56)."""
    try:
        return f"{float(val):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except (TypeError, ValueError):
        return "0,00"


def _parse_cell_value(val: Any) -> float:
    """Converte célula (string BR 7.937,93, float, int) para float."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return 0.0
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).strip()
    if not s:
        return 0.0
    # Formato BR: 7.937,93 ou 637,45
    s_clean = s.replace(".", "").replace(",", ".")
    try:
        return float(s_clean)
    except ValueError:
        return 0.0


# Mapeamento encargo -> nomes possíveis de coluna (após normalização lower + replace space por _ e . por _)
_ENCARGO_COLUMN_MAP = {
    "inss": ["inss"],
    "irrf": ["irrf"],
    "fgts": ["fgts", "fundo_garantia", "grf", "f_g_t_s", "guia_fgts"],
    "pis": ["pis"],
    "iss": ["iss"],
    "contrib_sindical": ["contrib_sindical", "contribuicao_sindical", "sindical", "contrib__sindical", "contrib._sindical"],
    "sat_rat": ["sat_rat", "sat", "rat"],
}


def _extract_encargos_from_columns(df: pd.DataFrame) -> Dict[str, float]:
    """
    Extrai valores de encargos quando a planilha tem colunas nomeadas por encargo.
    Regra 1: Preferir uma única linha de total geral (mesma linha com vários encargos).
    Regra 2: Senão, preferir linha 'total' por encargo (mesma linha quando possível).
    Fallback: primeira linha com valor > 0 na coluna (não soma).
    """
    out: Dict[str, float] = {}
    if df is None or df.empty:
        return out
    cols_lower = {str(c).lower().strip().replace(" ", "_").replace(".", "_"): c for c in df.columns}
    # Mapear encargo -> coluna raw
    encargo_cols: Dict[str, Any] = {}
    for encargo, names in _ENCARGO_COLUMN_MAP.items():
        col_raw = None
        for n in names:
            for key, c in cols_lower.items():
                if key == n:
                    col_raw = c
                    break
                if len(n) >= 3 and (n in key or key.endswith("_" + n) or key.startswith(n + "_")):
                    col_raw = c
                    break
            if col_raw is not None:
                break
        if col_raw is not None:
            encargo_cols[encargo] = col_raw

    # Regra 1: Detectar linha de total geral (uma linha com "total" e pelo menos 2 encargos preenchidos)
    total_geral_keywords = ("total", "total geral", "total do mes", "total do mês", "resumo encargos", "resumo encargo")
    best_row_idx: Optional[int] = None
    best_count = 0
    for idx in range(len(df)):
        row = df.iloc[idx]
        row_text = _get_row_text_for_search(row, df)
        row_norm = _norm_text(row_text)
        if not any(kw in row_norm for kw in total_geral_keywords):
            continue
        count = 0
        for encargo, col_raw in encargo_cols.items():
            v = _parse_cell_value(row.get(col_raw))
            if v > 0:
                count += 1
        if count >= 2 and count > best_count:
            best_count = count
            best_row_idx = idx
    if best_row_idx is not None:
        row = df.iloc[best_row_idx]
        for encargo, col_raw in encargo_cols.items():
            v = _parse_cell_value(row.get(col_raw))
            if v > 0:
                out[encargo] = v
                logger.debug(f"[ENCARGO_COL] Linha total geral (idx={best_row_idx}) para {encargo}: R$ {v}")
        return out

    # Regra 2 e fallback: por encargo. Preferir mesma linha entre encargos; senão primeira "total"; senão primeira com valor
    # Coletar por encargo: listas (idx, valor) onde linha tem "total" e valor > 0 (e texto sugere encargo ou geral)
    total_candidates: Dict[str, List[Tuple[int, float]]] = {e: [] for e in encargo_cols}
    for idx in range(len(df)):
        row = df.iloc[idx]
        row_text = _get_row_text_for_search(row, df)
        row_norm = _norm_text(row_text)
        for encargo, col_raw in encargo_cols.items():
            v = _parse_cell_value(row.get(col_raw))
            if v <= 0:
                continue
            enc_in_text = encargo.replace("_", "") in row_norm or "encargo" in row_norm or "tributo" in row_norm
            if "total" in row_norm and enc_in_text:
                total_candidates[encargo].append((idx, v))
    # Preferir um row_idx que apareça na maioria dos encargos (mesma linha)
    all_idx = []
    for cands in total_candidates.values():
        for idx, _ in cands:
            all_idx.append(idx)
    common_idx = None
    if all_idx:
        c = Counter(all_idx)
        common_idx = c.most_common(1)[0][0]
    for encargo, col_raw in encargo_cols.items():
        total_row_val = None
        if common_idx is not None:
            for idx, v in total_candidates[encargo]:
                if idx == common_idx:
                    total_row_val = v
                    break
        if total_row_val is None and total_candidates[encargo]:
            total_row_val = total_candidates[encargo][0][1]
        if total_row_val is not None:
            out[encargo] = total_row_val
            continue
        # Fallback final: primeira linha com valor > 0 na coluna (não soma)
        series = df[col_raw]
        for idx in range(len(df)):
            v = _parse_cell_value(series.iloc[idx])
            if v > 0:
                out[encargo] = v
                logger.debug(f"[ENCARGO_COL] Primeira linha com valor para {encargo}: R$ {v}")
                break
    return out


def _norm_text(s: Any) -> str:
    """Normaliza texto para comparação."""
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return ""
    t = str(s).lower().strip()
    for old, new in [('é', 'e'), ('á', 'a'), ('í', 'i'), ('ó', 'o'), ('ú', 'u'), ('ã', 'a'), ('õ', 'o'), ('ç', 'c')]:
        t = t.replace(old, new)
    return t


def _matches_keywords(text: str, patterns: List[str]) -> bool:
    """Verifica se o texto contém algum dos padrões."""
    if not text:
        return False
    text_norm = _norm_text(text)
    for pattern in patterns:
        try:
            if re.search(pattern, text_norm, re.IGNORECASE):
                return True
        except re.error:
            if pattern.lower().replace(r'\b', '') in text_norm:
                return True
    return False


def _should_exclude_row(row_text: str, exclusion_patterns: List[str]) -> bool:
    """
    Verifica se uma linha deve ser excluída baseada nas palavras de exclusão.
    
    Args:
        row_text: Texto completo da linha (normalizado)
        exclusion_patterns: Lista de padrões regex para exclusão
        
    Returns:
        True se a linha deve ser excluída, False caso contrário
    """
    if not row_text or not exclusion_patterns:
        return False
    
    text_norm = _norm_text(row_text)
    for pattern in exclusion_patterns:
        try:
            if re.search(pattern, text_norm, re.IGNORECASE):
                logger.debug(f"[EXCLUSÃO] Linha excluída por padrão '{pattern}': {row_text[:100]}")
                return True
        except re.error:
            # Fallback: busca simples se regex falhar
            pattern_clean = pattern.lower().replace(r'\b', '').replace('\\', '')
            if pattern_clean in text_norm:
                logger.debug(f"[EXCLUSÃO] Linha excluída por padrão '{pattern_clean}': {row_text[:100]}")
                return True
    return False


def _get_row_text_for_search(row: pd.Series, df: pd.DataFrame) -> str:
    """Concatena todos os textos da linha (descricao, conta, historico, etc.) para busca."""
    text_parts = []
    for col in df.columns:
        if df[col].dtype == object or pd.api.types.is_string_dtype(df[col]):
            val = row.get(col)
            if val is not None and not (isinstance(val, float) and pd.isna(val)):
                text_parts.append(str(val).strip())
    return " ".join(text_parts).lower()


def _get_row_value(row: pd.Series, df: pd.DataFrame) -> float:
    """Obtém valor monetário da linha: coluna 'valor' ou débito/crédito (comum em balancetes)."""
    def _parse_val(x: Any) -> float:
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return 0.0
        if isinstance(x, (int, float)):
            return float(x)
        try:
            s = str(x).strip().replace(".", "").replace(",", ".")
            return float(s) if s else 0.0
        except (TypeError, ValueError):
            return 0.0

    # 1) Tentar coluna valor
    if "valor" in df.columns:
        v = _parse_val(row.get("valor"))
        if v != 0:
            return v
    # 2) Colunas débito (despesas)
    for deb_col in ("débito", "debito", "débitos", "debitos", "valor débito", "valor_debito"):
        if deb_col in df.columns:
            v = _parse_val(row.get(deb_col))
            if v > 0:
                return v
    # 3) Colunas crédito (receitas)
    for cred_col in ("crédito", "credito", "créditos", "creditos", "valor crédito", "valor_credito"):
        if cred_col in df.columns:
            v = _parse_val(row.get(cred_col))
            if v > 0:
                return v
    # 4) Colunas com "valor", "total", "saldo" no nome (PDFs com estrutura variada)
    for col in df.columns:
        col_lower = str(col).lower()
        if "valor" in col_lower or "total" in col_lower or "saldo" in col_lower:
            if "descricao" in col_lower or "conta" in col_lower or "historico" in col_lower:
                continue
            v = _parse_val(row.get(col))
            if v > 0:
                return v
    # 5) Fallback: última coluna numérica (comum em tabelas PDF/Excel com nomes variados)
    for col in reversed(list(df.columns)):
        if "descricao" in str(col).lower() or "conta" in str(col).lower() or "historico" in str(col).lower():
            continue
        v = _parse_val(row.get(col))
        if 100 <= v <= 500_000:  # plausível para folha
            return v
    return 0.0


def _extract_transactions_by_keywords(
    df: pd.DataFrame, 
    patterns: List[str], 
    exclusion_patterns: Optional[List[str]] = None,
    column: str = "descricao"
) -> pd.DataFrame:
    """
    Extrai transações que correspondem aos padrões, excluindo linhas que correspondem aos padrões de exclusão.
    Usa todas as colunas de texto (descricao, conta, etc.) para maximizar extração.
    
    Args:
        df: DataFrame com transações
        patterns: Lista de padrões regex para INCLUSÃO
        exclusion_patterns: Lista opcional de padrões regex para EXCLUSÃO
        column: Nome da coluna (mantido para compatibilidade, mas não usado)
        
    Returns:
        DataFrame filtrado com transações que correspondem aos padrões de inclusão
        e NÃO correspondem aos padrões de exclusão
    """
    if df is None or df.empty:
        return pd.DataFrame()
    
    # Sempre usar texto combinado de todas as colunas de texto (planilha pode ter Conta, Histórico, etc.)
    def _row_matches(row: pd.Series) -> bool:
        row_text = _get_row_text_for_search(row, df)
        # Primeiro verificar exclusões (prioridade)
        if exclusion_patterns and _should_exclude_row(row_text, exclusion_patterns):
            return False
        # Depois verificar inclusões
        return _matches_keywords(row_text, patterns)
    
    mask = df.apply(_row_matches, axis=1)
    out = df[mask].copy()
    
    # Garantir que valor efetivo está disponível (débito/crédito quando valor for 0)
    if not out.empty:
        out["valor"] = out.apply(lambda row: _get_row_value(row, df), axis=1)
    
    return out


def _get_total_folha_linha_prestacao(df_salarios: pd.DataFrame) -> Optional[float]:
    """
    Se no balancete existir uma linha que indique total de folha (ex.: "Folha de pagamento", "Total folha")
    com valor plausível (10k a 500k), retorna esse valor para evitar somar prestação com detalhes da folha.
    """
    if df_salarios is None or df_salarios.empty:
        return None
    total_patterns = [
        r"folha\s+de\s+pagamento", r"total\s+folha", r"total\s+sal[aá]rios?", r"total\s+da\s+folha",
        r"folha\s+pagamento", r"total\s+geral\s+folha",
        r"^\s*folha\s*$", r"\bfolha\b",  # linha só "Folha" ou célula "Folha" (ex.: total folha dezembro)
    ]
    candidates: List[Tuple[float, str]] = []
    for _, row in df_salarios.iterrows():
        text = _get_row_text_for_search(row, df_salarios)
        val = _get_row_value(row, df_salarios)
        if val < 10_000 or val > 500_000:
            continue
        if any(re.search(p, text) for p in total_patterns):
            candidates.append((val, text))
    if not candidates:
        return None
    # Preferir valor na faixa de folha mensal típica (25k–60k); dentro dela, preferir "total" no texto
    in_range = [(v, t) for v, t in candidates if FOLHA_MENSAL_MIN <= v <= FOLHA_MENSAL_MAX]
    if in_range:
        with_total = [(v, t) for v, t in in_range if "total" in t]
        chosen = with_total[0][0] if with_total else in_range[0][0]
        return round(chosen, 2)
    # Nenhum na faixa: manter lógica anterior (preferir "total", senão primeiro)
    with_total = [(v, t) for v, t in candidates if "total" in t]
    chosen = with_total[0][0] if with_total else candidates[0][0]
    return round(chosen, 2)


def _calculate_total(df: pd.DataFrame) -> float:
    """Calcula soma dos valores de um DataFrame. Usa valor efetivo (valor ou débito/crédito) por linha."""
    if df is None or df.empty:
        return 0.0
    try:
        if "valor" in df.columns:
            total = float(pd.to_numeric(df["valor"], errors="coerce").fillna(0).sum())
            if total != 0:
                return total
        return float(df.apply(lambda row: _get_row_value(row, df), axis=1).sum())
    except Exception:
        return 0.0


def compute_base_remuneracao_mais_13(df: pd.DataFrame) -> float:
    """
    Calcula a base da folha (valor da folha do mês) a partir do DataFrame do documento do mês anterior (X-1).
    Usado para definir valor_base_folha do mês X quando o documento X-1 está disponível (interligação dos meses).
    Preferência: se existir linha de total de folha (ex.: "Folha", "Total folha", "Folha de pagamento") com valor
    plausível, usa esse valor; senão retorna total_salários + total_adiantamento + valor_13º.
    """
    if df is None or df.empty:
        return 0.0
    # 1) Buscar primeiro no DataFrame COMPLETO (não só em df_sal) - linha pode estar em qualquer seção
    total_folha_linha = _get_total_folha_linha_prestacao(df)
    if total_folha_linha is not None and total_folha_linha > 0:
        logger.info(f"[BASE MÊS ANTERIOR] Usando linha total folha: R$ {total_folha_linha:,.2f}")
        return round(total_folha_linha, 2)
    # 2) Fallback: soma salários + adiantamento + 13º
    df_sal = _extract_transactions_by_keywords(df, KEYWORDS_SALARIO, exclusion_patterns=EXCLUSION_KEYWORDS_ENCARGOS)
    df_adi = _extract_transactions_by_keywords(df, KEYWORDS_ADIANTAMENTO, exclusion_patterns=EXCLUSION_KEYWORDS_ENCARGOS)
    df_13 = _extract_transactions_by_keywords(df, KEYWORDS_DECIMO_TERCEIRO, exclusion_patterns=EXCLUSION_KEYWORDS_13_SALARIO)
    total_sal = _calculate_total(df_sal)
    total_adi = _calculate_total(df_adi)
    total_13 = _calculate_total(df_13)
    result = round(total_sal + total_adi + total_13, 2)
    if result > 0:
        logger.info(f"[BASE MÊS ANTERIOR] Usando soma (sal+adiant+13º): R$ {result:,.2f} (sal={total_sal:,.2f} adi={total_adi:,.2f} 13º={total_13:,.2f})")
        return result
    # 3) Último fallback: soma = 0 – procurar linha com "folha"/"dezembro"; priorizar faixa folha mensal (25k–60k)
    def _scan_folha_dezembro(val_min: float, val_max: float) -> Optional[float]:
        for _, row in df.iterrows():
            text = _get_row_text_for_search(row, df)
            if "folha" not in text and "dezembro" not in text:
                continue
            val = _get_row_value(row, df)
            if val <= 0:
                for col in df.columns:
                    if "descricao" in str(col).lower() or "conta" in str(col).lower() or "historico" in str(col).lower():
                        continue
                    try:
                        x = row.get(col)
                        if x is None or (isinstance(x, float) and pd.isna(x)):
                            continue
                        if isinstance(x, (int, float)):
                            v = float(x)
                        else:
                            s = str(x).strip().replace(".", "").replace(",", ".")
                            v = float(s) if s else 0.0
                    except (TypeError, ValueError):
                        continue
                    if val_min <= v <= val_max:
                        val = v
                        break
            if val_min <= val <= val_max:
                return round(val, 2)
        return None

    found = _scan_folha_dezembro(FOLHA_MENSAL_MIN, FOLHA_MENSAL_MAX)
    if found is not None:
        logger.info(f"[BASE MÊS ANTERIOR] Usando valor de linha com folha/dezembro (fallback 25k–60k): R$ {found:,.2f}")
        return found
    found = _scan_folha_dezembro(10_000, 500_000)
    if found is not None:
        logger.info(f"[BASE MÊS ANTERIOR] Usando valor de linha com folha/dezembro (fallback 10k–500k): R$ {found:,.2f}")
        return found
    logger.info(f"[BASE MÊS ANTERIOR] Soma e fallback = 0. Retornando 0.")
    return 0.0


def _valor_encargo_preferindo_total(df_encargo: pd.DataFrame, nome_encargo: str) -> float:
    """
    Retorna o valor do encargo preferindo uma linha que indique "total" daquele encargo
    (evita somar detalhe + total e duplicar). Se não houver linha de total, usa primeira linha com valor > 0 (não soma).
    """
    if df_encargo is None or df_encargo.empty:
        return 0.0
    nome_lower = nome_encargo.lower().replace(" ", "")
    # Padrões que indicam "total deste encargo" (ex: total inss, inss total, total fgts)
    total_patterns = [
        re.compile(r"total\s+" + re.escape(nome_lower), re.IGNORECASE),
        re.compile(re.escape(nome_lower) + r"\s+total", re.IGNORECASE),
        re.compile(r"total\s+.*" + re.escape(nome_lower), re.IGNORECASE),
    ]
    for _, row in df_encargo.iterrows():
        row_text = _get_row_text_for_search(row, df_encargo)
        row_text_norm = _norm_text(row_text)
        for pat in total_patterns:
            try:
                if pat.search(row_text_norm):
                    val = _get_row_value(row, df_encargo)
                    if val > 0:
                        logger.debug(f"[ENCARGO] Usando linha 'total' para {nome_encargo}: R$ {val}")
                        return val
            except re.error:
                pass
    # Fallback: primeira linha (por ordem) com valor > 0 (não soma)
    for i in range(len(df_encargo)):
        row = df_encargo.iloc[i]
        val = _get_row_value(row, df_encargo)
        if val > 0:
            logger.debug(f"[ENCARGO] Fallback primeira linha para {nome_encargo}: R$ {val}")
            return val
    return 0.0


def _get_transactions_list(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """Converte DataFrame para lista de dicts com transações."""
    if df is None or df.empty:
        return []
    result = []
    for _, row in df.iterrows():
        try:
            item = {
                "data": str(row.get("data", "")),
                "descricao": str(row.get("descricao", ""))[:200],
                "valor": float(row.get("valor", 0)) if pd.notna(row.get("valor")) else 0.0,
                "tipo": str(row.get("tipo", "despesa"))
            }
            result.append(item)
        except Exception:
            continue
    return result


def _estimate_payroll_from_fgts(valor_fgts: float) -> float:
    """
    Estima folha salarial bruta a partir do valor de FGTS pago.
    
    Args:
        valor_fgts: Valor do FGTS pago
        
    Returns:
        Folha bruta estimada (FGTS ÷ FGTS_PERCENT)
    """
    if valor_fgts <= 0:
        return 0.0
    return valor_fgts / FGTS_PERCENT


def _validate_13th_salary_provision(holerites: List[Dict[str, Any]], valor_provisao: float, periodo: str) -> Dict[str, Any]:
    """
    Valida provisão mensal de 13º salário.
    
    Args:
        holerites: Lista de holerites estruturados
        valor_provisao: Valor de provisão encontrado
        periodo: Período analisado (formato "YYYY-MM")
        
    Returns:
        Dict com validação da provisão
    """
    validacao = {
        "provisao_esperada_mensal": 0.0,
        "provisao_encontrada": round(valor_provisao, 2),
        "coerente": False,
        "detalhes": ""
    }
    
    if not holerites or valor_provisao == 0:
        validacao["detalhes"] = "Não há holerites ou provisão não encontrada."
        return validacao
    
    # Calcular média salarial anual dos holerites
    salarios_anuais = []
    for h in holerites:
        if isinstance(h, dict):
            salario_bruto = h.get("salario_bruto", 0) or h.get("salario_liquido", 0)
            if salario_bruto > 0:
                salarios_anuais.append(float(salario_bruto))
    
    if not salarios_anuais:
        validacao["detalhes"] = "Não foi possível calcular média salarial dos holerites."
        return validacao
    
    # Calcular média salarial anual
    media_salarial_anual = sum(salarios_anuais) / len(salarios_anuais)
    
    # Provisão mensal esperada = (média salarial anual / 12) × número de funcionários
    num_funcionarios = len(holerites)
    provisao_esperada_mensal = (media_salarial_anual / 12) * num_funcionarios
    
    validacao["provisao_esperada_mensal"] = round(provisao_esperada_mensal, 2)
    
    # Comparar provisão encontrada com esperada (tolerância de 20%)
    diff = abs(valor_provisao - provisao_esperada_mensal)
    tolerance = provisao_esperada_mensal * 0.20
    
    if diff <= tolerance:
        validacao["coerente"] = True
        validacao["detalhes"] = f"Provisão coerente. Esperada: R$ {_fmt_brl(provisao_esperada_mensal)}, Encontrada: R$ {_fmt_brl(valor_provisao)}."
    else:
        validacao["coerente"] = False
        validacao["detalhes"] = f"Provisão fora do esperado. Esperada: R$ {_fmt_brl(provisao_esperada_mensal)}, Encontrada: R$ {_fmt_brl(valor_provisao)}."
    
    return validacao


def _validate_vacation_payment(valor_ferias: float, holerites: List[Dict[str, Any]], periodo: str) -> Dict[str, Any]:
    """
    Valida pagamento de férias conforme cálculo CLT (salário + 1/3 adicional).
    
    Args:
        valor_ferias: Valor de férias pago
        holerites: Lista de holerites estruturados
        periodo: Período analisado
        
    Returns:
        Dict com validação do pagamento de férias
    """
    validacao = {
        "valor_esperado_ferias": 0.0,
        "valor_pago": round(valor_ferias, 2),
        "coerente": False,
        "detalhes": ""
    }
    
    if not holerites or valor_ferias == 0:
        validacao["detalhes"] = "Não há holerites ou pagamento de férias não encontrado."
        return validacao
    
    # Calcular valor esperado de férias = salário + (salário × 1/3)
    salarios = []
    for h in holerites:
        if isinstance(h, dict):
            salario_bruto = h.get("salario_bruto", 0) or h.get("salario_liquido", 0)
            if salario_bruto > 0:
                salarios.append(float(salario_bruto))
    
    if not salarios:
        validacao["detalhes"] = "Não foi possível calcular salários dos holerites."
        return validacao
    
    # Calcular valor esperado: para cada funcionário, férias = salário × 1.3333 (1 + 1/3)
    valor_esperado_total = sum(sal * 1.3333 for sal in salarios)
    
    validacao["valor_esperado_ferias"] = round(valor_esperado_total, 2)
    
    # Comparar com tolerância de 20%
    diff = abs(valor_ferias - valor_esperado_total)
    tolerance = valor_esperado_total * 0.20
    
    if diff <= tolerance:
        validacao["coerente"] = True
        validacao["detalhes"] = f"Pagamento de férias coerente. Esperado: R$ {_fmt_brl(valor_esperado_total)}, Pago: R$ {_fmt_brl(valor_ferias)}."
    else:
        validacao["coerente"] = False
        validacao["detalhes"] = f"Pagamento de férias fora do esperado. Esperado: R$ {_fmt_brl(valor_esperado_total)}, Pago: R$ {_fmt_brl(valor_ferias)}."
    
    return validacao


def _validate_13th_payment_months(df_13: pd.DataFrame, periodo: str) -> List[str]:
    """
    Valida se pagamento de 13º está nos meses corretos (novembro/dezembro).
    
    Args:
        df_13: DataFrame com transações de 13º salário
        periodo: Período analisado (formato "YYYY-MM")
        
    Returns:
        Lista de meses onde houve pagamento de 13º
    """
    meses_pagamento = []
    
    if df_13 is None or df_13.empty or "data" not in df_13.columns:
        return meses_pagamento
    
    try:
        # Extrair mês de cada transação
        df_13["mes"] = pd.to_datetime(df_13["data"], errors="coerce").dt.strftime("%Y-%m")
        meses_unicos = df_13["mes"].dropna().unique().tolist()
        meses_pagamento = [str(m) for m in meses_unicos if m]
        
        # Verificar se há pagamentos fora de nov/dez
        periodo_year = periodo.split("-")[0] if "-" in periodo else None
        if periodo_year:
            nov_esperado = f"{periodo_year}-11"
            dez_esperado = f"{periodo_year}-12"
            
            meses_fora = [m for m in meses_pagamento if m not in [nov_esperado, dez_esperado]]
            if meses_fora:
                logger.warning(f"[VALIDAÇÃO 13º] Pagamento de 13º encontrado em meses fora do esperado (nov/dez): {meses_fora}")
    except Exception as e:
        logger.debug(f"[VALIDAÇÃO 13º] Erro ao validar meses de pagamento: {e}")
    
    return meses_pagamento


def _has_explicit_payroll(holerites_detalhados: List[Dict[str, Any]], total_salarios: float, df_salarios: pd.DataFrame) -> bool:
    """
    Detecta se há folha explícita (holerites detalhados válidos ou transações de salário claras).
    
    Args:
        holerites_detalhados: Lista de holerites estruturados
        total_salarios: Total de salários encontrados em transações
        df_salarios: DataFrame com transações de salários
        
    Returns:
        True se há folha explícita, False caso contrário
    """
    # Verificar se há holerites detalhados válidos
    if holerites_detalhados:
        valid_holerites = [
            h for h in holerites_detalhados
            if isinstance(h, dict) and (
                h.get("salario_liquido", 0) > 0 or
                h.get("salario_bruto", 0) > 0 or
                (h.get("descontos", {}) and isinstance(h.get("descontos"), dict) and h.get("descontos", {}).get("total", 0) > 0)
            )
        ]
        if valid_holerites:
            return True
    
    # Verificar se há transações de salário claras e significativas
    if total_salarios > 0 and df_salarios is not None and not df_salarios.empty:
        # Se há múltiplas transações de salário ou valor significativo, considerar explícito
        if len(df_salarios) > 0 and total_salarios > 1000:  # Valor mínimo para considerar explícito
            return True
    
    return False


def _calcular_irrf_salario_2025(salario_bruto: float) -> float:
    """
    Calcula IRRF por salário segundo tabela 2025.
    Base aproximada: salário bruto (na prática seria bruto - INSS - deduções).
    """
    if salario_bruto <= 5000.00:
        return 0.0
    for limite, aliquota, parcela in TABELA_IRRF_2025[1:]:
        if salario_bruto <= limite:
            return max(0.0, salario_bruto * aliquota - parcela)
    return max(0.0, salario_bruto * TABELA_IRRF_2025[-1][1] - TABELA_IRRF_2025[-1][2])


def _calcular_irrf_total_holerites(holerites: List[Dict[str, Any]]) -> Tuple[float, int]:
    """
    Soma IRRF esperado de todos os holerites com salario_bruto.
    Retorna (total_irrf_esperado, quantidade de holerites com salário).
    """
    total = 0.0
    count = 0
    for h in holerites or []:
        if not isinstance(h, dict):
            continue
        bruto = h.get("salario_bruto", 0) or h.get("salario_liquido", 0) or 0
        if bruto > 0:
            total += _calcular_irrf_salario_2025(float(bruto))
            count += 1
    return (round(total, 2), count)


def refine_irrf_with_holerites(labor_analysis: Dict[str, Any], holerites: List[Dict[str, Any]]) -> None:
    """
    Refina a análise de IRRF quando há holerites individuais.
    IRRF é recolhido por salário; só é possível validar com holerites na pasta.
    Modifica labor_analysis in-place.
    """
    if not holerites or not labor_analysis:
        return
    enc = (labor_analysis.get("encargos") or {}).get("irrf") or {}
    valor_pago = enc.get("valor_pago", 0) or 0
    irrf_esperado, n_holerites = _calcular_irrf_total_holerites(holerites)
    if n_holerites == 0:
        return
    labor_analysis.setdefault("encargos", {})["irrf"] = enc
    enc["valor_calculado"] = irrf_esperado
    enc["analise_por_holerites"] = True
    tol = irrf_esperado * 0.15 if irrf_esperado > 0 else 0
    diff = abs(valor_pago - irrf_esperado)
    if valor_pago > 0:
        if diff <= tol:
            enc["status"] = "aplicado_conforme_tabela"
            enc["icon"] = "OK"
            enc["detalhes"] = (
                f"Lançado no balancete (R$ {_fmt_brl(valor_pago)}). "
                f"IRRF esperado (tabela 2025, {n_holerites} holerite(s)): R$ {_fmt_brl(irrf_esperado)}. "
                "Conferir comprovantes (DARFs) no demonstrativo."
            )
        else:
            enc["status"] = "irrf_divergente"
            enc["icon"] = "!"
            enc["detalhes"] = (
                f"Lançado no balancete (R$ {_fmt_brl(valor_pago)}). "
                f"IRRF esperado (tabela 2025, {n_holerites} holerite(s)): R$ {_fmt_brl(irrf_esperado)}. "
                "Divergência significativa. Conferir comprovantes e holerites."
            )
    else:
        if irrf_esperado > 0:
            enc["status"] = "possivel_inconsistencia"
            enc["icon"] = "!"
            enc["detalhes"] = (
                f"IRRF esperado (tabela 2025, {n_holerites} holerite(s)): R$ {_fmt_brl(irrf_esperado)}, "
                "mas não encontrado lançamento no balancete. Possível inconsistência."
            )
        else:
            enc["status"] = "aplicado_conforme_tabela"
            enc["icon"] = "OK"
            enc["detalhes"] = "IRRF isento (salários até R$ 5.000,00). Nenhum lançamento esperado."


def refine_base_calculo_from_holerites(labor_analysis: Dict[str, Any]) -> None:
    """
    Quando há holerites extraídos e a base da prestação está ausente (valor_base_folha = 0),
    usa a soma dos salários brutos dos holerites como base para cálculo de FGTS, INSS e PIS.
    Modifica labor_analysis in-place e recalcula valor_calculado/valor_esperado dos encargos.
    """
    if not labor_analysis:
        return
    base_calculo = labor_analysis.get("base_calculo") or {}
    holerites_detalhados = base_calculo.get("holerites_detalhados") or []
    if not holerites_detalhados:
        return

    soma_bruto_holerites = 0.0
    for h in holerites_detalhados:
        if not isinstance(h, dict):
            continue
        bruto = h.get("salario_bruto", 0) or 0
        if bruto <= 0:
            desc_total = (h.get("descontos") or {})
            if isinstance(desc_total, dict):
                desc_total = desc_total.get("total", 0) or 0
            bruto = (h.get("salario_liquido", 0) or 0) + (desc_total if isinstance(desc_total, (int, float)) else 0)
        if bruto > 0:
            soma_bruto_holerites += float(bruto)

    soma_bruto_holerites = round(soma_bruto_holerites, 2)
    base_calculo["folha_bruta_holerites"] = soma_bruto_holerites

    valor_base = base_calculo.get("valor_base_folha") or 0
    folha_total = base_calculo.get("folha_pagamento_total") or 0
    folha_por_estimativa = base_calculo.get("folha_por_estimativa", False)
    # Usar holerites como base só quando a prestação não trouxe folha (nem estimada)
    if soma_bruto_holerites > 0 and (valor_base == 0 or (folha_total == 0 and not folha_por_estimativa)):
        base_calculo["valor_base_folha"] = soma_bruto_holerites
        base_calculo["origem_base_impostos"] = "holerites_bruto"
        base_impostos = soma_bruto_holerites

        encargos = labor_analysis.setdefault("encargos", {})
        # FGTS
        fgts = encargos.get("fgts") or {}
        valor_fgts_calc = round(base_impostos * FGTS_PERCENT, 2)
        fgts["valor_calculado"] = valor_fgts_calc
        fgts["valor_esperado"] = valor_fgts_calc
        fgts["valor_encontrado"] = fgts.get("valor_pago") or 0
        fgts["base_calculo_utilizada"] = round(base_impostos, 2)
        vp = fgts.get("valor_pago") or 0
        fgts["valor_exibicao"] = round(vp, 2) if vp > 0 else valor_fgts_calc
        encargos["fgts"] = fgts

        # INSS
        inss = encargos.get("inss") or {}
        valor_inss_calc = round(base_impostos * INSS_PATRONAL_CONDOMINIO_PERCENT, 2)
        inss["valor_calculado"] = valor_inss_calc
        inss["valor_esperado"] = valor_inss_calc
        inss["valor_encontrado"] = inss.get("valor_pago") or 0
        inss["base_calculo_utilizada"] = round(base_impostos, 2)
        vp_inss = inss.get("valor_pago") or 0
        inss["valor_exibicao"] = round(vp_inss, 2) if vp_inss > 0 else valor_inss_calc
        encargos["inss"] = inss

        # PIS (1% sobre folha; base dos holerites sem exclusão 13º aqui)
        tributos = labor_analysis.setdefault("tributos", {})
        pis = tributos.get("pis") or {}
        num_meses = base_calculo.get("num_meses_periodo") or 1
        base_pis = base_impostos / num_meses if num_meses > 1 else base_impostos
        valor_pis_calc = round(base_pis * PIS_CONDOMINIO_PERCENT, 2)
        pis["valor_calculado"] = valor_pis_calc
        pis["valor_esperado"] = valor_pis_calc
        pis["valor_encontrado"] = pis.get("valor_pago") or 0
        pis["base_calculo_utilizada"] = round(base_impostos, 2)
        pis["base_para_comparacao"] = round(base_pis, 2)
        vp_pis = pis.get("valor_pago") or 0
        pis["valor_exibicao"] = round(vp_pis, 2) if vp_pis > 0 else valor_pis_calc
        tributos["pis"] = pis

        logger.info(f"[BASE HOLERITES] Base de cálculo definida a partir dos holerites: R$ {_fmt_brl(soma_bruto_holerites)} (FGTS/INSS/PIS recalculados).")


def _extract_transaction_context(df_transactions: pd.DataFrame, df_original: pd.DataFrame) -> Dict[str, Any]:
    """
    Extrai informações de contexto (data, conta) das transações.
    
    Args:
        df_transactions: DataFrame com transações filtradas
        df_original: DataFrame original completo
        
    Returns:
        Dict com data_pagamento e conta_utilizada
    """
    context = {
        "data_pagamento": None,
        "conta_utilizada": None
    }
    
    if df_transactions is None or df_transactions.empty:
        return context
    
    # Extrair data da primeira transação significativa
    if "data" in df_transactions.columns:
        dates = df_transactions["data"].dropna()
        if not dates.empty:
            try:
                date_val = pd.to_datetime(dates.iloc[0], errors="coerce")
                if pd.notna(date_val):
                    context["data_pagamento"] = date_val.strftime("%Y-%m-%d")
            except Exception:
                pass
    
    # Extrair conta utilizada (procurar em colunas como "conta", "conta_credito", "conta_debito", "historico")
    conta_keywords = ["conta", "conta_credito", "conta_debito", "historico", "histórico", "descricao"]
    for col in df_transactions.columns:
        if any(kw in col.lower() for kw in conta_keywords):
            values = df_transactions[col].dropna()
            if not values.empty:
                conta_val = str(values.iloc[0]).strip()
                if conta_val and len(conta_val) > 0:
                    # Procurar padrões de conta (ex: "01-ORDINARIA", "PESSOAL", etc.)
                    if "ordinaria" in conta_val.lower() or "ordinária" in conta_val.lower() or "pessoal" in conta_val.lower():
                        context["conta_utilizada"] = conta_val[:100]  # Limitar tamanho
                        break
    
    return context


def analyze_labor_charges(df: pd.DataFrame, document_context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Analisa encargos trabalhistas no DataFrame. Se document_context tiver base_folha_mes_anterior, usa como valor_base_folha."""
    result = {
        "base_calculo": {
            "folha_pagamento_total": 0.0,
            "valor_base_folha": 0.0,  # Base para cálculo dos impostos (folha)
            "origem_base_impostos": None,  # "holerites_bruto" | "folha_pagamento_total" | "folha_estimada_fgts"
            "inclui_adiantamento": False,
            "periodo": None,
            "transacoes_folha": [],
            "holerites_detalhados": [],
            "folha_por_estimativa": False,  # NOVO
            "folha_estimada_fgts": 0.0,  # NOVO
            "alerta_folha_ausente": False,  # NOVO
            "cross_reference": {  # NOVO
                "folha_liquida": 0.0,
                "adiantamentos": 0.0,
                "ferias_rescisoes": 0.0,
                "beneficios": 0.0
            }
        },
        "encargos": {
            "fgts": {
                "percentual": TABELA_ENCARGOS["fgts"]["percentual"],
                "base_calculo": TABELA_ENCARGOS["fgts"]["base_calculo"],
                "quem_paga": TABELA_ENCARGOS["fgts"]["quem_paga"],
                "valor_calculado": 0.0,
                "valor_pago": 0.0,
                "status": "nao_auditavel",
                "icon": "?",
                "detalhes": FRASE_ENCARGO_NAO_AUDITAVEL,
                "transacoes": [],
                "data_pagamento": None,
                "conta_utilizada": None,
                "analise_por_estimativa": False,
            },
            "inss": {
                "tipo": "patronal, funcionários e terceiros",
                "percentual": TABELA_ENCARGOS["inss"]["percentual"],
                "base_calculo": TABELA_ENCARGOS["inss"]["base_calculo"],
                "quem_paga": TABELA_ENCARGOS["inss"]["quem_paga"],
                "valor_calculado": 0.0,
                "valor_pago": 0.0,
                "status": "nao_auditavel",
                "icon": "?",
                "detalhes": FRASE_ENCARGO_NAO_AUDITAVEL,
                "transacoes": [],
                "data_pagamento": None,
                "conta_utilizada": None,
                "analise_por_estimativa": False,
            },
            "irrf": {
                "percentual": TABELA_ENCARGOS["irrf"]["percentual"],
                "base_calculo": TABELA_ENCARGOS["irrf"]["base_calculo"],
                "quem_paga": TABELA_ENCARGOS["irrf"]["quem_paga"],
                "valor_pago": 0.0,
                "status": "nao_auditavel",
                "icon": "?",
                "detalhes": FRASE_ENCARGO_NAO_AUDITAVEL,
                "transacoes": [],
                "data_pagamento": None,
                "conta_utilizada": None,
                "analise_por_estimativa": False,
            },
            "contrib_sindical": {
                "percentual": TABELA_ENCARGOS["contrib_sindical"]["percentual"],
                "base_calculo": TABELA_ENCARGOS["contrib_sindical"]["base_calculo"],
                "quem_paga": TABELA_ENCARGOS["contrib_sindical"]["quem_paga"],
                "valor_pago": 0.0,
                "status": "nao_auditavel",
                "icon": "?",
                "detalhes": FRASE_ENCARGO_NAO_AUDITAVEL,
                "transacoes": [],
                "data_pagamento": None,
                "conta_utilizada": None,
            },
            "sat_rat": {
                "percentual": TABELA_ENCARGOS["sat_rat"]["percentual"],
                "base_calculo": TABELA_ENCARGOS["sat_rat"]["base_calculo"],
                "quem_paga": TABELA_ENCARGOS["sat_rat"]["quem_paga"],
                "valor_pago": 0.0,
                "status": "nao_auditavel",
                "icon": "?",
                "detalhes": FRASE_ENCARGO_NAO_AUDITAVEL,
                "transacoes": [],
                "data_pagamento": None,
                "conta_utilizada": None,
            },
        },
        "tributos": {
            "pis": {
                "codigo": "8301",
                "percentual": TABELA_ENCARGOS["pis"]["percentual"],
                "base_calculo": TABELA_ENCARGOS["pis"]["base_calculo"],
                "quem_paga": TABELA_ENCARGOS["pis"]["quem_paga"],
                "valor_pago": 0.0,
                "status": "nao_auditavel",
                "icon": "?",
                "detalhes": FRASE_ENCARGO_NAO_AUDITAVEL,
                "transacoes": [],
                "data_pagamento": None,
                "conta_utilizada": None,
                "analise_por_estimativa": False,
            },
            "iss": {
                "percentual": TABELA_ENCARGOS["iss"]["percentual"],
                "base_calculo": TABELA_ENCARGOS["iss"]["base_calculo"],
                "quem_paga": TABELA_ENCARGOS["iss"]["quem_paga"],
                "valor_pago": 0.0,
                "status": "nao_auditavel",
                "icon": "?",
                "detalhes": FRASE_ENCARGO_NAO_AUDITAVEL,
                "transacoes": [],
            }
        },
        "folha_invalida": False,
        "ferias_13": {
            "provisao": {"presente": False, "valor": 0.0, "detalhes": "Não foi identificada provisão de férias/13º", "icon": "?"},
            "pagamentos": {"ferias_no_periodo": False, "decimo_terceiro_no_periodo": False, "valor_ferias": 0.0, "valor_13": 0.0, "detalhes": "Não aparece movimentação de pagamento de férias nem 13º neste período"},
            "validacao_provisao": {  # NOVO
                "provisao_esperada_mensal": 0.0,
                "provisao_encontrada": 0.0,
                "coerente": False,
                "detalhes": ""
            },
            "validacao_pagamento": {  # NOVO
                "valor_esperado_13": 0.0,
                "valor_esperado_ferias": 0.0,
                "meses_pagamento_13": [],
                "coerente": False
            }
        },
        "resumo": "Análise de encargos trabalhistas não identificou transações relevantes.",
        "analise_por_estimativa": False  # NOVO
    }
    
    if df is None or df.empty:
        return result
    
    # Extração por coluna (planilhas com colunas INSS, IRRF, FGTS, PIS, etc.)
    encargos_from_cols = _extract_encargos_from_columns(df)
    
    num_meses = 1
    if "data" in df.columns:
        try:
            dates = pd.to_datetime(df["data"], errors="coerce").dropna()
            if not dates.empty:
                dmin, dmax = dates.min(), dates.max()
                result["base_calculo"]["periodo"] = f"{dmin.strftime('%Y-%m')} a {dmax.strftime('%Y-%m')}"
                meses_diff = (dmax.year - dmin.year) * 12 + (dmax.month - dmin.month) + 1
                num_meses = max(1, int(meses_diff))
        except Exception:
            pass
    
    # Extrair salários e adiantamentos, excluindo 13º/provisão (PIS incide só sobre folha) e linhas não relacionadas
    exclusion_folha = EXCLUSION_KEYWORDS_ENCARGOS + KEYWORDS_DECIMO_TERCEIRO + KEYWORDS_PROVISAO
    df_salarios = _extract_transactions_by_keywords(df, KEYWORDS_SALARIO, exclusion_patterns=exclusion_folha)
    df_adiantamento = _extract_transactions_by_keywords(df, KEYWORDS_ADIANTAMENTO, exclusion_patterns=EXCLUSION_KEYWORDS_ENCARGOS)
    total_adiantamento = _calculate_total(df_adiantamento)
    total_salarios = _calculate_total(df_salarios)
    # Heurística: se existir linha de "total folha" na prestação, usar só esse valor (evitar misturar com detalhes)
    total_folha_linha = _get_total_folha_linha_prestacao(df_salarios)
    if total_folha_linha is not None:
        total_salarios = total_folha_linha
        result["base_calculo"]["folha_por_linha_total"] = True
    # Holerites servem só para has_explicit_payroll e exibição; não alteram folha_pagamento_total
    holerites_detalhados = result["base_calculo"].get("holerites_detalhados", [])
    
    # PASSO 2: Verificar se há folha explícita e aplicar estimativa se necessário
    has_explicit = _has_explicit_payroll(holerites_detalhados, total_salarios, df_salarios)
    folha_estimada_fgts = 0.0
    
    if not has_explicit:
        # Não há folha explícita - vamos tentar estimar pelo FGTS
        logger.info("[ESTIMATIVA] Folha explícita não encontrada. Tentando estimar pelo FGTS...")
        result["base_calculo"]["alerta_folha_ausente"] = True
    
    result["base_calculo"]["folha_pagamento_total"] = total_salarios + total_adiantamento
    result["base_calculo"]["inclui_adiantamento"] = total_adiantamento > 0
    result["base_calculo"]["transacoes_folha"] = _get_transactions_list(df_salarios)
    result["base_calculo"]["num_meses_periodo"] = num_meses
    
    # Extração antecipada de 13º e provisão para eventual exclusão da base PIS (PIS não incide sobre 13º)
    df_13_early = _extract_transactions_by_keywords(df, KEYWORDS_DECIMO_TERCEIRO, exclusion_patterns=EXCLUSION_KEYWORDS_13_SALARIO)
    df_provisao_early = _extract_transactions_by_keywords(df, KEYWORDS_PROVISAO, exclusion_patterns=EXCLUSION_KEYWORDS_13_SALARIO)
    valor_13_early = _calculate_total(df_13_early)
    valor_provisao_early = _calculate_total(df_provisao_early)
    
    # PASSO 1: Extrair FGTS com data e conta (identificação de encargos)
    df_fgts = _extract_transactions_by_keywords(df, KEYWORDS_FGTS, exclusion_patterns=EXCLUSION_KEYWORDS_ENCARGOS)
    valor_fgts_pago = _valor_encargo_preferindo_total(df_fgts, "fgts")
    if encargos_from_cols.get("fgts", 0) > 0:
        valor_fgts_pago = encargos_from_cols["fgts"]
    
    # Extrair contexto (data e conta) do FGTS
    fgts_context = _extract_transaction_context(df_fgts, df)
    result["encargos"]["fgts"]["data_pagamento"] = fgts_context.get("data_pagamento")
    result["encargos"]["fgts"]["conta_utilizada"] = fgts_context.get("conta_utilizada")
    
    # PASSO 2: Se não há folha explícita e há FGTS, estimar folha
    if not has_explicit and valor_fgts_pago > 0:
        folha_estimada_fgts = _estimate_payroll_from_fgts(valor_fgts_pago)
        result["base_calculo"]["folha_por_estimativa"] = True
        result["base_calculo"]["folha_estimada_fgts"] = round(folha_estimada_fgts, 2)
        result["base_calculo"]["folha_pagamento_total"] = folha_estimada_fgts  # Usar folha estimada como base
        result["encargos"]["fgts"]["analise_por_estimativa"] = True
        logger.info(f"[ESTIMATIVA] Folha estimada pelo FGTS: R$ {_fmt_brl(folha_estimada_fgts)}")
    elif not has_explicit and valor_fgts_pago == 0:
        logger.warning("[ESTIMATIVA] Folha explícita não encontrada e FGTS não identificado. Não é possível estimar folha.")
    
    # Valor base da folha para cálculo dos impostos (INSS, FGTS, PIS): SEMPRE da prestação de contas.
    # Não usar holerites para essa base, para evitar misturar folha de pagamento com prestação.
    holerites_detalhados = result["base_calculo"].get("holerites_detalhados", [])
    soma_bruto_holerites = 0.0
    for h in holerites_detalhados:
        if not isinstance(h, dict):
            continue
        bruto = h.get("salario_bruto", 0) or 0
        if bruto <= 0:
            desc_total = (h.get("descontos") or {})
            if isinstance(desc_total, dict):
                desc_total = desc_total.get("total", 0) or 0
            bruto = (h.get("salario_liquido", 0) or 0) + (desc_total if isinstance(desc_total, (int, float)) else 0)
        if bruto > 0:
            soma_bruto_holerites += float(bruto)
    result["base_calculo"]["folha_bruta_holerites"] = round(soma_bruto_holerites, 2)  # Informativo; não usado como base
    base_folha_mes_anterior = None
    if document_context and isinstance(document_context, dict):
        base_folha_mes_anterior = document_context.get("base_folha_mes_anterior")
    if base_folha_mes_anterior is not None and float(base_folha_mes_anterior) > 0:
        result["base_calculo"]["valor_base_folha"] = round(float(base_folha_mes_anterior), 2)
        result["base_calculo"]["origem_base_impostos"] = "folha_mes_anterior"
        if document_context.get("period_mes_anterior"):
            result["base_calculo"]["period_mes_anterior"] = document_context["period_mes_anterior"]
        if document_context.get("period_mes_principal"):
            result["base_calculo"]["period_mes_principal"] = document_context["period_mes_principal"]
    else:
        result["base_calculo"]["valor_base_folha"] = result["base_calculo"]["folha_pagamento_total"]
        result["base_calculo"]["origem_base_impostos"] = "folha_estimada_fgts" if result["base_calculo"].get("folha_por_estimativa") else "folha_pagamento_total"
    base_impostos = result["base_calculo"]["valor_base_folha"]
    
    valor_fgts_calculado = base_impostos * FGTS_PERCENT
    result["encargos"]["fgts"]["valor_calculado"] = round(valor_fgts_calculado, 2)
    result["encargos"]["fgts"]["valor_pago"] = round(valor_fgts_pago, 2)
    result["encargos"]["fgts"]["base_calculo_utilizada"] = round(base_impostos, 2)
    result["encargos"]["fgts"]["percentual_baseline"] = round(FGTS_PERCENT * 100, 1)
    result["encargos"]["fgts"]["valor_esperado"] = round(valor_fgts_calculado, 2)
    result["encargos"]["fgts"]["valor_encontrado"] = round(valor_fgts_pago, 2)
    result["encargos"]["fgts"]["valor_exibicao"] = round(valor_fgts_pago, 2) if valor_fgts_pago > 0 else round(valor_fgts_calculado, 2)
    result["encargos"]["fgts"]["encontrado_no_documento"] = valor_fgts_pago > 0
    result["encargos"]["fgts"]["transacoes"] = _get_transactions_list(df_fgts)
    
    if valor_fgts_pago > 0:
        tolerance = max(valor_fgts_calculado * TOLERANCIA_PERCENT_VALIDACAO, TOLERANCIA_ABSOLUTA_VALIDACAO)
        diff = abs(valor_fgts_pago - valor_fgts_calculado)
        if diff <= tolerance:
            result["encargos"]["fgts"]["status"] = "correto"
            result["encargos"]["fgts"]["icon"] = "OK"
            result["encargos"]["fgts"]["detalhes"] = f"Valor encontrado (R$ {_fmt_brl(valor_fgts_pago)}) conferido: está alinhado à base (R$ {_fmt_brl(base_impostos)}) e ao percentual (8%). Conferir comprovantes (guias/GPS) no demonstrativo."
        elif valor_fgts_pago > valor_fgts_calculado:
            result["encargos"]["fgts"]["status"] = "incorreto"
            result["encargos"]["fgts"]["icon"] = "!"
            result["encargos"]["fgts"]["base_implicita"] = round(valor_fgts_pago / FGTS_PERCENT, 2)
            result["encargos"]["fgts"]["detalhes"] = f"Valor encontrado (R$ {_fmt_brl(valor_fgts_pago)}) acima do esperado (R$ {_fmt_brl(valor_fgts_calculado)}) com base R$ {_fmt_brl(base_impostos)} e 8%. Conferir comprovantes e conferência manual."
        else:
            result["encargos"]["fgts"]["status"] = "incorreto"
            result["encargos"]["fgts"]["icon"] = "!"
            result["encargos"]["fgts"]["base_implicita"] = round(valor_fgts_pago / FGTS_PERCENT, 2)
            result["encargos"]["fgts"]["detalhes"] = f"Valor encontrado (R$ {_fmt_brl(valor_fgts_pago)}) difere do esperado (R$ {_fmt_brl(valor_fgts_calculado)}) com base R$ {_fmt_brl(base_impostos)} e 8%. Conferir comprovantes e conferência manual."
    elif valor_fgts_calculado > 0:
        result["encargos"]["fgts"]["status"] = "nao_auditavel"
        result["encargos"]["fgts"]["icon"] = "?"
        result["encargos"]["fgts"]["detalhes"] = FRASE_ENCARGO_NAO_AUDITAVEL
    
    # Validação cruzada da base: FGTS e holerites
    TOLERANCIA_PCT_BASE_FGTS = 15.0  # diferença <= 15% = base coerente com FGTS
    base_utilizada = result["base_calculo"]["valor_base_folha"]
    if valor_fgts_pago > 0 and base_utilizada > 0:
        base_implicita_fgts = round(valor_fgts_pago / FGTS_PERCENT, 2)
        ref = max(base_utilizada, base_implicita_fgts)
        diferenca_pct_fgts = abs(base_utilizada - base_implicita_fgts) / ref * 100.0 if ref > 0 else 0.0
        coerente_fgts = diferenca_pct_fgts <= TOLERANCIA_PCT_BASE_FGTS
        result["base_calculo"]["validacao_base_fgts"] = {
            "base_utilizada": round(base_utilizada, 2),
            "base_implicita_fgts": base_implicita_fgts,
            "diferenca_percentual": round(diferenca_pct_fgts, 2),
            "coerente": coerente_fgts,
        }
    else:
        result["base_calculo"]["validacao_base_fgts"] = None
        coerente_fgts = False
    folha_bruta_h = result["base_calculo"].get("folha_bruta_holerites", 0) or 0
    if folha_bruta_h > 0 and base_utilizada > 0:
        ref_h = max(base_utilizada, folha_bruta_h)
        diferenca_pct_holerites = abs(base_utilizada - folha_bruta_h) / ref_h * 100.0
        result["base_calculo"]["validacao_base_holerites"] = {
            "base_prestacao": round(base_utilizada, 2),
            "soma_holerites": round(folha_bruta_h, 2),
            "diferenca_percentual": round(diferenca_pct_holerites, 2),
        }
        result["base_calculo"]["diferenca_pct_base_holerites"] = round(diferenca_pct_holerites, 2)
    else:
        result["base_calculo"]["validacao_base_holerites"] = None
    # Indicador de confiança da base
    folha_por_linha_total = result["base_calculo"].get("folha_por_linha_total", False)
    folha_por_estimativa = result["base_calculo"].get("folha_por_estimativa", False)
    if folha_por_linha_total:
        result["base_calculo"]["confianca_base"] = "alta"
        result["base_calculo"]["motivo_confianca"] = "Base obtida por linha de total na prestação."
    elif not folha_por_estimativa and valor_fgts_pago > 0 and coerente_fgts:
        result["base_calculo"]["confianca_base"] = "alta"
        result["base_calculo"]["motivo_confianca"] = "Base coerente com o valor de FGTS lançado (8%)."
    elif not folha_por_estimativa and valor_fgts_pago > 0 and not coerente_fgts:
        result["base_calculo"]["confianca_base"] = "media"
        result["base_calculo"]["motivo_confianca"] = "Base da prestação (soma de transações). Divergente do FGTS; conferir comprovantes."
    elif not folha_por_estimativa:
        result["base_calculo"]["confianca_base"] = "media"
        result["base_calculo"]["motivo_confianca"] = "Base da prestação (soma de transações). Sem conferência com FGTS; conferir comprovantes."
    elif folha_por_estimativa:
        result["base_calculo"]["confianca_base"] = "baixa"
        result["base_calculo"]["motivo_confianca"] = "Base estimada pelo FGTS."
    else:
        result["base_calculo"]["confianca_base"] = "baixa"
        result["base_calculo"]["motivo_confianca"] = "Base sem conferência cruzada disponível; recomenda-se conferir demonstrativo."
    
    # INSS: valor encontrado = extraído do documento (keywords ou colunas). Esperado = base_impostos (valor_base_folha) × %.
    # PASSO 1: Extrair INSS com data e conta
    df_inss = _extract_transactions_by_keywords(df, KEYWORDS_INSS, exclusion_patterns=EXCLUSION_KEYWORDS_ENCARGOS)
    valor_inss_pago = _valor_encargo_preferindo_total(df_inss, "inss")
    if encargos_from_cols.get("inss", 0) > 0:
        valor_inss_pago = encargos_from_cols["inss"]
    
    # Extrair contexto (data e conta) do INSS
    inss_context = _extract_transaction_context(df_inss, df)
    result["encargos"]["inss"]["data_pagamento"] = inss_context.get("data_pagamento")
    result["encargos"]["inss"]["conta_utilizada"] = inss_context.get("conta_utilizada")
    
    # PASSO 3: Cálculo e validação INSS (baseline 27,8% patronal sobre valor base da folha)
    valor_inss_estimado = base_impostos * INSS_PATRONAL_CONDOMINIO_PERCENT
    inss_min_esperado = base_impostos * 0.25  # Faixa validação 25% a 30%
    inss_max_esperado = base_impostos * 0.30
    if result["base_calculo"]["folha_por_estimativa"]:
        result["encargos"]["inss"]["analise_por_estimativa"] = True
    
    result["encargos"]["inss"]["valor_calculado"] = round(valor_inss_estimado, 2)
    result["encargos"]["inss"]["valor_pago"] = round(valor_inss_pago, 2)
    result["encargos"]["inss"]["base_calculo_utilizada"] = round(base_impostos, 2)
    result["encargos"]["inss"]["percentual_baseline"] = round(INSS_PATRONAL_CONDOMINIO_PERCENT * 100, 1)
    result["encargos"]["inss"]["valor_esperado"] = round(valor_inss_estimado, 2)
    result["encargos"]["inss"]["valor_encontrado"] = round(valor_inss_pago, 2)
    result["encargos"]["inss"]["valor_exibicao"] = round(valor_inss_pago, 2) if valor_inss_pago > 0 else round(valor_inss_estimado, 2)
    result["encargos"]["inss"]["encontrado_no_documento"] = valor_inss_pago > 0
    result["encargos"]["inss"]["transacoes"] = _get_transactions_list(df_inss)

    # Referência a partir dos holerites extraídos (soma do INSS descontado nos holerites = parte do empregado)
    inss_soma_holerites = 0.0
    for h in holerites_detalhados:
        if not isinstance(h, dict):
            continue
        desc = h.get("descontos") or {}
        if isinstance(desc, dict):
            inss_h = desc.get("inss", 0) or 0
            if inss_h > 0:
                inss_soma_holerites += float(inss_h)
    if inss_soma_holerites > 0:
        result["encargos"]["inss"]["inss_soma_holerites"] = round(inss_soma_holerites, 2)
        result["encargos"]["inss"]["inss_referencia_holerites"] = True
    
    # Baseline 27,8% é patronal e vale somente sobre salário; pró-labore ou outro lançamento pode alterar o valor.
    _inss_note_patronal = " O baseline 27,8% é patronal (empregador)."
    _inss_note_somente_salario = " O baseline 27,8% vale somente sobre salário; se houver pró-labore ou outra remuneração, o valor pode diferir. Recomenda-se conferir com o condomínio."
    _inss_note_menor = " O valor lançado pode ser apenas o INSS descontado dos funcionários (parte do empregado). O total patronal (27,8%) pode estar em outra linha ou guia."
    _inss_note_prolabore_incorreto = " Pode haver pró-labore ou outro lançamento nas contas do condomínio; confirmar com o condomínio se existe esse tipo de pagamento."
    if valor_inss_pago > 0:
        tolerance = max(valor_inss_estimado * TOLERANCIA_PERCENT_VALIDACAO, TOLERANCIA_ABSOLUTA_VALIDACAO)
        diff = abs(valor_inss_pago - valor_inss_estimado)
        if result["base_calculo"]["folha_por_estimativa"]:
            if inss_min_esperado <= valor_inss_pago <= inss_max_esperado:
                result["encargos"]["inss"]["status"] = "correto"
                result["encargos"]["inss"]["icon"] = "OK"
                result["encargos"]["inss"]["detalhes"] = f"Valor encontrado (R$ {_fmt_brl(valor_inss_pago)}) conferido: está alinhado à base estimada e ao percentual (27,8% patronal).{_inss_note_somente_salario} Conferir comprovantes (guias/GPS) no demonstrativo."
            else:
                result["encargos"]["inss"]["status"] = "incorreto"
                result["encargos"]["inss"]["icon"] = "!"
                result["encargos"]["inss"]["base_implicita"] = round(valor_inss_pago / INSS_PATRONAL_CONDOMINIO_PERCENT, 2)
                result["encargos"]["inss"]["alerta_prolabore_ou_extra"] = True
                result["encargos"]["inss"]["detalhes"] = f"Valor encontrado (R$ {_fmt_brl(valor_inss_pago)}) difere do esperado (R$ {_fmt_brl(valor_inss_estimado)}) com base R$ {_fmt_brl(base_impostos)} e 27,8% (fora da faixa esperada).{_inss_note_patronal}{_inss_note_menor if valor_inss_pago < valor_inss_estimado else ''}{_inss_note_prolabore_incorreto}{_inss_note_somente_salario} Conferir comprovantes e conferência manual."
        else:
            if diff <= tolerance:
                result["encargos"]["inss"]["status"] = "correto"
                result["encargos"]["inss"]["icon"] = "OK"
                result["encargos"]["inss"]["detalhes"] = f"Valor encontrado (R$ {_fmt_brl(valor_inss_pago)}) conferido: está alinhado à base (R$ {_fmt_brl(base_impostos)}) e ao percentual (27,8% patronal).{_inss_note_somente_salario} Conferir comprovantes (guias/GPS) no demonstrativo."
            else:
                result["encargos"]["inss"]["status"] = "incorreto"
                result["encargos"]["inss"]["icon"] = "!"
                result["encargos"]["inss"]["base_implicita"] = round(valor_inss_pago / INSS_PATRONAL_CONDOMINIO_PERCENT, 2)
                result["encargos"]["inss"]["alerta_prolabore_ou_extra"] = True
                result["encargos"]["inss"]["detalhes"] = f"Valor encontrado (R$ {_fmt_brl(valor_inss_pago)}) difere do esperado (R$ {_fmt_brl(valor_inss_estimado)}) com base R$ {_fmt_brl(base_impostos)} e 27,8%.{_inss_note_patronal}{_inss_note_menor if valor_inss_pago < valor_inss_estimado else ''}{_inss_note_prolabore_incorreto}{_inss_note_somente_salario} Conferir comprovantes e conferência manual."
    elif valor_inss_estimado > 0:
        result["encargos"]["inss"]["status"] = "nao_auditavel"
        result["encargos"]["inss"]["icon"] = "?"
        result["encargos"]["inss"]["detalhes"] = (
            FRASE_ENCARGO_NAO_AUDITAVEL + " Recomenda-se cobrar do condomínio a confirmação: se há recolhimento de INSS e onde está lançado nas contas, para conferência."
        )
    
    # PASSO 1: Extrair IRRF com data e conta
    df_irrf = _extract_transactions_by_keywords(df, KEYWORDS_IRRF, exclusion_patterns=EXCLUSION_KEYWORDS_ENCARGOS)
    valor_irrf_pago = _valor_encargo_preferindo_total(df_irrf, "irrf")
    if encargos_from_cols.get("irrf", 0) > 0:
        valor_irrf_pago = encargos_from_cols["irrf"]
    
    # Extrair contexto (data e conta) do IRRF
    irrf_context = _extract_transaction_context(df_irrf, df)
    result["encargos"]["irrf"]["data_pagamento"] = irrf_context.get("data_pagamento")
    result["encargos"]["irrf"]["conta_utilizada"] = irrf_context.get("conta_utilizada")
    
    result["encargos"]["irrf"]["valor_pago"] = round(valor_irrf_pago, 2)
    result["encargos"]["irrf"]["transacoes"] = _get_transactions_list(df_irrf)
    
    # IRRF: recolhido por salário individual, não pelo total da folha.
    # Sem holerites de cada funcionário na pasta, NÃO é possível calcular nem validar.
    # A prestação de contas não traz holerites; refine_irrf_with_holerites será chamado
    # após merge de holerites (se houver).
    if valor_irrf_pago > 0:
        result["encargos"]["irrf"]["status"] = "nao_auditavel"
        result["encargos"]["irrf"]["icon"] = "?"
        result["encargos"]["irrf"]["detalhes"] = (
            f"Lançado no balancete (R$ {_fmt_brl(valor_irrf_pago)}). "
            + FRASE_IRRF_NAO_AUDITAVEL_SEM_HOLERITES
        )
    else:
        result["encargos"]["irrf"]["status"] = "nao_auditavel"
        result["encargos"]["irrf"]["icon"] = "?"
        result["encargos"]["irrf"]["detalhes"] = FRASE_IRRF_NAO_AUDITAVEL_SEM_HOLERITES
    
    # PIS: valor encontrado = extraído do documento (keywords ou colunas). Esperado = base_impostos (valor_base_folha) × %.
    # PASSO 1: Extrair PIS com data e conta
    df_pis = _extract_transactions_by_keywords(df, KEYWORDS_PIS, exclusion_patterns=EXCLUSION_KEYWORDS_ENCARGOS)
    valor_pis_pago = _valor_encargo_preferindo_total(df_pis, "pis")
    if encargos_from_cols.get("pis", 0) > 0:
        valor_pis_pago = encargos_from_cols["pis"]
    
    # Extrair contexto (data e conta) do PIS
    pis_context = _extract_transaction_context(df_pis, df)
    result["tributos"]["pis"]["data_pagamento"] = pis_context.get("data_pagamento")
    result["tributos"]["pis"]["conta_utilizada"] = pis_context.get("conta_utilizada")
    
    if result["base_calculo"]["folha_por_estimativa"]:
        result["tributos"]["pis"]["analise_por_estimativa"] = True
    
    # PIS: baseline 1% só da folha (legislação: não incide sobre 13º); excluir 13º e provisão da base em qualquer origem
    base_antes_exclusao = base_impostos
    valor_excluido_pis = 0.0
    if valor_13_early > 0 or valor_provisao_early > 0:
        valor_excluido_pis = valor_13_early + valor_provisao_early
        base_antes_exclusao = max(0.0, base_impostos - valor_excluido_pis)
    num_meses_pis = result["base_calculo"].get("num_meses_periodo", 1) or 1
    base_pis = base_antes_exclusao / num_meses_pis if num_meses_pis > 1 else base_antes_exclusao
    valor_pis_calculado = base_pis * PIS_CONDOMINIO_PERCENT
    result["tributos"]["pis"]["valor_calculado"] = round(valor_pis_calculado, 2)
    result["tributos"]["pis"]["valor_pago"] = round(valor_pis_pago, 2)
    result["tributos"]["pis"]["base_calculo_utilizada"] = round(base_antes_exclusao, 2)
    result["tributos"]["pis"]["base_para_comparacao"] = round(base_pis, 2)  # base mensal quando multi-mês
    result["tributos"]["pis"]["num_meses_periodo"] = num_meses_pis
    result["tributos"]["pis"]["percentual_baseline"] = round(PIS_CONDOMINIO_PERCENT * 100, 1)
    result["tributos"]["pis"]["valor_esperado"] = round(valor_pis_calculado, 2)
    result["tributos"]["pis"]["valor_encontrado"] = round(valor_pis_pago, 2)
    result["tributos"]["pis"]["valor_exibicao"] = round(valor_pis_pago, 2) if valor_pis_pago > 0 else round(valor_pis_calculado, 2)
    result["tributos"]["pis"]["encontrado_no_documento"] = valor_pis_pago > 0
    result["tributos"]["pis"]["transacoes"] = _get_transactions_list(df_pis)
    base_pis_explicacao = f"Base PIS: R$ {_fmt_brl(base_antes_exclusao)}" + (f" (base folha R$ {_fmt_brl(base_impostos)} menos 13º/provisão R$ {_fmt_brl(valor_excluido_pis)})" if valor_excluido_pis > 0 else "")
    if valor_pis_pago > 0:
        tolerance_pis = max(valor_pis_calculado * TOLERANCIA_PERCENT_VALIDACAO, TOLERANCIA_ABSOLUTA_VALIDACAO)
        diff_pis = abs(valor_pis_pago - valor_pis_calculado)
        if diff_pis <= tolerance_pis:
            result["tributos"]["pis"]["status"] = "correto"
            result["tributos"]["pis"]["icon"] = "OK"
            base_txt = f"R$ {_fmt_brl(base_pis)}{' mensal' if num_meses_pis > 1 else ''}"
            result["tributos"]["pis"]["detalhes"] = f"Valor encontrado (R$ {_fmt_brl(valor_pis_pago)}) conferido: está alinhado à base ({base_txt}) e ao percentual (1%). {base_pis_explicacao}. Conferir comprovantes (DARFs) no demonstrativo."
        else:
            result["tributos"]["pis"]["status"] = "incorreto"
            result["tributos"]["pis"]["icon"] = "!"
            result["tributos"]["pis"]["base_implicita"] = round(valor_pis_pago / PIS_CONDOMINIO_PERCENT, 2)
            result["tributos"]["pis"]["detalhes"] = f"Valor encontrado (R$ {_fmt_brl(valor_pis_pago)}) difere do esperado (R$ {_fmt_brl(valor_pis_calculado)}) com base R$ {_fmt_brl(base_antes_exclusao)} e 1%. {base_pis_explicacao}. Conferir comprovantes. Verificar se a base inclui 13º ou período diferente do analisado."
    
    # Extrair ISS, excluindo linhas não relacionadas
    df_iss = _extract_transactions_by_keywords(df, KEYWORDS_ISS, exclusion_patterns=EXCLUSION_KEYWORDS_ENCARGOS)
    valor_iss_pago = _valor_encargo_preferindo_total(df_iss, "iss")
    if encargos_from_cols.get("iss", 0) > 0:
        valor_iss_pago = encargos_from_cols["iss"]
    result["tributos"]["iss"]["valor_pago"] = round(valor_iss_pago, 2)
    result["tributos"]["iss"]["transacoes"] = _get_transactions_list(df_iss)
    if valor_iss_pago > 0:
        result["tributos"]["iss"]["status"] = "recolhido_quando_devido"
        result["tributos"]["iss"]["icon"] = "OK"
        result["tributos"]["iss"]["detalhes"] = f"Lançado no balancete (R$ {_fmt_brl(valor_iss_pago)}). Recomenda-se conferir comprovantes no PDF."
    
    # Extrair Contribuição Sindical e SAT/RAT
    df_contrib_sindical = _extract_transactions_by_keywords(df, KEYWORDS_CONTRIB_SINDICAL, exclusion_patterns=EXCLUSION_KEYWORDS_ENCARGOS)
    valor_contrib_sindical_pago = _valor_encargo_preferindo_total(df_contrib_sindical, "sindical")
    if encargos_from_cols.get("contrib_sindical", 0) > 0:
        valor_contrib_sindical_pago = encargos_from_cols["contrib_sindical"]
    contrib_context = _extract_transaction_context(df_contrib_sindical, df)
    result["encargos"]["contrib_sindical"]["valor_pago"] = round(valor_contrib_sindical_pago, 2)
    result["encargos"]["contrib_sindical"]["transacoes"] = _get_transactions_list(df_contrib_sindical)
    result["encargos"]["contrib_sindical"]["data_pagamento"] = contrib_context.get("data_pagamento")
    result["encargos"]["contrib_sindical"]["conta_utilizada"] = contrib_context.get("conta_utilizada")
    if valor_contrib_sindical_pago > 0:
        result["encargos"]["contrib_sindical"]["status"] = "recolhido"
        result["encargos"]["contrib_sindical"]["icon"] = "OK"
        result["encargos"]["contrib_sindical"]["detalhes"] = f"Lançado no balancete (R$ {_fmt_brl(valor_contrib_sindical_pago)}). Recomenda-se conferir comprovantes no PDF."
    
    df_sat_rat = _extract_transactions_by_keywords(df, KEYWORDS_SAT_RAT, exclusion_patterns=EXCLUSION_KEYWORDS_ENCARGOS)
    valor_sat_rat_pago = _valor_encargo_preferindo_total(df_sat_rat, "sat")
    if encargos_from_cols.get("sat_rat", 0) > 0:
        valor_sat_rat_pago = encargos_from_cols["sat_rat"]
    sat_context = _extract_transaction_context(df_sat_rat, df)
    result["encargos"]["sat_rat"]["valor_pago"] = round(valor_sat_rat_pago, 2)
    result["encargos"]["sat_rat"]["transacoes"] = _get_transactions_list(df_sat_rat)
    result["encargos"]["sat_rat"]["data_pagamento"] = sat_context.get("data_pagamento")
    result["encargos"]["sat_rat"]["conta_utilizada"] = sat_context.get("conta_utilizada")
    if valor_sat_rat_pago > 0:
        result["encargos"]["sat_rat"]["status"] = "recolhido"
        result["encargos"]["sat_rat"]["icon"] = "OK"
        result["encargos"]["sat_rat"]["detalhes"] = f"Lançado no balancete (R$ {_fmt_brl(valor_sat_rat_pago)}). Recomenda-se conferir comprovantes no PDF."
    
    # PASSO 5: Cruzamento com indícios de pagamentos de pessoal
    # Comparar folha estimada/explícita com outros pagamentos relacionados
    folha_liquida_estimada = 0.0
    if holerites_detalhados:
        folha_liquida_estimada = sum(
            h.get("salario_liquido", 0)
            for h in holerites_detalhados
            if isinstance(h, dict)
        )
    
    valor_ferias_rescisoes = 0.0
    valor_beneficios = 0.0
    
    # Extrair férias e 13º salário, excluindo linhas não relacionadas
    # Para 13º salário, usar exclusões mais específicas para evitar falsos positivos
    df_ferias = _extract_transactions_by_keywords(df, KEYWORDS_FERIAS, exclusion_patterns=EXCLUSION_KEYWORDS_ENCARGOS)
    df_13 = _extract_transactions_by_keywords(df, KEYWORDS_DECIMO_TERCEIRO, exclusion_patterns=EXCLUSION_KEYWORDS_13_SALARIO)
    # Provisão precisa de cuidado: só incluir se realmente for provisão de férias/13º
    df_provisao = _extract_transactions_by_keywords(df, KEYWORDS_PROVISAO, exclusion_patterns=EXCLUSION_KEYWORDS_13_SALARIO)
    valor_ferias = _calculate_total(df_ferias)
    valor_13 = _calculate_total(df_13)
    valor_provisao = _calculate_total(df_provisao)
    valor_ferias_rescisoes = valor_ferias + valor_13
    
    # Atualizar cross_reference
    result["base_calculo"]["cross_reference"]["folha_liquida"] = round(folha_liquida_estimada, 2)
    result["base_calculo"]["cross_reference"]["adiantamentos"] = round(total_adiantamento, 2)
    result["base_calculo"]["cross_reference"]["ferias_rescisoes"] = round(valor_ferias_rescisoes, 2)
    result["base_calculo"]["cross_reference"]["beneficios"] = round(valor_beneficios, 2)
    
    # Validação de provisão de 13º salário
    periodo_str = result["base_calculo"].get("periodo", "")
    if periodo_str:
        periodo_mes = periodo_str.split(" a ")[0] if " a " in periodo_str else periodo_str
    else:
        periodo_mes = ""
    
    validacao_provisao = _validate_13th_salary_provision(holerites_detalhados, valor_provisao, periodo_mes)
    result["ferias_13"]["validacao_provisao"] = validacao_provisao
    
    if valor_provisao > 0:
        result["ferias_13"]["provisao"]["presente"] = True
        result["ferias_13"]["provisao"]["valor"] = round(valor_provisao, 2)
        if validacao_provisao["coerente"]:
            result["ferias_13"]["provisao"]["detalhes"] = f"Há provisão separada (R$ {_fmt_brl(valor_provisao)}). {validacao_provisao['detalhes']}"
        else:
            result["ferias_13"]["provisao"]["detalhes"] = f"Há provisão separada (R$ {_fmt_brl(valor_provisao)}). {validacao_provisao['detalhes']}"
        result["ferias_13"]["provisao"]["icon"] = "OK" if validacao_provisao["coerente"] else "!"
    
    # Validação de pagamento de férias (cálculo CLT)
    validacao_ferias = _validate_vacation_payment(valor_ferias, holerites_detalhados, periodo_mes)
    
    # Validação de pagamento de 13º (meses nov/dez)
    meses_pagamento_13 = _validate_13th_payment_months(df_13, periodo_mes)
    
    # Calcular valor esperado de 13º (média salarial anual)
    valor_esperado_13 = 0.0
    if holerites_detalhados:
        salarios_anuais = []
        for h in holerites_detalhados:
            if isinstance(h, dict):
                salario_bruto = h.get("salario_bruto", 0) or h.get("salario_liquido", 0)
                if salario_bruto > 0:
                    salarios_anuais.append(float(salario_bruto))
        if salarios_anuais:
            media_salarial = sum(salarios_anuais) / len(salarios_anuais)
            valor_esperado_13 = media_salarial * len(holerites_detalhados)
    
    result["ferias_13"]["validacao_pagamento"]["valor_esperado_13"] = round(valor_esperado_13, 2)
    result["ferias_13"]["validacao_pagamento"]["valor_esperado_ferias"] = validacao_ferias["valor_esperado_ferias"]
    result["ferias_13"]["validacao_pagamento"]["meses_pagamento_13"] = meses_pagamento_13
    
    # Validar coerência geral
    coerente_13 = True
    if valor_esperado_13 > 0 and valor_13 > 0:
        diff_13 = abs(valor_13 - valor_esperado_13)
        tolerance_13 = valor_esperado_13 * 0.20
        coerente_13 = diff_13 <= tolerance_13
    
    result["ferias_13"]["validacao_pagamento"]["coerente"] = validacao_ferias["coerente"] and coerente_13
    
    result["ferias_13"]["pagamentos"]["valor_ferias"] = round(valor_ferias, 2)
    result["ferias_13"]["pagamentos"]["valor_13"] = round(valor_13, 2)
    result["ferias_13"]["pagamentos"]["ferias_no_periodo"] = valor_ferias > 0
    result["ferias_13"]["pagamentos"]["decimo_terceiro_no_periodo"] = valor_13 > 0
    
    # Adicionar detalhes de validação
    detalhes_pagamento = []
    if valor_ferias > 0:
        detalhes_pagamento.append(f"Férias: R$ {_fmt_brl(valor_ferias)}")
        if not validacao_ferias["coerente"]:
            detalhes_pagamento.append(f"({validacao_ferias['detalhes']})")
    if valor_13 > 0:
        detalhes_pagamento.append(f"13º: R$ {_fmt_brl(valor_13)}")
        if meses_pagamento_13:
            meses_str = ", ".join(meses_pagamento_13)
            detalhes_pagamento.append(f"(pagamento em {meses_str})")
            # Verificar se está em nov/dez
            periodo_year = periodo_mes.split("-")[0] if "-" in periodo_mes else None
            if periodo_year:
                nov_esperado = f"{periodo_year}-11"
                dez_esperado = f"{periodo_year}-12"
                if not any(m in meses_pagamento_13 for m in [nov_esperado, dez_esperado]):
                    detalhes_pagamento.append("⚠️ Pagamento fora de nov/dez")
    
    if detalhes_pagamento:
        result["ferias_13"]["pagamentos"]["detalhes"] = ". ".join(detalhes_pagamento)
    elif valor_ferias == 0 and valor_13 == 0:
        result["ferias_13"]["pagamentos"]["detalhes"] = "Não aparece movimentação de pagamento de férias nem 13º neste período."
    
    encargos_ok = []
    encargos_pendentes = []
    for enc_name in ["fgts", "inss", "irrf"]:
        enc = result["encargos"][enc_name]
        if enc["status"] in ["correto", "compativel", "aplicado_conforme_tabela"]:
            encargos_ok.append(enc_name.upper())
        elif enc["status"] in ["pendente", "nao_auditavel", "verificar", "valor_menor", "valor_maior", "incorreto"]:
            encargos_pendentes.append(enc_name.upper())
    for trib_name in ["pis", "iss"]:
        trib = result["tributos"][trib_name]
        if trib["status"] in ["recolhido", "recolhido_quando_devido", "correto"]:
            encargos_ok.append(trib_name.upper())
        elif trib["status"] in ("pendente", "nao_auditavel", "incorreto"):
            encargos_pendentes.append(trib_name.upper())
    
    if encargos_ok and not encargos_pendentes:
        result["resumo"] = f"Encargos verificados OK: {', '.join(encargos_ok)}."
    elif encargos_ok and encargos_pendentes:
        result["resumo"] = f"Encargos OK: {', '.join(encargos_ok)}. Verificar: {', '.join(encargos_pendentes)}."
    elif encargos_pendentes:
        result["resumo"] = f"Encargos a verificar: {', '.join(encargos_pendentes)}."

    # REGRA 5: Holerite inválido invalida análise trabalhista (líquido=0, descontos=0, rubricas genéricas, sem CPF/cargo/matrícula).
    holerites = result["base_calculo"].get("holerites_detalhados", [])
    if is_folha_invalida(holerites):
        result["folha_invalida"] = True
        result["resumo"] = "Estrutura de folha inválida. Encargos indetermináveis."

    # PASSO 6: Conclusão obrigatória quando análise foi feita por estimativa
    if result["base_calculo"]["folha_por_estimativa"]:
        result["analise_por_estimativa"] = True
        conclusao_estimativa = "A análise foi feita por estimativa e depende da apresentação da folha detalhada."
        alerta_folha = "Encargos trabalhistas pagos sem apresentação clara da folha salarial."
        
        # Adicionar ao resumo
        if result["resumo"] and result["resumo"] != "Análise de encargos trabalhistas não identificou transações relevantes.":
            result["resumo"] = f"{result['resumo']} {conclusao_estimativa} {alerta_folha}"
        else:
            result["resumo"] = f"{conclusao_estimativa} {alerta_folha}"
        
        # Adicionar detalhes específicos para cada encargo quando aplicável
        for enc_name in ["fgts", "inss", "irrf"]:
            enc = result["encargos"].get(enc_name, {})
            if enc.get("analise_por_estimativa") and enc.get("valor_pago", 0) > 0:
                detalhes_orig = enc.get("detalhes", "")
                if "estimativa" not in detalhes_orig.lower():
                    enc["detalhes"] = f"{detalhes_orig} Valores analisados por estimativa baseada em FGTS."
        
        logger.info("[ESTIMATIVA] Análise concluída por estimativa. Alerta de folha ausente gerado.")

    return result


def is_folha_invalida(holerites: List[Dict[str, Any]]) -> bool:
    """
    REGRA 5: Detecta folha inválida quando holerites apresentam:
    líquido=0, descontos=0, rubricas genéricas ('energia', 'água'), ausência de CPF, cargo ou matrícula.
    """
    if not holerites:
        return False
    rubricas_genericas = ("energia", "agua", "água", "luz", "água", "conta", "outros", "outras")
    invalid_count = 0
    for h in holerites:
        if not isinstance(h, dict):
            continue
        liq = h.get("salario_liquido") or 0
        try:
            liq = float(liq)
        except (TypeError, ValueError):
            liq = 0
        descontos = h.get("descontos") or {}
        total_desc = descontos.get("total") if isinstance(descontos, dict) else 0
        try:
            total_desc = float(total_desc or 0)
        except (TypeError, ValueError):
            total_desc = 0
        # Líquido e descontos zerados
        if liq == 0 and total_desc == 0:
            invalid_count += 1
            continue
        # Rubricas genéricas como únicas descrições
        desc_text = " ".join(str(v).lower() for v in (h.get("descricao", ""), h.get("cargo", ""), *((descontos or {}).keys() if isinstance(descontos, dict) else [])))
        if any(r in desc_text for r in rubricas_genericas) and not (h.get("cpf") or h.get("matricula") or (h.get("cargo") and str(h.get("cargo", "")).strip() and "energia" not in str(h.get("cargo", "")).lower())):
            invalid_count += 1
            continue
        # Ausência de CPF, cargo e matrícula
        if not (h.get("cpf") or h.get("matricula") or (h.get("cargo") and str(h.get("cargo", "")).strip()) or (h.get("funcionario") and str(h.get("funcionario", "")).strip())):
            invalid_count += 1
    return invalid_count >= max(1, len(holerites) // 2)


def get_labor_summary(labor_analysis: Dict[str, Any]) -> str:
    """Retorna um resumo textual da análise de encargos."""
    if not labor_analysis:
        return "Análise de encargos não disponível."
    return labor_analysis.get("resumo", "Análise de encargos não disponível.")
