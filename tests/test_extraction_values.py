"""
Teste direto para verificar se os valores esperados estão sendo encontrados no documento.
"""
import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import pandas as pd
import logging
from app.extraction.legacy import load_document, extract_financial_totals_from_text, clean_data
from app.extraction.legacy.financial_extractor import extract_monthly_financial_data
from app.audit.financial_consolidator import calculate_financial_totals_correct

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

VALORES_ESPERADOS = {
    "receitas": 65395.04,
    "despesas": 70095.37,
    "saldo_final": 782983.90
}

def test_extraction(file_path: str):
    """Testa extração de valores do documento"""
    logger.info(f"=== TESTE DE EXTRAÇÃO PARA: {file_path} ===")
    
    try:
        result = load_document(file_path)
        if isinstance(result, tuple):
            df_raw, text = result[0], result[1]
        else:
            df_raw = result
            text = ""
        logger.info(f"✅ Documento carregado (bruto): {len(df_raw)} linhas, {len(df_raw.columns)} colunas")
        logger.info(f"✅ Texto extraído: {len(text)} caracteres")
        
        logger.info("\n1.1. Normalizando DataFrame com clean_data...")
        df = clean_data(df_raw)
        logger.info(f"✅ DataFrame normalizado: {len(df)} linhas, {len(df.columns)} colunas")
        logger.info(f"   Colunas: {list(df.columns)}")
        
        if "valor" in df.columns and "descricao" in df.columns:
            total_rows = df[df["descricao"].str.contains("TOTAL", case=False, na=False)]
            if not total_rows.empty:
                logger.info(f"\n   Linhas com 'TOTAL' encontradas: {len(total_rows)}")
                for idx, row in total_rows.iterrows():
                    logger.info(f"   - {row['descricao']}: tipo={row.get('tipo', 'N/A')}, valor={row.get('valor', 0):,.2f}")
                    if abs(row.get('valor', 0) - VALORES_ESPERADOS["receitas"]) < 0.01:
                        logger.info(f"      ✅ VALOR DE RECEITAS ESPERADO ENCONTRADO!")
                    if abs(row.get('valor', 0) - VALORES_ESPERADOS["despesas"]) < 0.01:
                        logger.info(f"      ✅ VALOR DE DESPESAS ESPERADO ENCONTRADO!")
    except Exception as e:
        logger.error(f"❌ Erro ao carregar documento: {e}", exc_info=True)
        return
    
    logger.info("\n2. Extraindo valores do texto...")
    try:
        totals_from_text = extract_financial_totals_from_text(text)
        values = totals_from_text.get("values", {}) if isinstance(totals_from_text, dict) else totals_from_text
        
        receitas_texto = values.get("total_receitas")
        despesas_texto = values.get("total_despesas")
        saldo_final_texto = values.get("saldo_final")
        
        logger.info(f"   Receitas do texto: {receitas_texto}")
        logger.info(f"   Despesas do texto: {despesas_texto}")
        logger.info(f"   Saldo final do texto: {saldo_final_texto}")
        
        if receitas_texto:
            diff = abs(receitas_texto - VALORES_ESPERADOS["receitas"])
            if diff < 0.01:
                logger.info(f"   ✅ Receitas CORRETAS encontradas no texto!")
            else:
                logger.warning(f"   ⚠️ Receitas diferentes do esperado: esperado {VALORES_ESPERADOS['receitas']:,.2f}, encontrado {receitas_texto:,.2f}")
        
        if despesas_texto:
            diff = abs(despesas_texto - VALORES_ESPERADOS["despesas"])
            if diff < 0.01:
                logger.info(f"   ✅ Despesas CORRETAS encontradas no texto!")
            else:
                logger.warning(f"   ⚠️ Despesas diferentes do esperado: esperado {VALORES_ESPERADOS['despesas']:,.2f}, encontrado {despesas_texto:,.2f}")
    except Exception as e:
        logger.error(f"❌ Erro ao extrair valores do texto: {e}", exc_info=True)
    
    logger.info("\n3. Extraindo valores do DataFrame...")
    try:
        extracted_data = extract_monthly_financial_data(df)
        receitas_df = extracted_data.get("receitas_mensais_extraidas", [])
        despesas_df = extracted_data.get("despesas_mensais_extraidas", [])
        
        total_receitas_df = sum(r.get("valor", 0) for r in receitas_df if r.get("valor") is not None)
        total_despesas_df = sum(d.get("valor", 0) for d in despesas_df if d.get("valor") is not None)
        
        logger.info(f"   Receitas do DataFrame: {len(receitas_df)} itens, total: {total_receitas_df:,.2f}")
        logger.info(f"   Despesas do DataFrame: {len(despesas_df)} itens, total: {total_despesas_df:,.2f}")
        
        if total_receitas_df > 0:
            diff = abs(total_receitas_df - VALORES_ESPERADOS["receitas"])
            if diff < 0.01:
                logger.info(f"   ✅ Receitas CORRETAS encontradas no DataFrame!")
            else:
                logger.warning(f"   ⚠️ Receitas diferentes do esperado: esperado {VALORES_ESPERADOS['receitas']:,.2f}, encontrado {total_receitas_df:,.2f}")
        
        if total_despesas_df > 0:
            diff = abs(total_despesas_df - VALORES_ESPERADOS["despesas"])
            if diff < 0.01:
                logger.info(f"   ✅ Despesas CORRETAS encontradas no DataFrame!")
            else:
                logger.warning(f"   ⚠️ Despesas diferentes do esperado: esperado {VALORES_ESPERADOS['despesas']:,.2f}, encontrado {total_despesas_df:,.2f}")
    except Exception as e:
        logger.error(f"❌ Erro ao extrair valores do DataFrame: {e}", exc_info=True)
    
    logger.info("\n4. Testando consolidação completa...")
    try:
        totals_extracted = extract_financial_totals_from_text(text)
        financial_totals = calculate_financial_totals_correct(df, extracted_totals=totals_extracted)
        
        receitas_final = financial_totals.get("total_receitas")
        despesas_final = financial_totals.get("total_despesas")
        saldo_final_final = financial_totals.get("saldo_final")
        
        logger.info(f"   Receitas finais: {receitas_final}")
        logger.info(f"   Despesas finais: {despesas_final}")
        logger.info(f"   Saldo final: {saldo_final_final}")
        
        if receitas_final:
            diff = abs(receitas_final - VALORES_ESPERADOS["receitas"])
            if diff < 0.01:
                logger.info(f"   ✅ Receitas FINAIS CORRETAS!")
            else:
                logger.warning(f"   ⚠️ Receitas finais diferentes: esperado {VALORES_ESPERADOS['receitas']:,.2f}, encontrado {receitas_final:,.2f}")
        
        if despesas_final:
            diff = abs(despesas_final - VALORES_ESPERADOS["despesas"])
            if diff < 0.01:
                logger.info(f"   ✅ Despesas FINAIS CORRETAS!")
            else:
                logger.warning(f"   ⚠️ Despesas finais diferentes: esperado {VALORES_ESPERADOS['despesas']:,.2f}, encontrado {despesas_final:,.2f}")
    except Exception as e:
        logger.error(f"❌ Erro na consolidação: {e}", exc_info=True)
    
    logger.info("\n5. Colunas disponíveis no DataFrame:")
    logger.info(f"   {list(df.columns)}")
    
    logger.info("\n6. Valores na coluna 'valor' (primeiros 20):")
    if "valor" in df.columns:
        valores = df[df["valor"] > 0]["valor"].dropna().head(20)
        for idx, val in enumerate(valores):
            logger.info(f"   [{idx+1}] R$ {val:,.2f}")
            if abs(val - VALORES_ESPERADOS["receitas"]) < 0.01:
                logger.info(f"       ✅ Este é o valor esperado de RECEITAS!")
            if abs(val - VALORES_ESPERADOS["despesas"]) < 0.01:
                logger.info(f"       ✅ Este é o valor esperado de DESPESAS!")
    
    logger.info("\n=== FIM DO TESTE ===")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
        test_extraction(file_path)
    else:
        print("Uso: python test_extraction_values.py <caminho_do_arquivo>")
        print("Exemplo: python test_extraction_values.py janeiro.xls")
