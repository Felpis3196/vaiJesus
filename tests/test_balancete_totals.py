# Teste: Créditos 65.395,04 e Débitos 70.095,37 após clean_data + consolidator
import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from app.extraction.legacy import load_document, clean_data
from app.audit.financial_consolidator import calculate_financial_totals_correct

def main():
    import pandas as pd
    path = os.path.join(PROJECT_ROOT, "Docs", "docs meses", "prestacao_contas_1_2026(2) (1).xlt")
    if not os.path.exists(path):
        print("Arquivo .xlt não encontrado. Teste manual com janeiro.xls se necessário.")
        return

    result = load_document(path)
    df = result[0] if isinstance(result, tuple) else result
    df_clean = clean_data(df.copy(), path)

    print(f"Linhas após clean_data: {len(df_clean)}")
    print(f"Colunas: {list(df_clean.columns)}")
    if "tipo" in df_clean.columns and "valor" in df_clean.columns:
        tipos = df_clean["tipo"].astype(str).str.strip().str.lower()
        rec = df_clean.loc[tipos.isin(["receita", "credito", "crédito"]), "valor"]
        des = df_clean.loc[tipos.isin(["despesa", "debito", "débito"]), "valor"]
        soma_rec = pd.to_numeric(rec, errors="coerce").fillna(0).sum()
        soma_des = pd.to_numeric(des, errors="coerce").fillna(0).sum()
        print(f"Soma receita (df direto): {soma_rec:,.2f}")
        print(f"Soma despesa (df direto): {soma_des:,.2f}")

    totals_extracted = {"values": {"total_receitas": 70095.37, "total_despesas": 70095.37}}
    ft = calculate_financial_totals_correct(df_clean, extracted_totals=totals_extracted)
    print(f"\nConsolidator total_receitas: {ft.get('total_receitas')}")
    print(f"Consolidator total_despesas: {ft.get('total_despesas')}")
    print(f"Consolidator saldo_final: {ft.get('saldo_final')}")

    esperado_rec = 65395.04
    esperado_des = 70095.37
    ok = (abs((ft.get("total_receitas") or 0) - esperado_rec) < 1.0 and
          abs((ft.get("total_despesas") or 0) - esperado_des) < 1.0)
    cred = ft.get("total_receitas")
    deb = ft.get("total_despesas")
    ok_distintos = cred is not None and deb is not None and abs((cred or 0) - (deb or 0)) > 0.01
    print(f"\nEsperado: Créditos {esperado_rec:,.2f}, Débitos {esperado_des:,.2f}")
    print("TESTE valores exatos:", "OK" if ok else "FALHOU")
    print("TESTE crédito != débito:", "OK" if ok_distintos else "FALHOU (iguais)")

def test_consolidator_com_df_65k_70k():
    """Simula df com soma receita=65.395,04 e despesa=70.095,37 para validar que o consolidator retorna esses valores."""
    import pandas as pd
    from app.audit.financial_consolidator import calculate_financial_totals_correct
    df = pd.DataFrame([
        {"tipo": "receita", "valor": 65395.04, "descricao": "Receitas do mês"},
        {"tipo": "despesa", "valor": 70095.37, "descricao": "Despesas do mês"},
    ])
    ft = calculate_financial_totals_correct(df, extracted_totals={"values": {"total_receitas": 70095.37, "total_despesas": 70095.37}})
    rec, des = ft.get("total_receitas"), ft.get("total_despesas")
    ok = rec is not None and des is not None and abs(rec - 65395.04) < 0.02 and abs(des - 70095.37) < 0.02
    print("\n[Simulação 65k/70k] total_receitas=%s, total_despesas=%s => %s" % (rec, des, "OK" if ok else "FALHOU"))
    return ok


if __name__ == "__main__":
    main()
    test_consolidator_com_df_65k_70k()
