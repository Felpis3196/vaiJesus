"""
Teste de análise da IA com documentos reais.
Carrega os arquivos em Docs/ e executa a auditoria completa (Resultado da Conferência).

Documentos usados:
  - Ed Led prestacao_contas_8_2025.ods
  - PrestContas 04.2025.pdf
  - Prest. Contas- Balancete Junho25 Acapulco Beach-2.pdf

Como executar (na raiz do projeto):
  1. Ative o ambiente virtual:  .venv\\Scripts\\activate   (ou use scripts/run_test_analysis_docs.bat)
  2. Rode:  python scripts/test_analysis_docs.py
"""
import os
import sys

# Garantir que o projeto está no path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
os.chdir(PROJECT_ROOT)

# Documentos a testar (em Docs/) - use argumentos para testar apenas alguns
# Ex.: python scripts/test_analysis_docs.py "PrestContas 04.2025.pdf"
# Ex.: python scripts/test_analysis_docs.py "Docs/PrestContas 04.2025.pdf"  (caminhos com espaço: use aspas)
DOCS_DEFAULT = [
    "Ed Led prestacao_contas_8_2025.ods",
    "PrestContas 04.2025.pdf",
    "Prest. Contas- Balancete Junho25 Acapulco Beach-2.pdf",
]


def load_file(file_path: str):
    """Carrega um arquivo (ODS, PDF, CSV, Excel) e retorna um DataFrame. Usa o carregador unificado do data_processor."""
    from app.extraction.legacy import load_document
    result = load_document(file_path)
    return result[0] if isinstance(result, tuple) else result


def _infer_period_from_filename(file_path: str):
    """
    Infere período (início e fim) a partir do nome do arquivo.
    Ex.: "PrestContas 04.2025.pdf" -> ("01/04/2025", "30/04/2025"); "Balancete Junho25" -> ("01/06/2025", "30/06/2025").
    Returns (periodo_inicio, periodo_fim) ou (None, None).
    """
    import re
    from calendar import monthrange
    name = os.path.basename(file_path)
    # Padrão MM.YYYY ou MM/YYYY (ex.: 04.2025, 04/2025)
    m = re.search(r"(?<!\d)(\d{1,2})[./](\d{4})(?!\d)", name)
    if m:
        mes, ano = int(m.group(1)), int(m.group(2))
        if 1 <= mes <= 12 and 2000 <= ano <= 2100:
            _, ultimo = monthrange(ano, mes)
            return (f"01/{mes:02d}/{ano}", f"{ultimo:02d}/{mes:02d}/{ano}")
    # Padrão "Junho25", "Junho 25", "JUN 25"
    meses = {"jan": 1, "fev": 2, "mar": 3, "abr": 4, "mai": 5, "jun": 6, "jul": 7, "ago": 8, "set": 9, "out": 10, "nov": 11, "dez": 12}
    m = re.search(r"(jan|fev|mar|abr|mai|jun|jul|ago|set|out|nov|dez)\w*\s*\'?(\d{2})", name, re.I)
    if m:
        mes_nome = m.group(1).lower()[:3]
        mes = meses.get(mes_nome)
        ano = 2000 + int(m.group(2)) if int(m.group(2)) < 100 else int(m.group(2))
        if mes and 2000 <= ano <= 2100:
            _, ultimo = monthrange(ano, mes)
            return (f"01/{mes:02d}/{ano}", f"{ultimo:02d}/{mes:02d}/{ano}")
    return (None, None)


def main():
    import pandas as pd
    from app.audit import AdvancedAuditSystem

    docs_dir = os.path.join(PROJECT_ROOT, "Docs")
    docs = sys.argv[1:] if len(sys.argv) > 1 else DOCS_DEFAULT

    def resolve_path(name: str) -> str:
        name = name.strip()
        if os.path.isabs(name):
            return os.path.normpath(name)
        # Se já contém "Docs" no caminho (ex: "Docs/PrestContas 04.2025.pdf" ou "Docs\PrestContas..."), resolver em relação à raiz do projeto
        normalized = name.replace("\\", "/").lower()
        if normalized.startswith("docs/"):
            return os.path.normpath(os.path.join(PROJECT_ROOT, name))
        # Caso contrário, procurar em Docs/
        return os.path.normpath(os.path.join(docs_dir, name))

    paths = [resolve_path(name) for name in docs]
    missing = [p for p in paths if not os.path.exists(p)]
    # Se o usuário passou vários argumentos e algum caminho não existe, pode ser que o nome do arquivo tenha espaço e não foi colocado entre aspas (ex.: "Docs/PrestContas 04.2025.pdf" vira "Docs/PrestContas" e "04.2025.pdf")
    if missing and len(docs) >= 2:
        merged = " ".join(docs)
        paths_merged = [resolve_path(merged)]
        if all(os.path.exists(p) for p in paths_merged):
            paths = paths_merged
            docs = [merged]
            missing = []
    if missing:
        print("Arquivos não encontrados:")
        for p in missing:
            print(f"  - {p}")
        print("\nDiretório Docs:", docs_dir)
        sys.exit(1)

    print("=" * 60)
    print("TESTE DE ANÁLISE DA IA - Documentos")
    print("=" * 60)
    print("\nArquivos:")
    for p in paths:
        size = os.path.getsize(p) / 1024
        print(f"  • {os.path.basename(p)} ({size:.1f} KB)")
    print()

    # Importar clean_data e extract_condominio_name para normalizar e extrair nome do condomínio
    from app.extraction.legacy import clean_data, extract_condominio_name

    all_dfs = []
    file_meta = []
    condominio_name = None
    for path in paths:
        try:
            df = load_file(path)
            if df is not None and not df.empty:
                # Extrair nome do condomínio antes da normalização (ODS/PDF costumam ter "Condomínio: NOME")
                if condominio_name is None:
                    nome = extract_condominio_name(df)
                    if nome:
                        condominio_name = nome
                # Normalizar cada arquivo antes de concatenar (detecta balancete ODS e padroniza colunas)
                try:
                    df = clean_data(df)
                except Exception as clean_err:
                    print(f"  [AVISO] clean_data em {os.path.basename(path)}: {clean_err}")
                all_dfs.append(df)
                file_meta.append({"path": path, "rows": len(df), "cols": len(df.columns)})
                print(f"  OK Carregado: {os.path.basename(path)} ({len(df)} linhas, {len(df.columns)} colunas)")
            else:
                print(f"  AVISO Vazio ou None: {os.path.basename(path)}")
        except Exception as e:
            print(f"  [ERRO] {os.path.basename(path)}: {e}")
            import traceback
            traceback.print_exc()

    if not all_dfs:
        print("\nNenhum dado carregado. Encerrando.")
        sys.exit(1)

    # Fallback: se nenhum PDF/ODS trouxe nome do condomínio, tentar extrair do nome do arquivo (ex.: "... Acapulco Beach-2.pdf")
    if condominio_name is None and paths:
        import re
        for path in paths:
            base = os.path.basename(path)
            m = re.search(r"[\s\-]([A-Za-zÀ-ÿ\s]+?)(?:\s*-\s*\d+)?\.(pdf|ods)$", base, re.I)
            if m:
                candidate = m.group(1).strip()
                if len(candidate) >= 3 and not re.match(r"^(Prest|Balancete|Contas|Junho|Maio|Abril)$", candidate, re.I):
                    condominio_name = candidate[:80]
                    break

    combined = pd.concat(all_dfs, ignore_index=True)
    print(f"\nDataFrame combinado: {len(combined)} linhas, {list(combined.columns)}")
    print()

    document_context = {
        "total_files": len(all_dfs),
        "has_financial_data": True,
        "by_category": {"financial_data": len([p for p in paths if p.endswith((".ods", ".sxc", ".xlsx", ".xls", ".xlt", ".csv"))]), "fiscal_document": len([p for p in paths if p.endswith(".pdf")])},
    }
    if condominio_name:
        document_context["condominio_name"] = condominio_name
    # Inferir período do primeiro arquivo (ex.: PrestContas 04.2025.pdf -> 01/04/2025 a 30/04/2025)
    periodo_ini, periodo_fim = None, None
    for p in paths:
        periodo_ini, periodo_fim = _infer_period_from_filename(p)
        if periodo_ini and periodo_fim:
            document_context["periodo_inicio"] = periodo_ini
            document_context["periodo_fim"] = periodo_fim
            break

    print("Executando auditoria (IA avançada + Resultado da Conferência)...")
    print()
    ai_system = AdvancedAuditSystem()
    result = ai_system.run_comprehensive_audit(
        df_input=combined,
        document_context=document_context,
    )

    print("=" * 60)
    print("RESULTADO")
    print("=" * 60)
    print(f"Sucesso: {result.get('success')}")
    print(f"Transações: {result.get('total_transactions')}")
    print(f"Anomalias detectadas: {result.get('anomalies_detected')}")
    print(f"Relatório: {result.get('report_file')}")
    print()

    errors = result.get("errors") or []
    if errors:
        print("Erros:")
        for e in errors:
            msg = e.get("message", e) if isinstance(e, dict) else e
            print(f"  • {msg}")
        print()

    warnings = result.get("warnings") or []
    if warnings:
        print("Avisos / Alertas:")
        for w in warnings:
            msg = w.get("message", w) if isinstance(w, dict) else w
            print(f"  • {msg}")
        print()

    summary = result.get("summary") or {}
    fin = summary.get("financial_summary", {})
    if fin:
        print("Resumo financeiro:")
        print(f"  Receitas: R$ {fin.get('total_receitas', 0):,.2f}")
        print(f"  Despesas: R$ {fin.get('total_despesas', 0):,.2f}")
        print(f"  Saldo: R$ {fin.get('saldo', 0):,.2f}")
    print()

    report_file = result.get("report_file")
    if report_file and os.path.exists(report_file):
        print("Início do relatório (Resultado da Conferência):")
        print("-" * 40)
        with open(report_file, "r", encoding="utf-8") as f:
            lines = f.readlines()
        for line in lines[:55]:
            # Evitar UnicodeEncodeError no console Windows (cp1252)
            try:
                print(line.rstrip())
            except UnicodeEncodeError:
                print(line.rstrip().encode("ascii", errors="replace").decode("ascii"))
        if len(lines) > 55:
            print("...")
        print("-" * 40)
        print(f"\nRelatório completo em: {report_file}")
    else:
        print("Relatório não gerado ou arquivo não encontrado.")
    print()
    print("Teste concluído.")


if __name__ == "__main__":
    main()
