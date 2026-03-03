"""
Diagnóstico faseado da extração de documentos (PDF, ODS, etc.).
Usa pipeline legado (load_document + clean_data) para diagnóstico; o fluxo principal é 100% LLM.

Uso (na raiz do projeto, com venv ativado):
  python scripts/diagnose_extraction.py "Docs/Prest. Contas- Balancete Junho25 Acapulco Beach-2.pdf"
  python scripts/diagnose_extraction.py "Docs/PrestContas 04.2025.pdf"
  python scripts/diagnose_extraction.py "Docs/Ed Led prestacao_contas_8_2025.ods"
"""
import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
os.chdir(PROJECT_ROOT)

# Carregar load_file do test_analysis_docs sem executar main
import importlib.util
_spec = importlib.util.spec_from_file_location(
    "test_analysis_docs",
    os.path.join(PROJECT_ROOT, "scripts", "test_analysis_docs.py"),
)
_tad = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_tad)
load_file = _tad.load_file


def resolve_path(name: str, docs_dir: str) -> str:
    name = name.strip()
    if os.path.isabs(name):
        return os.path.normpath(name)
    normalized = name.replace("\\", "/").lower()
    if normalized.startswith("docs/"):
        return os.path.normpath(os.path.join(PROJECT_ROOT, name))
    return os.path.normpath(os.path.join(docs_dir, name))


def main():
    import pandas as pd
    docs_dir = os.path.join(PROJECT_ROOT, "Docs")
    path_arg = sys.argv[1] if len(sys.argv) > 1 else "Docs/Prest. Contas- Balancete Junho25 Acapulco Beach-2.pdf"
    path = resolve_path(path_arg, docs_dir)
    if not os.path.exists(path):
        print(f"Arquivo nao encontrado: {path}")
        sys.exit(1)

    print("=" * 70)
    print("DIAGNOSTICO FASEADO DA EXTRACAO")
    print("=" * 70)
    print(f"Arquivo: {path}")
    print(f"Tamanho: {os.path.getsize(path) / 1024:.1f} KB")
    print()

    # --- Fase 1: Carregamento bruto ---
    print("-" * 70)
    print("FASE 1: Carregamento bruto (load_file)")
    print("-" * 70)
    try:
        df1 = load_file(path)
    except Exception as e:
        print(f"ERRO: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    print(f"Colunas: {list(df1.columns)}")
    print(f"Shape: {df1.shape[0]} linhas x {df1.shape[1]} colunas")
    if "texto_extraido" in df1.columns:
        text = df1["texto_extraido"].iloc[0] if len(df1) > 0 else ""
        if isinstance(text, str):
            print(f"Tipo: PDF como TEXTO (balancete ou texto unico)")
            print(f"Tamanho do texto: {len(text)} caracteres")
            print(f"Quantidade de linhas (split por \\n): {len([l for l in text.split(chr(10)) if l.strip()])}")
            amostra = text[:500].replace("\n", " | ")
            print(f"Amostra (500 chars): {amostra}...")
        else:
            print(f"Tipo: coluna texto_extraido presente mas celula nao e string")
    else:
        print("Tipo: TABELAS (extract_tables) ou planilha (ODS/Excel)")
        print("Amostra (primeiras 3 linhas):")
        print(df1.head(3).to_string())
    print()

    # --- Fase 2: Nome do condomínio ---
    print("-" * 70)
    print("FASE 2: Nome do condominio (extract_condominio_name)")
    print("-" * 70)
    if "texto_extraido" in df1.columns and len(df1) > 0:
        text = df1["texto_extraido"].iloc[0]
        if isinstance(text, str):
            for needle in ("condominio", "condomínio", "Condominio", "Condomínio"):
                pos = text.lower().find(needle.lower())
                if pos >= 0:
                    snippet = text[max(0, pos - 5) : pos + len(needle) + 80].replace("\n", " | ")
                    print(f"Trecho com '{needle}' (pos {pos}): ...{snippet}...")
                    break
            else:
                print("AVISO: 'condominio' / 'condomínio' NAO encontrado no texto extraido.")
    from app.extraction.legacy import extract_condominio_name
    nome = extract_condominio_name(df1)
    if nome:
        print(f"Nome extraido: {nome}")
    else:
        print("Nome nao encontrado.")
    print()

    # --- Fase 3: Normalização (clean_data) ---
    print("-" * 70)
    print("FASE 3: Normalizacao (clean_data -> balancete ou PDF texto)")
    print("-" * 70)
    from app.extraction.legacy import clean_data
    rows_antes = len(df1)
    try:
        df3 = clean_data(df1.copy())
    except Exception as e:
        print(f"ERRO em clean_data: {e}")
        import traceback
        traceback.print_exc()
        df3 = df1
    print(f"Linhas antes: {rows_antes} | Linhas depois: {len(df3)}")
    print(f"Colunas depois: {list(df3.columns)}")
    if "valor" in df3.columns:
        try:
            v_num = pd.to_numeric(df3["valor"], errors="coerce").fillna(0)
            total_valor = float(v_num.sum())
            print(f"Soma bruta da coluna valor: {total_valor:.2f}")
        except Exception as e:
            print(f"Soma da coluna valor: (erro ao somar) {e}")
    if "tipo" in df3.columns:
        try:
            rec = df3[df3["tipo"].astype(str).str.lower() == "receita"]
            des = df3[df3["tipo"].astype(str).str.lower() == "despesa"]
            total_rec = pd.to_numeric(rec["valor"], errors="coerce").fillna(0).sum() if "valor" in rec.columns else 0
            total_des = pd.to_numeric(des["valor"], errors="coerce").fillna(0).sum() if "valor" in des.columns else 0
            print(f"Total receitas (tipo=receita): {float(total_rec):.2f} | Total despesas (tipo=despesa): {float(total_des):.2f}")
        except Exception as e:
            print(f"Totais por tipo: (erro) {e}")
    print("Amostra (3 linhas):")
    print(df3.head(3).to_string())
    print()

    # --- Fase 4: Resumo final ---
    print("-" * 70)
    print("FASE 4: Resumo para o relatorio")
    print("-" * 70)
    if "valor" in df3.columns and "tipo" in df3.columns:
        rec = df3[df3["tipo"].astype(str).str.lower() == "receita"]
        des = df3[df3["tipo"].astype(str).str.lower() == "despesa"]
        total_rec = float(pd.to_numeric(rec["valor"], errors="coerce").fillna(0).sum())
        total_des = float(pd.to_numeric(des["valor"], errors="coerce").fillna(0).sum())
        print(f"Receitas: R$ {total_rec:,.2f} ({len(rec)} itens)")
        print(f"Despesas: R$ {total_des:,.2f} ({len(des)} itens)")
        print(f"Saldo: R$ {total_rec - total_des:,.2f}")
    else:
        print("Colunas valor/tipo ausentes - ver Fase 3.")
    print()
    print("=" * 70)
    print("Fim do diagnostico. Verifique em qual fase os dados divergem do documento.")
    print("=" * 70)


if __name__ == "__main__":
    main()
