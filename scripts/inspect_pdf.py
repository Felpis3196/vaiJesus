"""Inspeciona extração de um PDF (colunas e amostra) para debug."""
import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)
os.chdir(PROJECT_ROOT)

def load_pdf_tables(file_path):
    import pdfplumber
    import pandas as pd
    tables = []
    with pdfplumber.open(file_path) as pdf:
        for page in pdf.pages[:20]:
            t = page.extract_tables()
            if t:
                tables.extend(t)
    if not tables:
        return None, "Nenhuma tabela encontrada"
    rows = []
    for t in tables:
        if not t:
            continue
        for row in t:
            if row and any(cell is not None for cell in row):
                rows.append(row)
    if not rows:
        return None, "Tabelas vazias"
    h = rows[0]  # primeira linha como cabecalho
    ncols = len(h)
    data_rows = [list(r)[:ncols] + [None] * (ncols - len(r)) for r in rows[1:]]
    df = pd.DataFrame(data_rows, columns=h)
    return df, None

if __name__ == "__main__":
    path = os.path.join(PROJECT_ROOT, "Docs", "PrestContas 04.2025.pdf")
    if not os.path.exists(path):
        print("Arquivo nao encontrado:", path)
        sys.exit(1)
    df, err = load_pdf_tables(path)
    if err:
        print(err)
        sys.exit(1)
    print("Colunas:", list(df.columns))
    print("Shape:", df.shape)
    print("\nPrimeiras linhas (raw):")
    print(df.head(15).to_string())
