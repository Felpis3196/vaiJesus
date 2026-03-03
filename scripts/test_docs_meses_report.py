#!/usr/bin/env python3
"""
Testa análise com os 4 arquivos da pasta Docs/docs meses e gera o PDF do full report.
Uso: python scripts/test_docs_meses_report.py
"""
import os
import sys
import time
import requests

# Raiz do projeto
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DOCS_MESES = os.path.join(BASE_DIR, "Docs", "docs meses")

FILES = [
    os.path.join(DOCS_MESES, "prestacao_contas_1_2026(2) (1).xlt"),
    os.path.join(DOCS_MESES, "prestacao_contas_11_2025(3).sxc"),
    os.path.join(DOCS_MESES, "prestacao_contas_12_2025.sxc"),
    os.path.join(DOCS_MESES, "prestacao_contas_2_2026.sxc"),
]

BASE_URL = os.environ.get("API_BASE_URL", "http://localhost:8000")
POLL_INTERVAL = 2
POLL_TIMEOUT = 300  # 5 min


def main():
    print("Testando com 4 arquivos de Docs/docs meses")
    print("=" * 50)

    for p in FILES:
        if not os.path.isfile(p):
            print(f"Arquivo não encontrado: {p}")
            sys.exit(1)
    print("Arquivos encontrados:", [os.path.basename(p) for p in FILES])

    session = requests.Session()

    # 1) Health
    try:
        r = session.get(f"{BASE_URL}/health", timeout=5)
        r.raise_for_status()
        print("API OK:", r.json().get("status", "ok"))
    except requests.RequestException as e:
        print("Erro ao conectar na API. Inicie o servidor (ex: python start_server.py):", e)
        sys.exit(1)

    # 2) POST /api/v1/analyze (multipart)
    print("\nEnviando análise (4 arquivos)...")
    files = []
    handles = []
    try:
        for path in FILES:
            f = open(path, "rb")
            handles.append(f)
            name = os.path.basename(path)
            files.append(("files", (name, f, "application/octet-stream")))
        r = session.post(f"{BASE_URL}/api/v1/analyze", files=files, timeout=60)
    finally:
        for h in handles:
            h.close()

    if r.status_code != 200:
        print("Erro no analyze:", r.status_code, r.text[:500])
        sys.exit(1)

    data = r.json()
    if not data.get("success") or not data.get("job_id"):
        print("Resposta sem job_id:", data)
        sys.exit(1)

    job_id = data["job_id"]
    print("Job ID:", job_id)

    # 3) Poll status até completed
    print("Aguardando conclusão (polling a cada", POLL_INTERVAL, "s)...")
    start = time.time()
    while True:
        if time.time() - start > POLL_TIMEOUT:
            print("Timeout aguardando conclusão.")
            sys.exit(1)
        r = session.get(f"{BASE_URL}/api/v1/analysis/status/{job_id}", timeout=10)
        if r.status_code != 200:
            print("Erro no status:", r.status_code, r.text[:200])
            time.sleep(POLL_INTERVAL)
            continue
        st = r.json()
        job = st.get("status") or st.get("job") or {}
        status = job.get("status") if isinstance(job, dict) else st.get("status")
        progress = job.get("progress") if isinstance(job, dict) else st.get("progress")
        msg = (job.get("message") or "") if isinstance(job, dict) else (st.get("message") or "")
        print(f"  status={status} progress={progress} {msg[:60]}")
        if status == "completed":
            print("Concluído.")
            break
        if status == "failed":
            err = (job.get("error") or st.get("error")) if isinstance(job, dict) else st.get("error")
            print("Job falhou:", err)
            sys.exit(1)
        time.sleep(POLL_INTERVAL)

    # 4) GET full report (JSON) e depois PDF
    print("\nObtendo full report (JSON)...")
    r = session.get(f"{BASE_URL}/api/v1/report/full", params={"job_id": job_id}, timeout=30)
    if r.status_code != 200:
        print("Erro no report/full:", r.status_code, r.text[:300])
        sys.exit(1)
    print("Full report (JSON) OK.")

    print("Obtendo PDF...")
    r = session.get(f"{BASE_URL}/api/v1/report/pdf", params={"job_id": job_id}, timeout=60)
    if r.status_code != 200:
        print("Erro no report/pdf:", r.status_code, r.text[:300] if r.headers.get("content-type", "").startswith("application/json") else "(binary)")
        sys.exit(1)

    out_path = os.path.join(BASE_DIR, "full_report_docs_meses.pdf")
    with open(out_path, "wb") as f:
        f.write(r.content)
    print("PDF salvo em:", out_path)
    print("=" * 50)
    print("Concluído.")


if __name__ == "__main__":
    main()
