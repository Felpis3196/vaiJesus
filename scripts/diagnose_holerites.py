"""
Diagnóstico do pipeline hyperlink → holerite.

Uso:
    python scripts/diagnose_holerites.py caminho/para/arquivo.xls
    python scripts/diagnose_holerites.py caminho/para/arquivo.ods
    python scripts/diagnose_holerites.py caminho/para/folha.pdf
    python scripts/diagnose_holerites.py caminho/para/arquivo.xls --dry-run

Com --dry-run as URLs não são acessadas pela rede; apenas a classificação é exibida.
Avisos [ALERTA] (link sem holerite, página de login, etc.) aparecem no console.
"""
import os
import sys
import argparse
import logging

# Garante que avisos do fgts_link_fetcher (link sem holerite, login, etc.) apareçam no console
logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(name)s: %(message)s")

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
os.chdir(PROJECT_ROOT)


SEP = "=" * 72
SEP2 = "-" * 72


def _print_section(title: str) -> None:
    print(f"\n{SEP}")
    print(f"  {title}")
    print(SEP)


def _print_holerite(h: dict, idx: int) -> None:
    func = h.get("funcionario") or "(sem nome)"
    periodo = h.get("periodo") or "N/A"
    bruto = h.get("salario_bruto", 0)
    liquido = h.get("salario_liquido", 0)
    descontos = h.get("descontos", 0)
    metodo = h.get("extraction_method", "?")
    fonte = h.get("source_url") or h.get("source_file") or "?"
    if isinstance(fonte, str) and len(fonte) > 60:
        fonte = fonte[:57] + "..."
    print(f"  [{idx}] {func} | período: {periodo}")
    print(f"       bruto: R$ {bruto:,.2f}  descontos: R$ {descontos:,.2f}  líquido: R$ {liquido:,.2f}")
    print(f"       método: {metodo} | fonte: {fonte}")


def diagnose_hyperlinks(file_path: str, dry_run: bool = False) -> None:
    ext = os.path.splitext(file_path)[1].lower()

    _print_section(f"FASE 1 – Extração de hyperlinks  ({os.path.basename(file_path)})")
    links = []
    if ext in (".xls", ".xlt", ".xlsx"):
        from app.extraction.legacy import extract_hyperlinks_from_excel
        links = extract_hyperlinks_from_excel(file_path)
    elif ext == ".ods":
        from app.extraction.legacy import extract_hyperlinks_from_ods
        links = extract_hyperlinks_from_ods(file_path)
    else:
        print(f"  Extensão '{ext}' não suporta extração de hyperlinks.")

    if not links:
        print("  Nenhum hyperlink encontrado no arquivo.")
    else:
        fgts_count = sum(1 for l in links if l.get("tipo") == "fgts_holerite")
        outro_count = len(links) - fgts_count
        print(f"  Total de links: {len(links)}  (fgts_holerite: {fgts_count}  outro: {outro_count})")
        for i, lk in enumerate(links, 1):
            url = lk.get("url", "")
            if len(url) > 80:
                url = url[:77] + "..."
            print(f"  [{i:2d}] tipo={lk.get('tipo','?'):<14} célula={lk.get('celula','?'):<6}  url={url}")

    _print_section("FASE 2 – Busca de holerites via URL")
    holerites_from_links: list = []

    if not links:
        print("  Sem links para buscar.")
    elif dry_run:
        print("  --dry-run: URLs não acessadas.")
        for lk in links:
            print(f"  Seria buscado: {lk.get('url','')[:80]}")
    else:
        from app.services.fgts_link_fetcher import fetch_holerites_from_url

        for lk in links:
            url = lk.get("url", "")
            if not url:
                continue
            print(f"\n  Buscando: {url[:80]}")
            try:
                found = fetch_holerites_from_url(url)
                if found:
                    print(f"  → {len(found)} holerite(s) extraído(s)")
                    holerites_from_links.extend(found)
                else:
                    print(f"  → 0 holerite(s). Verifique avisos [ALERTA] acima (login, conteúdo vazio, etc.).")
            except Exception as exc:
                print(f"  → ERRO: {exc}")

        if holerites_from_links:
            print(f"\n  Total de holerites de links: {len(holerites_from_links)}")
            for i, h in enumerate(holerites_from_links, 1):
                _print_holerite(h, i)
        else:
            print("\n  Nenhum holerite extraído via URL.")
            print("  Possíveis causas:")
            print("    • URLs requerem autenticação (FGTS/Caixa/INSS não são acessíveis sem login)")
            print("    • Conteúdo retornado não contém indicadores de holerite")
            print("    • URLs expiradas ou incorretas")

    _print_section("FASE 3 – Extração de texto do arquivo")
    holerites_from_text: list = []

    if ext == ".pdf":
        try:
            from app.extraction.legacy import load_document
            result = load_document(file_path)
            raw_text = result[1] if isinstance(result, tuple) and len(result) == 2 else ""
            if raw_text and isinstance(raw_text, str):
                print(f"  Texto extraído: {len(raw_text)} caracteres")
                print(f"  Amostra: {raw_text[:300].replace(chr(10), ' | ')}")
                from app.extraction.legacy.holerite_extractor import extract_holerites_from_text
                holerites_from_text = extract_holerites_from_text(raw_text, filename=os.path.basename(file_path))
                print(f"\n  Holerites encontrados no texto: {len(holerites_from_text)}")
                for i, h in enumerate(holerites_from_text, 1):
                    _print_holerite(h, i)
            else:
                print("  Nenhum texto extraído do PDF.")
        except Exception as exc:
            print(f"  Erro ao extrair texto do PDF: {exc}")
    elif ext in (".xls", ".xlt", ".xlsx", ".ods"):
        try:
            from app.extraction.legacy import load_document, dataframe_to_text_br
            result = load_document(file_path)
            df = result[0] if isinstance(result, tuple) else result
            if df is not None and not df.empty:
                snippet = dataframe_to_text_br(df)
                print(f"  Texto do DataFrame: {len(snippet)} caracteres")
                from app.extraction.legacy.holerite_extractor import extract_holerites_from_dataframe, extract_holerites_from_text
                holerites_from_text = extract_holerites_from_dataframe(df, filename=os.path.basename(file_path))
                if not holerites_from_text and snippet:
                    holerites_from_text = extract_holerites_from_text(snippet, filename=os.path.basename(file_path))
                print(f"  Holerites encontrados no DataFrame/texto: {len(holerites_from_text)}")
                for i, h in enumerate(holerites_from_text, 1):
                    _print_holerite(h, i)
            else:
                print("  DataFrame vazio.")
        except Exception as exc:
            print(f"  Erro ao processar planilha: {exc}")
    else:
        print(f"  Extração de texto não implementada para '{ext}'.")

    _print_section("RESUMO FINAL")
    total = len(holerites_from_links) + len(holerites_from_text)
    print(f"  Holerites via URL    : {len(holerites_from_links)}")
    print(f"  Holerites via texto  : {len(holerites_from_text)}")
    print(f"  TOTAL                : {total}")
    if total == 0:
        print()
        print("  DIAGNOSTICO: Nenhum holerite encontrado.")
        if links and not holerites_from_links and not dry_run:
            print("  [!] Links foram encontrados mas as URLs nao retornaram holerites.")
            print("      Verifique se as URLs sao publicas ou se necessitam de autenticacao.")
        if not links and ext in (".xls", ".xlt", ".xlsx", ".ods"):
            print("  [!] Arquivo nao contem hyperlinks. Verifique se o arquivo correto foi passado.")
        if not holerites_from_text and ext == ".pdf":
            print("  [!] PDF nao contem texto de holerite reconhecivel.")
            print("      Verifique se o PDF e escaneado (OCR nao suportado) ou em formato diferente.")
    else:
        print()
        print("  DIAGNOSTICO: Holerites encontrados com sucesso.")
    print()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Diagnostico do pipeline hyperlink -> holerite"
    )
    parser.add_argument("file", help="Caminho para o arquivo a diagnosticar (.xls, .xlsx, .ods, .pdf)")
    parser.add_argument("--dry-run", action="store_true", help="Não acessar URLs pela rede")
    args = parser.parse_args()

    path = args.file
    if not os.path.isabs(path):
        path = os.path.join(PROJECT_ROOT, path)
    path = os.path.normpath(path)

    if not os.path.exists(path):
        print(f"Arquivo não encontrado: {path}")
        sys.exit(1)

    print(SEP)
    print("  DIAGNÓSTICO DE EXTRAÇÃO DE HOLERITES")
    print(SEP)
    print(f"  Arquivo  : {path}")
    print(f"  Tamanho  : {os.path.getsize(path) / 1024:.1f} KB")
    print(f"  Dry-run  : {'Sim' if args.dry_run else 'Não'}")

    diagnose_hyperlinks(path, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
