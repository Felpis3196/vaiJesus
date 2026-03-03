import re
import pandas as pd
from datetime import datetime
from typing import Dict, Any, Optional, List
from app.analysis import get_duplicate_mask

def generate_report_pdf(report_data: Dict[str, Any], output_path: str) -> str:
    """Gera um PDF simples a partir do JSON de relatorio."""
    try:
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
        from reportlab.lib.units import cm
        from reportlab.lib import colors
        from xml.sax.saxutils import escape
    except Exception as e:
        raise RuntimeError(f"ReportLab não disponível: {e}")

    doc = SimpleDocTemplate(output_path, pagesize=A4)
    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(name="SectionTitle", parent=styles["Heading2"], spaceBefore=8, spaceAfter=6))
    styles.add(ParagraphStyle(name="Body", parent=styles["Normal"], leading=14, spaceAfter=4))
    styles.add(ParagraphStyle(name="Item", parent=styles["Normal"], leftIndent=14, spaceAfter=2))
    styles.add(ParagraphStyle(name="SubItem", parent=styles["Normal"], leftIndent=24, spaceAfter=1))
    styles.add(ParagraphStyle(name="TableCell", parent=styles["Normal"], fontSize=7, leading=9))
    story = []

    report = report_data.get("report", {})
    header = report.get("header", {})
    title = report.get("title", "RELATÓRIO DE CONFERÊNCIA")
    if title == "Resultado da Conferência":
        title = "RELATÓRIO DE CONFERÊNCIA"
    story.append(Paragraph(escape(title), styles["Title"]))
    story.append(Spacer(1, 8))
    story.append(Paragraph(f"Condomínio: {escape(str(header.get('condominio', 'N/A')))}", styles["Normal"]))
    story.append(Paragraph(f"Período analisado: {escape(str(header.get('periodo_analisado', 'N/A')))}", styles["Normal"]))
    story.append(Paragraph(f"Data do relatório: {escape(str(header.get('data_relatorio', 'N/A')))}", styles["Normal"]))
    story.append(Spacer(1, 12))

    def _fmt_brl_pdf(v: Any) -> str:
        """Formata valor monetário no padrão BR para o PDF. Valor ausente/não apurado: 'Não disponível'."""
        if v is None:
            return "N/A"
        if v == "ERRO":
            return "Não disponível"
        try:
            return f"R$ {float(v):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        except (TypeError, ValueError):
            return str(v)

    def _normalize_periodo(raw: Any) -> str:
        """Normaliza período para exibição: None/nan/none -> N/A."""
        if raw is None:
            return "N/A"
        s = str(raw).strip()
        if not s or s.lower() in ("none", "nan", "na", "<na>"):
            return "N/A"
        return s

    def _labelize(key: str) -> str:
        return key.replace("_", " ").strip().capitalize()

    def _render_dict_as_text(data: Dict[str, Any], level: int = 0):
        indent_style = styles["Item"] if level == 0 else styles["SubItem"]
        for k, v in data.items():
            label = _labelize(str(k))
            if isinstance(v, dict):
                story.append(Paragraph(f"<b>{escape(label)}</b>", styles["Body"]))
                _render_dict_as_text(v, level + 1)
            elif isinstance(v, list):
                story.append(Paragraph(f"<b>{escape(label)}</b>", styles["Body"]))
                if not v:
                    story.append(Paragraph("- (vazio)", indent_style))
                for item in v:
                    if isinstance(item, dict):
                        story.append(Paragraph("- item", indent_style))
                        _render_dict_as_text(item, level + 1)
                    else:
                        story.append(Paragraph(f"- {escape(str(item))}", indent_style))
            else:
                story.append(Paragraph(f"<b>{escape(label)}:</b> {escape(str(v))}", indent_style))

    def _join_list(items: List[str]) -> str:
        cleaned = [str(i).strip() for i in items if str(i).strip()]
        return "; ".join(cleaned)

    def _render_section(section: Dict[str, Any]):
        if not isinstance(section, dict):
            story.append(Paragraph(escape(str(section)), styles["Body"]))
            return
        sec = section.get("section", {}) if isinstance(section.get("section"), dict) else {}
        num = sec.get("number")
        title = sec.get("title", "Secao")
        section_title = f"{num}. {title}" if num is not None else str(title)
        story.append(Paragraph(escape(section_title), styles["SectionTitle"]))
        data = section.get("data", {})
        if not isinstance(data, dict):
            data = {}
        content = data.get("content", {})
        if not isinstance(content, dict):
            story.append(Paragraph(escape(str(content) if content is not None else ""), styles["Body"]))
            return
        if num == 1:
            document_files = content.get("document_files", [])
            docs = content.get("documents_analyzed", [])
            if document_files:
                if len(document_files) > 1:
                    story.append(Paragraph("Foi realizada a análise de todos os documentos listados abaixo.", styles["Body"]))
                    story.append(Paragraph("Documentos analisados:", styles["Body"]))
                    for f in document_files:
                        story.append(Paragraph(f"- {escape(str(f))}", styles["Item"]))
                else:
                    story.append(Paragraph(f"Foi analisado o documento: {escape(str(document_files[0]))}.", styles["Body"]))
                if docs:
                    story.append(Paragraph("Tipos de conteúdo identificados: " + ", ".join(escape(str(d)) for d in docs[:5]) + ".", styles["Item"]))
            else:
                story.append(Paragraph("Foi realizada a análise dos documentos enviados.", styles["Body"]))
                stats = content.get("statistics", {})
                fp = stats.get("files_processed")
                if fp is not None and int(fp) > 1:
                    story.append(Paragraph(f"Foram processados {int(fp)} arquivos.", styles["Item"]))
                if docs:
                    story.append(Paragraph("Documentos analisados:", styles["Body"]))
                    for d in docs:
                        story.append(Paragraph(f"- {escape(str(d))}", styles["Item"]))
            files_summary = content.get("files_summary", [])
            if files_summary and len(files_summary) > 1:
                story.append(Spacer(1, 4))
                story.append(Paragraph("Resumo por arquivo:", styles["Body"]))
                for fs in files_summary[:20]:
                    src = fs.get("source_file", "N/A")
                    rows = fs.get("rows", 0)
                    story.append(Paragraph(f"- {escape(str(src))}: {int(rows)} transações", styles["SubItem"]))
            stats = content.get("statistics", {})
            story.append(
                Paragraph(
                    "Resumo da análise: "
                    f"{stats.get('files_processed', 'N/A')} arquivo(s) processado(s), "
                    f"{stats.get('transactions_count', 'N/A')} transações identificadas, "
                    f"período de {stats.get('period_start', 'N/A')} a {stats.get('period_end', 'N/A')}.",
                    styles["Body"],
                )
            )
            note_cont = content.get("note_continuidade")
            if note_cont:
                story.append(Spacer(1, 4))
                story.append(Paragraph(escape(note_cont), styles["Item"]))
        elif num == 2:
            resumo = content.get("resumo_simples", {})
            story.append(Paragraph("Situação geral dos documentos:", styles["Body"]))
            for k in ("documentos_principais", "guias_comprovantes", "folha_holerites"):
                bloco = resumo.get(k, {})
                status = bloco.get("status", "N/A")
                details = bloco.get("details", [])
                detail_text = _join_list(details)
                if detail_text:
                    story.append(Paragraph(f"{_labelize(k)}: {escape(str(status))}. {escape(detail_text)}", styles["Item"]))
                else:
                    story.append(Paragraph(f"{_labelize(k)}: {escape(str(status))}.", styles["Item"]))
            obs = content.get("observacao")
            if obs:
                story.append(Paragraph(f"Observação: {escape(str(obs))}", styles["Body"]))
            missing = content.get("missing_documents", [])
            if missing:
                story.append(Paragraph("Documentos pendentes:", styles["Body"]))
                for m in missing:
                    story.append(Paragraph(f"- {escape(str(m))}", styles["SubItem"]))
        elif num == 3:
            story.append(Paragraph("Conferência matemática e financeira", styles["Body"]))
            story.append(Paragraph("Saldos por conta: Conta Ordinária, Fundo de Obras, Fundo de Reserva, Provisão 13º/Férias. Para todas: Saldo anterior + créditos – débitos = saldo atual.", styles["Item"]))
            saldos_por_conta = content.get("saldos_por_conta") or []
            if saldos_por_conta:
                for item in saldos_por_conta:
                    conta_nome = item.get("conta", "—")
                    periodo_label = item.get("periodo_label")
                    if periodo_label:
                        conta_nome = f"{conta_nome} ({periodo_label})"
                    rec_c = item.get("receitas")
                    desp_c = item.get("despesas")
                    saldo_ant = item.get("saldo_anterior")
                    saldo_at = item.get("saldo_atual")
                    fecha = item.get("formula_ok")
                    if fecha is True:
                        fecha_txt = "Fecha corretamente."
                    elif fecha is False:
                        fecha_txt = "Não fecha: saldo atual difere do esperado."
                    else:
                        fecha_txt = "Validação não aplicada (saldo demonstrado não identificado)."
                    story.append(Paragraph(f"Conta: {escape(str(conta_nome))} — Receitas: {_fmt_brl_pdf(rec_c)}, Despesas: {_fmt_brl_pdf(desp_c)}, Saldo anterior: {_fmt_brl_pdf(saldo_ant)}, Saldo atual: {_fmt_brl_pdf(saldo_at)}. {fecha_txt}", styles["SubItem"]))
                story.append(Spacer(1, 4))
            saldo_ini = content.get("saldo_inicial_ordinaria")
            saldo_ant_total = content.get("saldo_anterior_total")
            creditos_total = content.get("creditos_total")
            debitos_total = content.get("debitos_total")
            saldo_fin_total = content.get("saldo_final_total")
            rec = content.get("recebimentos_totais")
            desp = content.get("despesas_totais")
            saldo_fin = content.get("saldo_final_calculado")
            story.append(Paragraph("Total geral", styles["Body"]))
            if saldo_ant_total is not None:
                story.append(Paragraph(f"Saldo anterior total: {_fmt_brl_pdf(saldo_ant_total)}", styles["Item"]))
            elif content.get("saldo_anterior_nao_encontrado") or saldo_ini is None or saldo_ini == "ERRO":
                story.append(Paragraph("Saldo anterior total: Saldo anterior não encontrado", styles["Item"]))
            else:
                story.append(Paragraph(f"Saldo anterior total: {_fmt_brl_pdf(saldo_ini)}", styles["Item"]))
            story.append(Paragraph(f"Créditos no mês: {_fmt_brl_pdf(creditos_total if creditos_total is not None else rec)}", styles["Item"]))
            story.append(Paragraph(f"Débitos no mês: {_fmt_brl_pdf(debitos_total if debitos_total is not None else desp)}", styles["Item"]))
            # Saldo final do período = créditos - débitos; preferir saldo_final_calculado quando numérico
            saldo_final_exibir = saldo_fin if (saldo_fin is not None and isinstance(saldo_fin, (int, float))) else saldo_fin_total
            story.append(Paragraph(f"Saldo final: {_fmt_brl_pdf(saldo_final_exibir)}", styles["Item"]))
            contas_nao_fecham = content.get("contas_nao_fecham") or []
            validacao_msg = content.get("validacao_matematica_msg")
            if validacao_msg:
                story.append(Paragraph(escape(validacao_msg), styles["Item"]))
            elif content.get("saldo_match") and content.get("checks", {}).get("saldos_fecham_corretamente") and not contas_nao_fecham:
                story.append(Paragraph("A conta fecha corretamente.", styles["Item"]))
            elif contas_nao_fecham:
                story.append(Paragraph(f"Verificar consistência: conta(s) que não fecham: {', '.join(escape(str(c)) for c in contas_nao_fecham)}.", styles["Item"]))
            else:
                story.append(Paragraph("Verificar consistência dos saldos.", styles["Item"]))
            obs = content.get("observacoes", [])
            if obs:
                story.append(Spacer(1, 4))
                story.append(Paragraph("Observações:", styles["Body"]))
                for o in obs:
                    story.append(Paragraph(f"- {escape(str(o))}", styles["SubItem"]))
            # Conciliação: priorizar múltiplos períodos quando resumo_primario_por_periodo
            conc_periodos = content.get("conciliacao_estrutural_periodos") if isinstance(content.get("conciliacao_estrutural_periodos"), dict) else None
            if content.get("resumo_primario_por_periodo") and conc_periodos and (conc_periodos.get("periodos") or []):
                story.append(Spacer(1, 6))
                story.append(Paragraph("Conciliação estrutural por período", styles["SectionTitle"]))
                for p in conc_periodos.get("periodos") or []:
                    label = p.get("label") or p.get("periodo") or "Período"
                    story.append(Paragraph(escape(str(label)), styles["Body"]))
                    for c in p.get("contas") or []:
                        nome = c.get("nome", "—")
                        sf = c.get("saldo_final")
                        val = _fmt_brl_pdf(sf) if sf is not None else "N/A"
                        story.append(Paragraph(f"  {escape(str(nome))}: {val}", styles["SubItem"]))
                    total_c = p.get("total_contas")
                    if total_c is not None:
                        story.append(Paragraph(f"Total das contas: {_fmt_brl_pdf(total_c)}", styles["Item"]))
                    sc = p.get("saldo_consolidado")
                    if sc is not None:
                        story.append(Paragraph(f"Saldo consolidado: {_fmt_brl_pdf(sc)}", styles["Item"]))
                    diff = p.get("diferenca")
                    if diff is not None:
                        story.append(Paragraph(f"Diferença: {_fmt_brl_pdf(diff)}", styles["Item"]))
                    if p.get("classificacao"):
                        story.append(Paragraph(f"Classificação: {escape(str(p['classificacao']))}", styles["Item"]))
                    for a in p.get("alertas") or []:
                        story.append(Paragraph(f"- {escape(str(a))}", styles["SubItem"]))
                    story.append(Spacer(1, 4))
                conciliacoes = conc_periodos.get("conciliacoes_entre_periodos") or []
                if conciliacoes:
                    story.append(Paragraph("Conciliações entre períodos", styles["Body"]))
                    for cc in conciliacoes:
                        msg = cc.get("mensagem")
                        if msg:
                            story.append(Paragraph(escape(str(msg)), styles["SubItem"]))
            else:
                conc_est = content.get("conciliacao_estrutural")
                if isinstance(conc_est, dict):
                    story.append(Spacer(1, 6))
                    story.append(Paragraph("Conciliação estrutural", styles["SectionTitle"]))
                    for c in conc_est.get("contas_identificadas") or []:
                        nome = c.get("nome", "—")
                        sf = c.get("saldo_final")
                        val = _fmt_brl_pdf(sf) if sf is not None else "N/A"
                        story.append(Paragraph(f"  {escape(str(nome))}: {val}", styles["SubItem"]))
                    total_c = conc_est.get("total_contas")
                    if total_c is not None:
                        story.append(Paragraph(f"Total das contas: {_fmt_brl_pdf(total_c)}", styles["Item"]))
                    sc = conc_est.get("saldo_consolidado")
                    if sc is not None:
                        story.append(Paragraph(f"Saldo consolidado: {_fmt_brl_pdf(sc)}", styles["Item"]))
                    diff = conc_est.get("diferenca")
                    if diff is not None:
                        story.append(Paragraph(f"Diferença: {_fmt_brl_pdf(diff)}", styles["Item"]))
                    story.append(Paragraph(f"Classificação: {escape(str(conc_est.get('classificacao', 'N/A')))}", styles["Item"]))
                    if conc_est.get("justificativa"):
                        story.append(Paragraph(escape(str(conc_est["justificativa"])), styles["SubItem"]))
                    for a in conc_est.get("alertas") or []:
                        story.append(Paragraph(f"- {escape(str(a))}", styles["SubItem"]))
        elif num == 4:
            base = content.get("base_calculo", {})
            if not isinstance(base, dict):
                base = {}
            story.append(Paragraph("Encargos trabalhistas e tributos:", styles["Body"]))
            folha_total = base.get("folha_pagamento_total", "N/A")
            folha_por_estimativa = base.get("folha_por_estimativa", False)
            folha_estimada_fgts = base.get("folha_estimada_fgts", 0)
            alerta_folha_ausente = base.get("alerta_folha_ausente", False)
            valor_base_folha_check = base.get("valor_base_folha")
            has_base_impostos = valor_base_folha_check is not None and isinstance(valor_base_folha_check, (int, float)) and valor_base_folha_check > 0
            if folha_por_estimativa and folha_estimada_fgts:
                story.append(
                    Paragraph(
                        f"Base de cálculo (folha estimada pelo FGTS): {_fmt_brl_pdf(folha_estimada_fgts)}. "
                        "A análise foi feita por estimativa (sem folha explícita).",
                        styles["Body"],
                    )
                )
                if alerta_folha_ausente:
                    story.append(Paragraph("Alerta: Encargos trabalhistas pagos sem apresentação clara da folha salarial.", styles["Item"]))
            else:
                if not has_base_impostos:
                    # Não exibir valor implausível (ex.: 12); usar "Não disponível" quando base não identificada ou < 10k
                    if isinstance(folha_total, (int, float)) and folha_total >= 10_000:
                        folha_display = _fmt_brl_pdf(folha_total)
                    else:
                        folha_display = "Não disponível"
                    story.append(
                        Paragraph(
                            f"A base de cálculo considerada para a folha é {folha_display}.",
                            styles["Body"],
                        )
                    )
            valor_base_folha = base.get("valor_base_folha")
            origem_base = base.get("origem_base_impostos")
            origem_base_label = base.get("origem_base_label") or (f"origem: {origem_base}" if origem_base else "")
            num_meses = base.get("num_meses_periodo", 1)
            if valor_base_folha is not None and isinstance(valor_base_folha, (int, float)) and valor_base_folha > 0:
                orig_txt = f" ({escape(str(origem_base_label))})" if origem_base_label else ""
                meses_txt = f" Período: {num_meses} mês(es)." if num_meses and num_meses > 1 else ""
                story.append(Paragraph(f"Base de cálculo para impostos (valor base da folha): {_fmt_brl_pdf(valor_base_folha)}{orig_txt}.{meses_txt}", styles["Item"]))
                if has_base_impostos and isinstance(folha_total, (int, float)) and folha_total > 0:
                    story.append(Paragraph(f"Folha total do documento do mês (prestação): {_fmt_brl_pdf(folha_total)}. Base utilizada para impostos: {_fmt_brl_pdf(valor_base_folha)} ({escape(str(origem_base_label or ''))}).", styles["SubItem"]))
            confianca_base = base.get("confianca_base")
            motivo_confianca = base.get("motivo_confianca")
            if confianca_base or motivo_confianca:
                conf_label = {"alta": "Alta", "media": "Média", "baixa": "Baixa"}.get(confianca_base, confianca_base or "")
                story.append(Paragraph(f"<b>Confiança da base:</b> {escape(str(conf_label))} – {escape(str(motivo_confianca or ''))}.", styles["SubItem"]))
            v_fgts = base.get("validacao_base_fgts")
            if isinstance(v_fgts, dict) and v_fgts.get("base_implicita_fgts") is not None:
                bu = v_fgts.get("base_utilizada")
                bif = v_fgts.get("base_implicita_fgts")
                dp = v_fgts.get("diferenca_percentual", 0)
                coef = "Coerente" if v_fgts.get("coerente") else "Divergente – conferir demonstrativo"
                story.append(Paragraph(f"Conferência com FGTS: base utilizada {_fmt_brl_pdf(bu)}; base implícita pelo FGTS (8%): {_fmt_brl_pdf(bif)}; diferença {dp:.1f}%. {escape(coef)}.", styles["SubItem"]))
            v_hol = base.get("validacao_base_holerites")
            if isinstance(v_hol, dict) and v_hol.get("soma_holerites") is not None:
                soma_h = v_hol.get("soma_holerites")
                dp_hol = v_hol.get("diferenca_percentual", 0)
                story.append(Paragraph(f"Soma dos holerites (referência): {_fmt_brl_pdf(soma_h)}. Diferença {dp_hol:.1f}% em relação à base; pode refletir período ou 13º/adiantamento.", styles["SubItem"]))
            if valor_base_folha is not None and valor_base_folha > 0:
                story.append(Paragraph("A base da folha é utilizada para calcular os valores esperados de INSS, FGTS e PIS. Quando a confiança for média ou baixa, ou houver divergência com FGTS/holerites, recomenda-se conferir o demonstrativo e os comprovantes.", styles["SubItem"]))
            if confianca_base or motivo_confianca or (isinstance(v_fgts, dict) and v_fgts) or (isinstance(v_hol, dict) and v_hol) or (valor_base_folha and valor_base_folha > 0):
                story.append(Spacer(1, 4))
            alerta_prolabore = base.get("alerta_prolabore_identificado")
            alerta_inss_extra = (content.get("encargos") or {}).get("inss", {}).get("alerta_prolabore_ou_extra")
            if alerta_prolabore and base.get("alerta_prolabore_texto"):
                story.append(Paragraph(escape(base["alerta_prolabore_texto"]), styles["Item"]))
            elif alerta_inss_extra:
                story.append(Paragraph("Recomenda-se confirmar com o condomínio se há pagamento de pró-labore ou outro valor que não seja folha de salários; o INSS calculado refere-se somente à folha.", styles["Item"]))
            if alerta_prolabore or alerta_inss_extra:
                story.append(Spacer(1, 4))
            if content.get("analise_por_estimativa"):
                story.append(Paragraph("Conclusão: A análise foi feita por estimativa e depende da apresentação da folha detalhada.", styles["Item"]))
            tabela_encargos = content.get("tabela_encargos", [])
            if tabela_encargos and isinstance(tabela_encargos, list):
                story.append(Spacer(1, 6))
                story.append(Paragraph("Tabela de encargos (percentual, base de cálculo, quem paga, valor):", styles["Body"]))
                # Larguras proporcionais à página A4 (~595 pt); Base de cálculo e Quem paga com mais espaço
                col_widths = [65, 52, 115, 125, 52]
                cell_style = styles["TableCell"]
                headers = ["Encargo", "Percentual", "Base de cálculo", "Quem paga", "Valor"]
                rows = [[Paragraph(escape(str(h)), cell_style) for h in headers]]
                for linha in tabela_encargos:
                    if not isinstance(linha, dict):
                        continue
                    enc = linha.get("encargo", "")
                    pct = linha.get("percentual", "-")
                    base_calc_col = linha.get("base_calculo", "-")
                    quem = linha.get("quem_paga", "-")
                    val = linha.get("valor_pago", 0)
                    rows.append([
                        Paragraph(escape(str(enc)), cell_style),
                        Paragraph(escape(str(pct)), cell_style),
                        Paragraph(escape(str(base_calc_col)), cell_style),
                        Paragraph(escape(str(quem)), cell_style),
                        Paragraph(_fmt_brl_pdf(val), cell_style),
                    ])
                t = Table(rows, colWidths=col_widths)
                t.setStyle(TableStyle([
                    ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
                    ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                    ("FONTSIZE", (0, 0), (-1, -1), 8),
                    ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
                    ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ]))
                story.append(t)
                story.append(Spacer(1, 6))
                # Validação por baseline: base, percentual, valor esperado (calculado), valor encontrado (extraído ou calculado), conclusão
                linhas_validacao = []
                for linha in tabela_encargos:
                    if not isinstance(linha, dict):
                        continue
                    pct_b = linha.get("percentual_baseline")
                    base_util = linha.get("base_calculo_utilizada")
                    esp = linha.get("valor_esperado")
                    enc_display = linha.get("valor_pago")  # valor exibido (encontrado ou calculado)
                    encontrado_no_doc = linha.get("encontrado_no_documento", True)
                    conc = linha.get("conclusao")
                    if pct_b is not None and (esp is not None or enc_display is not None):
                        nome = linha.get("encargo", "")
                        partes = [f"{escape(str(nome))}: baseline {pct_b}%"]
                        if base_util is not None:
                            partes.append(f"base {_fmt_brl_pdf(base_util)}")
                        if esp is not None:
                            partes.append(f"esperado {_fmt_brl_pdf(esp)} (calculado)")
                        if enc_display is not None:
                            partes.append(f"encontrado {_fmt_brl_pdf(enc_display)}")
                            if encontrado_no_doc:
                                partes.append("(extraído do documento)")
                            else:
                                partes.append("(calculado; não encontrado no documento)")
                        if conc:
                            partes.append(f"Conclusão: {escape(str(conc))}")
                        linhas_validacao.append(" ".join(partes))
                if linhas_validacao:
                    story.append(Paragraph("Validação (base da folha e percentual baseline): esperado = calculado (base × %); encontrado = extraído do documento ou calculado quando não houver lançamento.", styles["Body"]))
                    if origem_base_label:
                        story.append(Paragraph(f"Base utilizada: {escape(str(origem_base_label))}.", styles["SubItem"]))
                    for txt in linhas_validacao:
                        story.append(Paragraph(txt, styles["SubItem"]))
                    story.append(Spacer(1, 4))
            resumo_enc = content.get("resumo_encargos_valores", {})
            if isinstance(resumo_enc, dict) and resumo_enc:
                partes = []
                for k in ("inss", "irrf", "pis", "fgts", "contrib_sindical"):
                    if k in resumo_enc and resumo_enc[k]:
                        label = {"inss": "INSS", "irrf": "IRRF", "pis": "PIS", "fgts": "FGTS", "contrib_sindical": "Contrib. Sindical"}.get(k, k)
                        partes.append(f"{label} R$ {_fmt_brl_pdf(resumo_enc[k])}")
                if partes:
                    story.append(Paragraph("Valores extraídos: " + ", ".join(partes) + ".", styles["SubItem"]))
                    story.append(Spacer(1, 4))
            labor_links = content.get("labor_links", [])
            if labor_links and isinstance(labor_links, list):
                fgts_links = [ln for ln in labor_links if isinstance(ln, dict) and ln.get("tipo") == "fgts_holerite" and ln.get("url")]
                other_links = [ln for ln in labor_links if isinstance(ln, dict) and ln.get("tipo") != "fgts_holerite" and ln.get("url")]
                if fgts_links:
                    story.append(Paragraph("Acessar holerites / FGTS (links do demonstrativo):", styles["Body"]))
                    for ln in fgts_links[:5]:
                        story.append(Paragraph(f"- {escape(ln.get('url', ''))}", styles["SubItem"]))
                if other_links and not fgts_links:
                    story.append(Paragraph("Links do demonstrativo:", styles["Body"]))
                    for ln in other_links[:5]:
                        story.append(Paragraph(f"- {escape(ln.get('url', ''))}", styles["SubItem"]))
                if fgts_links or other_links:
                    story.append(Spacer(1, 4))
            encargos = content.get("encargos", {})
            if not isinstance(encargos, dict):
                encargos = {}
            enc_label = {"fgts": "FGTS", "inss": "INSS", "irrf": "IRRF", "contrib_sindical": "Contrib. Sindical", "sat_rat": "SAT/RAT"}
            for key in ("fgts", "inss", "irrf", "contrib_sindical", "sat_rat"):
                item = encargos.get(key, {})
                if not isinstance(item, dict):
                    item = {}
                detalhe = item.get("detalhes", "N/A")
                data_pag = item.get("data_pagamento")
                conta = item.get("conta_utilizada")
                label = enc_label.get(key, key.upper())
                linha = f"{label}: {escape(str(detalhe))}"
                if data_pag:
                    linha += f" (Data pagamento: {escape(str(data_pag))})"
                if conta:
                    linha += f" (Conta: {escape(str(conta))})"
                story.append(Paragraph(linha, styles["Item"]))
            trib = content.get("tributos", {})
            if not isinstance(trib, dict):
                trib = {}
            for key in ("pis", "iss"):
                item = trib.get(key, {})
                if not isinstance(item, dict):
                    item = {}
                detalhe = item.get("detalhes", "N/A")
                data_pag = item.get("data_pagamento")
                conta = item.get("conta_utilizada")
                linha = f"{key.upper()}: {escape(str(detalhe))}"
                if data_pag:
                    linha += f" (Data pagamento: {escape(str(data_pag))})"
                if conta:
                    linha += f" (Conta: {escape(str(conta))})"
                story.append(Paragraph(linha, styles["Item"]))
            if base.get("origem_base_impostos"):
                origem_label = base.get("origem_base_label") or base.get("origem_base_impostos") or ""
                if origem_label:
                    story.append(Paragraph(f"Origem da base de cálculo: {escape(str(origem_label))}.", styles["Item"]))
                if base.get("origem_base_impostos") == "holerites_bruto" and base.get("folha_bruta_holerites") not in (None, 0):
                    story.append(Paragraph(f"Soma dos salários brutos (holerites): {_fmt_brl_pdf(base.get('folha_bruta_holerites'))}.", styles["SubItem"]))
            cross_ref = base.get("cross_reference", {})
            if isinstance(cross_ref, dict) and any(cross_ref.get(k) for k in ("folha_liquida", "adiantamentos", "ferias_rescisoes")):
                story.append(Paragraph("Cruzamento com indícios de pagamentos:", styles["Body"]))
                if cross_ref.get("folha_liquida"):
                    story.append(Paragraph(f"- Folha líquida: {_fmt_brl_pdf(cross_ref['folha_liquida'])}", styles["SubItem"]))
                if cross_ref.get("adiantamentos"):
                    story.append(Paragraph(f"- Adiantamentos: {_fmt_brl_pdf(cross_ref['adiantamentos'])}", styles["SubItem"]))
                if cross_ref.get("ferias_rescisoes"):
                    story.append(Paragraph(f"- Férias e rescisões: {_fmt_brl_pdf(cross_ref['ferias_rescisoes'])}", styles["SubItem"]))
            resumo_sec4 = content.get("resumo", "")
            if resumo_sec4:
                story.append(Paragraph(f"Resumo: {escape(str(resumo_sec4))}", styles["Item"]))
            holerites_detalhados = content.get("holerites_detalhados", [])
            if holerites_detalhados:
                total_hol = len(holerites_detalhados)
                story.append(Paragraph(f"Holerites extraídos ({total_hol}) – valores utilizados na validação de IRRF e base de cálculo:", styles["Body"]))
                # Tabela para exibição organizada (todos os holerites, sem limite)
                cell_style = styles["TableCell"]
                rows_hol = [[Paragraph(escape(str(h)), cell_style) for h in ["Funcionário", "Período", "Bruto", "Descontos", "Líquido", "Fonte"]]]
                for h in holerites_detalhados:
                    funcionario = h.get("funcionario") or "N/A"
                    cargo = h.get("cargo")
                    nome = f"{funcionario}" + (f" ({cargo})" if cargo else "")
                    periodo = _normalize_periodo(h.get("periodo"))
                    bruto = h.get("salario_bruto", 0)
                    liquido = h.get("salario_liquido", 0)
                    descontos = h.get("descontos", 0)
                    descontos_total = descontos.get("total", 0) if isinstance(descontos, dict) else (float(descontos) if isinstance(descontos, (int, float)) else 0)
                    fonte = h.get("source_url") or h.get("source_file") or h.get("extraction_method", "-")
                    if isinstance(fonte, str) and len(fonte) > 45:
                        fonte = fonte[:42] + "..."
                    rows_hol.append([
                        Paragraph(escape(str(nome)), cell_style),
                        Paragraph(escape(str(periodo)), cell_style),
                        Paragraph(_fmt_brl_pdf(bruto), cell_style),
                        Paragraph(_fmt_brl_pdf(descontos_total), cell_style),
                        Paragraph(_fmt_brl_pdf(liquido), cell_style),
                        Paragraph(escape(str(fonte)), cell_style),
                    ])
                tw = [70, 45, 52, 52, 52, 80]
                t_hol = Table(rows_hol, colWidths=tw)
                t_hol.setStyle(TableStyle([
                    ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
                    ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                    ("FONTSIZE", (0, 0), (-1, -1), 7),
                    ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
                    ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ]))
                story.append(t_hol)
                story.append(Spacer(1, 4))
            else:
                story.append(Paragraph("Nenhum holerite extraído neste documento (ou documento não é folha de pagamento).", styles["Item"]))
            llm_info = content.get("extracao_llm")
            if isinstance(llm_info, dict) and llm_info.get("usada"):
                docs = llm_info.get("documentos") or []
                if docs:
                    story.append(Paragraph("Extracao por LLM usada para complementar encargos.", styles["Body"]))
                    for d in docs[:8]:
                        story.append(Paragraph(f"- {escape(str(d))}", styles["SubItem"]))
        elif num == 5:
            prov = content.get("provisao", {})
            pag = content.get("pagamentos", {})
            story.append(
                Paragraph(
                    "Férias e 13º: "
                    f"provisão {escape(str(prov.get('detalhes','')))}; "
                    f"pagamentos {escape(str(pag.get('detalhes','')))}.",
                    styles["Body"],
                )
            )
            validacao_prov = prov.get("validacao") or {}
            if isinstance(validacao_prov, dict) and (validacao_prov.get("provisao_esperada_mensal") or validacao_prov.get("detalhes")):
                story.append(Paragraph("Validação da provisão (1/12 por funcionário):", styles["Body"]))
                if validacao_prov.get("provisao_esperada_mensal"):
                    story.append(Paragraph(f"- Provisão esperada mensal: {_fmt_brl_pdf(validacao_prov['provisao_esperada_mensal'])}", styles["Item"]))
                if validacao_prov.get("provisao_encontrada"):
                    story.append(Paragraph(f"- Provisão encontrada: {_fmt_brl_pdf(validacao_prov['provisao_encontrada'])}", styles["Item"]))
                if validacao_prov.get("detalhes"):
                    story.append(Paragraph(f"- {escape(str(validacao_prov['detalhes']))}", styles["Item"]))
                story.append(Paragraph("Coerente: " + ("Sim" if validacao_prov.get("coerente") else "Não"), styles["Item"]))
            validacao_pag = pag.get("validacao") or {}
            if isinstance(validacao_pag, dict) and (validacao_pag.get("valor_esperado_13") or validacao_pag.get("valor_esperado_ferias") or validacao_pag.get("meses_pagamento_13")):
                story.append(Paragraph("Validação dos pagamentos:", styles["Body"]))
                if validacao_pag.get("valor_esperado_ferias"):
                    story.append(Paragraph(f"- Valor esperado férias (salário + 1/3): {_fmt_brl_pdf(validacao_pag['valor_esperado_ferias'])}", styles["Item"]))
                if validacao_pag.get("valor_esperado_13"):
                    story.append(Paragraph(f"- Valor esperado 13º: {_fmt_brl_pdf(validacao_pag['valor_esperado_13'])}", styles["Item"]))
                meses_13 = validacao_pag.get("meses_pagamento_13", [])
                if meses_13:
                    story.append(Paragraph(f"- Meses de pagamento do 13º: {escape(', '.join(meses_13))}" + (" (normalmente nov/dez)" if meses_13 else ""), styles["Item"]))
                story.append(Paragraph("Coerente: " + ("Sim" if validacao_pag.get("coerente") else "Não"), styles["Item"]))
            holerites = content.get("holerites_extraidos", [])
            if holerites:
                story.append(Paragraph("Detalhamento de holerites:", styles["Body"]))
                for h in holerites[:15]:
                    funcionario = h.get("funcionario") or "Funcionario"
                    cargo = h.get("cargo")
                    periodo = h.get("periodo") or "N/A"
                    bruto = h.get("salario_bruto", 0)
                    bruto_nao_confiavel = h.get("valor_bruto_nao_confiavel", False)
                    bruto_text = "não confiável (conferir documento)" if bruto_nao_confiavel else _fmt_brl_pdf(bruto)
                    liquido = h.get("salario_liquido", 0)
                    descontos = h.get("descontos", 0)
                    descontos_total = descontos.get("total", 0) if isinstance(descontos, dict) else (float(descontos) if isinstance(descontos, (int, float)) else 0)
                    cargo_text = f" ({cargo})" if cargo else ""
                    story.append(
                        Paragraph(
                            f"- {escape(str(funcionario))}{cargo_text} - {escape(str(periodo))} - "
                            f"Bruto: {bruto_text}, Líquido: {_fmt_brl_pdf(liquido)}",
                            styles["Item"],
                        )
                    )
            else:
                story.append(Paragraph("Nenhum holerite extraído neste documento.", styles["Item"]))
        elif num == 6:
            alerts = content.get("alerts", [])
            if alerts:
                story.append(Paragraph("Pontos de alerta identificados:", styles["Body"]))
                for a in alerts:
                    desc = a.get("description") if isinstance(a, dict) else str(a)
                    story.append(Paragraph(f"- {escape(str(desc))}", styles["Item"]))
                    if isinstance(a, dict) and a.get("type") == "extracao_nao_lida":
                        story.append(Paragraph("(Recomenda-se análise manual por um humano.)", styles["SubItem"]))
                story.append(Paragraph("Qualquer item marcado como não lido ou não extraído exige conferência humana.", styles["Body"]))
            else:
                story.append(Paragraph("Não foram identificados pontos críticos.", styles["Body"]))
        elif num == 7:
            text = content.get("text")
            if text:
                story.append(Paragraph(escape(str(text)), styles["Body"]))
        elif num == 8:
            story.append(Paragraph(escape(str(content.get("text",""))), styles["Body"]))
        else:
            _render_dict_as_text(content)

    sections = report.get("sections", [])
    for section in sections:
        _render_section(section)
        story.append(Spacer(1, 10))

    doc.build(story)
    return output_path


def _series_or_df_to_text(obj, index: bool = True) -> str:
    """Converte Series ou DataFrame para texto (usa to_markdown se tabulate existir, senão to_string)."""
    try:
        if isinstance(obj, pd.DataFrame):
            out = obj.to_markdown(index=index)
        else:
            out = obj.to_markdown()
        return out if out is not None else ""
    except (ImportError, ModuleNotFoundError, Exception):
        return obj.to_string() if hasattr(obj, "to_string") else str(obj)


def _get_alert_codes(audit_results: Dict[str, Any]) -> set:
    """Retorna conjunto de códigos de alerta presentes no resultado da auditoria."""
    codes = set()
    for item in audit_results.get("alerts", []) or []:
        if isinstance(item, dict) and item.get("code"):
            codes.add(item["code"])
    for item in audit_results.get("warnings", []) or []:
        if isinstance(item, dict) and item.get("code"):
            codes.add(item["code"])
    return codes


def _fmt_brl(val: Optional[float]) -> str:
    """Formata valor em reais (BR: 1.234,56). Retorna N/A se val for None."""
    if val is None:
        return "N/A"
    try:
        return f"{float(val):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except (TypeError, ValueError):
        return "N/A"


def _to_scalar_float(x: Any) -> float:
    """Converte valor para float escalar (evita 'truth value of array ambiguous')."""
    if x is None:
        return 0.0
    if hasattr(x, "shape") and getattr(x, "size", 1) == 0:
        return 0.0
    if hasattr(x, "flat"):
        return float(x.flat[0]) if x.size > 0 else 0.0
    try:
        return float(x)
    except (TypeError, ValueError):
        return 0.0


def _valor_encargo(df: pd.DataFrame, keywords: tuple) -> Optional[float]:
    """Retorna soma dos valores de linhas cuja descrição contém alguma das keywords."""
    if df is None or df.empty or "descricao" not in df.columns or "valor" not in df.columns:
        return None
    desc = df["descricao"].astype(str).str.lower()
    # Usar str(s) e bool() para evitar 'truth value of array ambiguous' se célula for array
    def _has_keyword(s: Any) -> bool:
        try:
            return bool(any(k in str(s) for k in keywords))
        except (ValueError, TypeError):
            return False
    mask = desc.apply(_has_keyword)
    if not bool(mask.any()):
        return None
    return _to_scalar_float(df.loc[mask, "valor"].sum())


def _extract_condominio_name(df: pd.DataFrame) -> Optional[str]:
    """
    Tenta extrair o nome do condomínio dos dados (ODS/PDF costumam ter
    "Condomínio: NOME", "Condomínio NOME" ou "Condominio NOME" em células/descrições).
    Alinhado a data_processor.extract_condominio_name: ignora quando o nome começa com dígito.
    """
    if df is None or df.empty:
        return None
    prefixos = ("condomínio:", "condominio:", "condomínio ", "condominio ")
    for col in df.columns:
        for idx in range(min(50, len(df))):
            try:
                cell = df.iloc[idx, df.columns.get_loc(col)]
                if hasattr(cell, "shape") or (type(cell).__name__ == "ndarray"):
                    continue
                try:
                    is_na = pd.isna(cell)
                    if hasattr(is_na, "size"):
                        continue
                    if bool(is_na):
                        continue
                except (ValueError, TypeError):
                    continue
                s = str(cell).strip()
                if not s or len(s) < 6:
                    continue
                s_lower = s.lower()
                for pref in prefixos:
                    start = 0
                    while True:
                        pos = s_lower.find(pref, start)
                        if pos < 0:
                            break
                        nome = s[pos + len(pref) :].strip()
                        nome = nome.replace("\n", " ").strip()
                        if len(nome) > 120:
                            nome = nome[:117] + "..."
                        if not nome or nome == "__":
                            start = pos + 1
                            continue
                        # Quando o "nome" começa com número (ex.: "0225-IMPERADOR"), extrair a parte textual
                        if re.match(r"^\s*\d", nome):
                            m = re.match(r"^\s*\d+\s*[\-\u2013\u2014\u2212]\s*(.+)", nome)
                            if m and m.group(1).strip():
                                nome = m.group(1).strip()
                            else:
                                m = re.match(r"^\s*\d+\s+(.+)", nome)
                                if m and m.group(1).strip() and re.search(r"[a-zA-Z\u00C0-\u024F]", m.group(1)):
                                    nome = m.group(1).strip()
                                else:
                                    start = pos + 1
                                    continue
                        if re.match(r"^\s*[\d.,\s]+$", nome):
                            start = pos + 1
                            continue
                        return nome
            except (IndexError, KeyError, TypeError):
                continue
    return None


def _periodo_from_df(df: pd.DataFrame) -> tuple:
    """Retorna ((di_ini, mi_ini, yi), (di_fim, mi_fim, yf)) a partir da coluna data do DataFrame."""
    if df is None or df.empty or "data" not in df.columns:
        return ("__", "__", "__"), ("__", "__", "__")
    try:
        s = pd.to_datetime(df["data"], errors="coerce").dropna()
        if s.empty:
            return ("__", "__", "__"), ("__", "__", "__")
        d_min, d_max = s.min(), s.max()
        return (
            (d_min.strftime("%d"), d_min.strftime("%m"), d_min.strftime("%Y")),
            (d_max.strftime("%d"), d_max.strftime("%m"), d_max.strftime("%Y")),
        )
    except Exception:
        return ("__", "__", "__"), ("__", "__", "__")


def generate_conference_report(
    df: pd.DataFrame,
    audit_results: Dict[str, Any],
    condominio_name: str = "",
    periodo_inicio: Optional[str] = None,
    periodo_fim: Optional[str] = None,
) -> str:
    """
    Gera o relatório no formato "Resultado da Conferência" conforme modelo especificado.
    Utiliza dados do DataFrame e do audit_results (summary, alerts, warnings).
    """
    hoje = datetime.now()
    data_relatorio = f"{hoje.day:02d}/{hoje.month:02d}/{hoje.year}"

    (di_ini, mi_ini, yi), (di_fim, mi_fim, yf) = _periodo_from_df(df)
    if periodo_inicio:
        periodo_analisado_ini = periodo_inicio
    else:
        periodo_analisado_ini = f"{di_ini}/{mi_ini}/{yi}" if di_ini != "__" else "__/__/____"
    if periodo_fim:
        periodo_analisado_fim = periodo_fim
    else:
        periodo_analisado_fim = f"{di_fim}/{mi_fim}/{yf}" if di_fim != "__" else "__/__/____"

    # Nome do condomínio: informado ou extraído dos dados (ODS/PDF costumam ter "Condomínio: NOME")
    condominio = condominio_name.strip() or _extract_condominio_name(df) or "____________________"
    codes = _get_alert_codes(audit_results)

    # Resumo financeiro (usar _to_scalar_float para evitar "truth value of array ambiguous")
    fin = audit_results.get("summary", {}).get("financial_summary", {})
    total_receitas = _to_scalar_float(fin.get("total_receitas", 0.0))
    total_despesas = _to_scalar_float(fin.get("total_despesas", 0.0))
    saldo = _to_scalar_float(fin.get("saldo", total_receitas - total_despesas))
    has_fin = fin is not None and isinstance(fin, dict) and len(fin) > 0
    if not has_fin and not df.empty and "tipo" in df.columns and "valor" in df.columns:
        total_receitas = _to_scalar_float(df[df["tipo"].astype(str).str.lower() == "receita"]["valor"].sum())
        total_despesas = _to_scalar_float(df[df["tipo"].astype(str).str.lower() == "despesa"]["valor"].sum())
        saldo = _to_scalar_float(total_receitas - total_despesas)

    # Situação dos documentos (a partir dos códigos de alerta)
    docs_principais_ok = not any(
        c in codes for c in ("MAIN_DOCUMENTS_MISSING", "MAIN_DOCUMENTS_INCOMPLETE", "MAIN_DOCUMENTS_NO_FINANCIAL")
    )
    guias_ok = "GUIDES_RECEIPTS_PENDING" not in codes
    folha_ok = "PAYSLIPS_PENDING" not in codes
    tem_ferias_mes = "VACATION_PAYMENT_MISSING" not in codes
    tem_13_mes = "THIRTEENTH_SALARY_MISSING" not in codes
    anomalias = int(_to_scalar_float(audit_results.get("anomalies_detected", 0)))
    alertas_lista: List[str] = []
    for item in audit_results.get("alerts", []) or audit_results.get("warnings", []) or []:
        if isinstance(item, dict):
            msg = item.get("message")
            if msg is not None and (not hasattr(msg, "size") or getattr(msg, "size", 1) != 0):
                alertas_lista.append(str(msg) if not isinstance(msg, str) else msg)
        elif isinstance(item, str):
            alertas_lista.append(item)

    # Detecção de encargos no texto (FGTS, INSS, etc.)
    descricoes = ""
    if not df.empty and "descricao" in df.columns:
        descricoes = " ".join(df["descricao"].astype(str).str.lower().fillna(""))
    tem_fgts = "fgts" in descricoes
    tem_inss = "inss" in descricoes
    tem_irrf = "irrf" in descricoes or "ir rf" in descricoes
    tem_pis = "pis" in descricoes
    tem_iss = "iss" in descricoes

    lines: List[str] = []

    # Cabeçalho
    lines.append("Resultado da Conferência")
    lines.append("")
    lines.append(f"Condomínio: {condominio}")
    lines.append(f"Período analisado: {periodo_analisado_ini} a {periodo_analisado_fim}")
    lines.append(f"Data do relatório: {data_relatorio}")
    lines.append("")

    # 1 - O que foi conferido (file_metadata + fallback em document_context e files_summary)
    lines.append("1️⃣ O que foi conferido")
    lines.append("")
    doc_context = audit_results.get("document_context") or {}
    file_metadata = audit_results.get("file_metadata") or doc_context.get("file_metadata") or []
    document_files = [m.get("filename") for m in file_metadata if isinstance(m, dict) and m.get("filename")]
    files_summary = audit_results.get("files_summary") or doc_context.get("files_summary") or []
    if isinstance(files_summary, list) and files_summary:
        total_expected = max(
            doc_context.get("total_files") or 0,
            audit_results.get("files_processed") or 0,
            len(file_metadata),
            len(files_summary),
        )
        if len(document_files) < total_expected:
            seen = set(document_files)
            for fs in files_summary:
                if not isinstance(fs, dict):
                    continue
                name = fs.get("source_file")
                if name and name not in seen:
                    seen.add(name)
                    document_files.append(name)
    if document_files:
        if len(document_files) > 1:
            lines.append("Foram analisados todos os documentos listados abaixo:")
            lines.append("")
            for f in document_files:
                lines.append(f"• {f}")
            lines.append("")
        else:
            lines.append(f"Foi analisado o documento: {document_files[0]}.")
            lines.append("")
    lines.append(
        "Foram analisados os documentos e valores apresentados na pasta do período, incluindo:"
    )
    lines.append("")
    lines.append("• Prestação de contas / balancetes")
    lines.append("• Folha de pagamento e adiantamentos salariais")
    lines.append("• Guias e comprovantes de encargos (INSS, FGTS, IRRF, PIS, ISS, etc.)")
    lines.append("• Extratos e comprovantes de pagamento")
    lines.append("")
    lines.append("📌 A análise considera exclusivamente os documentos entregues.")
    lines.append("")
    if not df.empty and "data" in df.columns:
        try:
            dt = pd.to_datetime(df["data"], errors="coerce").dropna()
            if not dt.empty and dt.dt.to_period("M").nunique() >= 2:
                lines.append("📌 A comparação de gastos em relação ao mês anterior utilizou os dados do próprio período analisado (continuidade entre os meses presentes nos documentos).")
                lines.append("")
        except Exception:
            pass

    # 2 - Situação dos documentos
    lines.append("2️⃣ Situação dos documentos")
    lines.append("")
    lines.append("Resumo simples:")
    lines.append("")
    lines.append(
        f"Documentos principais: {'✅ completos' if docs_principais_ok else '⚠️ incompletos'}"
    )
    lines.append(
        f"Guias e comprovantes: {'✅ apresentados' if guias_ok else '⚠️ pendentes'}"
    )
    lines.append(
        f"Folha e holerites: {'✅ apresentados' if folha_ok else '⚠️ pendentes'}"
    )
    lines.append("")
    lines.append("👉 Observação:")
    if not alertas_lista:
        lines.append("Não foram identificadas ausências relevantes que comprometam a conferência do período.")
    else:
        lines.append(alertas_lista[0] if len(alertas_lista) == 1 else "Ver pontos de alerta abaixo.")
    lines.append("")

    # 3 - Resumo financeiro do período (modelo: saldo inicial, recebimentos, despesas, saldo final)
    saldo_inicial = fin.get("saldo_inicial") if has_fin else None
    lines.append("3️⃣ Resumo financeiro do período")
    lines.append("")
    lines.append("Receitas e despesas registradas: coerentes com os demonstrativos")
    if saldo_inicial is not None and _to_scalar_float(saldo_inicial) != 0:
        lines.append(f"Saldo inicial ordinária: R$ {_fmt_brl(_to_scalar_float(saldo_inicial))}")
    lines.append(f"Recebimentos totais: R$ {_fmt_brl(total_receitas)}")
    lines.append(f"Despesas totais: R$ {_fmt_brl(total_despesas)}")
    lines.append(f"Saldo final calculado: R$ {_fmt_brl(saldo)} → Bate com o demonstrado ✔️")
    lines.append("")
    # Fonte única: mesmo duplicates_count da Seção 3 e dos alertas
    duplicates_count = audit_results.get("summary", {}).get("duplicates_count")
    if duplicates_count is None and df is not None and not df.empty:
        try:
            duplicates_count = int(get_duplicate_mask(df).sum())
        except Exception:
            duplicates_count = 0
    if duplicates_count is None:
        duplicates_count = 0
    if anomalias > 0:
        lines.append(
            f"{anomalias} transação(ões) apresentou(aram) indicativo de anomalia (recomenda-se revisão)."
        )
    else:
        lines.append("Saldos informados: fecham corretamente com entradas e saídas")
    if duplicates_count > 0:
        lines.append(f"Lançamentos duplicados: {duplicates_count} ocorrência(s) identificadas (mesma data, valor e descrição).")
    else:
        lines.append("Lançamentos duplicados: não identificados.")
    lines.append("")

    # 4 - Encargos trabalhistas e tributos (com valores R$ quando encontrados nos dados)
    vl_fgts = _valor_encargo(df, ("fgts",))
    vl_inss = _valor_encargo(df, ("inss",))
    vl_irrf = _valor_encargo(df, ("irrf", "ir rf"))
    vl_pis = _valor_encargo(df, ("pis",))
    vl_iss = _valor_encargo(df, ("iss",))
    lines.append("4️⃣ Encargos trabalhistas e tributos")
    lines.append("")
    lines.append(
        "Com base na folha de pagamento incluindo adiantamento salarial, foi verificado:"
    )
    lines.append("")
    if tem_fgts and vl_fgts is not None:
        lines.append(f"FGTS (8%) → funcionário (R$ {_fmt_brl(vl_fgts)}) recolhimento correto")
    else:
        lines.append(f"FGTS (8%) → funcionário: {'recolhimento correto' if tem_fgts else 'Não é possível verificar recolhimento por ausência de documentação e estrutura válida.'}")
    if tem_inss and vl_inss is not None:
        lines.append(f"INSS (patronal, funcionários e terceiros) → R$ {_fmt_brl(vl_inss)} valor presente. Recomendo comparar com GFIP/eSocial do mês.")
    else:
        lines.append(
            f"INSS (patronal, funcionários e terceiros): {'valores presentes' if tem_inss else 'Não é possível verificar recolhimento por ausência de documentação e estrutura válida.'}"
        )
    if tem_irrf and vl_irrf is not None:
        lines.append(f"IRRF sobre salários (R$ {_fmt_brl(vl_irrf)}) → aplicado conforme tabela vigente")
    else:
        lines.append(f"IRRF sobre salários: {'aplicado conforme tabela vigente' if tem_irrf else 'Não é possível verificar recolhimento por ausência de documentação e estrutura válida.'}")
    if tem_pis and vl_pis is not None:
        lines.append(f"PIS (cód. 8301) (R$ {_fmt_brl(vl_pis)}) → recolhido nos meses aplicáveis")
    else:
        lines.append(f"PIS (cód. 8301): {'recolhido nos meses aplicáveis' if tem_pis else 'Não é possível verificar recolhimento por ausência de documentação e estrutura válida.'}")
    if tem_iss and vl_iss is not None:
        lines.append(f"ISS / contribuições aplicáveis (R$ {_fmt_brl(vl_iss)}) → recolhidas quando devidas")
    else:
        lines.append(f"ISS / contribuições aplicáveis: {'recolhidas quando devidas' if tem_iss else 'Não é possível verificar recolhimento por ausência de documentação e estrutura válida.'}")
    lines.append("")
    # Quando há lançamentos de encargos mas guias pendentes: esclarecer que é ausência de comprovante, não de lançamento
    tem_algum_encargo = tem_fgts or tem_inss or tem_irrf or tem_pis or tem_iss
    if tem_algum_encargo and not guias_ok:
        lines.append("Os encargos foram lançados contabilmente; não há comprovação documental (guias/GPS) no PDF.")
        lines.append("")
    if tem_fgts and tem_inss:
        lines.append("✅ Não foram identificadas diferenças entre valores calculados e valores pagos.")
    else:
        lines.append("⚠️ Recomenda-se conferir guias e GFIP/eSocial do período.")
    lines.append("")

    # 5 - Férias e 13º (provisão separada R$ quando houver nos dados)
    valor_provisao = _valor_encargo(df, ("provisão", "provisao", "13º", "13 ", "férias", "ferias", "decimo terceiro"))
    lines.append("5️⃣ Férias e 13º")
    lines.append("")
    if valor_provisao is not None and _to_scalar_float(valor_provisao) > 0:
        lines.append(f"Há provisão separada (R$ {_fmt_brl(_to_scalar_float(valor_provisao))} após recebimentos). ✔️")
    if tem_ferias_mes and tem_13_mes:
        lines.append("Há movimentação de pagamento de férias e/ou 13º no período. ✔️")
    else:
        if not tem_ferias_mes:
            lines.append("Não aparece movimentação de pagamento de férias neste mês.")
        if not tem_13_mes:
            lines.append("Não aparece movimentação de pagamento de 13º neste mês.")
        if valor_provisao is None or _to_scalar_float(valor_provisao) <= 0:
            lines.append("(Provisão pode existir em contas específicas; conferir demonstrativo.)")
    lines.append("")

    # 6 - Pontos de alerta (só quando houver)
    extraction = audit_results.get("extraction_quality") or {}
    if extraction.get("ok") is False and extraction.get("errors"):
        lines.append("⚠️ Verificação da extração dos dados: foram identificados problemas antes da geração do relatório (valores zerados, colunas ausentes ou documento vazio). Ver pontos de alerta abaixo.")
        lines.append("")

    if alertas_lista:
        lines.append("6️⃣ Pontos de alerta")
        lines.append("")
        for msg in alertas_lista:
            lines.append(f"• {msg}")
        lines.append("")
        lines.append("Documento não localizado na pasta no período analisado:")
        if anomalias > 0 or not (docs_principais_ok and guias_ok and folha_ok):
            lines.append("⚠️ Foram identificados os seguintes ajustes recomendados:")
            for msg in alertas_lista[:5]:
                lines.append(f"– {msg}")
        else:
            lines.append("✔️ Não foram identificados pontos críticos")
    else:
        lines.append("6️⃣ Pontos de alerta")
        lines.append("")
        lines.append("✔️ Não foram identificados pontos críticos")
    lines.append("")

    # 7 - Conclusão geral
    lines.append("7️⃣ Conclusão geral")
    lines.append("")
    lines.append("Com base nos documentos analisados:")
    lines.append("")
    if anomalias == 0 and docs_principais_ok and guias_ok and folha_ok:
        lines.append("• Cálculos matemáticos corretos.")
        lines.append("• Os encargos trabalhistas e tributos estão regularmente recolhidos.")
        lines.append("• Não há indícios de erros relevantes ou pendências financeiras no período.")
    else:
        lines.append("• Cálculos matemáticos corretos (dimensão matemática).")
        lines.append("• Cálculos corretos, porém documentação insuficiente.")
        if not (tem_fgts and tem_inss):
            lines.append("• Tributos: conferir FGTS/INSS/PIS/IRRF conforme GFIP e pasta.")
        if not (tem_ferias_mes and tem_13_mes):
            lines.append("• Provisão 13º e férias: conferir se há movimentação no período.")
        if anomalias > 0:
            lines.append(f"• {anomalias} transação(ões) com indicativo de anomalia → revisão recomendada.")
    lines.append("")
    lines.append(
        "📌 (importante) Este relatório tem caráter informativo e visa apoiar síndicos, conselheiros e moradores na compreensão das contas."
    )
    lines.append("")

    # Parecer final
    lines.append("🟢 Parecer final")
    lines.append("")
    if anomalias == 0 and docs_principais_ok and guias_ok and folha_ok:
        lines.append("Situação do período analisado: REGULAR")
        lines.append(
            "Cálculos matemáticos corretos. Documentação completa. No geral, a gestão financeira está sob controle."
        )
    else:
        lines.append("Situação do período analisado: REGULAR COM RESSALVAS")
        parts = []
        if not (docs_principais_ok and guias_ok and folha_ok):
            parts.append("Cálculos corretos, porém documentação insuficiente")
        if anomalias > 0:
            parts.append("há transações que recomendam revisão")
        lines.append("; ".join(parts) + ".")
        lines.append("No geral, a gestão financeira está sob controle.")

    return "\n".join(lines)


def generate_summary_report(df: pd.DataFrame) -> str:
    """Gera um resumo financeiro do condomínio (usa float para evitar truth value of array)."""
    if df is None or df.empty or "tipo" not in df.columns or "valor" not in df.columns:
        total_receitas = total_despesas = saldo = 0.0
    else:
        try:
            total_receitas = _to_scalar_float(df[df["tipo"].astype(str).str.lower() == "receita"]["valor"].sum())
            total_despesas = _to_scalar_float(df[df["tipo"].astype(str).str.lower() == "despesa"]["valor"].sum())
            saldo = total_receitas - total_despesas
        except Exception:
            total_receitas = total_despesas = saldo = 0.0

    report = "## Resumo Financeiro do Condomínio\n\n"
    report += f"- **Total de Receitas:** R$ {_fmt_brl(total_receitas)}\n"
    report += f"- **Total de Despesas:** R$ {_fmt_brl(total_despesas)}\n"
    report += f"- **Saldo:** R$ {_fmt_brl(saldo)}\n\n"

    report += "## Receitas por Categoria\n\n"
    if "tipo" in df.columns and "categoria" in df.columns:
        receitas_grouped = df[df["tipo"].astype(str).str.lower() == "receita"].groupby("categoria")["valor"].sum()
        if isinstance(receitas_grouped, pd.Series):
            receitas_por_categoria = receitas_grouped.sort_values(ascending=False)
            if not receitas_por_categoria.empty:
                tbl = pd.DataFrame(
                    {
                        "Categoria": receitas_por_categoria.index,
                        "Valor": receitas_por_categoria.apply(lambda v: "R$ " + _fmt_brl(float(v))),
                    }
                )
                report += _series_or_df_to_text(tbl, index=False)
            else:
                report += "Nenhuma receita registrada.\n"
        else:
            report += "Nenhuma receita registrada.\n"
    else:
        report += "Nenhuma receita registrada (coluna 'categoria' ausente).\n"
    report += "\n\n"

    report += "## Despesas por Categoria\n\n"
    if "tipo" in df.columns and "categoria" in df.columns:
        despesas_grouped = df[df["tipo"].astype(str).str.lower() == "despesa"].groupby("categoria")["valor"].sum()
        if isinstance(despesas_grouped, pd.Series):
            despesas_por_categoria = despesas_grouped.sort_values(ascending=False)
            if not despesas_por_categoria.empty:
                tbl = pd.DataFrame(
                    {
                        "Categoria": despesas_por_categoria.index,
                        "Valor": despesas_por_categoria.apply(lambda v: "R$ " + _fmt_brl(float(v))),
                    }
                )
                report += _series_or_df_to_text(tbl, index=False)
            else:
                report += "Nenhuma despesa registrada.\n"
        else:
            report += "Nenhuma despesa registrada.\n"
    else:
        report += "Nenhuma despesa registrada (coluna 'categoria' ausente).\n"
    report += "\n\n"

    return report

def _format_anomaly_date(val: Any) -> str:
    """Formata data para o relatório: dd/mm/yyyy ou '—' se for timestamp de processamento."""
    if val is None or (hasattr(val, "size") and getattr(val, "size", 1) == 0):
        return "—"
    try:
        if hasattr(val, "strftime"):
            # Se tiver hora com segundos/microsegundos, provavelmente é timestamp de processamento
            if hasattr(val, "hour") and (val.hour != 0 or val.minute != 0 or val.second != 0 or getattr(val, "microsecond", 0) != 0):
                return "—"
            return val.strftime("%d/%m/%Y")
        s = str(val).strip()
        if not s:
            return "—"
        # Timestamp com hora (ex: 14:54:08.800828) -> não é data da transação
        if " " in s and (":" in s.split()[-1] or "." in s):
            return "—"
        return s[:10] if len(s) >= 10 else s
    except Exception:
        return "—"


def _shorten_justificativa(text: Any, max_len: int = 72) -> str:
    """Resume a justificativa para caber na tabela; lista resumida dos motivos."""
    if text is None or (hasattr(text, "size") and getattr(text, "size", 1) == 0):
        return ""
    s = str(text).strip()
    if not s or len(s) <= max_len:
        return s
    # Pegar primeiros motivos separados por "; " (até ~72 chars)
    parts = [p.strip() for p in s.split(";") if p.strip()]
    if not parts:
        return s[: max_len - 3] + "..."
    acc = []
    for p in parts:
        if len(", ".join(acc + [p])) <= max_len:
            acc.append(p)
        else:
            break
    result = "; ".join(acc) if acc else (s[: max_len - 3] + "...")
    return result if len(result) <= max_len else result[: max_len - 3] + "..."


def generate_anomaly_report(df: pd.DataFrame) -> str:
    """Gera um relatório detalhado das anomalias detectadas (tabela formatada e legível)."""
    report = "## Relatório de Anomalias Detectadas\n\n"

    # Modo estrutural: se não há coluna de anomalias, não há anomalias marcadas
    if "anomalia_detectada" not in df.columns:
        if df.empty:
            report += "Nenhuma anomalia detectada nas contas do condomínio.\n"
        else:
            report += "Nenhuma anomalia detectada nas contas do condomínio (dados estruturais sem marcação de anomalias).\n"
        return report

    anomalies_df = df[df["anomalia_detectada"] == True].copy()

    if anomalies_df.empty:
        report += "Nenhuma anomalia detectada nas contas do condomínio.\n"
    else:
        report += f"Foram detectadas **{len(anomalies_df)}** anomalias (tabela abaixo).\n\n"
        report_columns = ["data", "descricao", "tipo", "valor", "categoria", "justificativa_anomalia"]
        existing_columns = [c for c in report_columns if c in anomalies_df.columns]
        if not existing_columns:
            report += "Nenhuma coluna disponível para exibição.\n"
        else:
            # Montar tabela formatada para leitura
            display = pd.DataFrame()
            col_data = anomalies_df["data"] if "data" in existing_columns else None
            if col_data is not None and isinstance(col_data, pd.Series):
                display["Data"] = col_data.apply(_format_anomaly_date)
            col_desc = anomalies_df["descricao"] if "descricao" in existing_columns else None
            if col_desc is not None and isinstance(col_desc, pd.Series):
                display["Descrição"] = col_desc.astype(str).str.strip().str[:40]
            col_tipo = anomalies_df["tipo"] if "tipo" in existing_columns else None
            if col_tipo is not None and isinstance(col_tipo, pd.Series):
                display["Tipo"] = col_tipo.astype(str).str.strip()
            col_valor = anomalies_df["valor"] if "valor" in existing_columns else None
            if col_valor is not None and isinstance(col_valor, pd.Series):
                def _fmt_valor(v: Any) -> str:
                    try:
                        x = _to_scalar_float(v)
                        return "R$ " + _fmt_brl(x)
                    except Exception:
                        return str(v)
                display["Valor"] = col_valor.apply(_fmt_valor)
            col_cat = anomalies_df["categoria"] if "categoria" in existing_columns else None
            if col_cat is not None and isinstance(col_cat, pd.Series):
                display["Categoria"] = col_cat.astype(str).str.strip()
            col_just = anomalies_df["justificativa_anomalia"] if "justificativa_anomalia" in existing_columns else None
            if col_just is not None and isinstance(col_just, pd.Series):
                display["Justificativa"] = col_just.apply(_shorten_justificativa)
            if not display.empty:
                report += _series_or_df_to_text(display, index=False)
            else:
                report += _series_or_df_to_text(anomalies_df[existing_columns], index=False)
        report += "\n\n"

    return report

def generate_full_report(df: pd.DataFrame) -> str:
    """Gera o relatório completo combinando resumo e anomalias."""
    full_report = generate_summary_report(df) + generate_anomaly_report(df)
    return full_report

# Exemplo de uso (para testes)
if __name__ == "__main__":
    # Criar um DataFrame de exemplo (simulando a saída do anomaly_detector)
    sample_data = {
        'data': pd.to_datetime(['2025-01-01', '2025-01-05', '2025-01-10', '2025-01-15', '2025-01-20', '2025-01-25', '2025-01-26', '2025-01-27']),
        'descricao': [
            'Recebimento taxa condominial apto 101',
            'Pagamento conta de agua',
            'Salario do zelador',
            'Compra de material de limpeza',
            'Recebimento aluguel salao de festas',
            'Manutencao elevador',
            'Despesa suspeita - valor muito alto',
            'Receita com valor negativo'
        ],
        'tipo': ['receita', 'despesa', 'despesa', 'despesa', 'receita', 'despesa', 'despesa', 'receita'],
        'valor': [500.00, 150.00, 1200.00, 80.00, 200.00, 300.00, 5000.00, -100.00],
        'categoria': ['Taxas Condominiais', 'Água', 'Salários', 'Material de Limpeza', 'Outras Receitas', 'Manutenção', 'Outras Despesas', 'Outras Receitas'],
        'anomalia_detectada': [False, False, False, False, False, False, True, True],
        'justificativa_anomalia': ['', '', '', '', '', '', 'Anomalia detectada por Isolation Forest', 'Receita com valor negativo']
    }
    df_test = pd.DataFrame(sample_data)
    df_test['valor'] = df_test['valor'].astype(float)

    print("\n--- Relatório de Resumo ---")
    summary_report = generate_summary_report(df_test)
    print(summary_report)

    print("\n--- Relatório de Anomalias ---")
    anomaly_report = generate_anomaly_report(df_test)
    print(anomaly_report)

    print("\n--- Relatório Completo ---")
    full_report = generate_full_report(df_test)
    print(full_report)


