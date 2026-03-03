"""
Script para gerar PDF documentando a estrutura dos arrays errors e warnings
"""
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib.colors import HexColor
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_JUSTIFY
from datetime import datetime
import os

def generate_arrays_structure_pdf(output_path: str = "Docs/ESTRUTURA_ARRAYS_ERROS_AVISOS.pdf"):
    """Gera PDF documentando a estrutura dos arrays errors e warnings"""
    
    # Criar diretório se não existir
    os.makedirs(os.path.dirname(output_path) if os.path.dirname(output_path) else ".", exist_ok=True)
    
    # Criar documento
    doc = SimpleDocTemplate(output_path, pagesize=A4)
    story = []
    
    # Estilos
    styles = getSampleStyleSheet()
    
    # Estilos customizados
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Heading1'],
        fontSize=24,
        textColor=HexColor('#1a1a1a'),
        spaceAfter=30,
        alignment=TA_CENTER,
        fontName='Helvetica-Bold'
    )
    
    heading_style = ParagraphStyle(
        'CustomHeading',
        parent=styles['Heading2'],
        fontSize=18,
        textColor=HexColor('#2c3e50'),
        spaceAfter=12,
        spaceBefore=20,
        fontName='Helvetica-Bold'
    )
    
    subheading_style = ParagraphStyle(
        'CustomSubHeading',
        parent=styles['Heading3'],
        fontSize=14,
        textColor=HexColor('#34495e'),
        spaceAfter=10,
        spaceBefore=15,
        fontName='Helvetica-Bold'
    )
    
    code_style = ParagraphStyle(
        'CodeStyle',
        parent=styles['Code'],
        fontSize=9,
        textColor=HexColor('#2c3e50'),
        fontName='Courier',
        leftIndent=20,
        rightIndent=20,
        backColor=HexColor('#f8f9fa'),
        borderPadding=10
    )
    
    normal_style = ParagraphStyle(
        'NormalStyle',
        parent=styles['Normal'],
        fontSize=11,
        textColor=HexColor('#2c3e50'),
        spaceAfter=12,
        alignment=TA_JUSTIFY,
        leading=14
    )
    
    # Título
    story.append(Paragraph("Estrutura dos Arrays", title_style))
    story.append(Paragraph("errors e warnings", title_style))
    story.append(Spacer(1, 0.3*inch))
    
    # Informações do documento
    info_text = f"""
    <b>Data:</b> {datetime.now().strftime('%d de %B de %Y')}<br/>
    <b>Versão:</b> 1.0.0<br/>
    <b>Sistema:</b> Auditoria de Condomínios com IA
    """
    story.append(Paragraph(info_text, normal_style))
    story.append(Spacer(1, 0.4*inch))
    
    # ========== SEÇÃO 1: VISÃO GERAL ==========
    story.append(Paragraph("📋 Visão Geral", heading_style))
    overview_text = """
    Os arrays <b>errors</b> e <b>warnings</b> fazem parte da estrutura de resposta dos endpoints 
    de análise da API. Eles fornecem informações sobre problemas encontrados durante o processamento 
    de dados financeiros.
    """
    story.append(Paragraph(overview_text, normal_style))
    story.append(Spacer(1, 0.3*inch))
    
    # ========== SEÇÃO 2: ARRAY ERRORS ==========
    story.append(Paragraph("🔴 Array errors", heading_style))
    
    story.append(Paragraph("<b>Descrição:</b>", subheading_style))
    story.append(Paragraph(
        "Array que contém mensagens de erros críticos que impediram ou afetaram o processamento da análise.",
        normal_style
    ))
    story.append(Spacer(1, 0.2*inch))
    
    story.append(Paragraph("<b>Tipo:</b>", subheading_style))
    story.append(Paragraph('<font face="Courier">errors: string[]</font>', code_style))
    story.append(Spacer(1, 0.2*inch))
    
    story.append(Paragraph("<b>Estrutura de um Item:</b>", subheading_style))
    story.append(Paragraph(
        "Cada item do array é uma <b>string</b> contendo a mensagem de erro:",
        normal_style
    ))
    
    # Exemplo visual de estrutura
    error_structure = """
    <font face="Courier" color="#c0392b">
    "Erro durante auditoria avançada: [descrição do erro]"
    </font>
    """
    story.append(Paragraph(error_structure, code_style))
    story.append(Spacer(1, 0.3*inch))
    
    story.append(Paragraph("<b>Exemplos de Itens Reais:</b>", subheading_style))
    
    # Exemplo 1
    example1_data = [
        ['<b>Exemplo 1:</b> Erro de Carregamento', ''],
        ['', ''],
        ['<font face="Courier" size="8">"Erro durante auditoria avançada: Erro ao limpar os dados: Colunas essenciais não encontradas: [\'data\', \'descricao\', \'tipo\', \'valor\']. Colunas disponíveis: [\'recebimento_no_mês_12.844\', \'56_55\', \'57%\']"</font>']
    ]
    example1_table = Table(example1_data, colWidths=[2*inch, 5*inch])
    example1_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (0, 0), HexColor('#e74c3c')),
        ('TEXTCOLOR', (0, 0), (0, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('FONTNAME', (0, 0), (0, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (0, 0), 10),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
        ('TOPPADDING', (0, 0), (-1, -1), 8),
        ('BACKGROUND', (1, 1), (1, 1), HexColor('#f8f9fa')),
        ('GRID', (0, 0), (-1, -1), 1, HexColor('#dee2e6')),
    ]))
    story.append(example1_table)
    story.append(Spacer(1, 0.2*inch))
    
    # Exemplo 2
    example2_data = [
        ['<b>Exemplo 2:</b> Erro de Processamento', ''],
        ['', ''],
        ['<font face="Courier" size="8">"Erro durante auditoria avançada: No valid files could be loaded. Caminhos fornecidos: /path/to/file.pdf | - /path/to/file.pdf: exists=True, size=75539734 bytes (72.04 MB), extension=.pdf"</font>']
    ]
    example2_table = Table(example2_data, colWidths=[2*inch, 5*inch])
    example2_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (0, 0), HexColor('#e74c3c')),
        ('TEXTCOLOR', (0, 0), (0, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('FONTNAME', (0, 0), (0, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (0, 0), 10),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
        ('TOPPADDING', (0, 0), (-1, -1), 8),
        ('BACKGROUND', (1, 1), (1, 1), HexColor('#f8f9fa')),
        ('GRID', (0, 0), (-1, -1), 1, HexColor('#dee2e6')),
    ]))
    story.append(example2_table)
    story.append(Spacer(1, 0.2*inch))
    
    # Exemplo 3
    example3_data = [
        ['<b>Exemplo 3:</b> Erro de Validação', ''],
        ['', ''],
        ['<font face="Courier" size="8">"Erro durante auditoria avançada: Erros críticos: DataFrame está vazio; Coluna \'valor\' tem 150 valores nulos"</font>']
    ]
    example3_table = Table(example3_data, colWidths=[2*inch, 5*inch])
    example3_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (0, 0), HexColor('#e74c3c')),
        ('TEXTCOLOR', (0, 0), (0, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('FONTNAME', (0, 0), (0, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (0, 0), 10),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
        ('TOPPADDING', (0, 0), (-1, -1), 8),
        ('BACKGROUND', (1, 1), (1, 1), HexColor('#f8f9fa')),
        ('GRID', (0, 0), (-1, -1), 1, HexColor('#dee2e6')),
    ]))
    story.append(example3_table)
    story.append(Spacer(1, 0.3*inch))
    
    story.append(Paragraph("<b>Quando é Populado:</b>", subheading_style))
    when_populated = """
    • Quando uma exceção é capturada durante o processamento<br/>
    • Quando a validação de dados falha criticamente<br/>
    • Quando não é possível carregar arquivos<br/>
    • Quando há erros de processamento de dados
    """
    story.append(Paragraph(when_populated, normal_style))
    story.append(Spacer(1, 0.2*inch))
    
    story.append(Paragraph("<b>Comportamento:</b>", subheading_style))
    behavior = """
    • Se o processamento for bem-sucedido, o array estará <b>vazio</b> (<font face="Courier">[]</font>)<br/>
    • Se houver erros, cada erro será adicionado como uma string no array<br/>
    • Múltiplos erros podem ser adicionados ao mesmo array
    """
    story.append(Paragraph(behavior, normal_style))
    
    story.append(PageBreak())
    
    # ========== SEÇÃO 3: ARRAY WARNINGS ==========
    story.append(Paragraph("⚠️ Array warnings", heading_style))
    
    story.append(Paragraph("<b>Descrição:</b>", subheading_style))
    story.append(Paragraph(
        "Array que contém avisos (warnings) sobre problemas não críticos que não impediram o processamento, mas que merecem atenção.",
        normal_style
    ))
    story.append(Spacer(1, 0.2*inch))
    
    story.append(Paragraph("<b>Tipo:</b>", subheading_style))
    story.append(Paragraph('<font face="Courier">warnings: string[]</font>', code_style))
    story.append(Spacer(1, 0.2*inch))
    
    story.append(Paragraph("<b>Estrutura de um Item:</b>", subheading_style))
    story.append(Paragraph(
        "Cada item do array é uma <b>string</b> contendo a mensagem de aviso:",
        normal_style
    ))
    
    # Exemplo visual de estrutura
    warning_structure = """
    <font face="Courier" color="#f39c12">
    "[contexto]: [descrição do aviso]"
    </font>
    """
    story.append(Paragraph(warning_structure, code_style))
    story.append(Spacer(1, 0.3*inch))
    
    story.append(Paragraph("<b>Exemplos de Itens Reais:</b>", subheading_style))
    
    # Exemplo 1
    warn_example1_data = [
        ['<b>Exemplo 1:</b> Aviso de Validação', ''],
        ['', ''],
        ['<font face="Courier" size="8">"Validação: Encontrados tipos inválidos: [\'RECEITA\', \'DESPESA_INVÁLIDA\']"</font>']
    ]
    warn_example1_table = Table(warn_example1_data, colWidths=[2*inch, 5*inch])
    warn_example1_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (0, 0), HexColor('#f39c12')),
        ('TEXTCOLOR', (0, 0), (0, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('FONTNAME', (0, 0), (0, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (0, 0), 10),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
        ('TOPPADDING', (0, 0), (-1, -1), 8),
        ('BACKGROUND', (1, 1), (1, 1), HexColor('#fffbf0')),
        ('GRID', (0, 0), (-1, -1), 1, HexColor('#dee2e6')),
    ]))
    story.append(warn_example1_table)
    story.append(Spacer(1, 0.2*inch))
    
    # Exemplo 2
    warn_example2_data = [
        ['<b>Exemplo 2:</b> Aviso de Dados Faltantes', ''],
        ['', ''],
        ['<font face="Courier" size="8">"Validação: Coluna \'categoria\' tem 50 valores nulos"</font>']
    ]
    warn_example2_table = Table(warn_example2_data, colWidths=[2*inch, 5*inch])
    warn_example2_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (0, 0), HexColor('#f39c12')),
        ('TEXTCOLOR', (0, 0), (0, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('FONTNAME', (0, 0), (0, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (0, 0), 10),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
        ('TOPPADDING', (0, 0), (-1, -1), 8),
        ('BACKGROUND', (1, 1), (1, 1), HexColor('#fffbf0')),
        ('GRID', (0, 0), (-1, -1), 1, HexColor('#dee2e6')),
    ]))
    story.append(warn_example2_table)
    story.append(Spacer(1, 0.2*inch))
    
    # Exemplo 3
    warn_example3_data = [
        ['<b>Exemplo 3:</b> Aviso de Arquivo Grande', ''],
        ['', ''],
        ['<font face="Courier" size="8">"PDF muito grande (72.04 MB). Processamento pode demorar..."</font>']
    ]
    warn_example3_table = Table(warn_example3_data, colWidths=[2*inch, 5*inch])
    warn_example3_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (0, 0), HexColor('#f39c12')),
        ('TEXTCOLOR', (0, 0), (0, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('FONTNAME', (0, 0), (0, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (0, 0), 10),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
        ('TOPPADDING', (0, 0), (-1, -1), 8),
        ('BACKGROUND', (1, 1), (1, 1), HexColor('#fffbf0')),
        ('GRID', (0, 0), (-1, -1), 1, HexColor('#dee2e6')),
    ]))
    story.append(warn_example3_table)
    story.append(Spacer(1, 0.3*inch))
    
    story.append(Paragraph("<b>Quando é Populado:</b>", subheading_style))
    when_warn_populated = """
    • Quando há avisos de validação (não críticos)<br/>
    • Quando arquivos são muito grandes<br/>
    • Quando há dados faltantes que não impedem o processamento<br/>
    • Quando há tipos de dados inválidos que foram corrigidos automaticamente<br/>
    • Quando há problemas de performance esperados
    """
    story.append(Paragraph(when_warn_populated, normal_style))
    story.append(Spacer(1, 0.2*inch))
    
    story.append(Paragraph("<b>Comportamento:</b>", subheading_style))
    warn_behavior = """
    • O array pode estar <b>vazio</b> (<font face="Courier">[]</font>) mesmo quando há avisos<br/>
    • Avisos não impedem o processamento<br/>
    • Múltiplos avisos podem ser adicionados ao mesmo array
    """
    story.append(Paragraph(warn_behavior, normal_style))
    
    story.append(PageBreak())
    
    # ========== SEÇÃO 4: EXEMPLO COMPLETO ==========
    story.append(Paragraph("📊 Exemplo Completo de Resposta", heading_style))
    
    story.append(Paragraph("<b>Resposta com Arrays Vazios (Sucesso):</b>", subheading_style))
    
    json_example = """
    <font face="Courier" size="8">
    {<br/>
    &nbsp;&nbsp;"success": true,<br/>
    &nbsp;&nbsp;"errors": [],<br/>
    &nbsp;&nbsp;"warnings": [],<br/>
    &nbsp;&nbsp;"total_transactions": 150,<br/>
    &nbsp;&nbsp;"anomalies_detected": 5,<br/>
    &nbsp;&nbsp;...<br/>
    }
    </font>
    """
    story.append(Paragraph(json_example, code_style))
    story.append(Spacer(1, 0.3*inch))
    
    story.append(Paragraph("<b>Resposta com Erros:</b>", subheading_style))
    
    json_error_example = """
    <font face="Courier" size="8">
    {<br/>
    &nbsp;&nbsp;"success": false,<br/>
    &nbsp;&nbsp;"errors": [<br/>
    &nbsp;&nbsp;&nbsp;&nbsp;"Erro durante auditoria avançada: Erro ao limpar os dados: Colunas essenciais não encontradas: ['data', 'descricao', 'tipo', 'valor']",<br/>
    &nbsp;&nbsp;&nbsp;&nbsp;"Erro durante auditoria avançada: No valid files could be loaded"<br/>
    &nbsp;&nbsp;],<br/>
    &nbsp;&nbsp;"warnings": [],<br/>
    &nbsp;&nbsp;"total_transactions": 0,<br/>
    &nbsp;&nbsp;"anomalies_detected": 0<br/>
    &nbsp;&nbsp;...<br/>
    }
    </font>
    """
    story.append(Paragraph(json_error_example, code_style))
    story.append(Spacer(1, 0.3*inch))
    
    story.append(Paragraph("<b>Resposta com Avisos:</b>", subheading_style))
    
    json_warn_example = """
    <font face="Courier" size="8">
    {<br/>
    &nbsp;&nbsp;"success": true,<br/>
    &nbsp;&nbsp;"errors": [],<br/>
    &nbsp;&nbsp;"warnings": [<br/>
    &nbsp;&nbsp;&nbsp;&nbsp;"Validação: Encontrados tipos inválidos: ['RECEITA_INVÁLIDA']",<br/>
    &nbsp;&nbsp;&nbsp;&nbsp;"Validação: Coluna 'categoria' tem 25 valores nulos",<br/>
    &nbsp;&nbsp;&nbsp;&nbsp;"PDF muito grande (72.04 MB). Processamento pode demorar..."<br/>
    &nbsp;&nbsp;],<br/>
    &nbsp;&nbsp;"total_transactions": 150,<br/>
    &nbsp;&nbsp;"anomalies_detected": 5<br/>
    &nbsp;&nbsp;...<br/>
    }
    </font>
    """
    story.append(Paragraph(json_warn_example, code_style))
    story.append(Spacer(1, 0.3*inch))
    
    # ========== SEÇÃO 5: USO NA INTEGRAÇÃO ==========
    story.append(Paragraph("🎯 Uso na Integração", heading_style))
    
    story.append(Paragraph("<b>JavaScript/TypeScript:</b>", subheading_style))
    
    js_code = """
    <font face="Courier" size="8">
    const response = await fetch('/api/v1/analyze', { ... });<br/>
    const data = await response.json();<br/>
    <br/>
    // Verificar erros<br/>
    if (data.errors && data.errors.length > 0) {<br/>
    &nbsp;&nbsp;console.error('Erros encontrados:', data.errors);<br/>
    &nbsp;&nbsp;data.errors.forEach(error => {<br/>
    &nbsp;&nbsp;&nbsp;&nbsp;console.error('Erro:', error);<br/>
    &nbsp;&nbsp;});<br/>
    }<br/>
    <br/>
    // Verificar avisos<br/>
    if (data.warnings && data.warnings.length > 0) {<br/>
    &nbsp;&nbsp;console.warn('Avisos encontrados:', data.warnings);<br/>
    &nbsp;&nbsp;data.warnings.forEach(warning => {<br/>
    &nbsp;&nbsp;&nbsp;&nbsp;console.warn('Aviso:', warning);<br/>
    &nbsp;&nbsp;});<br/>
    }
    </font>
    """
    story.append(Paragraph(js_code, code_style))
    story.append(Spacer(1, 0.2*inch))
    
    story.append(Paragraph("<b>Python:</b>", subheading_style))
    
    python_code = """
    <font face="Courier" size="8">
    response = requests.post('/api/v1/analyze', ...)<br/>
    data = response.json()<br/>
    <br/>
    if data.get('errors'):<br/>
    &nbsp;&nbsp;print("Erros encontrados:")<br/>
    &nbsp;&nbsp;for error in data['errors']:<br/>
    &nbsp;&nbsp;&nbsp;&nbsp;print(f"  - {error}")<br/>
    <br/>
    if data.get('warnings'):<br/>
    &nbsp;&nbsp;print("Avisos encontrados:")<br/>
    &nbsp;&nbsp;for warning in data['warnings']:<br/>
    &nbsp;&nbsp;&nbsp;&nbsp;print(f"  - {warning}")
    </font>
    """
    story.append(Paragraph(python_code, code_style))
    
    # Rodapé
    story.append(Spacer(1, 0.5*inch))
    footer_text = f"""
    <i>Documento gerado em {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}<br/>
    Sistema de Auditoria de Condomínios com IA - Versão 1.0.0</i>
    """
    story.append(Paragraph(footer_text, ParagraphStyle(
        'Footer',
        parent=styles['Normal'],
        fontSize=9,
        textColor=HexColor('#7f8c8d'),
        alignment=TA_CENTER
    )))
    
    # Construir PDF
    doc.build(story)
    print(f"✅ PDF gerado com sucesso: {output_path}")
    return output_path

if __name__ == "__main__":
    output = generate_arrays_structure_pdf()
    print(f"📄 Documento disponível em: {os.path.abspath(output)}")

