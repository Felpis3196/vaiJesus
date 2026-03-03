"""
Pré-processador OCR para PDFs Escaneados
Garante extração robusta de dados financeiros de PDFs escaneados usando OCR
"""
import os
import io
import re
import logging
import pandas as pd
from typing import Optional, Tuple, List, Dict, Any, TYPE_CHECKING
from datetime import datetime

if TYPE_CHECKING:
    from PIL import Image

logger = logging.getLogger(__name__)

# Regex para valores monetários (formato BR: 1.234,56 ou 1234,56)
_RE_VALOR_BR = re.compile(r"\d{1,3}(?:\.\d{3})*,\d{2}")
_RE_VALOR_BR_COM_SINAL = re.compile(r"-?\d{1,3}(?:\.\d{3})*,\d{2}")

# Keywords para identificar seções
_KEYWORDS_RECEITAS = [
    "recebimentos", "receita", "ordinária", "ordinaria", "taxa condominial",
    "multa", "juros", "rendimento", "aplicação", "aplicacao"
]

_KEYWORDS_DESPESAS = [
    "despesas", "despesa", "pagamento", "salário", "salario", "folha",
    "inss", "fgts", "água", "agua", "luz", "energia", "manutenção", "manutencao"
]

# Linhas que são totais/subtotais (não criar transação)
_KEYWORDS_TOTAIS = [
    "total geral", "totais", "total das receitas", "total das despesas",
    "total dos recebimentos", "total dos recebimento", "total da conta",
    "subtotal", "soma ", "somatório", "somatorio",
]


def is_pdf_scanned(file_path: str) -> bool:
    """
    Detecta se PDF é escaneado tentando extrair texto.
    Retorna True se extraiu muito pouco texto (< 100 caracteres por página).
    """
    try:
        import pdfplumber
        
        logger.info(f"[OCR PREPROCESSOR] Verificando se PDF é escaneado: {file_path}")
        
        with pdfplumber.open(file_path) as pdf:
            total_pages = len(pdf.pages)
            logger.info(f"[OCR PREPROCESSOR] PDF tem {total_pages} páginas")
            
            if total_pages == 0:
                logger.warning("[OCR PREPROCESSOR] PDF sem páginas, assumindo escaneado")
                return True
            
            text_length = 0
            pages_checked = min(5, total_pages)
            for page_num, page in enumerate(pdf.pages[:pages_checked], 1):
                page_text = page.extract_text()
                if page_text:
                    page_text_len = len(page_text.strip())
                    text_length += page_text_len
                    logger.debug(f"[OCR PREPROCESSOR] Página {page_num}: {page_text_len} caracteres")
                else:
                    logger.debug(f"[OCR PREPROCESSOR] Página {page_num}: sem texto")
            
            chars_per_page = text_length / pages_checked if pages_checked > 0 else 0
            is_scanned = chars_per_page < 50
            
            logger.info(f"[OCR PREPROCESSOR] Resultado: {total_pages} páginas, {text_length} chars nas primeiras {pages_checked} páginas, média: {chars_per_page:.1f} chars/página. Escaneado: {is_scanned}")
            return is_scanned
    except Exception as e:
        logger.error(f"[OCR PREPROCESSOR] Erro ao detectar se PDF é escaneado: {e}", exc_info=True)
        return True  # Assumir escaneado em caso de erro


def _check_tesseract_installation() -> Tuple[bool, str]:
    """
    Verifica se Tesseract está instalado e acessível.
    Faz teste funcional para garantir que realmente funciona.
    
    Returns:
        Tupla (está_instalado, mensagem)
    """
    try:
        import pytesseract
        from PIL import Image
        
        # Configurar caminho se fornecido
        t_cmd = os.getenv("TESSERACT_CMD")
        if t_cmd:
            pytesseract.pytesseract.tesseract_cmd = t_cmd
        
        # Verificar versão
        try:
            version = pytesseract.get_tesseract_version()
            version_str = f"Tesseract {version}"
        except Exception:
            version_str = "Tesseract (versão desconhecida)"
        
        # Teste funcional: criar imagem de teste e tentar OCR
        try:
            from PIL import Image as PILImage, ImageDraw, ImageFont
            test_img = PILImage.new('RGB', (200, 50), color='white')
            # Adicionar algum texto simples
            draw = ImageDraw.Draw(test_img)
            try:
                # Tentar usar fonte padrão
                font = ImageFont.load_default()
            except:
                font = None
            draw.text((10, 10), "TEST", fill='black', font=font)
            
            # Tentar OCR na imagem de teste
            test_result = pytesseract.image_to_string(test_img, config='--psm 7')
            if test_result:
                # Verificar idiomas disponíveis
                try:
                    langs = pytesseract.get_languages()
                    por_available = 'por' in langs or 'por' in [l.lower() for l in langs]
                    lang_info = f", idiomas disponíveis: {', '.join(langs[:5])}"
                    if por_available:
                        lang_info += " (português OK)"
                    else:
                        lang_info += " (português NÃO encontrado)"
                except:
                    lang_info = ""
                
                return True, f"{version_str} instalado e funcionando{lang_info}"
            else:
                return True, f"{version_str} instalado mas teste retornou vazio"
        except Exception as test_e:
            return True, f"{version_str} instalado mas teste funcional falhou: {test_e}"
        
    except ImportError:
        return False, "pytesseract não está instalado. Instale com: pip install pytesseract"
    except Exception as e:
        t_cmd = os.getenv("TESSERACT_CMD")
        if t_cmd:
            return False, f"Tesseract não encontrado no caminho {t_cmd}: {e}"
        return False, f"Tesseract não encontrado no PATH: {e}"


def _preprocess_image_for_ocr(img: "Image.Image") -> "Image.Image":
    """
    Pré-processa imagem para melhorar qualidade do OCR.
    Aplica melhorias de contraste, binarização, deskew e redimensionamento.
    """
    try:
        from PIL import ImageEnhance, ImageFilter, ImageOps
        import numpy as np
        
        # Converter para escala de cinza se necessário
        if img.mode != 'L':
            img = img.convert('L')
        
        # Aplicar binarização (threshold) para melhorar contraste
        # Converter para array numpy para processamento
        try:
            img_array = np.array(img)
            # Aplicar threshold adaptativo (média local)
            threshold = np.mean(img_array)
            img_array = np.where(img_array > threshold * 0.8, 255, 0).astype(np.uint8)
            img = Image.fromarray(img_array)
        except Exception:
            # Se numpy não disponível, usar método PIL simples
            img = ImageOps.autocontrast(img, cutoff=5)
        
        # Aumentar contraste
        enhancer = ImageEnhance.Contrast(img)
        img = enhancer.enhance(2.5)  # Aumentar contraste em 2.5x
        
        # Aumentar nitidez
        enhancer = ImageEnhance.Sharpness(img)
        img = enhancer.enhance(2.5)
        
        # Aplicar filtro para reduzir ruído
        img = img.filter(ImageFilter.MedianFilter(size=3))
        
        # Aplicar desenho de imagem (deskew) básico - correção de rotação
        # Nota: Deskew completo requer biblioteca adicional, mas podemos tentar autocontrast
        img = ImageOps.autocontrast(img, cutoff=2)
        
        # Redimensionar se muito pequena (melhora OCR)
        width, height = img.size
        if width < 1200 or height < 1200:
            scale = max(1200 / width, 1200 / height)
            new_width = int(width * scale)
            new_height = int(height * scale)
            # Usar LANCZOS para melhor qualidade
            from PIL.Image import Resampling
            img = img.resize((new_width, new_height), Resampling.LANCZOS)
        
        # Redimensionar se muito grande (evita timeout)
        width, height = img.size
        if width > 4000 or height > 4000:
            scale = min(4000 / width, 4000 / height)
            new_width = int(width * scale)
            new_height = int(height * scale)
            from PIL.Image import Resampling
            img = img.resize((new_width, new_height), Resampling.LANCZOS)
        
        return img
    except Exception as e:
        logger.debug(f"[PREPROCESS] Erro no pré-processamento de imagem: {e}")
        # Retornar imagem original se pré-processamento falhar
        return img


def extract_text_with_ocr(file_path: str, max_pages: Optional[int] = None, return_page_info: bool = False) -> Tuple[str, bool, Optional[Dict[int, int]]]:
    """
    Extrai texto de PDF usando OCR com pré-processamento melhorado.
    
    Args:
        file_path: Caminho do arquivo PDF
        max_pages: Número máximo de páginas para processar (None = todas)
        return_page_info: Se True, retorna também dict {line_index: page_number}
        
    Returns:
        Tupla (texto_extraido, ocr_usado, page_info) onde page_info é None se return_page_info=False
    """
    try:
        import pdfplumber
        import pytesseract
        from PIL import Image
        
        # Configurar Tesseract
        t_cmd = os.getenv("TESSERACT_CMD")
        if t_cmd:
            pytesseract.pytesseract.tesseract_cmd = t_cmd
        
        # Verificar se Tesseract está disponível
        tesseract_ok, tesseract_msg = _check_tesseract_installation()
        if not tesseract_ok:
            logger.error(f"[OCR] {tesseract_msg}")
            logger.error("[OCR] Instale o Tesseract OCR:")
            logger.error("  Windows: https://github.com/UB-Mannheim/tesseract/wiki")
            logger.error("  Linux: sudo apt-get install tesseract-ocr tesseract-ocr-por")
            logger.error("  Mac: brew install tesseract tesseract-lang")
            logger.error("[OCR] Ou configure TESSERACT_CMD apontando para o executável")
            return "", False
        
        logger.info(f"[OCR] {tesseract_msg}")
        
        ocr_text_parts = []
        page_info_map = {}  # {line_index: page_number} para rastreamento
        current_line_offset = 0  # Offset de linhas acumulado
        
        # Configurações mais agressivas do Tesseract (ordem de prioridade)
        tesseract_configs = [
            '--oem 3 --psm 6 -l por',   # Bloco uniforme, português (melhor para documentos estruturados)
            '--oem 3 --psm 11 -l por',   # Texto esparso, português (melhor para texto livre)
            '--oem 3 --psm 4 -l por',    # Coluna única, português
            '--oem 3 --psm 3 -l por',   # Bloco totalmente automático, português
            '--oem 3 --psm 6',          # Bloco uniforme, sem idioma
            '--oem 3 --psm 11',         # Texto esparso, sem idioma
            '--oem 1 --psm 6 -l por',   # LSTM engine, português
            '--oem 3 --psm 1',          # Orientação e detecção de script
        ]
        
        with pdfplumber.open(file_path) as pdf:
            total_pages = len(pdf.pages)
            pages_to_process = min(max_pages or total_pages, total_pages)
            
            logger.info(f"[OCR] Iniciando OCR em {pages_to_process} páginas do PDF {os.path.basename(file_path)}")
            
            config_used = None
            pages_with_text = 0
            pages_failed = 0
            
            # Tentar múltiplas resoluções para páginas difíceis
            resolutions = [400, 300, 600]  # Ordem: alta, média, muito alta
            
            for page_num, page in enumerate(pdf.pages[:pages_to_process], 1):
                try:
                    logger.info(f"[OCR] Processando página {page_num}/{pages_to_process}...")
                    
                    ocr_text = None
                    best_config = None
                    best_resolution = None
                    
                    # Tentar diferentes resoluções
                    for resolution in resolutions:
                        try:
                            # Converter página para imagem
                            img = page.to_image(resolution=resolution).original
                            if not isinstance(img, Image.Image):
                                continue
                            
                            logger.debug(f"[OCR] Página {page_num}: Tentando resolução {resolution} DPI...")
                            
                            # Pré-processar imagem
                            img_processed = _preprocess_image_for_ocr(img)
                            
                            # Tentar OCR com diferentes configurações
                            for tesseract_config in tesseract_configs:
                                try:
                                    test_text = pytesseract.image_to_string(
                                        img_processed, 
                                        config=tesseract_config,
                                        timeout=30  # Timeout de 30 segundos por página
                                    )
                                    if test_text and len(test_text.strip()) > 10:
                                        # Se encontrou texto melhor que o anterior, usar
                                        if ocr_text is None or len(test_text.strip()) > len(ocr_text.strip()):
                                            ocr_text = test_text
                                            best_config = tesseract_config
                                            best_resolution = resolution
                                            logger.debug(f"[OCR] Página {page_num}: Melhor resultado com resolução {resolution} DPI, config: {best_config}")
                                except Exception as e:
                                    logger.debug(f"[OCR] Página {page_num}: Config {tesseract_config} falhou: {e}")
                                    continue
                            
                            # Se encontrou texto suficiente, parar de tentar outras resoluções
                            if ocr_text and len(ocr_text.strip()) > 50:
                                break
                                
                        except Exception as res_e:
                            logger.debug(f"[OCR] Página {page_num}: Erro com resolução {resolution}: {res_e}")
                            continue
                    
                    if ocr_text and len(ocr_text.strip()) > 10:
                        ocr_text_parts.append(ocr_text)
                        pages_with_text += 1
                        if config_used is None:
                            config_used = best_config
                        # Rastrear página de origem para cada linha (se return_page_info=True)
                        if return_page_info:
                            page_lines = ocr_text.split('\n')
                            for line_idx, _ in enumerate(page_lines):
                                page_info_map[current_line_offset + line_idx] = page_num
                            current_line_offset += len(page_lines)
                        logger.info(f"[OCR] ✓ Página {page_num}/{pages_to_process}: {len(ocr_text.strip())} caracteres extraídos (resolução: {best_resolution} DPI, config: {best_config})")
                        logger.debug(f"[OCR] Primeiros 200 chars da página {page_num}:\n{ocr_text[:200]}")
                    else:
                        pages_failed += 1
                        logger.warning(f"[OCR] ✗ Página {page_num}/{pages_to_process}: OCR não extraiu texto suficiente ({len(ocr_text.strip()) if ocr_text else 0} chars)")
                        
                        # Salvar imagem de debug da primeira página que falha
                        if pages_failed == 1:
                            try:
                                import tempfile
                                debug_img = page.to_image(resolution=300).original
                                debug_path = os.path.join(tempfile.gettempdir(), f"ocr_debug_page{page_num}.png")
                                debug_img.save(debug_path)
                                logger.info(f"[OCR] Imagem de debug da página {page_num} salva em: {debug_path}")
                            except Exception:
                                pass
                        
                except Exception as e:
                    pages_failed += 1
                    logger.warning(f"[OCR] Erro ao processar página {page_num} com OCR: {e}", exc_info=True)
                    continue
        
        if ocr_text_parts:
            full_text = "\n".join(ocr_text_parts)
            logger.info(f"[OCR] ✓ SUCESSO: Extraídos {len(full_text)} caracteres de {pages_with_text}/{pages_to_process} páginas")
            logger.info(f"[OCR] Estatísticas: {pages_with_text} páginas com texto, {pages_failed} páginas falharam")
            if config_used:
                logger.info(f"[OCR] Configuração mais eficaz: {config_used}")
            if return_page_info:
                return full_text, True, page_info_map
            return full_text, True, None
        else:
            logger.error(f"[OCR] ✗ FALHA: Não conseguiu extrair texto de nenhuma das {pages_to_process} páginas processadas")
            logger.error(f"[OCR] Estatísticas: {pages_failed} páginas falharam completamente")
            
            # Verificar se Tesseract está realmente funcionando
            try:
                test_img = Image.new('RGB', (200, 50), color='white')
                from PIL import ImageDraw
                draw = ImageDraw.Draw(test_img)
                draw.text((10, 10), "TEST", fill='black')
                test_text = pytesseract.image_to_string(test_img, config='--psm 7')
                if test_text:
                    logger.info("[OCR] Tesseract está funcionando (teste básico OK)")
                else:
                    logger.warning("[OCR] Tesseract respondeu mas teste retornou vazio")
            except Exception as test_e:
                logger.error(f"[OCR] Tesseract não está funcionando corretamente: {test_e}")
            
            # Verificar idiomas disponíveis
            try:
                langs = pytesseract.get_languages()
                logger.info(f"[OCR] Idiomas disponíveis no Tesseract: {', '.join(langs)}")
                if 'por' not in langs and 'por' not in [l.lower() for l in langs]:
                    logger.error("[OCR] ⚠️ Idioma português (por) NÃO está instalado!")
                    logger.error("[OCR] Instale com:")
                    logger.error("  Linux/Docker: apt-get install tesseract-ocr-por")
                    logger.error("  Windows: Baixe pacote de idioma português")
                    logger.error("  Mac: brew install tesseract-lang")
            except Exception as lang_e:
                logger.warning(f"[OCR] Não foi possível verificar idiomas: {lang_e}")
            
            logger.error("[OCR] Possíveis causas:")
            logger.error("  1. Tesseract não está instalado ou não está no PATH")
            logger.error("  2. Idioma português não está instalado no Tesseract")
            logger.error("  3. PDF tem imagens de baixa qualidade ou muito ruído")
            logger.error("  4. PDF está corrompido ou protegido")
            logger.error("  5. Imagens do PDF são muito grandes ou complexas")
            logger.error("  6. PDF pode ter proteção ou criptografia")
            
            if return_page_info:
                return "", False, None
            return "", False, None
            
    except ImportError as e:
        logger.error(f"[OCR] Bibliotecas não instaladas: {e}")
        logger.error("[OCR] Instale com: pip install pytesseract pdfplumber pillow")
        if return_page_info:
            return "", False, None
        return "", False, None
    except Exception as e:
        logger.error(f"[OCR] Erro ao executar OCR: {e}", exc_info=True)
        if return_page_info:
            return "", False, None
        return "", False, None


def parse_ocr_text_to_dataframe(text: str, page_info: Optional[Dict[int, int]] = None) -> Optional[pd.DataFrame]:
    """
    Parse texto OCR e extrai transações financeiras estruturadas.
    Versão melhorada que captura mais padrões e valores.
    
    Args:
        text: Texto extraído via OCR
        page_info: Dict opcional {line_index: page_number} para rastrear página de origem
        
    Returns:
        DataFrame com colunas: data, descricao, valor, tipo, _page (se disponível), _line (se disponível)
    """
    if not text or len(text.strip()) < 100:
        logger.warning("[PARSE OCR] Texto OCR muito curto para parsing")
        return None
    
    transactions = []
    lines = text.split('\n')
    page_info = page_info or {}
    
    # Valores monetários: apenas formato BR com vírgula decimal obrigatória (,\d{2}) para fidelidade ao documento
    # Padrão para datas
    data_pattern = re.compile(r'(\d{1,2})[/\-\.](\d{1,2})[/\-\.](\d{2,4})')
    
    # Rastrear seção atual
    secao_atual = ""
    current_date = None
    
    logger.info(f"[PARSE OCR] Parseando {len(lines)} linhas do texto OCR")
    
    valores_encontrados = 0
    linhas_processadas = 0
    
    for line_idx, line in enumerate(lines):
        line = line.strip()
        if not line or len(line) < 3:
            continue
        
        linhas_processadas += 1
        line_lower = line.lower()
        
        # Atualizar seção ao encontrar títulos (mais flexível)
        if "recebimentos" in line_lower or "ordinária" in line_lower or "ordinaria" in line_lower or "receita" in line_lower:
            if any(kw in line_lower for kw in ["recebimentos", "ordinária", "ordinaria", "receita"]):
                secao_atual = "receita"
                logger.debug(f"[PARSE OCR] Linha {line_idx}: Seção RECEITA detectada: {line[:50]}")
                continue
        
        if "despesas" in line_lower or "despesa " in line_lower:
            secao_atual = "despesa"
            logger.debug(f"[PARSE OCR] Linha {line_idx}: Seção DESPESA detectada: {line[:50]}")
            continue
        
        if any(kw in line_lower for kw in _KEYWORDS_TOTAIS):
            continue
        
        # Tentar extrair data
        data_match = data_pattern.search(line)
        if data_match:
            day, month, year = data_match.groups()
            try:
                year = int(year)
                if year < 100:
                    year += 2000 if year < 50 else 1900
                current_date = f"{year:04d}-{month:02d}-{day:02d}"
                logger.debug(f"[PARSE OCR] Linha {line_idx}: Data detectada: {current_date}")
            except:
                pass
        
        # Valores monetários: só aceitar formato BR com vírgula decimal (,\d{2}) para fidelidade ao documento
        valor_matches = list(_RE_VALOR_BR_COM_SINAL.finditer(line))
        if not valor_matches:
            continue
        
        valores_encontrados += len(valor_matches)
        valor_match = valor_matches[-1]
        valor_str = valor_match.group(0)
        if not valor_str:
            continue
        
        is_neg = valor_str.startswith("-") or (valor_match.start() > 0 and line[valor_match.start()-1] == '-')
        
        try:
            valor_clean = valor_str.lstrip("-").replace('.', '').replace(',', '.')
            valor = float(valor_clean)
            if is_neg:
                valor = -abs(valor)
        except (ValueError, AttributeError):
            continue
        
        if abs(valor) < 0.01:
            continue
        if abs(valor) > 50_000_000:
            logger.debug(f"[PARSE OCR] Valor muito grande ignorado: {valor}")
            continue
        
        descricao = line[:valor_match.start()].strip()
        descricao = data_pattern.sub('', descricao).strip()
        descricao = re.sub(r'R\$\s*', '', descricao, flags=re.IGNORECASE).strip()
        descricao = re.sub(r'\s+', ' ', descricao).strip()
        
        if len(descricao) < 2:
            descricao_after = line[valor_match.end():].strip()
            if len(descricao_after) > len(descricao):
                descricao = descricao_after
            if len(descricao) < 2:
                descricao = "Item extraído do demonstrativo"
        
        tipo = _classify_transaction_type(line_lower, secao_atual)
        
        trans_dict = {
            "data": current_date or datetime.now().strftime("%Y-%m-%d"),
            "descricao": descricao[:500],
            "valor": abs(valor) if valor < 0 else valor,
            "tipo": tipo,
        }
        if page_info and line_idx in page_info:
            trans_dict["_page"] = page_info[line_idx]
            trans_dict["_line"] = line_idx + 1
        transactions.append(trans_dict)
    
    logger.info(f"[PARSE OCR] Processadas {linhas_processadas} linhas, encontrados {valores_encontrados} valores monetários")
    
    if not transactions:
        logger.warning("[PARSE OCR] Nenhuma transação extraída do texto OCR")
        # Logar amostra do texto para debug
        logger.debug(f"[PARSE OCR] Primeiras 50 linhas do texto:\n{chr(10).join(lines[:50])}")
        return None
    
    df = pd.DataFrame(transactions)
    logger.info(f"[PARSE OCR] Extraídas {len(df)} transações do texto OCR")
    
    # Estatísticas
    receitas = len(df[df["tipo"] == "receita"])
    despesas = len(df[df["tipo"] == "despesa"])
    total_receitas = df[df["tipo"] == "receita"]["valor"].sum()
    total_despesas = df[df["tipo"] == "despesa"]["valor"].sum()
    
    logger.info(f"[PARSE OCR] Estatísticas: {receitas} receitas (R$ {total_receitas:,.2f}), {despesas} despesas (R$ {total_despesas:,.2f})")
    
    return df


def _classify_transaction_type(line_lower: str, secao_atual: str) -> str:
    """Classifica transação como receita ou despesa."""
    if secao_atual == "receita":
        return "receita"
    if secao_atual == "despesa":
        return "despesa"
    
    # Verificar keywords
    if any(kw in line_lower for kw in _KEYWORDS_RECEITAS):
        return "receita"
    if any(kw in line_lower for kw in _KEYWORDS_DESPESAS):
        return "despesa"
    
    # Default: despesa
    return "despesa"


def preprocess_scanned_pdf(file_path: str) -> Optional[pd.DataFrame]:
    """
    Pré-processa PDF escaneado: detecta, executa OCR e retorna DataFrame estruturado.
    Inclui informações de página/linha para rastreamento de erros de extração.
    
    Args:
        file_path: Caminho do arquivo PDF
        
    Returns:
        DataFrame com transações estruturadas ou None se falhar
    """
    logger.info(f"[OCR PREPROCESSOR] ===== INICIANDO PRÉ-PROCESSAMENTO OCR =====")
    logger.info(f"[OCR PREPROCESSOR] Arquivo: {file_path}")
    
    # Verificar se arquivo existe
    if not os.path.exists(file_path):
        logger.error(f"[OCR PREPROCESSOR] Arquivo não encontrado: {file_path}")
        return None
    
    file_size = os.path.getsize(file_path)
    logger.info(f"[OCR PREPROCESSOR] Tamanho do arquivo: {file_size / 1024 / 1024:.2f} MB")
    
    # 1. Verificar se é PDF escaneado
    logger.info("[OCR PREPROCESSOR] Passo 1: Verificando se PDF é escaneado...")
    if not is_pdf_scanned(file_path):
        logger.info("[OCR PREPROCESSOR] PDF não parece ser escaneado, pulando OCR")
        return None
    
    # 2. Executar OCR com rastreamento de página
    logger.info("[OCR PREPROCESSOR] Passo 2: Executando OCR com rastreamento de página...")
    ocr_text, ocr_used, page_info = extract_text_with_ocr(file_path, return_page_info=True)
    if not ocr_text or len(ocr_text.strip()) < 100:
        logger.error(f"[OCR PREPROCESSOR] OCR não conseguiu extrair texto suficiente. Texto extraído: {len(ocr_text) if ocr_text else 0} caracteres")
        return None
    
    logger.info(f"[OCR PREPROCESSOR] OCR extraiu {len(ocr_text)} caracteres")
    logger.debug(f"[OCR PREPROCESSOR] Primeiros 500 caracteres do texto OCR:\n{ocr_text[:500]}")
    
    # 3. Parsear texto em DataFrame com informações de página/linha
    logger.info("[OCR PREPROCESSOR] Passo 3: Parseando texto OCR em transações...")
    df = parse_ocr_text_to_dataframe(ocr_text, page_info=page_info)
    if df is None or df.empty:
        logger.error("[OCR PREPROCESSOR] Falha ao parsear texto OCR em transações")
        logger.debug(f"[OCR PREPROCESSOR] Texto completo para debug:\n{ocr_text[:2000]}")
        return None
    
    # 4. Adicionar metadados
    df["_ocr_used"] = True
    df["_source_file"] = os.path.basename(file_path)
    
    logger.info(f"[OCR PREPROCESSOR] ===== PRÉ-PROCESSAMENTO OCR CONCLUÍDO =====")
    logger.info(f"[OCR PREPROCESSOR] Transações extraídas: {len(df)}")
    logger.info(f"[OCR PREPROCESSOR] Colunas: {list(df.columns)}")
    
    # Estatísticas finais
    if "valor" in df.columns and "tipo" in df.columns:
        receitas = df[df["tipo"] == "receita"]["valor"].sum()
        despesas = df[df["tipo"] == "despesa"]["valor"].sum()
        logger.info(f"[OCR PREPROCESSOR] Total receitas: R$ {receitas:,.2f}")
        logger.info(f"[OCR PREPROCESSOR] Total despesas: R$ {despesas:,.2f}")
        logger.info(f"[OCR PREPROCESSOR] Saldo: R$ {receitas - despesas:,.2f}")
    
    return df
