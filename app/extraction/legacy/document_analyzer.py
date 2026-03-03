"""
Análise de Documentos Fiscais e Impostos
Extrai e valida dados de documentos fiscais e correlaciona com lançamentos
"""
import pandas as pd
import re
from typing import Dict, List, Optional, Any
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class DocumentAnalyzer:
    """Analisador de documentos fiscais e impostos"""
    
    def __init__(self):
        """Inicializa o analisador de documentos"""
        self.supported_document_types = ['NF-e', 'NFS-e', 'Recibo', 'Comprovante', 'XML']
        
    def extract_fiscal_data(self, document_content: str, document_type: str = 'auto') -> Dict[str, Any]:
        """
        Extrai dados fiscais de um documento
        
        Args:
            document_content: Conteúdo do documento (texto, XML, etc.)
            document_type: Tipo do documento ('NF-e', 'NFS-e', 'auto')
            
        Returns:
            Dict com dados extraídos do documento
        """
        try:
            # FASE 6: Logs detalhados
            content_length = len(document_content) if document_content else 0
            logger.info(f"Iniciando extração de dados fiscais. Tipo: {document_type}, Tamanho do conteúdo: {content_length} caracteres")
            
            if document_type == 'auto':
                document_type = self._detect_document_type(document_content)
                logger.debug(f"Tipo de documento detectado automaticamente: {document_type}")
            
            # Extrair dados baseado no tipo
            if document_type == 'NF-e':
                result = self._extract_nfe_data(document_content)
            elif document_type == 'NFS-e':
                result = self._extract_nfse_data(document_content)
            elif document_type == 'XML':
                result = self._extract_xml_data(document_content)
            else:
                result = self._extract_generic_data(document_content)
            
            # Log de resultados
            fields_extracted = sum(1 for v in result.values() if v is not None and v != [])
            logger.info(f"Extração concluída. Tipo: {document_type}, Campos extraídos: {fields_extracted}/{len(result)}")
            
            return result
                
        except Exception as e:
            logger.error(f"Erro ao extrair dados fiscais: {e}", exc_info=True)
            return {'document_type': 'Generic', 'error': str(e)}
    
    def _detect_document_type(self, content: str) -> str:
        """Detecta o tipo de documento automaticamente"""
        if not content or len(content.strip()) == 0:
            logger.warning("Conteúdo vazio para detecção de tipo de documento")
            return 'Generic'
        
        content_upper = content.upper()
        
        # FASE 5: Melhorar detecção de tipo
        
        # Detectar XML primeiro (mais específico)
        if content.strip().startswith('<?xml'):
            if '<nfe' in content_upper or 'infnfe' in content_upper:
                logger.debug("Documento detectado como XML de NF-e")
                return 'NF-e'
            elif '<nfse' in content_upper or 'infnfse' in content_upper:
                logger.debug("Documento detectado como XML de NFS-e")
                return 'NFS-e'
            else:
                logger.debug("Documento detectado como XML genérico")
                return 'XML'
        
        # Detectar NF-e por palavras-chave
        nfe_keywords = [
            'NFE', 'NOTA FISCAL ELETRONICA', 'NOTA FISCAL ELETRÔNICA',
            'CHAVE DE ACESSO', 'CHAVE ACESSO', 'CÓDIGO DE BARRAS',
            'DANFE', 'DACTE'
        ]
        if any(keyword in content_upper for keyword in nfe_keywords):
            logger.debug("Documento detectado como NF-e por palavras-chave")
            return 'NF-e'
        
        # Detectar NFS-e por palavras-chave
        nfse_keywords = [
            'NFSE', 'NOTA FISCAL DE SERVICOS', 'NOTA FISCAL DE SERVIÇOS',
            'PRESTADOR DE SERVICOS', 'TOMADOR DE SERVICOS'
        ]
        if any(keyword in content_upper for keyword in nfse_keywords):
            logger.debug("Documento detectado como NFS-e por palavras-chave")
            return 'NFS-e'
        
        # Se não encontrou nada específico, retornar Generic
        logger.debug("Documento detectado como Generic (tipo não identificado)")
        return 'Generic'
    
    def _extract_nfe_data(self, content: str) -> Dict[str, Any]:
        """Extrai dados de NF-e"""
        data = {
            'document_type': 'NF-e',
            'cnpj_emissor': None,
            'cnpj_destinatario': None,
            'numero_nf': None,
            'serie': None,
            'data_emissao': None,
            'valor_total': None,
            'valor_icms': None,
            'valor_ipi': None,
            'chave_acesso': None,
            'itens': []
        }
        
        # FASE 3: Melhorar padrões de regex para extração
        
        # Extrair CNPJ - Múltiplos padrões (formatado e sem formatação)
        cnpj_patterns = [
            r'\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}',  # Formatado: 12.345.678/0001-90
            r'\d{14}',  # Sem formatação: 12345678000190 (14 dígitos)
        ]
        cnpjs = []
        for pattern in cnpj_patterns:
            matches = re.findall(pattern, content)
            for match in matches:
                # Validar se é CNPJ (14 dígitos)
                digits_only = re.sub(r'[^\d]', '', match)
                if len(digits_only) == 14 and digits_only not in [m.replace('.', '').replace('/', '').replace('-', '') for m in cnpjs]:
                    if '.' in match:
                        cnpjs.append(match)  # Já formatado
                    else:
                        # Formatar CNPJ
                        cnpjs.append(f"{digits_only[:2]}.{digits_only[2:5]}.{digits_only[5:8]}/{digits_only[8:12]}-{digits_only[12:]}")
        
        if cnpjs:
            data['cnpj_emissor'] = cnpjs[0]
            data['cnpj_destinatario'] = cnpjs[1] if len(cnpjs) > 1 else None
        
        # Extrair número da NF - Múltiplos padrões
        nf_patterns = [
            r'N[úu]mero[:\s]+(\d+)',
            r'N[úu]m[.:\s]+(\d+)',
            r'NF[:\s]+(\d+)',
            r'Nota[:\s]+Fiscal[:\s]+N[úu]mero[:\s]+(\d+)',
            r'N[úu]mero\s+da\s+Nota[:\s]+(\d+)',
        ]
        for pattern in nf_patterns:
            nf_match = re.search(pattern, content, re.IGNORECASE)
            if nf_match:
                data['numero_nf'] = nf_match.group(1)
                break
        
        # Extrair série
        serie_patterns = [
            r'S[ée]rie[:\s]+(\d+)',
            r'Ser[.:\s]+(\d+)',
        ]
        for pattern in serie_patterns:
            serie_match = re.search(pattern, content, re.IGNORECASE)
            if serie_match:
                data['serie'] = serie_match.group(1)
                break
        
        # Extrair valores - Múltiplos padrões
        valor_patterns = [
            r'Total[:\s]+R\$\s*([\d.,]+)',
            r'Valor\s+Total[:\s]+R\$\s*([\d.,]+)',
            r'Total\s+da\s+Nota[:\s]+R\$\s*([\d.,]+)',
            r'R\$\s*([\d.,]+)\s*Total',
            r'Total[:\s]+([\d.,]+)',
        ]
        for pattern in valor_patterns:
            valor_match = re.search(pattern, content, re.IGNORECASE)
            if valor_match:
                try:
                    valor_str = valor_match.group(1).replace('.', '').replace(',', '.')
                    data['valor_total'] = float(valor_str)
                    break
                except (ValueError, AttributeError):
                    continue
        
        # Extrair ICMS
        icms_patterns = [
            r'ICMS[:\s]+R\$\s*([\d.,]+)',
            r'Valor\s+ICMS[:\s]+R\$\s*([\d.,]+)',
            r'ICMS[:\s]+([\d.,]+)',
        ]
        for pattern in icms_patterns:
            icms_match = re.search(pattern, content, re.IGNORECASE)
            if icms_match:
                try:
                    icms_str = icms_match.group(1).replace('.', '').replace(',', '.')
                    data['valor_icms'] = float(icms_str)
                    break
                except (ValueError, AttributeError):
                    continue
        
        # Extrair IPI
        ipi_patterns = [
            r'IPI[:\s]+R\$\s*([\d.,]+)',
            r'Valor\s+IPI[:\s]+R\$\s*([\d.,]+)',
            r'IPI[:\s]+([\d.,]+)',
        ]
        for pattern in ipi_patterns:
            ipi_match = re.search(pattern, content, re.IGNORECASE)
            if ipi_match:
                try:
                    ipi_str = ipi_match.group(1).replace('.', '').replace(',', '.')
                    data['valor_ipi'] = float(ipi_str)
                    break
                except (ValueError, AttributeError):
                    continue
        
        # Extrair data de emissão
        data_patterns = [
            r'Data\s+de\s+Emiss[ãa]o[:\s]+(\d{2}[/-]\d{2}[/-]\d{4})',
            r'Emiss[ãa]o[:\s]+(\d{2}[/-]\d{2}[/-]\d{4})',
            r'Data[:\s]+(\d{2}[/-]\d{2}[/-]\d{4})',
        ]
        for pattern in data_patterns:
            data_match = re.search(pattern, content, re.IGNORECASE)
            if data_match:
                data['data_emissao'] = data_match.group(1)
                break
        
        # Extrair chave de acesso - Múltiplos padrões
        chave_patterns = [
            r'Chave\s+de\s+Acesso[:\s]+([0-9]{44})',
            r'Chave\s+Acesso[:\s]+([0-9]{44})',
            r'Chave[:\s]+([0-9]{44})',
            r'([0-9]{44})',  # Apenas 44 dígitos consecutivos
        ]
        for pattern in chave_patterns:
            chave_match = re.search(pattern, content)
            if chave_match:
                chave = chave_match.group(1)
                # Validar se parece com chave de acesso (44 dígitos)
                if len(chave) == 44 and chave.isdigit():
                    data['chave_acesso'] = chave
                    break
        
        logger.debug(f"Extraídos dados NF-e: CNPJ={data['cnpj_emissor']}, NF={data['numero_nf']}, Valor={data['valor_total']}")
        return data
    
    def _extract_nfse_data(self, content: str) -> Dict[str, Any]:
        """Extrai dados de NFS-e"""
        data = {
            'document_type': 'NFS-e',
            'cnpj_prestador': None,
            'cnpj_tomador': None,
            'numero_nfse': None,
            'data_emissao': None,
            'valor_servico': None,
            'valor_iss': None,
            'codigo_servico': None
        }
        
        # FASE 3: Melhorar padrões de regex (similar ao NF-e)
        
        # Extrair CNPJ - Múltiplos padrões
        cnpj_patterns = [
            r'\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}',  # Formatado
            r'\d{14}',  # Sem formatação
        ]
        cnpjs = []
        for pattern in cnpj_patterns:
            matches = re.findall(pattern, content)
            for match in matches:
                digits_only = re.sub(r'[^\d]', '', match)
                if len(digits_only) == 14 and digits_only not in [m.replace('.', '').replace('/', '').replace('-', '') for m in cnpjs]:
                    if '.' in match:
                        cnpjs.append(match)
                    else:
                        cnpjs.append(f"{digits_only[:2]}.{digits_only[2:5]}.{digits_only[5:8]}/{digits_only[8:12]}-{digits_only[12:]}")
        
        if cnpjs:
            data['cnpj_prestador'] = cnpjs[0]
            data['cnpj_tomador'] = cnpjs[1] if len(cnpjs) > 1 else None
        
        # Extrair número da NFS-e
        nfse_patterns = [
            r'N[úu]mero[:\s]+(\d+)',
            r'N[úu]m[.:\s]+(\d+)',
            r'NFSE[:\s]+(\d+)',
            r'Nota[:\s]+Fiscal[:\s]+de[:\s]+Servi[çc]os[:\s]+N[úu]mero[:\s]+(\d+)',
        ]
        for pattern in nfse_patterns:
            nfse_match = re.search(pattern, content, re.IGNORECASE)
            if nfse_match:
                data['numero_nfse'] = nfse_match.group(1)
                break
        
        # Extrair valores - Múltiplos padrões
        valor_patterns = [
            r'Valor\s+do\s+Servi[çc]o[:\s]+R\$\s*([\d.,]+)',
            r'Valor\s+Servi[çc]o[:\s]+R\$\s*([\d.,]+)',
            r'Total\s+do\s+Servi[çc]o[:\s]+R\$\s*([\d.,]+)',
            r'Valor[:\s]+R\$\s*([\d.,]+)',
        ]
        for pattern in valor_patterns:
            valor_match = re.search(pattern, content, re.IGNORECASE)
            if valor_match:
                try:
                    valor_str = valor_match.group(1).replace('.', '').replace(',', '.')
                    data['valor_servico'] = float(valor_str)
                    break
                except (ValueError, AttributeError):
                    continue
        
        # Extrair ISS
        iss_patterns = [
            r'ISS[:\s]+R\$\s*([\d.,]+)',
            r'Valor\s+ISS[:\s]+R\$\s*([\d.,]+)',
            r'Imposto\s+sobre\s+Servi[çc]os[:\s]+R\$\s*([\d.,]+)',
            r'ISS[:\s]+([\d.,]+)',
        ]
        for pattern in iss_patterns:
            iss_match = re.search(pattern, content, re.IGNORECASE)
            if iss_match:
                try:
                    iss_str = iss_match.group(1).replace('.', '').replace(',', '.')
                    data['valor_iss'] = float(iss_str)
                    break
                except (ValueError, AttributeError):
                    continue
        
        # Extrair data de emissão
        data_patterns = [
            r'Data\s+de\s+Emiss[ãa]o[:\s]+(\d{2}[/-]\d{2}[/-]\d{4})',
            r'Emiss[ãa]o[:\s]+(\d{2}[/-]\d{2}[/-]\d{4})',
            r'Data[:\s]+(\d{2}[/-]\d{2}[/-]\d{4})',
        ]
        for pattern in data_patterns:
            data_match = re.search(pattern, content, re.IGNORECASE)
            if data_match:
                data['data_emissao'] = data_match.group(1)
                break
        
        logger.debug(f"Extraídos dados NFS-e: CNPJ Prestador={data['cnpj_prestador']}, NFSE={data['numero_nfse']}, Valor={data['valor_servico']}")
        return data
    
    def _extract_xml_data(self, content: str) -> Dict[str, Any]:
        """Extrai dados de XML fiscal"""
        try:
            # FASE 4: Melhorar parsing de XML com lxml
            from lxml import etree
            
            data = {
                'document_type': 'XML',
                'chave_acesso': None,
                'cnpj_emissor': None,
                'cnpj_destinatario': None,
                'numero_nf': None,
                'serie': None,
                'valor_total': None,
                'valor_icms': None,
                'valor_ipi': None,
                'data_emissao': None
            }
            
            # Parsear XML
            try:
                root = etree.fromstring(content.encode('utf-8') if isinstance(content, str) else content)
            except:
                # Fallback para ElementTree
                import xml.etree.ElementTree as ET
                root = ET.fromstring(content)
            
            # Namespaces comuns de NF-e
            namespaces = {
                'nfe': 'http://www.portalfiscal.inf.br/nfe',
                'ns': 'http://www.portalfiscal.inf.br/nfe'
            }
            
            # Tentar extrair chave de acesso (múltiplos caminhos)
            chave_paths = [
                './/{http://www.portalfiscal.inf.br/nfe}infNFe',
                './/infNFe',
                './/NFe/infNFe',
            ]
            for path in chave_paths:
                try:
                    inf_nfe = root.find(path)
                    if inf_nfe is not None:
                        chave_id = inf_nfe.get('Id', '')
                        if chave_id:
                            data['chave_acesso'] = chave_id.replace('NFe', '').replace('NFe', '')
                            break
                except:
                    continue
            
            # Extrair CNPJ do emitente
            cnpj_paths = [
                './/{http://www.portalfiscal.inf.br/nfe}emit/{http://www.portalfiscal.inf.br/nfe}CNPJ',
                './/emit/CNPJ',
                './/CNPJ',
            ]
            for path in cnpj_paths:
                try:
                    cnpj_elem = root.find(path)
                    if cnpj_elem is not None and cnpj_elem.text:
                        cnpj = cnpj_elem.text.strip()
                        if len(cnpj) == 14:
                            # Formatar CNPJ
                            data['cnpj_emissor'] = f"{cnpj[:2]}.{cnpj[2:5]}.{cnpj[5:8]}/{cnpj[8:12]}-{cnpj[12:]}"
                            break
                except:
                    continue
            
            # Extrair CNPJ do destinatário
            dest_cnpj_paths = [
                './/{http://www.portalfiscal.inf.br/nfe}dest/{http://www.portalfiscal.inf.br/nfe}CNPJ',
                './/dest/CNPJ',
            ]
            for path in dest_cnpj_paths:
                try:
                    cnpj_elem = root.find(path)
                    if cnpj_elem is not None and cnpj_elem.text:
                        cnpj = cnpj_elem.text.strip()
                        if len(cnpj) == 14:
                            data['cnpj_destinatario'] = f"{cnpj[:2]}.{cnpj[2:5]}.{cnpj[5:8]}/{cnpj[8:12]}-{cnpj[12:]}"
                            break
                except:
                    continue
            
            # Extrair número da NF
            nf_paths = [
                './/{http://www.portalfiscal.inf.br/nfe}ide/{http://www.portalfiscal.inf.br/nfe}nNF',
                './/ide/nNF',
                './/nNF',
            ]
            for path in nf_paths:
                try:
                    nf_elem = root.find(path)
                    if nf_elem is not None and nf_elem.text:
                        data['numero_nf'] = nf_elem.text.strip()
                        break
                except:
                    continue
            
            # Extrair série
            serie_paths = [
                './/{http://www.portalfiscal.inf.br/nfe}ide/{http://www.portalfiscal.inf.br/nfe}serie',
                './/ide/serie',
                './/serie',
            ]
            for path in serie_paths:
                try:
                    serie_elem = root.find(path)
                    if serie_elem is not None and serie_elem.text:
                        data['serie'] = serie_elem.text.strip()
                        break
                except:
                    continue
            
            # Extrair valor total
            valor_paths = [
                './/{http://www.portalfiscal.inf.br/nfe}total/{http://www.portalfiscal.inf.br/nfe}ICMSTot/{http://www.portalfiscal.inf.br/nfe}vNF',
                './/total/ICMSTot/vNF',
                './/vNF',
            ]
            for path in valor_paths:
                try:
                    valor_elem = root.find(path)
                    if valor_elem is not None and valor_elem.text:
                        try:
                            data['valor_total'] = float(valor_elem.text.strip())
                            break
                        except (ValueError, TypeError):
                            continue
                except:
                    continue
            
            # Extrair ICMS
            icms_paths = [
                './/{http://www.portalfiscal.inf.br/nfe}total/{http://www.portalfiscal.inf.br/nfe}ICMSTot/{http://www.portalfiscal.inf.br/nfe}vICMS',
                './/total/ICMSTot/vICMS',
                './/vICMS',
            ]
            for path in icms_paths:
                try:
                    icms_elem = root.find(path)
                    if icms_elem is not None and icms_elem.text:
                        try:
                            data['valor_icms'] = float(icms_elem.text.strip())
                            break
                        except (ValueError, TypeError):
                            continue
                except:
                    continue
            
            # Extrair IPI
            ipi_paths = [
                './/{http://www.portalfiscal.inf.br/nfe}total/{http://www.portalfiscal.inf.br/nfe}IPITot/{http://www.portalfiscal.inf.br/nfe}vIPI',
                './/total/IPITot/vIPI',
                './/vIPI',
            ]
            for path in ipi_paths:
                try:
                    ipi_elem = root.find(path)
                    if ipi_elem is not None and ipi_elem.text:
                        try:
                            data['valor_ipi'] = float(ipi_elem.text.strip())
                            break
                        except (ValueError, TypeError):
                            continue
                except:
                    continue
            
            # Extrair data de emissão
            data_paths = [
                './/{http://www.portalfiscal.inf.br/nfe}ide/{http://www.portalfiscal.inf.br/nfe}dhEmi',
                './/ide/dhEmi',
                './/dhEmi',
            ]
            for path in data_paths:
                try:
                    data_elem = root.find(path)
                    if data_elem is not None and data_elem.text:
                        # Converter formato ISO para DD/MM/YYYY
                        try:
                            from datetime import datetime
                            dt = datetime.fromisoformat(data_elem.text.replace('T', ' ').split('-')[0])
                            data['data_emissao'] = dt.strftime('%d/%m/%Y')
                            break
                        except:
                            data['data_emissao'] = data_elem.text.strip()
                            break
                except:
                    continue
            
            logger.debug(f"Extraídos dados XML: CNPJ={data['cnpj_emissor']}, NF={data['numero_nf']}, Valor={data['valor_total']}")
            return data
            
        except ImportError:
            # Fallback para ElementTree se lxml não estiver disponível
            logger.warning("lxml não disponível, usando ElementTree")
            import xml.etree.ElementTree as ET
            try:
                root = ET.fromstring(content)
                data = {
                    'document_type': 'XML',
                    'chave_acesso': None,
                    'cnpj_emissor': None,
                    'valor_total': None,
                    'data_emissao': None
                }
                # Tentar extrações básicas
                inf_nfe = root.find('.//{http://www.portalfiscal.inf.br/nfe}infNFe')
                if inf_nfe is not None:
                    data['chave_acesso'] = inf_nfe.get('Id', '').replace('NFe', '')
                return data
            except Exception as e:
                logger.error(f"Erro ao processar XML com ElementTree: {e}")
                return {'document_type': 'XML', 'error': str(e)}
        except Exception as e:
            logger.error(f"Erro ao processar XML: {e}", exc_info=True)
            return {'document_type': 'XML', 'error': str(e)}
    
    def _extract_generic_data(self, content: str) -> Dict[str, Any]:
        """Extrai dados genéricos de documentos"""
        data = {
            'document_type': 'Generic',
            'cnpj_cpf': None,
            'valor': None,
            'data': None,
            'descricao': None
        }
        
        # FASE 3: Melhorar padrões de regex
        
        # Extrair CNPJ/CPF - Múltiplos padrões
        cnpj_cpf_patterns = [
            r'\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}',  # CNPJ formatado
            r'\d{3}\.\d{3}\.\d{3}-\d{2}',  # CPF formatado
            r'\d{14}',  # CNPJ sem formatação
            r'\d{11}',  # CPF sem formatação
        ]
        
        for pattern in cnpj_cpf_patterns:
            matches = re.findall(pattern, content)
            if matches:
                # Pegar o primeiro match válido
                match = matches[0]
                digits_only = re.sub(r'[^\d]', '', match)
                if len(digits_only) == 14:  # CNPJ
                    if '.' in match:
                        data['cnpj_cpf'] = match
                    else:
                        data['cnpj_cpf'] = f"{digits_only[:2]}.{digits_only[2:5]}.{digits_only[5:8]}/{digits_only[8:12]}-{digits_only[12:]}"
                elif len(digits_only) == 11:  # CPF
                    if '.' in match:
                        data['cnpj_cpf'] = match
                    else:
                        data['cnpj_cpf'] = f"{digits_only[:3]}.{digits_only[3:6]}.{digits_only[6:9]}-{digits_only[9:]}"
                if data['cnpj_cpf']:
                    break
        
        # Extrair valores - Múltiplos padrões
        valor_patterns = [
            r'R\$\s*([\d.,]+)',
            r'Valor[:\s]+R\$\s*([\d.,]+)',
            r'Total[:\s]+R\$\s*([\d.,]+)',
            r'R\$\s*([\d.,]+)',
        ]
        
        for pattern in valor_patterns:
            valores = re.findall(pattern, content, re.IGNORECASE)
            if valores:
                try:
                    # Pegar o maior valor encontrado (geralmente é o total)
                    valores_float = []
                    for v in valores:
                        try:
                            v_float = float(v.replace('.', '').replace(',', '.'))
                            valores_float.append(v_float)
                        except:
                            continue
                    if valores_float:
                        data['valor'] = max(valores_float)  # Pegar o maior valor
                        break
                except (ValueError, AttributeError):
                    continue
        
        # Extrair data - Múltiplos formatos
        data_patterns = [
            r'(\d{2}[/-]\d{2}[/-]\d{4})',  # DD/MM/YYYY ou DD-MM-YYYY
            r'(\d{4}[/-]\d{2}[/-]\d{2})',  # YYYY/MM/DD ou YYYY-MM-DD
            r'(\d{2}\.\d{2}\.\d{4})',  # DD.MM.YYYY
        ]
        
        for pattern in data_patterns:
            data_match = re.search(pattern, content)
            if data_match:
                data['data'] = data_match.group(1)
                break
        
        logger.debug(f"Extraídos dados genéricos: CNPJ/CPF={data['cnpj_cpf']}, Valor={data['valor']}, Data={data['data']}")
        return data
    
    def correlate_with_transactions(self, documents: List[Dict], transactions_df: pd.DataFrame) -> pd.DataFrame:
        """
        Correlaciona documentos fiscais com lançamentos financeiros
        
        Args:
            documents: Lista de documentos fiscais extraídos
            transactions_df: DataFrame com lançamentos financeiros
            
        Returns:
            DataFrame com correlações encontradas
        """
        correlations = []
        
        for doc in documents:
            doc_valor = doc.get('valor_total') or doc.get('valor_servico') or doc.get('valor', 0)
            doc_cnpj = doc.get('cnpj_emissor') or doc.get('cnpj_prestador') or doc.get('cnpj_cpf')
            doc_data = doc.get('data_emissao') or doc.get('data')
            
            # Buscar lançamentos correspondentes
            matches = transactions_df.copy()
            
            # Filtrar por valor (tolerância de 1%)
            if doc_valor > 0 and isinstance(matches, pd.DataFrame):
                tolerance = doc_valor * 0.01
                filtered = matches[
                    (matches['valor'].abs() >= doc_valor - tolerance) &
                    (matches['valor'].abs() <= doc_valor + tolerance)
                ]
                # Garantir que filtered continue sendo um DataFrame
                if isinstance(filtered, pd.DataFrame):
                    matches = filtered
                else:
                    matches = pd.DataFrame()
            
            # Filtrar por CNPJ/CPF se disponível
            if doc_cnpj and isinstance(matches, pd.DataFrame) and not matches.empty:
                # Normalizar CNPJ para comparação
                doc_cnpj_clean = re.sub(r'[^\d]', '', doc_cnpj)
                # Assumindo que transactions_df tem coluna 'fornecedor' ou similar
                if 'fornecedor' in matches.columns:
                    filtered = matches[matches['fornecedor'].astype(str).str.contains(doc_cnpj_clean, na=False, regex=False)]
                    if isinstance(filtered, pd.DataFrame):
                        matches = filtered
                    else:
                        matches = pd.DataFrame()
            
            # Adicionar correlações encontradas
            if isinstance(matches, pd.DataFrame) and not matches.empty:
                for idx, match in matches.iterrows():
                    correlations.append({
                        'document_id': doc.get('chave_acesso') or doc.get('numero_nf') or f"doc_{len(correlations)}",
                        'document_type': doc.get('document_type'),
                        'document_value': doc_valor,
                        'transaction_id': idx,
                        'transaction_value': match.get('valor', 0) if hasattr(match, 'get') else 0,
                        'transaction_date': match.get('data') if hasattr(match, 'get') else None,
                        'match_confidence': self._calculate_match_confidence(doc, match),
                        'status': 'matched'
                    })
        
        return pd.DataFrame(correlations)
    
    def _calculate_match_confidence(self, doc: Dict, transaction: pd.Series) -> float:
        """Calcula confiança na correlação entre documento e lançamento"""
        confidence = 0.0
        
        # Match de valor (peso 40%)
        doc_valor = doc.get('valor_total') or doc.get('valor_servico') or doc.get('valor', 0)
        trans_valor_raw = transaction.get('valor', 0)
        trans_valor = abs(trans_valor_raw) if trans_valor_raw is not None else 0
        if doc_valor > 0 and trans_valor > 0:
            diff = abs(doc_valor - trans_valor) / doc_valor
            confidence += (1 - min(diff, 1.0)) * 0.4
        
        # Match de data (peso 30%)
        doc_data = doc.get('data_emissao') or doc.get('data')
        trans_data = transaction.get('data')
        if doc_data and trans_data:
            try:
                if isinstance(doc_data, str):
                    doc_date = pd.to_datetime(doc_data)
                else:
                    doc_date = doc_data
                if isinstance(trans_data, str):
                    trans_date = pd.to_datetime(trans_data)
                else:
                    trans_date = trans_data
                
                days_diff = abs((doc_date - trans_date).days)
                confidence += max(0, 1 - days_diff / 30) * 0.3  # 30 dias de tolerância
            except:
                pass
        
        # Match de CNPJ/CPF (peso 30%)
        doc_cnpj = doc.get('cnpj_emissor') or doc.get('cnpj_prestador') or doc.get('cnpj_cpf')
        if doc_cnpj and 'fornecedor' in transaction:
            doc_cnpj_clean = re.sub(r'[^\d]', '', str(doc_cnpj))
            trans_fornecedor = str(transaction.get('fornecedor', ''))
            if doc_cnpj_clean in trans_fornecedor:
                confidence += 0.3
        
        return min(confidence, 1.0)
    
    def analyze_taxes(self, documents: List[Dict], transactions_df: pd.DataFrame) -> Dict[str, Any]:
        """
        Analisa impostos dos documentos e compara com lançamentos
        
        Args:
            documents: Lista de documentos fiscais
            transactions_df: DataFrame com lançamentos
            
        Returns:
            Dict com análise de impostos
        """
        tax_analysis = {
            'total_icms': 0.0,
            'total_iss': 0.0,
            'total_ipi': 0.0,
            'total_impostos': 0.0,
            'tax_by_document': [],
            'discrepancies': []
        }
        
        logger.info(f"Analisando impostos de {len(documents)} documentos")
        
        for doc in documents:
            # CORREÇÃO: Tratar valores None explicitamente
            # Se o campo existe mas é None, usar 0.0
            valor_icms = doc.get('valor_icms')
            valor_iss = doc.get('valor_iss')
            valor_ipi = doc.get('valor_ipi')
            
            # Converter None para 0.0
            icms = float(valor_icms) if valor_icms is not None else 0.0
            iss = float(valor_iss) if valor_iss is not None else 0.0
            ipi = float(valor_ipi) if valor_ipi is not None else 0.0
            
            doc_taxes = {
                'document_id': doc.get('chave_acesso') or doc.get('numero_nf') or doc.get('numero_nfse'),
                'icms': icms,
                'iss': iss,
                'ipi': ipi
            }
            
            # Log para debug
            if icms > 0 or iss > 0 or ipi > 0:
                logger.info(f"Documento {doc_taxes['document_id']}: ICMS={icms}, ISS={iss}, IPI={ipi}")
            else:
                logger.warning(f"Documento {doc_taxes['document_id']}: Nenhum imposto extraído. Tipo: {doc.get('document_type')}, Campos disponíveis: {list(doc.keys())}")
            
            tax_analysis['total_icms'] += icms
            tax_analysis['total_iss'] += iss
            tax_analysis['total_ipi'] += ipi
            tax_analysis['tax_by_document'].append(doc_taxes)
        
        tax_analysis['total_impostos'] = (
            tax_analysis['total_icms'] +
            tax_analysis['total_iss'] +
            tax_analysis['total_ipi']
        )
        
        logger.info(f"Análise de impostos concluída: Total ICMS={tax_analysis['total_icms']}, Total ISS={tax_analysis['total_iss']}, Total IPI={tax_analysis['total_ipi']}")
        
        return tax_analysis

