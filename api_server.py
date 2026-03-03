from fastapi import FastAPI, File, UploadFile, HTTPException, Request, Form, Query, BackgroundTasks, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse
import os
import logging
import time
import io
import uuid
import shutil
from pathlib import Path
import calendar
from datetime import datetime, date
from typing import Dict, Any, Optional, List
import uvicorn
import pandas as pd
from app.audit import AdvancedAuditSystem
from app.core import ConfigManager
from app.extraction.legacy import load_document, load_document_from_bytes, dataframe_to_text_br, extract_hyperlinks_from_excel, extract_hyperlinks_from_ods, extract_folha_value_from_text
from app.reporting.report_generator import generate_report_pdf
import psutil
import json
import ipaddress
import re

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('api.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Inicializar FastAPI
app = FastAPI(
    title="IA de Auditoria de Condomínios",
    description="API para análise de dados financeiros com IA",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Configurar CORS - Apenas localhost
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost", "http://localhost:8000", "http://127.0.0.1", "http://127.0.0.1:8000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Inicializar sistema de IA
ai_system = AdvancedAuditSystem()
config = ConfigManager()

# IPs permitidos (apenas localhost)
ALLOWED_IPS = [
    "127.0.0.1",
    "localhost",
    "::1",  # IPv6 localhost
    "0.0.0.0"  # Para desenvolvimento
]

def is_local_ip(client_host: Optional[str]) -> bool:
    """Verifica se o IP do cliente é localhost"""
    if client_host is None:
        return False
    
    try:
        # Verificar se é localhost por nome
        if client_host in ["localhost", "127.0.0.1", "::1"]:
            return True
        
        # Verificar se é IP local
        ip = ipaddress.ip_address(client_host)
        return ip.is_loopback or ip.is_private or str(ip) in ALLOWED_IPS
    except (ValueError, AttributeError):
        # Se não conseguir parsear, verificar por string
        return client_host in ALLOWED_IPS or "localhost" in client_host.lower()

# Métricas globais
api_metrics = {
    "total_requests": 0,
    "successful_analyses": 0,
    "failed_analyses": 0,
    "total_processing_time": 0,
    "start_time": datetime.now()
}

# Sistema de Status de Análises
# Stub para quando o status_manager não estiver disponível
class StatusManagerStub:
    """Stub do StatusManager quando não disponível"""
    def create_job(self, *args, **kwargs): return str(uuid.uuid4())
    def start_job(self, *args, **kwargs): return False
    def update_progress(self, *args, **kwargs): return False
    def complete_job(self, *args, **kwargs): return False
    def fail_job(self, *args, **kwargs): return False
    def get_job_status(self, *args, **kwargs): return None
    def list_jobs(self, *args, **kwargs): return []
    def get_statistics(self, *args, **kwargs): return {}

try:
    from app.services.analysis_status import AnalysisStatusManager
    status_manager = AnalysisStatusManager(max_jobs=1000)
    STATUS_MANAGER_AVAILABLE = True
    logger.info("AnalysisStatusManager inicializado com sucesso")
except ImportError as e:
    STATUS_MANAGER_AVAILABLE = False
    status_manager = StatusManagerStub()
    logger.warning(f"AnalysisStatusManager nao disponivel. Status de analises desabilitado. Erro: {e}")
except Exception as e:
    STATUS_MANAGER_AVAILABLE = False
    status_manager = StatusManagerStub()
    logger.error(f"Erro ao inicializar AnalysisStatusManager: {e}", exc_info=True)

# Gerenciador de conexões WebSocket para atualização em tempo real
class WebSocketManager:
    """Gerencia conexões WebSocket para broadcast de atualizações de status"""
    def __init__(self):
        self.active_connections: Dict[str, List[WebSocket]] = {}  # job_id -> [WebSocket]
        self.all_connections: List[WebSocket] = []  # Todas as conexões ativas
    
    async def connect(self, websocket: WebSocket, job_id: Optional[str] = None):
        """Conecta um WebSocket, opcionalmente associado a um job_id"""
        await websocket.accept()
        self.all_connections.append(websocket)
        
        if job_id:
            if job_id not in self.active_connections:
                self.active_connections[job_id] = []
            self.active_connections[job_id].append(websocket)
            logger.info(f"WebSocket conectado para job {job_id}")
        else:
            logger.info("WebSocket conectado (monitoramento geral)")
    
    def disconnect(self, websocket: WebSocket, job_id: Optional[str] = None):
        """Desconecta um WebSocket"""
        if websocket in self.all_connections:
            self.all_connections.remove(websocket)
        
        if job_id and job_id in self.active_connections:
            if websocket in self.active_connections[job_id]:
                self.active_connections[job_id].remove(websocket)
            if not self.active_connections[job_id]:
                del self.active_connections[job_id]
            logger.info(f"WebSocket desconectado do job {job_id}")
    
    async def broadcast_to_job(self, job_id: str, message: Dict[str, Any]):
        """Envia mensagem para todos os WebSockets conectados a um job específico"""
        if job_id in self.active_connections:
            disconnected = []
            for connection in self.active_connections[job_id]:
                try:
                    await connection.send_json(message)
                except Exception as e:
                    logger.warning(f"Erro ao enviar mensagem WebSocket: {e}")
                    disconnected.append(connection)
            
            # Remover conexões desconectadas
            for conn in disconnected:
                self.disconnect(conn, job_id)
    
    async def broadcast_to_all(self, message: Dict[str, Any]):
        """Envia mensagem para todos os WebSockets conectados"""
        disconnected = []
        for connection in self.all_connections:
            try:
                await connection.send_json(message)
            except Exception as e:
                logger.warning(f"Erro ao enviar mensagem WebSocket: {e}")
                disconnected.append(connection)
        
        # Remover conexões desconectadas
        for conn in disconnected:
            self.disconnect(conn)

# Inicializar gerenciador WebSocket
websocket_manager = WebSocketManager()

# Sistema de notificações (webhooks)
class NotificationManager:
    """Gerencia notificações via webhook quando análises completam"""
    def __init__(self):
        self.webhooks: Dict[str, str] = {}  # job_id -> callback_url
    
    def register_webhook(self, job_id: str, callback_url: str):
        """Registra um webhook para um job"""
        self.webhooks[job_id] = callback_url
        logger.info(f"Webhook registrado para job {job_id}: {callback_url}")
    
    async def notify_completion(self, job_id: str, status: Dict[str, Any]):
        """Notifica via webhook quando um job completa"""
        if job_id in self.webhooks:
            callback_url = self.webhooks[job_id]
            try:
                import aiohttp
                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        callback_url,
                        json={
                            "job_id": job_id,
                            "status": status.get("status"),
                            "result": status.get("result"),
                            "completed_at": status.get("completed_at"),
                            "message": status.get("message")
                        },
                        timeout=aiohttp.ClientTimeout(total=10)
                    ) as response:
                        if response.status == 200:
                            logger.info(f"Webhook notificado com sucesso para job {job_id}")
                        else:
                            logger.warning(f"Webhook retornou status {response.status} para job {job_id}")
            except Exception as e:
                logger.error(f"Erro ao notificar webhook para job {job_id}: {e}")
            finally:
                # Remover webhook após notificação
                del self.webhooks[job_id]

# Inicializar gerenciador de notificações
notification_manager = NotificationManager()

# Middleware de segurança - Bloquear acesso não local
@app.middleware("http")
async def check_local_access(request: Request, call_next):
    """Middleware para bloquear acesso apenas para localhost"""
    client_host = request.client.host if request.client else None
    
    # Permitir acesso a endpoints públicos (health, docs)
    public_paths = ["/", "/health", "/docs", "/redoc", "/openapi.json"]
    if request.url.path in public_paths:
        response = await call_next(request)
        return response
    
    # Verificar se é acesso local
    if client_host is None or not is_local_ip(client_host):
        logger.warning(f"Acesso bloqueado de IP não local: {client_host}")
        return JSONResponse(
            status_code=403,
            content={
                "error": "Forbidden",
                "message": "Acesso permitido apenas de localhost",
                "client_ip": client_host or "unknown"
            }
        )
    
    # Continuar com a requisição
    response = await call_next(request)
    return response

# Middleware de logging e métricas
@app.middleware("http")
async def log_requests(request: Request, call_next):
    start_time = time.time()
    api_metrics["total_requests"] += 1
    
    response = await call_next(request)
    
    process_time = time.time() - start_time
    api_metrics["total_processing_time"] += process_time
    
    client_ip = request.client.host if request.client else "unknown"
    logger.info(f"{request.method} {request.url} - {response.status_code} - {process_time:.3f}s - IP: {client_ip}")
    
    return response

def _extract_text_from_file(file_content: bytes, file_extension: str, filename: str = "") -> str:
    """
    Extrai texto de arquivos binários (PDF, ODS, etc.) baseado na extensão
    
    Args:
        file_content: Conteúdo binário do arquivo
        file_extension: Extensão do arquivo (ex: '.pdf', '.ods', '.xml')
        filename: Nome do arquivo (para logs)
        
    Returns:
        String com texto extraído do arquivo
    """
    try:
        file_extension = file_extension.lower()
        
        # FASE 1: PDF - Usar pdfplumber com OCR automático para PDFs escaneados
        if file_extension == '.pdf':
            import pdfplumber
            text_parts = []
            total_pages = 0
            
            with pdfplumber.open(io.BytesIO(file_content)) as pdf:
                total_pages = len(pdf.pages)
                logger.info(f"PDF {filename} tem {total_pages} página(s)")
                
                # Tentar extrair texto de todas as páginas
                for page_num, page in enumerate(pdf.pages, 1):
                    page_text = page.extract_text()
                    if page_text and len(page_text.strip()) > 10:  # Ignorar textos muito pequenos
                        text_parts.append(page_text)
                    logger.debug(f"Extraído texto da página {page_num}/{total_pages} do PDF {filename}")
            
            # Verificar se extraiu texto suficiente
            full_text = "\n".join(text_parts) if text_parts else ""
            text_length = len(full_text.strip())
            
            # Detectar PDF escaneado: se extraiu muito pouco texto (< 100 caracteres por página em média)
            # OU se não extraiu nada, usar OCR automaticamente
            chars_per_page = text_length / total_pages if total_pages > 0 else 0
            is_scanned = text_length < 100 or (total_pages > 0 and chars_per_page < 50)
            
            if is_scanned or text_length == 0:
                logger.info(f"PDF {filename} detectado como escaneado (texto extraído: {text_length} chars, média: {chars_per_page:.1f} chars/página). Usando OCR...")
                try:
                    import pytesseract
                    from PIL import Image
                    t_cmd = os.getenv("TESSERACT_CMD")
                    if t_cmd:
                        pytesseract.pytesseract.tesseract_cmd = t_cmd
                    
                    ocr_parts = []
                    # Configurações otimizadas do Tesseract para português (com fallback)
                    tesseract_configs = [
                        '--oem 3 --psm 6 -l por',  # Tentar primeiro com português
                        '--oem 3 --psm 6',  # Fallback sem idioma específico
                    ]
                    
                    with pdfplumber.open(io.BytesIO(file_content)) as pdf:
                        # Processar TODAS as páginas, não apenas 30
                        config_used = None
                        for page_num, page in enumerate(pdf.pages, 1):
                            try:
                                # Aumentar resolução para melhor qualidade (300 DPI)
                                img = page.to_image(resolution=300).original
                                if isinstance(img, Image.Image):
                                    # Pré-processar imagem para melhorar OCR (converter para escala de cinza se necessário)
                                    if img.mode != 'RGB':
                                        img = img.convert('RGB')
                                    
                                    # Tentar com diferentes configurações
                                    ocr_text = None
                                    for tesseract_config in tesseract_configs:
                                        try:
                                            ocr_text = pytesseract.image_to_string(img, config=tesseract_config)
                                            if config_used is None:
                                                config_used = tesseract_config
                                            if ocr_text and len(ocr_text.strip()) > 10:
                                                break  # Sucesso, usar esta configuração
                                        except Exception:
                                            continue  # Tentar próxima configuração
                                    
                                    if ocr_text and len(ocr_text.strip()) > 10:
                                        ocr_parts.append(ocr_text)
                                        logger.debug(f"OCR página {page_num}/{total_pages}: {len(ocr_text)} caracteres")
                            except Exception as e:
                                logger.warning(f"Erro ao processar página {page_num} com OCR: {e}")
                                continue
                        
                        if config_used:
                            logger.debug(f"OCR usando configuração: {config_used}")
                    
                    if ocr_parts:
                        ocr_text = "\n".join(ocr_parts)
                        logger.info(f"OCR extraído {len(ocr_text)} caracteres do PDF {filename} ({len(ocr_parts)} páginas processadas)")
                        return ocr_text
                    else:
                        logger.warning(f"OCR não conseguiu extrair texto do PDF {filename}")
                        return full_text  # Retornar texto mínimo se houver
                except ImportError:
                    logger.warning(f"pytesseract não instalado. Instale com: pip install pytesseract pillow")
                    return full_text
                except Exception as e:
                    logger.warning(f"OCR não disponível ou falhou para {filename}: {e}")
                    return full_text  # Retornar texto mínimo se houver
            
            # PDF digital normal - retornar texto extraído
            if full_text:
                logger.info(f"Extraído {text_length} caracteres do PDF digital {filename} ({len(text_parts)} páginas com texto)")
                return full_text
            
            # Fallback: tentar OCR mesmo assim se texto muito pequeno
            logger.warning(f"Texto extraído muito pequeno ({text_length} chars). Tentando OCR como fallback...")
            try:
                import pytesseract
                from PIL import Image
                t_cmd = os.getenv("TESSERACT_CMD")
                if t_cmd:
                    pytesseract.pytesseract.tesseract_cmd = t_cmd
                
                ocr_parts = []
                tesseract_configs = [
                    '--oem 3 --psm 6 -l por',  # Tentar primeiro com português
                    '--oem 3 --psm 6',  # Fallback sem idioma específico
                ]
                
                with pdfplumber.open(io.BytesIO(file_content)) as pdf:
                    for page_num, page in enumerate(pdf.pages, 1):
                        try:
                            img = page.to_image(resolution=300).original
                            if isinstance(img, Image.Image):
                                if img.mode != 'RGB':
                                    img = img.convert('RGB')
                                
                                # Tentar com diferentes configurações
                                ocr_text = None
                                for tesseract_config in tesseract_configs:
                                    try:
                                        ocr_text = pytesseract.image_to_string(img, config=tesseract_config)
                                        if ocr_text and len(ocr_text.strip()) > 10:
                                            break  # Sucesso
                                    except Exception:
                                        continue  # Tentar próxima configuração
                                
                                if ocr_text and len(ocr_text.strip()) > 10:
                                    ocr_parts.append(ocr_text)
                        except Exception:
                            continue
                
                if ocr_parts:
                    ocr_text = "\n".join(ocr_parts)
                    logger.info(f"OCR fallback extraiu {len(ocr_text)} caracteres do PDF {filename}")
                    return ocr_text
            except Exception as e:
                logger.debug(f"OCR fallback falhou: {e}")
            
            return full_text if full_text else ""
        
        # FASE 2: ODS - Usar pandas com engine odf
        elif file_extension == '.ods':
            try:
                # Tentar com pandas usando engine odf
                df = pd.read_excel(io.BytesIO(file_content), engine='odf')
                # Converter DataFrame para texto estruturado (melhor formato para regex)
                text_lines = []
                # Adicionar cabeçalhos com separador claro
                if not df.empty:
                    header_line = " | ".join(str(col) for col in df.columns)
                    text_lines.append(header_line)
                    text_lines.append("-" * len(header_line))  # Separador visual
                    
                    # Adicionar linhas (aumentar limite para capturar mais dados)
                    max_rows = min(2000, len(df))  # Até 2000 linhas
                    for idx, row in df.head(max_rows).iterrows():
                        row_values = []
                        for val in row.values:
                            if pd.notna(val):
                                # Formatar valores monetários preservando formato BR
                                if isinstance(val, (int, float)):
                                    # Tentar preservar como número se for monetário
                                    val_str = f"{val:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                                else:
                                    val_str = str(val)
                                row_values.append(val_str)
                            else:
                                row_values.append("")
                        text_lines.append(" | ".join(row_values))
                    
                    # Adicionar informações sobre estrutura (útil para regex)
                    text_lines.append(f"\n[ESTRUTURA] Total de linhas: {len(df)}, Colunas: {', '.join(str(c) for c in df.columns[:10])}")
                
                full_text = "\n".join(text_lines)
                logger.info(f"Extraído {len(full_text)} caracteres do ODS {filename} ({len(df)} linhas, {len(df.columns)} colunas)")
                return full_text
            except Exception as e:
                logger.warning(f"Erro ao extrair ODS com pandas: {e}. Tentando como texto...")
                # Fallback: tentar como texto
                try:
                    return file_content.decode('utf-8', errors='ignore')
                except:
                    return ""
        
        # XML - Já é texto, apenas decodificar
        elif file_extension == '.xml':
            try:
                # Tentar UTF-8 primeiro
                return file_content.decode('utf-8', errors='ignore')
            except:
                # Fallback para latin-1
                return file_content.decode('latin-1', errors='ignore')
        
        # CSV, TXT - Decodificar como texto
        elif file_extension in ['.csv', '.txt']:
            try:
                return file_content.decode('utf-8', errors='ignore')
            except:
                return file_content.decode('latin-1', errors='ignore')
        
        # Outros formatos - Tentar decodificar como texto
        else:
            logger.warning(f"Formato não reconhecido: {file_extension}. Tentando como texto...")
            try:
                return file_content.decode('utf-8', errors='ignore')
            except:
                return file_content.decode('latin-1', errors='ignore')
                
    except Exception as e:
        logger.error(f"Erro ao extrair texto do arquivo {filename}: {e}")
        return ""


def _df_to_text_snippet(df: pd.DataFrame, max_rows: int = 200, max_chars: int = 12000) -> str:
    if df is None or df.empty:
        return ""
    try:
        text = df.head(max_rows).to_string(index=False)
    except Exception:
        try:
            text = str(df.head(max_rows))
        except Exception:
            return ""
    return text[:max_chars]


def _get_text_for_llm(file_path: str, filename: str, file_extension: str) -> Optional[str]:
    """Obtém texto de um arquivo para extração via LLM. Não usa clean_data (apenas loader + serialização)."""
    file_extension = file_extension.lower()
    try:
        if file_extension == ".xml":
            with open(file_path, "rb") as f:
                content = f.read()
            return _extract_text_from_file(content, file_extension, filename) or None
        if file_extension not in (".csv", ".xlsx", ".xls", ".xlt", ".sxc", ".pdf", ".ods"):
            return None
        result = load_document(file_path)
        df = result[0] if isinstance(result, tuple) else result
        if df is None or df.empty:
            return None
        if file_extension == ".pdf" and isinstance(result, tuple) and len(result) >= 2 and result[1]:
            return result[1] or None
        return dataframe_to_text_br(df) or _df_to_text_snippet(df, max_chars=150000) or None
    except Exception as e:
        logger.debug("_get_text_for_llm %s: %s", filename, e)
        return None


# Diretório temporário para arquivos uploadados (processamento assíncrono)
TEMP_UPLOADS_DIR = Path("temp_uploads")
TEMP_UPLOADS_DIR.mkdir(exist_ok=True)

# Limite de jobs simultâneos
MAX_CONCURRENT_JOBS = 5
active_jobs_count = 0


def _normalize_file_paths_for_categorizer(file_paths: Optional[str]) -> Optional[List[str]]:
    """Ignora placeholders (ex.: 'string' do Swagger) e retorna lista de caminhos ou None."""
    if file_paths is None:
        return None
    s = str(file_paths).strip()
    if s == "" or s.lower() == "string":
        return None
    parts = [p.strip() for p in file_paths.split(",") if p.strip() and p.strip().lower() != "string"]
    return parts if parts else None


# Funções auxiliares para processamento assíncrono
async def save_uploaded_files(files: List[UploadFile]) -> List[Dict[str, Any]]:
    """
    Salva arquivos uploadados em diretório temporário para processamento em background
    
    Returns:
        Lista de dicionários com informações dos arquivos salvos
    """
    saved_files = []
    for file in files:
        if not file.filename:
            continue
        
        try:
            file_content = await file.read()
            file_id = str(uuid.uuid4())
            temp_path = TEMP_UPLOADS_DIR / f"{file_id}_{file.filename}"
            temp_path.write_bytes(file_content)
            
            saved_files.append({
                "temp_path": str(temp_path),
                "filename": file.filename,
                "file_size": len(file_content),
                "file_id": file_id
            })
            logger.debug(f"Arquivo salvo temporariamente: {temp_path}")
        except Exception as e:
            logger.error(f"Erro ao salvar arquivo {file.filename}: {e}")
    
    return saved_files

def cleanup_temp_files(file_paths: List[str]):
    """Remove arquivos temporários após processamento"""
    for path_str in file_paths:
        try:
            path = Path(path_str)
            if path.exists():
                path.unlink()
                logger.debug(f"Arquivo temporário removido: {path}")
        except Exception as e:
            logger.warning(f"Erro ao remover arquivo temporário {path_str}: {e}")

# Função helper para serializar datetime (compartilhada entre endpoints)
def serialize_datetime(obj):
    """
    Converte objetos datetime, timedelta, NumPy types, Pandas types e outros tipos 
    não serializáveis para tipos JSON válidos.
    
    Esta função é recursiva e trata:
    - datetime, date, timedelta
    - pd.Timestamp, pd.NA, pd.NaT
    - Tipos NumPy (int64, float64, bool_, etc.)
    - Arrays NumPy
    - Series e DataFrames do Pandas
    - Estruturas aninhadas (dict, list, tuple, set)
    """
    from datetime import timedelta
    
    # Tipos None - retornar direto
    if obj is None:
        return None
    
    # Tipos datetime
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    elif isinstance(obj, timedelta):
        return obj.total_seconds()
    elif isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    
    # Valores especiais do Pandas
    try:
        if pd.isna(obj) or obj is pd.NA or obj is pd.NaT:
            return None
    except (TypeError, ValueError):
        pass  # Se pd.isna falhar, continuar
    
    # Tipos NumPy - detectar pelo nome da classe (compatível com type checkers)
    if hasattr(obj, '__class__'):
        class_name = obj.__class__.__name__
        class_module = getattr(obj.__class__, '__module__', '')
        
        # Tipos NumPy inteiros
        if class_module == 'numpy' and class_name in ('int8', 'int16', 'int32', 'int64', 'int_', 'intc', 'intp',
                                                       'uint8', 'uint16', 'uint32', 'uint64', 'integer'):
            try:
                return int(obj)
            except (ValueError, OverflowError):
                return str(obj)
        
        # Tipos NumPy float
        elif class_module == 'numpy' and class_name in ('float16', 'float32', 'float64', 'float_', 'floating'):
            try:
                val = float(obj)
                # Verificar se é NaN ou Inf
                if pd.isna(val) or not (val == val):  # NaN check
                    return None
                return val
            except (ValueError, OverflowError):
                return str(obj)
        
        # Tipo NumPy bool
        elif class_module == 'numpy' and class_name == 'bool_':
            return bool(obj)
        
        # Arrays NumPy
        elif class_module == 'numpy' and class_name == 'ndarray':
            try:
                return serialize_datetime(obj.tolist())
            except (AttributeError, ValueError):
                return [serialize_datetime(item) for item in obj]
    
    # Estruturas de dados
    if isinstance(obj, dict):
        return {str(k): serialize_datetime(v) for k, v in obj.items()}
    elif isinstance(obj, (list, tuple)):
        return [serialize_datetime(item) for item in obj]
    elif isinstance(obj, set):
        return [serialize_datetime(item) for item in sorted(obj, key=str)]
    elif isinstance(obj, pd.Series):
        try:
            return serialize_datetime(obj.to_dict())
        except (AttributeError, ValueError):
            return [serialize_datetime(item) for item in obj]
    elif isinstance(obj, pd.DataFrame):
        try:
            return serialize_datetime(obj.to_dict('records'))
        except (AttributeError, ValueError):
            return []
    
    # Tipos nativos Python que são JSON serializáveis
    if isinstance(obj, (str, int, float, bool)):
        return obj
    
    # Fallback: tentar converter tipos que implementam métodos de conversão
    try:
        # Tentar converter para int
        if hasattr(obj, '__int__') and not isinstance(obj, (str, bytes)):
            return int(obj)
        # Tentar converter para float
        elif hasattr(obj, '__float__') and not isinstance(obj, (str, bytes)):
            val = float(obj)
            if pd.isna(val) or not (val == val):
                return None
            return val
        # Tentar converter para string
        else:
            return str(obj)
    except (TypeError, ValueError, OverflowError) as e:
        logger.warning(f"Tipo não serializável encontrado: {type(obj).__name__}, valor: {obj}, erro: {e}")
        return None

# Função helper para serializar job_status antes de usar em JSON/WebSocket
def serialize_job_status(job_status: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Serializa job_status removendo objetos não serializáveis"""
    if job_status is None:
        return None
    serialized = serialize_datetime(job_status)
    # Garantir que retorna um dict (não str, float, etc.)
    if isinstance(serialized, dict):
        return serialized
    # Se não for dict, criar um wrapper
    return {"data": serialized} if serialized is not None else None

# Endpoints da API
@app.get("/")
async def root():
    """Endpoint raiz da API"""
    return {
        "message": "IA de Auditoria de Condomínios API",
        "version": "1.0.0",
        "status": "online",
        "timestamp": datetime.now().isoformat(),
        "uptime": str(datetime.now() - api_metrics["start_time"])
    }

@app.get("/health")
async def health_check():
    """Health check da API"""
    try:
        # Verificar status do sistema de IA
        ai_status = ai_system.get_system_info()
        
        # Verificar recursos do sistema
        cpu_percent = psutil.cpu_percent()
        memory_percent = psutil.virtual_memory().percent
        disk_percent = psutil.disk_usage('/').percent
        
        health_status = "healthy"
        if cpu_percent > 90 or memory_percent > 90 or disk_percent > 90:
            health_status = "degraded"
        
        return {
            "status": health_status,
            "ai_module": ai_status,
            "system_resources": {
                "cpu_percent": cpu_percent,
                "memory_percent": memory_percent,
                "disk_percent": disk_percent
            },
            "timestamp": datetime.now().isoformat(),
            "uptime": str(datetime.now() - api_metrics["start_time"])
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=503, detail="Service unavailable")

async def process_analysis_async(
    job_id: str,
    saved_files: Optional[List[Dict[str, Any]]],
    file_paths: Optional[str],
    client_id: Optional[str],
    categorized_files: Optional[Dict] = None
):
    """
    Processa análise financeira em background
    
    Args:
        job_id: ID do job de análise
        saved_files: Lista de arquivos salvos temporariamente (se upload)
        file_paths: Caminhos de arquivos físicos (se não upload)
        client_id: ID do cliente
        categorized_files: Arquivos categorizados (opcional)
    """
    global active_jobs_count
    active_jobs_count += 1
    
    temp_file_paths = []
    
    try:
        if STATUS_MANAGER_AVAILABLE:
            status_manager.start_job(job_id)
            status_manager.update_progress(job_id, 0.1, "Carregando arquivos...")
        
        all_dataframes = []
        file_metadata = []
        document_texts = []
        combined_df = None
        document_context = None
        files_summary = []

        try:
            from app.extraction.llm import is_llm_available
        except ImportError:
            def is_llm_available():
                return False
        if not is_llm_available():
            raise HTTPException(
                status_code=503,
                detail="Extração requer LLM. Configure LLM_BASE_URL ou OPENAI_API_KEY e garanta que o serviço esteja disponível.",
            )

        logger.info("Extração via LLM (único fluxo)")
        document_texts = []
        file_metadata = []
        if saved_files:
            for file_info in saved_files:
                temp_path = file_info.get("temp_path")
                filename = file_info.get("filename", "")
                if not temp_path or not os.path.exists(temp_path):
                    continue
                ext = os.path.splitext(filename)[1].lower()
                if ext not in (".csv", ".xlsx", ".xls", ".xlt", ".sxc", ".pdf", ".ods", ".xml"):
                    continue
                text = _get_text_for_llm(temp_path, filename, ext)
                if text:
                    document_texts.append({"filename": filename, "text": text})
                file_metadata.append({"filename": filename, "file_size": file_info.get("file_size") or os.path.getsize(temp_path), "rows": 0})
        elif file_paths:
            paths_list = [p.strip() for p in file_paths.split(",")]
            for path in paths_list:
                if not os.path.exists(path):
                    continue
                filename = os.path.basename(path)
                ext = os.path.splitext(path)[1].lower()
                if ext not in (".csv", ".xlsx", ".xls", ".xlt", ".sxc", ".pdf", ".ods", ".xml"):
                    continue
                text = _get_text_for_llm(path, filename, ext)
                if text:
                    document_texts.append({"filename": filename, "text": text})
                file_metadata.append({"filename": filename, "file_path": path, "file_size": os.path.getsize(path), "rows": 0})
        if not document_texts:
            raise ValueError("Nenhum arquivo pôde ser lido para extração LLM. Verifique formatos e conteúdo.")
        from app.extraction.llm import extract_document_llm as llm_extract, build_dataframe_and_context
        extraction = llm_extract(document_texts)
        combined_df, document_context = build_dataframe_and_context(extraction, document_texts)
        if combined_df is None or (isinstance(combined_df, pd.DataFrame) and combined_df.empty):
            # Permitir fluxo quando a LLM retornou apenas contas/totais (sem transações)
            has_structural = bool(document_context.get("structural_extraction")) if isinstance(document_context, dict) else False
            totals = (document_context.get("totals_extracted") or {}) if isinstance(document_context, dict) else {}
            values = totals.get("values", totals) if isinstance(totals, dict) else {}
            has_saldo_final = isinstance(values, dict) and values.get("saldo_final") is not None
            if not (has_structural or has_saldo_final):
                err_detail = ""
                if extraction.get("errors"):
                    err_detail = " " + (extraction["errors"][0] if extraction["errors"] else "")
                raise ValueError(
                    "Extração LLM não retornou dados. Verifique se a LLM está acessível (LLM_BASE_URL ou OPENAI_API_KEY) e se os arquivos contêm texto legível."
                    + err_detail
                )
            logger.warning("Extração LLM retornou sem transações; prosseguindo com contas/totais estruturais.")
        document_context["file_metadata"] = file_metadata
        document_context["original_dataframes"] = [{"dataframe": combined_df, "filename": "consolidado"}]
        try:
            from app.audit.financial_consolidator import calculate_financial_totals_correct
            ft = calculate_financial_totals_correct(
                combined_df,
                saldo_inicial=document_context.get("saldo_anterior"),
                extracted_totals=document_context.get("totals_extracted"),
            )
            document_context["files_summary"] = [{"source_file": "consolidado", "rows": len(combined_df), "total_receitas": ft["total_receitas"], "total_despesas": ft["total_despesas"], "saldo": ft["saldo"]}]
        except Exception:
            document_context["files_summary"] = []
        files_summary = document_context.get("files_summary") or []
        all_dataframes = [combined_df]

        
        if not all_dataframes:
            error_details = []
            if saved_files:
                error_details.append(f"{len(saved_files)} arquivo(s) salvo(s) mas nenhum pôde ser carregado")
                for file_info in saved_files:
                    temp_path = file_info["temp_path"]
                    filename = file_info["filename"]
                    exists = os.path.exists(temp_path)
                    size = os.path.getsize(temp_path) if exists else 0
                    error_details.append(f"  - {filename}: path={temp_path}, exists={exists}, size={size} bytes")
            elif file_paths:
                error_details.append(f"Caminhos fornecidos: {file_paths}")
                paths_list = [p.strip() for p in file_paths.split(',')]
                for path in paths_list:
                    exists = os.path.exists(path)
                    size = os.path.getsize(path) if exists else 0
                    extension = os.path.splitext(path)[1].lower()
                    error_details.append(f"  - {path}: exists={exists}, size={size} bytes ({size / 1024 / 1024:.2f} MB), extension={extension}")
            else:
                error_details.append("Nenhum arquivo fornecido (saved_files=None e file_paths=None)")
            
            error_msg = "No valid files could be loaded. " + " | ".join(error_details)
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        # Base da folha do mês anterior (X-1): para calcular mês X usar remuneração + 13º do documento X-1
        try:
            from app.audit.labor_analyzer import compute_base_remuneracao_mais_13
            period_list = []
            for idx, fm in enumerate(file_metadata):
                if not isinstance(fm, dict):
                    continue
                ps = fm.get("period_start")
                if not ps or idx >= len(all_dataframes) or all_dataframes[idx] is None or all_dataframes[idx].empty:
                    continue
                period_list.append((ps, idx))
            if len(period_list) >= 1:
                period_list.sort(key=lambda x: x[0] or "")
                period_main = period_list[-1][0]  # mês principal X (mais recente)
                try:
                    y, m = int(period_main[:4]), int(period_main[5:7])
                    if m == 1:
                        y_prev, m_prev = y - 1, 12
                    else:
                        y_prev, m_prev = y, m - 1
                    period_prev = f"{y_prev:04d}-{m_prev:02d}"
                    period_main_ym = f"{y:04d}-{m:02d}"
                    idx_prev = None
                    for ps, idx in period_list:
                        if ps and ps.startswith(period_prev):
                            idx_prev = idx
                            break
                    if idx_prev is None:
                        logger.info(f"Documento do mês anterior ({period_prev}) não encontrado. Base da folha do mês {period_main_ym} usará folha do próprio documento.")
                    elif idx_prev < len(all_dataframes):
                        df_mes_anterior = all_dataframes[idx_prev]
                        if df_mes_anterior is not None and not df_mes_anterior.empty:
                            base_folha = compute_base_remuneracao_mais_13(df_mes_anterior)
                            if base_folha and base_folha > 0:
                                document_context["base_folha_mes_anterior"] = base_folha
                                document_context["period_mes_anterior"] = period_prev
                                document_context["period_mes_principal"] = period_main_ym
                                logger.info(f"Base da folha do mês anterior ({period_prev}): R$ {base_folha:,.2f} (para cálculos do mês {period_main_ym})")
                            else:
                                # Fallback: extrair do texto do documento (ex.: PDF "folha de dezembro" com estrutura diferente)
                                filename_prev = file_metadata[idx_prev].get("filename", "") if idx_prev < len(file_metadata) else ""
                                base_from_text = None
                                if document_texts and filename_prev:
                                    for dt in document_texts:
                                        if isinstance(dt, dict) and dt.get("filename") == filename_prev and dt.get("text"):
                                            base_from_text = extract_folha_value_from_text(dt["text"])
                                            if base_from_text and base_from_text > 0:
                                                break
                                if base_from_text and base_from_text > 0:
                                    document_context["base_folha_mes_anterior"] = base_from_text
                                    document_context["period_mes_anterior"] = period_prev
                                    document_context["period_mes_principal"] = period_main_ym
                                    logger.info(f"Base da folha do mês anterior ({period_prev}): R$ {base_from_text:,.2f} (extraído do texto, para cálculos do mês {period_main_ym})")
                                else:
                                    logger.info(f"Documento {period_prev} encontrado mas base da folha extraída = 0. Mês {period_main_ym} usará folha do próprio documento.")
                        else:
                            logger.info(f"DataFrame do mês anterior ({period_prev}) vazio. Base da folha do mês {period_main_ym} usará folha do próprio documento.")
                except (ValueError, IndexError) as e:
                    logger.debug(f"Período mês anterior não calculado: {e}")
        except Exception as e:
            logger.warning(f"Erro ao calcular base folha mês anterior: {e}")

        # Valor base da folha explícito no documento atual (ex.: "Base de cálculo para impostos: R$ 33.619,22")
        try:
            if document_texts:
                valor_base_folha_doc = None
                for dt in document_texts:
                    if not isinstance(dt, dict) or not dt.get("text"):
                        continue
                    v = extract_folha_value_from_text(dt["text"])
                    if v is not None and v > 0:
                        valor_base_folha_doc = v
                if valor_base_folha_doc is not None:
                    document_context["valor_base_folha_documento"] = valor_base_folha_doc
                    logger.info(f"Base da folha extraída do texto do documento: R$ {valor_base_folha_doc:,.2f}")
        except Exception as e:
            logger.debug(f"Erro ao extrair valor base folha do documento: {e}")
        
        if categorized_files and FILE_CATEGORIZER_AVAILABLE and isinstance(categorized_files, dict):
            try:
                stats = file_categorizer.get_statistics(categorized_files)
                document_context["by_category"] = stats.get("by_category", {})
            except Exception:
                pass
        # REGRA 4: Planilha ≠ Balancete. Se só .xls/.xlsx sem período, condomínio ou extrato vinculado → Controle interno.
        if file_metadata:
            only_spreadsheet = all(
                (fm.get("filename") or "").lower().endswith((".xls", ".xlsx"))
                for fm in file_metadata if isinstance(fm, dict)
            ) and len(file_metadata) > 0
            has_period = bool(document_context.get("period_start") and document_context.get("period_end"))
            has_condominio = bool(document_context.get("condominio_name"))
            has_pdf_or_csv = any(
                (fm.get("filename") or "").lower().endswith((".pdf", ".csv"))
                for fm in file_metadata if isinstance(fm, dict)
            )
            if only_spreadsheet and not has_pdf_or_csv and (not has_period or not has_condominio):
                document_context["controle_interno"] = True
        start_time = time.time()
        result = ai_system.run_comprehensive_audit(
            df_input=combined_df,
            document_context=document_context,
        )
        processing_time = time.time() - start_time

        if result.get("success") is False:
            api_metrics["failed_analyses"] += 1
            error_parts = result.get("errors") or []
            error_msg = (
                "; ".join(
                    e.get("message", str(e)) if isinstance(e, dict) else str(e)
                    for e in error_parts
                )
                if error_parts
                else "Auditoria falhou."
            )
            logger.error("Auditoria retornou success=False (job %s): %s", job_id, error_msg)
            if STATUS_MANAGER_AVAILABLE:
                status_manager.fail_job(job_id, error_msg, message="Auditoria falhou")
                job_status = status_manager.get_job_status(job_id)
                if job_status:
                    job_status_serialized = serialize_job_status(job_status)
                    await websocket_manager.broadcast_to_job(job_id, {
                        "type": "status_update",
                        "job_id": job_id,
                        "status": job_status_serialized
                    })
                    await websocket_manager.broadcast_to_all({
                        "type": "job_failed",
                        "job_id": job_id,
                        "status": job_status_serialized
                    })
            return
        
        # Garantir file_metadata/files_summary no result (fallback a partir de document_context)
        if not result.get("file_metadata") and document_context.get("file_metadata"):
            result["file_metadata"] = document_context["file_metadata"]
        if not result.get("files_summary") and document_context.get("files_summary"):
            result["files_summary"] = document_context["files_summary"]
        # Adicionar metadados (sobrescreve com valores locais; fallback se locais vazios)
        result.update({
            "files_processed": len(all_dataframes),
            "file_metadata": file_metadata or result.get("file_metadata") or [],
            "files_summary": files_summary or result.get("files_summary") or [],
            "total_rows": len(combined_df),
            "processed_at": datetime.now().isoformat(),
            "api_version": "1.0.0",
            "processing_time": processing_time
        })
        
        # Extrair anomalies_detected de forma segura (antes de serializar)
        anomalies_count = 0
        if isinstance(result, dict):
            anomalies_count = result.get('anomalies_detected', 0)
        
        # Adicionar categorização se disponível (antes de serializar)
        if categorized_files and FILE_CATEGORIZER_AVAILABLE and isinstance(categorized_files, dict):
            try:
                stats = file_categorizer.get_statistics(categorized_files)
                if isinstance(result, dict):
                    result["file_categorization"] = {
                        "total_files": stats['total_files'],
                        "by_category": stats['by_category'],
                        "by_type": stats['by_type']
                    }
            except Exception as e:
                logger.warning(f"Erro ao obter estatísticas de categorização: {e}")
        
        # Pré-calcular totais financeiros para o relatório (evita depender do df em outro worker)
        if combined_df is not None and not combined_df.empty:
            try:
                from app.audit.financial_consolidator import calculate_financial_totals_correct
                ft = calculate_financial_totals_correct(
                    combined_df,
                    saldo_inicial=document_context.get("saldo_anterior"),
                    extracted_totals=document_context.get("totals_extracted")
                )
                result["financial_extraction_result"] = {
                    "total_receitas": ft.get("total_receitas"),
                    "total_despesas": ft.get("total_despesas"),
                    "saldo_final": ft.get("saldo_final"),
                    "extracted_data": ft.get("extracted_data", {}),
                }
                logger.info("[API] Totais financeiros pré-calculados para relatório: receitas=%s, despesas=%s",
                            ft.get("total_receitas"), ft.get("total_despesas"))
            except Exception as e:
                logger.warning("Erro ao pré-calcular totais financeiros: %s", e)
        
        # Serializar datetime
        result_serialized = serialize_datetime(result)
        
        # Garantir que result_serialized é um dict para complete_job
        if not isinstance(result_serialized, dict):
            result_serialized = {"data": result_serialized}
        
        # Marcar como concluído
        if STATUS_MANAGER_AVAILABLE:
            # Salvar DataFrame para uso em relatórios PDF
            if combined_df is not None and not combined_df.empty and hasattr(status_manager, 'save_dataframe'):
                status_manager.save_dataframe(job_id, combined_df)
                logger.info(f"DataFrame salvo para job {job_id}: {len(combined_df)} linhas")
            
            status_manager.complete_job(
                job_id,
                result={
                    "files_processed": len(all_dataframes),
                    "total_rows": len(combined_df),
                    "anomalies_detected": anomalies_count,
                    "processing_time": processing_time,
                    "data": result_serialized
                },
                message=f"Análise concluída: {len(all_dataframes)} arquivo(s), {anomalies_count} anomalias"
            )
            
            # Broadcast via WebSocket
            job_status = status_manager.get_job_status(job_id)
            if job_status:
                job_status_serialized = serialize_job_status(job_status)
                await websocket_manager.broadcast_to_job(job_id, {
                    "type": "status_update",
                    "job_id": job_id,
                    "status": job_status_serialized
                })
                await websocket_manager.broadcast_to_all({
                    "type": "job_completed",
                    "job_id": job_id,
                    "status": job_status_serialized
                })
                
                # Notificar via webhook
                if job_status_serialized:
                    await notification_manager.notify_completion(job_id, job_status_serialized)
        
        api_metrics["successful_analyses"] += 1
        logger.info(f"Analysis completed: {len(all_dataframes)} file(s), {anomalies_count} anomalies")
        
    except Exception as e:
        api_metrics["failed_analyses"] += 1
        error_msg = str(e)
        logger.error(f"Error in async processing (job {job_id}): {error_msg}")
        
        if STATUS_MANAGER_AVAILABLE:
            status_manager.fail_job(job_id, error_msg, "Erro no processamento assíncrono")
            # Broadcast via WebSocket
            job_status = status_manager.get_job_status(job_id)
            if job_status:
                job_status_serialized = serialize_job_status(job_status)
                await websocket_manager.broadcast_to_job(job_id, {
                    "type": "status_update",
                    "job_id": job_id,
                    "status": job_status_serialized
                })
                await websocket_manager.broadcast_to_all({
                    "type": "job_failed",
                    "job_id": job_id,
                    "status": job_status_serialized
                })
    
    finally:
        # Limpar arquivos temporários
        if temp_file_paths:
            cleanup_temp_files(temp_file_paths)
        
        active_jobs_count -= 1

async def process_documents_async(
    job_id: str,
    saved_files: Optional[List[Dict[str, Any]]],
    file_paths: Optional[str],
    client_id: Optional[str],
    categorized_files: Optional[Dict] = None
):
    """
    Processa análise de documentos fiscais em background
    
    Args:
        job_id: ID do job de análise
        saved_files: Lista de arquivos salvos temporariamente (se upload)
        file_paths: Caminhos de arquivos físicos (se não upload)
        client_id: ID do cliente
        categorized_files: Arquivos categorizados (opcional)
    """
    global active_jobs_count
    active_jobs_count += 1
    
    temp_file_paths = []
    
    try:
        if STATUS_MANAGER_AVAILABLE:
            status_manager.start_job(job_id)
            status_manager.update_progress(job_id, 0.1, "Carregando documentos...")
        
        documents = []
        
        # Carregar arquivos salvos temporariamente
        if saved_files:
            logger.info(f"Processing {len(saved_files)} document(s) from temp storage")
            for file_info in saved_files:
                temp_path = file_info["temp_path"]
                temp_file_paths.append(temp_path)
                filename = file_info["filename"]
                file_extension = os.path.splitext(filename)[1].lower()
                
                try:
                    with open(temp_path, 'rb') as f:
                        file_content = f.read()
                    
                    content_str = _extract_text_from_file(file_content, file_extension, filename)
                    
                    if not content_str:
                        logger.warning(f"Não foi possível extrair texto do arquivo: {filename}")
                        doc_data = {
                            'document_type': 'Generic',
                            'filename': filename,
                            'file_size': file_info["file_size"],
                            'error': 'Não foi possível extrair texto do arquivo'
                        }
                        documents.append(doc_data)
                        continue
                    
                    doc_data = document_analyzer.extract_fiscal_data(content_str)
                    doc_data['filename'] = filename
                    doc_data['file_size'] = file_info["file_size"]
                    documents.append(doc_data)
                except Exception as e:
                    logger.error(f"Error processing document {filename}: {e}")
                    continue
        
        # Carregar arquivos de caminhos físicos
        elif file_paths:
            logger.info(f"Processing documents from paths: {file_paths}")
            paths_list = [p.strip() for p in file_paths.split(',')]
            
            for file_path in paths_list:
                if not os.path.exists(file_path):
                    logger.warning(f"Arquivo não encontrado: {file_path}")
                    continue
                
                file_extension = os.path.splitext(file_path)[1].lower()
                
                try:
                    with open(file_path, 'rb') as f:
                        file_content = f.read()
                    
                    content_str = _extract_text_from_file(file_content, file_extension, file_path)
                    
                    if not content_str:
                        logger.warning(f"Não foi possível extrair texto do arquivo: {file_path}")
                        doc_data = {
                            'document_type': 'Generic',
                            'filename': os.path.basename(file_path),
                            'file_path': file_path,
                            'file_size': os.path.getsize(file_path),
                            'error': 'Não foi possível extrair texto do arquivo'
                        }
                        documents.append(doc_data)
                        continue
                    
                    doc_data = document_analyzer.extract_fiscal_data(content_str)
                    doc_data['filename'] = os.path.basename(file_path)
                    doc_data['file_path'] = file_path
                    doc_data['file_size'] = os.path.getsize(file_path)
                    documents.append(doc_data)
                except Exception as e:
                    logger.error(f"Error processing document {file_path}: {e}")
                    continue
        
        if not documents:
            raise ValueError("No valid documents could be processed")
        
        # Registrar cliente
        if CLIENT_MANAGER_AVAILABLE and client_id:
            month = datetime.now().strftime('%Y-%m')
            client_manager.register_analysis(
                client_id=client_id,
                files_count=len(documents),
                month=month,
                metadata={
                    "endpoint": "/api/v1/analyze/documents",
                    "documents_count": len(documents),
                    "job_id": job_id
                }
            )
        
        # Preparar resultado
        result = {
            "documents_processed": len(documents),
            "documents": documents,
            "message": f"Processed {len(documents)} document(s)"
        }
        
        # Adicionar categorização se disponível (antes de serializar)
        if categorized_files and FILE_CATEGORIZER_AVAILABLE and isinstance(categorized_files, dict):
            try:
                stats = file_categorizer.get_statistics(categorized_files)
                if isinstance(result, dict):
                    result["file_categorization"] = {
                        "total_files": stats['total_files'],
                        "by_category": stats['by_category'],
                        "by_type": stats['by_type']
                    }
            except Exception as e:
                logger.warning(f"Erro ao obter estatísticas de categorização: {e}")
        
        # Serializar datetime
        result_serialized = serialize_datetime(result)
        
        # Garantir que result_serialized é um dict
        if not isinstance(result_serialized, dict):
            result_serialized = {"data": result_serialized}
        
        # Marcar como concluído
        if STATUS_MANAGER_AVAILABLE:
            status_manager.complete_job(
                job_id,
                result=result_serialized,
                message=f"Análise de documentos concluída: {len(documents)} documento(s) processado(s)"
            )
        
        logger.info(f"Documents analysis completed: {len(documents)} document(s)")
        
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Error in async documents processing (job {job_id}): {error_msg}")
        
        if STATUS_MANAGER_AVAILABLE:
            status_manager.fail_job(job_id, error_msg, "Erro no processamento assíncrono de documentos")
            # Broadcast via WebSocket
            job_status = status_manager.get_job_status(job_id)
            if job_status:
                job_status_serialized = serialize_job_status(job_status)
                await websocket_manager.broadcast_to_job(job_id, {
                    "type": "status_update",
                    "job_id": job_id,
                    "status": job_status_serialized
                })
                await websocket_manager.broadcast_to_all({
                    "type": "job_failed",
                    "job_id": job_id,
                    "status": job_status_serialized
                })
    
    finally:
        # Limpar arquivos temporários
        if temp_file_paths:
            cleanup_temp_files(temp_file_paths)
        
        active_jobs_count -= 1

async def process_taxes_async(
    job_id: str,
    saved_files: Optional[List[Dict[str, Any]]],
    file_paths: Optional[str],
    transactions_file_path: Optional[str],
    transactions_path: Optional[str],
    client_id: Optional[str],
    categorized_files: Optional[Dict] = None
):
    """
    Processa análise de impostos em background
    
    Args:
        job_id: ID do job de análise
        saved_files: Lista de arquivos salvos temporariamente (se upload)
        file_paths: Caminhos de arquivos físicos (se não upload)
        transactions_file_path: Caminho do arquivo de transações salvo temporariamente (se upload)
        transactions_path: Caminho do arquivo de transações físico
        client_id: ID do cliente
        categorized_files: Arquivos categorizados (opcional)
    """
    global active_jobs_count
    active_jobs_count += 1
    
    temp_file_paths = []
    
    try:
        if STATUS_MANAGER_AVAILABLE:
            status_manager.start_job(job_id)
            status_manager.update_progress(job_id, 0.1, "Carregando documentos...")
        
        documents = []
        
        # Carregar documentos
        if saved_files:
            logger.info(f"Processing {len(saved_files)} document(s) from temp storage")
            for file_info in saved_files:
                temp_path = file_info["temp_path"]
                temp_file_paths.append(temp_path)
                filename = file_info["filename"]
                file_extension = os.path.splitext(filename)[1].lower()
                
                try:
                    with open(temp_path, 'rb') as f:
                        file_content = f.read()
                    
                    content_str = _extract_text_from_file(file_content, file_extension, filename)
                    
                    if not content_str:
                        logger.warning(f"Não foi possível extrair texto do arquivo: {filename}")
                        continue
                    
                    doc_data = document_analyzer.extract_fiscal_data(content_str)
                    documents.append(doc_data)
                except Exception as e:
                    logger.error(f"Error processing document {filename}: {e}")
                    continue
        
        elif file_paths:
            logger.info(f"Processing documents from paths: {file_paths}")
            paths_list = [p.strip() for p in file_paths.split(',')]
            
            for file_path in paths_list:
                if not os.path.exists(file_path):
                    logger.warning(f"Arquivo não encontrado: {file_path}")
                    continue
                
                file_extension = os.path.splitext(file_path)[1].lower()
                
                try:
                    with open(file_path, 'rb') as f:
                        file_content = f.read()
                    
                    content_str = _extract_text_from_file(file_content, file_extension, file_path)
                    
                    if not content_str:
                        logger.warning(f"Não foi possível extrair texto do arquivo: {file_path}")
                        continue
                    
                    doc_data = document_analyzer.extract_fiscal_data(content_str)
                    documents.append(doc_data)
                except Exception as e:
                    logger.error(f"Error processing document {file_path}: {e}")
                    continue
        
        # Carregar lançamentos financeiros
        if STATUS_MANAGER_AVAILABLE:
            status_manager.update_progress(job_id, 0.4, "Carregando lançamentos financeiros...")
        
        transactions_df = None
        if transactions_file_path:
            temp_file_paths.append(transactions_file_path)
            if os.path.exists(transactions_file_path):
                transactions_df = pd.read_csv(transactions_file_path, encoding='utf-8')
        elif transactions_path:
            if os.path.exists(transactions_path):
                transactions_df = pd.read_csv(transactions_path, encoding='utf-8')
        
        # Analisar impostos
        if STATUS_MANAGER_AVAILABLE:
            status_manager.update_progress(job_id, 0.6, "Analisando impostos...")
            # Broadcast via WebSocket
            job_status = status_manager.get_job_status(job_id)
            if job_status:
                job_status_serialized = serialize_job_status(job_status)
                await websocket_manager.broadcast_to_job(job_id, {
                    "type": "progress_update",
                    "job_id": job_id,
                    "status": job_status_serialized
                })
        
        tax_analysis = document_analyzer.analyze_taxes(
            documents, 
            transactions_df if transactions_df is not None else pd.DataFrame()
        )
        
        # Registrar cliente
        if CLIENT_MANAGER_AVAILABLE and client_id:
            month = datetime.now().strftime('%Y-%m')
            client_manager.register_analysis(
                client_id=client_id,
                files_count=len(documents),
                month=month,
                metadata={
                    "endpoint": "/api/v1/analyze/taxes",
                    "documents_count": len(documents),
                    "job_id": job_id
                }
            )
        
        # Preparar resultado
        result = {
            "tax_analysis": tax_analysis,
            "documents_processed": len(documents),
            "message": "Tax analysis completed"
        }
        
        # Adicionar categorização se disponível (antes de serializar)
        if categorized_files and FILE_CATEGORIZER_AVAILABLE and isinstance(categorized_files, dict):
            try:
                stats = file_categorizer.get_statistics(categorized_files)
                if isinstance(result, dict):
                    result["file_categorization"] = {
                        "total_files": stats['total_files'],
                        "by_category": stats['by_category'],
                        "by_type": stats['by_type']
                    }
            except Exception as e:
                logger.warning(f"Erro ao obter estatísticas de categorização: {e}")
        
        # Serializar datetime
        result_serialized = serialize_datetime(result)
        
        # Garantir que result_serialized é um dict
        if not isinstance(result_serialized, dict):
            result_serialized = {"data": result_serialized}
        
        # Marcar como concluído
        if STATUS_MANAGER_AVAILABLE:
            status_manager.complete_job(
                job_id,
                result=result_serialized,
                message=f"Análise de impostos concluída: {len(documents)} documento(s) processado(s)"
            )
            
            # Broadcast via WebSocket
            job_status = status_manager.get_job_status(job_id)
            if job_status:
                job_status_serialized = serialize_job_status(job_status)
                await websocket_manager.broadcast_to_job(job_id, {
                    "type": "status_update",
                    "job_id": job_id,
                    "status": job_status_serialized
                })
                await websocket_manager.broadcast_to_all({
                    "type": "job_completed",
                    "job_id": job_id,
                    "status": job_status_serialized
                })
                
                # Notificar via webhook
                if job_status_serialized:
                    await notification_manager.notify_completion(job_id, job_status_serialized)
        
        logger.info(f"Taxes analysis completed: {len(documents)} document(s)")
        
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Error in async taxes processing (job {job_id}): {error_msg}")
        
        if STATUS_MANAGER_AVAILABLE:
            status_manager.fail_job(job_id, error_msg, "Erro no processamento assíncrono de impostos")
            # Broadcast via WebSocket
            job_status = status_manager.get_job_status(job_id)
            if job_status:
                job_status_serialized = serialize_job_status(job_status)
                await websocket_manager.broadcast_to_job(job_id, {
                    "type": "status_update",
                    "job_id": job_id,
                    "status": job_status_serialized
                })
                await websocket_manager.broadcast_to_all({
                    "type": "job_failed",
                    "job_id": job_id,
                    "status": job_status_serialized
                })
    
    finally:
        # Limpar arquivos temporários
        if temp_file_paths:
            cleanup_temp_files(temp_file_paths)
        
        active_jobs_count -= 1

@app.post("/api/v1/analyze")
async def analyze_financial_data(
    files: Optional[List[UploadFile]] = File(None),
    file_paths: Optional[str] = Form(None),
    client_id: Optional[str] = Query(None, description="ID do cliente para rastreamento histórico"),
    callback_url: Optional[str] = Query(None, description="URL de callback (webhook) para notificação quando análise completar"),
    background_tasks: BackgroundTasks = BackgroundTasks()
):
    """
    Endpoint principal para análise de dados financeiros
    Aceita múltiplos arquivos via upload OU caminhos de arquivos físicos
    
    Processamento é ASSÍNCRONO: retorna job_id imediatamente e processa em background.
    Use GET /api/v1/analysis/status/{job_id} para acompanhar o progresso.
    
    Args:
        files: Lista de arquivos via upload (multipart/form-data)
        file_paths: Caminhos de arquivos separados por vírgula (ex: "path1.csv,path2.xlsx")
        client_id: ID do cliente para rastreamento histórico (opcional)
        background_tasks: Tarefas em background (injetado pelo FastAPI)
    
    Acesso permitido apenas de localhost
    """
    global active_jobs_count
    
    # Verificar limite de jobs simultâneos
    if active_jobs_count >= MAX_CONCURRENT_JOBS:
        raise HTTPException(
            status_code=503,
            detail=f"Servidor ocupado. Limite de {MAX_CONCURRENT_JOBS} análises simultâneas atingido. Tente novamente em alguns instantes."
        )
    
    # Validar entrada
    if not files and not file_paths:
        raise HTTPException(
            status_code=400,
            detail="No files provided. Use 'files' (upload) or 'file_paths' (comma-separated paths)"
        )
    
    job_id = None
    try:
        # Criar job de análise
        files_count = len(files) if files else (len(file_paths.split(',')) if file_paths else 0)
        if STATUS_MANAGER_AVAILABLE:
            job_id = status_manager.create_job(
                endpoint="/api/v1/analyze",
                files_count=files_count,
                client_id=client_id,
                message="Análise financeira iniciada"
            )
        else:
            job_id = str(uuid.uuid4())
        
        # Categorizar arquivos (se disponível)
        categorized_files = None
        if FILE_CATEGORIZER_AVAILABLE:
            file_paths_list = _normalize_file_paths_for_categorizer(file_paths)
            categorized_files = file_categorizer.categorize_files(
                files=files if files else [],
                file_paths=file_paths_list
            )

            if categorized_files:
                stats = file_categorizer.get_statistics(categorized_files)
                logger.info(
                    f"📁 Categorização: {stats['total_files']} arquivo(s) - "
                    f"Financeiros: {stats['by_category'].get('financial_data', 0)}, "
                    f"Fiscais: {stats['by_category'].get('fiscal_document', 0)}, "
                    f"Outros: {stats['by_category'].get('unknown', 0)}"
                )
        
        # Salvar arquivos temporariamente (se upload)
        saved_files = None
        if files:
            # Validar tamanho dos arquivos antes de salvar
            data_processing = config.config.data_processing
            max_size_mb = data_processing.max_file_size_mb if data_processing is not None else 100
            max_size = max_size_mb * 1024 * 1024
            
            for file in files:
                if not file.filename:
                    continue
                
                # Validar tipo
                file_extension = os.path.splitext(file.filename)[1].lower()
                allowed_extensions = ['.csv', '.xlsx', '.xls', '.xlt', '.sxc', '.pdf', '.ods', '.xml']
                if file_extension not in allowed_extensions:
                    raise HTTPException(
                        status_code=400,
                        detail=f"File type not supported: {file_extension}. Allowed: {', '.join(allowed_extensions)}"
                    )
            
            # Salvar arquivos
            saved_files = await save_uploaded_files(files)
            
            if not saved_files:
                raise HTTPException(
                    status_code=400,
                    detail="No valid files could be saved for processing"
                )
        
        # Adicionar tarefa em background
        background_tasks.add_task(
            process_analysis_async,
            job_id=job_id,
            saved_files=saved_files,
            file_paths=file_paths,
            client_id=client_id,
            categorized_files=categorized_files
        )
        
        # Retornar job_id IMEDIATAMENTE
        response_data = {
            "success": True,
            "job_id": job_id,
            "message": "Análise iniciada em background. Use GET /api/v1/analysis/status/{job_id} ou WebSocket ws://localhost:8000/ws/analysis/{job_id} para acompanhar o progresso.",
            "status_url": f"/api/v1/analysis/status/{job_id}",
            "websocket_url": f"ws://localhost:8000/ws/analysis/{job_id}",
            "files_count": files_count
        }
        
        if client_id:
            response_data["client_id"] = client_id
        
        if callback_url:
            response_data["webhook_registered"] = True
        
        logger.info(f"Análise iniciada em background. Job ID: {job_id}, Arquivos: {files_count}")
        
        return JSONResponse(response_data)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error initiating analysis: {e}")
        if STATUS_MANAGER_AVAILABLE and job_id:
            status_manager.fail_job(job_id, str(e), "Erro ao iniciar análise")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@app.get("/api/v1/config")
async def get_config():
    """Obter configurações da IA - Acesso apenas localhost"""
    try:
        # Retornar configurações do sistema
        # Garantir que os objetos de configuração não são None
        anomaly_detection = config.config.anomaly_detection
        data_processing = config.config.data_processing
        report = config.config.report
        
        config_data = {
            "anomaly_detection": {
                "z_score_threshold": anomaly_detection.z_score_threshold if anomaly_detection is not None else 3.0,
                "isolation_forest_contamination": anomaly_detection.isolation_forest_contamination if anomaly_detection is not None else 0.05,
                "max_salary_threshold": anomaly_detection.max_salary_threshold if anomaly_detection is not None else 5000.0
            },
            "data_processing": {
                "supported_formats": data_processing.supported_formats if data_processing is not None else ['.csv', '.xlsx', '.xls', '.xlt', '.sxc'],
                "max_file_size_mb": data_processing.max_file_size_mb if data_processing is not None else 100,
                "encoding": data_processing.encoding if data_processing is not None else 'utf-8'
            },
            "report": {
                "output_format": report.output_format if report is not None else 'markdown',
                "include_charts": report.include_charts if report is not None else True,
                "language": report.language if report is not None else 'pt-BR'
            }
        }
        return JSONResponse({
            "success": True,
            "config": config_data
        })
    except Exception as e:
        logger.error(f"Error getting config: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/v1/config")
async def update_config(config_data: Dict[str, Any]):
    """Atualizar configurações da IA - Acesso apenas localhost"""
    try:
        # Log da tentativa de atualização
        logger.info(f"Configuration update requested: {config_data}")
        
        # Nota: Atualização dinâmica de configurações não está implementada
        # Use o arquivo de configuração para mudanças permanentes
        
        return JSONResponse({
            "success": True,
            "message": "Configuration update not fully implemented. Use config file for changes.",
            "note": "To update configuration, modify the config file and restart the server."
        })
    except Exception as e:
        logger.error(f"Error updating config: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/stats")
async def get_stats():
    """Obter estatísticas da API - Acesso apenas localhost"""
    try:
        # Calcular estatísticas
        uptime = datetime.now() - api_metrics["start_time"]
        avg_processing_time = 0
        if api_metrics["successful_analyses"] > 0:
            avg_processing_time = api_metrics["total_processing_time"] / api_metrics["successful_analyses"]
        
        stats = {
            "total_requests": api_metrics["total_requests"],
            "successful_analyses": api_metrics["successful_analyses"],
            "failed_analyses": api_metrics["failed_analyses"],
            "average_processing_time": round(avg_processing_time, 3),
            "uptime": str(uptime),
            "success_rate": round(
                api_metrics["successful_analyses"] / max(api_metrics["total_requests"], 1) * 100, 2
            )
        }
        
        return JSONResponse({
            "success": True,
            "stats": stats
        })
    except Exception as e:
        logger.error(f"Error getting stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.websocket("/ws/analysis/{job_id}")
async def websocket_analysis_status(websocket: WebSocket, job_id: str):
    """
    WebSocket para receber atualizações em tempo real do status de uma análise
    
    Args:
        job_id: ID do job de análise a monitorar
    
    Uso:
        Conecte via WebSocket e receba atualizações automáticas quando o status mudar.
        Não é necessário fazer polling.
    """
    await websocket_manager.connect(websocket, job_id)
    
    try:
        # Enviar status inicial
        if STATUS_MANAGER_AVAILABLE:
            job_status = status_manager.get_job_status(job_id)
            if job_status:
                # Serializar objetos datetime/timedelta no job_status
                job_status_serialized = serialize_datetime(job_status)
                await websocket.send_json({
                    "type": "status_update",
                    "job_id": job_id,
                    "status": job_status_serialized
                })
        
        # Manter conexão aberta e escutar por mensagens do cliente
        while True:
            try:
                # Cliente pode enviar mensagens (ex: ping/pong)
                data = await websocket.receive_text()
                if data == "ping":
                    await websocket.send_json({"type": "pong"})
            except WebSocketDisconnect:
                break
    except WebSocketDisconnect:
        pass
    finally:
        websocket_manager.disconnect(websocket, job_id)

@app.websocket("/ws/analysis")
async def websocket_all_analyses(websocket: WebSocket):
    """
    WebSocket para receber atualizações de todas as análises
    
    Uso:
        Conecte via WebSocket e receba atualizações de todos os jobs ativos.
        Útil para dashboards administrativos.
    """
    await websocket_manager.connect(websocket)
    
    try:
        # Enviar lista inicial de jobs ativos
        if STATUS_MANAGER_AVAILABLE:
            jobs = status_manager.list_jobs(limit=50)
            await websocket.send_json({
                "type": "initial_status",
                "jobs": jobs
            })
        
        # Manter conexão aberta
        while True:
            try:
                data = await websocket.receive_text()
                if data == "ping":
                    await websocket.send_json({"type": "pong"})
            except WebSocketDisconnect:
                break
    except WebSocketDisconnect:
        pass
    finally:
        websocket_manager.disconnect(websocket)

@app.get("/api/v1/analysis/status/{job_id}")
async def get_analysis_status(job_id: str):
    """
    Obtém o status de uma análise pelo ID do job
    
    Args:
        job_id: ID único do job de análise
        
    Returns:
        Status atual da análise
    """
    try:
        logger.debug(f"Consultando status do job: {job_id}")
        
        if not STATUS_MANAGER_AVAILABLE:
            logger.warning(f"Status manager nao disponivel para job {job_id}")
            return JSONResponse(
                status_code=503,
                content={
                    "success": False,
                    "error": "Status manager not available",
                    "detail": "O serviço de status não está disponível no momento"
                }
            )
        
        job_status = status_manager.get_job_status(job_id)
        logger.debug(f"Status obtido para job {job_id}: {job_status is not None}")
        
        if not job_status:
            return JSONResponse(
                status_code=404,
                content={
                    "success": False,
                    "error": "Job not found",
                    "detail": f"Job {job_id} não encontrado",
                    "job_id": job_id
                }
            )
        
        # Serializar objetos datetime/timedelta no job_status
        try:
            job_status_serialized = serialize_job_status(job_status)
            
            if job_status_serialized is None:
                return JSONResponse(
                    status_code=500,
                    content={
                        "success": False,
                        "error": "Serialization error",
                        "detail": "Erro ao serializar status do job",
                        "job_id": job_id
                    }
                )
            
            return JSONResponse({
                "success": True,
                "job_id": job_id,
                "status": job_status_serialized
            })
        except Exception as serialization_error:
            logger.error(f"Erro ao serializar job_status para {job_id}: {serialization_error}", exc_info=True)
            return JSONResponse(
                status_code=500,
                content={
                    "success": False,
                    "error": "Serialization error",
                    "detail": f"Erro ao serializar status: {str(serialization_error)}",
                    "job_id": job_id
                }
            )
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro inesperado ao obter status do job {job_id}: {e}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={
                "success": False,
                "error": "Internal server error",
                "detail": f"Erro ao processar requisição: {str(e)}",
                "job_id": job_id
            }
        )

@app.get("/api/v1/analysis/status")
async def list_analysis_status(limit: int = 50, status_filter: Optional[str] = None):
    """
    Lista status de análises recentes (útil para debug/admin)
    
    Args:
        limit: Limite de resultados (padrão: 50)
        status_filter: Filtrar por status (opcional: pending, processing, completed, failed)
        
    Returns:
        Lista de jobs de análise
    """
    try:
        if not STATUS_MANAGER_AVAILABLE:
            return JSONResponse(
                status_code=503,
                content={
                    "success": False,
                    "error": "Status manager not available",
                    "detail": "O serviço de status não está disponível no momento"
                }
            )
        
        from app.services.analysis_status import AnalysisStatus
        status_enum = None
        if status_filter:
            try:
                status_enum = AnalysisStatus(status_filter)
            except ValueError:
                return JSONResponse(
                    status_code=400,
                    content={
                        "success": False,
                        "error": "Invalid status filter",
                        "detail": f"Status inválido: {status_filter}. Valores válidos: pending, processing, completed, failed, cancelled"
                    }
                )
        
        jobs = status_manager.list_jobs(status=status_enum, limit=limit)
        stats = status_manager.get_statistics()
        
        # Serializar objetos datetime/timedelta nos jobs
        try:
            jobs_serialized = serialize_datetime(jobs)
            stats_serialized = serialize_datetime(stats)
            
            return JSONResponse({
                "success": True,
                "jobs": jobs_serialized,
                "statistics": stats_serialized,
                "limit": limit,
                "total_returned": len(jobs_serialized) if isinstance(jobs_serialized, list) else 0
            })
        except Exception as serialization_error:
            logger.error(f"Erro ao serializar jobs/statistics: {serialization_error}", exc_info=True)
            return JSONResponse(
                status_code=500,
                content={
                    "success": False,
                    "error": "Serialization error",
                    "detail": f"Erro ao serializar dados: {str(serialization_error)}"
                }
            )
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro inesperado ao listar status: {e}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={
                "success": False,
                "error": "Internal server error",
                "detail": f"Erro ao processar requisição: {str(e)}"
            }
        )

@app.get("/api/v1/client/{client_id}")
async def get_client_info(client_id: str):
    """
    Obtém informações e histórico de um cliente
    
    Args:
        client_id: ID do cliente
        
    Returns:
        Informações e histórico do cliente
    """
    if not CLIENT_MANAGER_AVAILABLE:
        raise HTTPException(
            status_code=503,
            detail="Client manager not available"
        )
    
    try:
        client_history = client_manager.get_client_history(client_id)
        
        # Serializar dados do cliente (pode conter tipos NumPy/Pandas)
        client_history_serialized = serialize_datetime(client_history)
        
        return JSONResponse({
            "success": True,
            "client": client_history_serialized
        })
    except Exception as e:
        logger.error(f"Erro ao obter informações do cliente {client_id}: {e}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={
                "success": False,
                "error": "Internal server error",
                "detail": f"Erro ao processar requisição: {str(e)}",
                "client_id": client_id
            }
        )

@app.get("/api/v1/clients")
async def list_clients(limit: int = Query(100, ge=1, le=1000)):
    """
    Lista todos os clientes (útil para admin)
    
    Args:
        limit: Limite de resultados (1-1000)
        
    Returns:
        Lista de clientes
    """
    try:
        if not CLIENT_MANAGER_AVAILABLE:
            return JSONResponse(
                status_code=503,
                content={
                    "success": False,
                    "error": "Client manager not available",
                    "detail": "O serviço de clientes não está disponível no momento"
                }
            )
        
        clients = client_manager.list_clients(limit=limit)
        stats = client_manager.get_statistics()
        
        # Serializar dados (pode conter tipos NumPy/Pandas)
        clients_serialized = serialize_datetime(clients)
        stats_serialized = serialize_datetime(stats)
        
        return JSONResponse({
            "success": True,
            "clients": clients_serialized,
            "statistics": stats_serialized,
            "limit": limit,
            "total_returned": len(clients_serialized) if isinstance(clients_serialized, list) else 0
        })
    except Exception as e:
        logger.error(f"Erro ao listar clientes: {e}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={
                "success": False,
                "error": "Internal server error",
                "detail": f"Erro ao processar requisição: {str(e)}"
            }
        )

@app.get("/api/v1/system")
async def get_system_info():
    """Obter informações do sistema - Acesso apenas localhost"""
    try:
        # Informações do sistema
        system_info = {
            "cpu_percent": psutil.cpu_percent(),
            "memory": {
                "total": psutil.virtual_memory().total,
                "available": psutil.virtual_memory().available,
                "percent": psutil.virtual_memory().percent
            },
            "disk": {
                "total": psutil.disk_usage('/').total,
                "used": psutil.disk_usage('/').used,
                "free": psutil.disk_usage('/').free,
                "percent": psutil.disk_usage('/').percent
            },
            "processes": len(psutil.pids()),
            "boot_time": datetime.fromtimestamp(psutil.boot_time()).isoformat()
        }
        
        return JSONResponse({
            "success": True,
            "system": system_info
        })
    except Exception as e:
        logger.error(f"Error getting system info: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/v1/test")
async def test_analysis():
    """Endpoint de teste para validar funcionamento da IA - Acesso apenas localhost"""
    try:
        # Criar dados de teste diretamente em memória (sem salvar em disco)
        test_data = {
            "Data": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "Descricao": ["Taxa de condomínio", "Manutenção elevador", "Despesa suspeita"],
            "Tipo": ["Receita", "Despesa", "Despesa"],
            "Valor": [5000.00, 1500.00, 50000.00]
        }
        
        df = pd.DataFrame(test_data)
        
        # Executar análise passando DataFrame diretamente (evita escrita/leitura duplicada)
        result = ai_system.run_comprehensive_audit(df_input=df)
        
        # Serializar objetos datetime no resultado
        result = serialize_datetime(result)
        
        return JSONResponse({
            "success": True,
            "message": "Test analysis completed",
            "data": result
        })
        
    except Exception as e:
        logger.error(f"Error in test analysis: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================================
# NOVOS ENDPOINTS: Análise de Documentos/Impostos (Item 5)
# ============================================================================

try:
    from app.extraction.legacy.document_analyzer import DocumentAnalyzer
    document_analyzer = DocumentAnalyzer()
    DOCUMENT_ANALYZER_AVAILABLE = True
except ImportError:
    DOCUMENT_ANALYZER_AVAILABLE = False

try:
    from app.extraction.legacy.file_categorizer import FileCategorizer, FileCategory, CategorizedFile
    file_categorizer = FileCategorizer()
    FILE_CATEGORIZER_AVAILABLE = True
except ImportError:
    FILE_CATEGORIZER_AVAILABLE = False
    logger.warning("File categorizer not available. Files will be processed without categorization.")

try:
    from app.services.client_manager import ClientManager
    client_manager = ClientManager(data_dir="client_data")
    CLIENT_MANAGER_AVAILABLE = True
except ImportError:
    CLIENT_MANAGER_AVAILABLE = False
    logger.warning("Client manager not available. Client identification disabled.")

@app.post("/api/v1/analyze/documents")
async def analyze_documents(
    files: Optional[List[UploadFile]] = File(None),
    file_paths: Optional[str] = Form(None),
    client_id: Optional[str] = Query(None, description="ID do cliente para rastreamento histórico"),
    callback_url: Optional[str] = Query(None, description="URL de callback (webhook) para notificação quando análise completar"),
    background_tasks: BackgroundTasks = BackgroundTasks()
):
    """
    Analisa documentos fiscais (NF-e, NFS-e, XML, etc.)
    Extrai dados fiscais e valida documentos
    
    Processamento é ASSÍNCRONO: retorna job_id imediatamente e processa em background.
    Use GET /api/v1/analysis/status/{job_id} para acompanhar o progresso.
    
    Args:
        files: Lista de documentos via upload
        file_paths: Caminhos de documentos separados por vírgula
        client_id: ID do cliente para rastreamento histórico (opcional)
        background_tasks: Tarefas em background (injetado pelo FastAPI)
    
    Acesso permitido apenas de localhost
    """
    if not DOCUMENT_ANALYZER_AVAILABLE:
        raise HTTPException(
            status_code=503,
            detail="Document analyzer not available. Install required dependencies."
        )
    
    global active_jobs_count
    
    # Verificar limite de jobs simultâneos
    if active_jobs_count >= MAX_CONCURRENT_JOBS:
        raise HTTPException(
            status_code=503,
            detail=f"Servidor ocupado. Limite de {MAX_CONCURRENT_JOBS} análises simultâneas atingido. Tente novamente em alguns instantes."
        )
    
    # Validar entrada
    if not files and not file_paths:
        raise HTTPException(
            status_code=400,
            detail="No documents provided. Use 'files' (upload) or 'file_paths' (comma-separated paths)"
        )
    
    job_id = None
    try:
        # Criar job de análise
        files_count = len(files) if files else (len(file_paths.split(',')) if file_paths else 0)
        if STATUS_MANAGER_AVAILABLE:
            job_id = status_manager.create_job(
                endpoint="/api/v1/analyze/documents",
                files_count=files_count,
                client_id=client_id,
                message="Análise de documentos fiscais iniciada"
            )
        else:
            job_id = str(uuid.uuid4())
        
        # Categorizar arquivos (se disponível)
        categorized_files = None
        if FILE_CATEGORIZER_AVAILABLE:
            file_paths_list = _normalize_file_paths_for_categorizer(file_paths)
            categorized_files = file_categorizer.categorize_files(
                files=files if files else [],
                file_paths=file_paths_list
            )
            
            if categorized_files:
                stats = file_categorizer.get_statistics(categorized_files)
                logger.info(
                    f"📁 Categorização (documentos): {stats['total_files']} arquivo(s) - "
                    f"Documentos fiscais: {stats['by_category'].get('fiscal_document', 0)}, "
                    f"Dados financeiros: {stats['by_category'].get('financial_data', 0)}"
                )
        
        # Salvar arquivos temporariamente (se upload)
        saved_files = None
        if files:
            saved_files = await save_uploaded_files(files)
            
            if not saved_files:
                raise HTTPException(
                    status_code=400,
                    detail="No valid files could be saved for processing"
                )
        
        # Adicionar tarefa em background
        background_tasks.add_task(
            process_documents_async,
            job_id=job_id,
            saved_files=saved_files,
            file_paths=file_paths,
            client_id=client_id,
            categorized_files=categorized_files
        )
        
        # Retornar job_id IMEDIATAMENTE
        response_data = {
            "success": True,
            "job_id": job_id,
            "message": "Análise de documentos iniciada em background. Use GET /api/v1/analysis/status/{job_id} ou WebSocket ws://localhost:8000/ws/analysis/{job_id} para acompanhar o progresso.",
            "status_url": f"/api/v1/analysis/status/{job_id}",
            "websocket_url": f"ws://localhost:8000/ws/analysis/{job_id}",
            "files_count": files_count
        }
        
        if client_id:
            response_data["client_id"] = client_id
        
        if callback_url:
            response_data["webhook_registered"] = True
        
        logger.info(f"Análise de documentos iniciada em background. Job ID: {job_id}, Arquivos: {files_count}")
        
        return JSONResponse(response_data)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error initiating documents analysis: {e}")
        if STATUS_MANAGER_AVAILABLE and job_id:
            status_manager.fail_job(job_id, str(e), "Erro ao iniciar análise de documentos")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@app.post("/api/v1/analyze/taxes")
async def analyze_taxes(
    files: Optional[List[UploadFile]] = File(None),
    file_paths: Optional[str] = Form(None),
    transactions_file: Optional[UploadFile] = File(None),
    transactions_path: Optional[str] = Form(None),
    client_id: Optional[str] = Query(None, description="ID do cliente para rastreamento histórico"),
    callback_url: Optional[str] = Query(None, description="URL de callback (webhook) para notificação quando análise completar"),
    background_tasks: BackgroundTasks = BackgroundTasks()
):
    """
    Analisa impostos de documentos fiscais e compara com lançamentos
    
    Processamento é ASSÍNCRONO: retorna job_id imediatamente e processa em background.
    Use GET /api/v1/analysis/status/{job_id} para acompanhar o progresso.
    
    Args:
        files: Lista de documentos fiscais via upload
        file_paths: Caminhos de documentos separados por vírgula
        transactions_file: Arquivo com lançamentos financeiros (upload)
        transactions_path: Caminho do arquivo com lançamentos
        client_id: ID do cliente para rastreamento histórico (opcional)
        background_tasks: Tarefas em background (injetado pelo FastAPI)
    
    Acesso permitido apenas de localhost
    """
    if not DOCUMENT_ANALYZER_AVAILABLE:
        raise HTTPException(
            status_code=503,
            detail="Document analyzer not available. Install required dependencies."
        )
    
    global active_jobs_count
    
    # Verificar limite de jobs simultâneos
    if active_jobs_count >= MAX_CONCURRENT_JOBS:
        raise HTTPException(
            status_code=503,
            detail=f"Servidor ocupado. Limite de {MAX_CONCURRENT_JOBS} análises simultâneas atingido. Tente novamente em alguns instantes."
        )
    
    job_id = None
    try:
        # Criar job de análise
        files_count = len(files) if files else (len(file_paths.split(',')) if file_paths else 0)
        if STATUS_MANAGER_AVAILABLE:
            job_id = status_manager.create_job(
                endpoint="/api/v1/analyze/taxes",
                files_count=files_count,
                client_id=client_id,
                message="Análise de impostos iniciada"
            )
        else:
            job_id = str(uuid.uuid4())
        
        # Categorizar arquivos (se disponível)
        categorized_files = None
        if FILE_CATEGORIZER_AVAILABLE:
            file_paths_list = _normalize_file_paths_for_categorizer(file_paths)
            categorized_files = file_categorizer.categorize_files(
                files=files if files else [],
                file_paths=file_paths_list
            )
            
            if categorized_files:
                stats = file_categorizer.get_statistics(categorized_files)
                logger.info(
                    f"📁 Categorização (impostos): {stats['total_files']} arquivo(s) - "
                    f"Documentos fiscais: {stats['by_category'].get('fiscal_document', 0)}, "
                    f"Dados financeiros: {stats['by_category'].get('financial_data', 0)}"
                )
        
        # Salvar arquivos temporariamente (se upload)
        saved_files = None
        if files:
            saved_files = await save_uploaded_files(files)
        
        # Salvar arquivo de transações temporariamente (se upload)
        transactions_file_path = None
        if transactions_file:
            transactions_content = await transactions_file.read()
            file_id = str(uuid.uuid4())
            transactions_file_path = str(TEMP_UPLOADS_DIR / f"{file_id}_transactions.csv")
            Path(transactions_file_path).write_bytes(transactions_content)
        
        # Registrar webhook se fornecido
        if callback_url:
            notification_manager.register_webhook(job_id, callback_url)
            logger.info(f"Webhook registrado para job {job_id}: {callback_url}")
        
        # Adicionar tarefa em background
        background_tasks.add_task(
            process_taxes_async,
            job_id=job_id,
            saved_files=saved_files,
            file_paths=file_paths,
            transactions_file_path=transactions_file_path,
            transactions_path=transactions_path,
            client_id=client_id,
            categorized_files=categorized_files
        )
        
        # Retornar job_id IMEDIATAMENTE
        response_data = {
            "success": True,
            "job_id": job_id,
            "message": "Análise de impostos iniciada em background. Use GET /api/v1/analysis/status/{job_id} ou WebSocket ws://localhost:8000/ws/analysis/{job_id} para acompanhar o progresso.",
            "status_url": f"/api/v1/analysis/status/{job_id}",
            "websocket_url": f"ws://localhost:8000/ws/analysis/{job_id}",
            "files_count": files_count
        }
        
        if client_id:
            response_data["client_id"] = client_id
        
        if callback_url:
            response_data["webhook_registered"] = True
        
        logger.info(f"Análise de impostos iniciada em background. Job ID: {job_id}, Arquivos: {files_count}")
        
        return JSONResponse(response_data)
        
    except Exception as e:
        logger.error(f"Error initiating taxes analysis: {e}")
        if STATUS_MANAGER_AVAILABLE and job_id:
            status_manager.fail_job(job_id, str(e), "Erro ao iniciar análise de impostos")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@app.post("/api/v1/correlate")
async def correlate_documents_transactions(
    documents_files: Optional[List[UploadFile]] = File(None),
    documents_paths: Optional[str] = Form(None),
    transactions_file: Optional[UploadFile] = File(None),
    transactions_path: Optional[str] = Form(None)
):
    """
    Correlaciona documentos fiscais com lançamentos financeiros
    Identifica matches, divergências e lançamentos sem documento
    
    Args:
        documents_files: Lista de documentos fiscais via upload
        documents_paths: Caminhos de documentos separados por vírgula
        transactions_file: Arquivo com lançamentos financeiros (upload)
        transactions_path: Caminho do arquivo com lançamentos
    
    Acesso permitido apenas de localhost
    """
    if not DOCUMENT_ANALYZER_AVAILABLE:
        raise HTTPException(
            status_code=503,
            detail="Document analyzer not available. Install required dependencies."
        )
    
    try:
        documents = []
        
        # Processar documentos
        if documents_files:
            for file in documents_files:
                if not file.filename:
                    continue
                file_content = await file.read()
                content_str = file_content.decode('utf-8', errors='ignore')
                doc_data = document_analyzer.extract_fiscal_data(content_str)
                documents.append(doc_data)
        elif documents_paths:
            paths_list = [p.strip() for p in documents_paths.split(',')]
            for file_path in paths_list:
                if os.path.exists(file_path):
                    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                        content_str = f.read()
                    doc_data = document_analyzer.extract_fiscal_data(content_str)
                    documents.append(doc_data)
        
        # Carregar lançamentos
        if not transactions_file and not transactions_path:
            raise HTTPException(
                status_code=400,
                detail="Transactions file required. Use 'transactions_file' or 'transactions_path'"
            )
        
        if transactions_file:
            file_content = await transactions_file.read()
            transactions_df = pd.read_csv(io.BytesIO(file_content), encoding='utf-8')
        elif transactions_path is not None:
            if not os.path.exists(transactions_path):
                raise HTTPException(status_code=400, detail="Transactions file not found")
            transactions_df = pd.read_csv(transactions_path, encoding='utf-8')
        else:
            raise HTTPException(status_code=400, detail="Transactions file or path required")
        
        # Correlacionar
        correlations_df = document_analyzer.correlate_with_transactions(documents, transactions_df)
        
        # Estatísticas de correlação
        total_documents = len(documents)
        total_transactions = len(transactions_df)
        matched = len(correlations_df[correlations_df['status'] == 'matched'])
        unmatched_docs = total_documents - matched
        unmatched_trans = total_transactions - matched
        
        return JSONResponse({
            "success": True,
            "correlations": correlations_df.to_dict('records'),
            "statistics": {
                "total_documents": total_documents,
                "total_transactions": total_transactions,
                "matched": matched,
                "unmatched_documents": unmatched_docs,
                "unmatched_transactions": unmatched_trans,
                "match_rate": round(matched / max(total_documents, 1) * 100, 2) if total_documents > 0 else 0
            },
            "message": "Correlation analysis completed"
        })
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error correlating documents: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

# =============================================================================
# ENDPOINTS DE RELATÓRIO ESTRUTURADO
# =============================================================================

try:
    from app.reporting.report_formatter import (
        format_section_1, format_section_2, format_section_3, format_section_4,
        format_section_5, format_section_6, format_section_7, format_section_8,
        format_full_report, get_section_formatter
    )
    REPORT_FORMATTER_AVAILABLE = True
    logger.info("ReportFormatter inicializado com sucesso")
except ImportError as e:
    REPORT_FORMATTER_AVAILABLE = False
    logger.warning(f"ReportFormatter nao disponivel. Erro: {e}")


@app.get("/api/v1/report/section/{section_number}")
async def get_report_section(section_number: int, job_id: str = Query(..., description="ID do job de análise")):
    """
    Obtém uma seção específica do relatório de conferência.
    
    Seções disponíveis:
        1. O que foi conferido
        2. Situação dos documentos
        3. Resumo financeiro do período
        4. Encargos trabalhistas e tributos
        5. Férias e 13º
        6. Pontos de alerta
        7. Conclusão geral
        8. Parecer final
    """
    if not REPORT_FORMATTER_AVAILABLE:
        raise HTTPException(status_code=503, detail="Report formatter not available.")
    
    if section_number < 1 or section_number > 8:
        raise HTTPException(status_code=400, detail=f"Invalid section number: {section_number}. Must be between 1 and 8.")
    
    if not STATUS_MANAGER_AVAILABLE:
        raise HTTPException(status_code=503, detail="Status manager not available.")
    
    try:
        job_status = status_manager.get_job_status(job_id)
        if not job_status:
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found.")
        
        if job_status.get("status") != "completed":
            return JSONResponse(
                status_code=202,
                content={
                    "success": False,
                    "error": "Análise ainda em andamento.",
                    "message": "A análise está sendo processada em background. Aguarde a conclusão e tente novamente. Verifique o status em GET /api/v1/analysis/status/{job_id}.",
                    "job_id": job_id,
                    "job_status": job_status.get("status"),
                    "progress": job_status.get("progress"),
                    "status_message": job_status.get("message"),
                    "status_url": f"/api/v1/analysis/status/{job_id}",
                },
            )
        
        audit_result = job_status.get("result", {})
        if isinstance(audit_result, dict) and "data" in audit_result:
            audit_result = audit_result.get("data", {})
        
        df = status_manager.get_dataframe(job_id) if hasattr(status_manager, 'get_dataframe') else None
        formatter = get_section_formatter(section_number)
        if not formatter:
            raise HTTPException(status_code=500, detail=f"Formatter for section {section_number} not found.")
        
        section_data = formatter(audit_result, df, job_id)
        return JSONResponse(section_data)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error generating report section {section_number}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/report/full")
async def get_full_report(job_id: str = Query(..., description="ID do job de análise")):
    """Obtém o relatório completo com todas as 8 seções."""
    if not REPORT_FORMATTER_AVAILABLE:
        raise HTTPException(status_code=503, detail="Report formatter not available.")
    
    if not STATUS_MANAGER_AVAILABLE:
        raise HTTPException(status_code=503, detail="Status manager not available.")
    
    try:
        job_status = status_manager.get_job_status(job_id)
        if not job_status:
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found.")
        
        if job_status.get("status") != "completed":
            return JSONResponse(
                status_code=202,
                content={
                    "success": False,
                    "error": "Análise ainda em andamento.",
                    "message": "A análise está sendo processada em background. Aguarde a conclusão e tente novamente em alguns segundos. Verifique o status em GET /api/v1/analysis/status/{job_id}.",
                    "job_id": job_id,
                    "job_status": job_status.get("status"),
                    "progress": job_status.get("progress"),
                    "status_message": job_status.get("message"),
                    "status_url": f"/api/v1/analysis/status/{job_id}",
                },
            )
        
        audit_result = job_status.get("result", {})
        if isinstance(audit_result, dict) and "data" in audit_result:
            audit_result = audit_result.get("data", {})
        
        df = status_manager.get_dataframe(job_id) if hasattr(status_manager, 'get_dataframe') else None
        report = format_full_report(audit_result, df, job_id)
        pdf_path = Path("reports") / f"report_{job_id}.pdf"
        if isinstance(report, dict):
            report.setdefault("report", {}).setdefault("extras", {})
            report["report"]["extras"]["pdf_available"] = pdf_path.exists()
            report["report"]["extras"]["pdf_endpoint"] = f"/api/v1/report/pdf?job_id={job_id}"
        return JSONResponse(report)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error generating full report: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/report/pdf")
async def get_report_pdf(job_id: str = Query(..., description="ID do job de análise")):
    """Gera e retorna o relatório completo em PDF."""
    if not REPORT_FORMATTER_AVAILABLE:
        raise HTTPException(status_code=503, detail="Report formatter not available.")
    if not STATUS_MANAGER_AVAILABLE:
        raise HTTPException(status_code=503, detail="Status manager not available.")
    try:
        job_status = status_manager.get_job_status(job_id)
        if not job_status:
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found.")
        if job_status.get("status") != "completed":
            return JSONResponse(
                status_code=202,
                content={
                    "success": False,
                    "error": "Análise ainda em andamento.",
                    "message": "A análise está sendo processada em background. Aguarde a conclusão e tente novamente em alguns segundos. Verifique o status em GET /api/v1/analysis/status/{job_id}.",
                    "job_id": job_id,
                    "job_status": job_status.get("status"),
                    "progress": job_status.get("progress"),
                    "status_message": job_status.get("message"),
                    "status_url": f"/api/v1/analysis/status/{job_id}",
                },
            )

        audit_result = job_status.get("result", {})
        if isinstance(audit_result, dict) and "data" in audit_result:
            audit_result = audit_result.get("data", {})
        df = status_manager.get_dataframe(job_id) if hasattr(status_manager, 'get_dataframe') else None
        report = format_full_report(audit_result, df, job_id)

        reports_dir = Path("reports")
        reports_dir.mkdir(exist_ok=True)
        pdf_path = reports_dir / f"report_{job_id}.pdf"
        generate_report_pdf(report, str(pdf_path))

        return FileResponse(
            path=str(pdf_path),
            media_type="application/pdf",
            filename=f"report_{job_id}.pdf"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error generating report PDF: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/report/sections")
async def list_report_sections():
    """Lista todas as seções disponíveis do relatório."""
    return JSONResponse({
        "success": True,
        "sections": [
            {"number": 1, "title": "O que foi conferido", "icon": "1", "endpoint": "/api/v1/report/section/1"},
            {"number": 2, "title": "Situação dos documentos", "icon": "2", "endpoint": "/api/v1/report/section/2"},
            {"number": 3, "title": "Resumo financeiro do período", "icon": "3", "endpoint": "/api/v1/report/section/3"},
            {"number": 4, "title": "Encargos trabalhistas e tributos", "icon": "4", "endpoint": "/api/v1/report/section/4"},
            {"number": 5, "title": "Férias e 13º", "icon": "5", "endpoint": "/api/v1/report/section/5"},
            {"number": 6, "title": "Pontos de alerta", "icon": "6", "endpoint": "/api/v1/report/section/6"},
            {"number": 7, "title": "Conclusão geral", "icon": "7", "endpoint": "/api/v1/report/section/7"},
            {"number": 8, "title": "Parecer final", "icon": "8", "endpoint": "/api/v1/report/section/8"}
        ],
        "usage": {
            "single_section": "/api/v1/report/section/{number}?job_id={job_id}",
            "full_report": "/api/v1/report/full?job_id={job_id}",
            "pdf_report": "/api/v1/report/pdf?job_id={job_id}"
        }
    })


if __name__ == "__main__":
    uvicorn.run(
        "api_server:app",
        host="0.0.0.0",
        port=8000,
        reload=False,
        log_level="info"
    )
