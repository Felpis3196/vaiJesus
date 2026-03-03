"""
Sistema de Status de Análises
Gerencia o status de análises em execução para consulta externa
"""
import uuid
import time
from datetime import datetime
from typing import Dict, Optional, Any
from enum import Enum
from dataclasses import dataclass, asdict
import logging

logger = logging.getLogger(__name__)


class AnalysisStatus(Enum):
    """Status possíveis de uma análise"""
    PENDING = "pending"  # Aguardando processamento
    PROCESSING = "processing"  # Em processamento
    COMPLETED = "completed"  # Concluída com sucesso
    FAILED = "failed"  # Falhou
    CANCELLED = "cancelled"  # Cancelada


@dataclass
class AnalysisJob:
    """Job de análise com status e informações"""
    job_id: str
    status: AnalysisStatus
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    progress: float = 0.0  # 0.0 a 1.0
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    endpoint: Optional[str] = None
    files_count: int = 0
    client_id: Optional[str] = None
    message: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Converte para dicionário (serializável)"""
        data = asdict(self)
        # Converter enums e datetimes para strings
        data['status'] = self.status.value
        data['created_at'] = self.created_at.isoformat() if self.created_at else None
        data['started_at'] = self.started_at.isoformat() if self.started_at else None
        data['completed_at'] = self.completed_at.isoformat() if self.completed_at else None
        
        # Serializar o campo 'result' se existir (pode conter tipos NumPy/Pandas não serializáveis)
        if data.get('result') is not None:
            try:
                data['result'] = self._serialize_result(data['result'])
            except Exception as e:
                logger.warning(f"Erro ao serializar result no job {self.job_id}: {e}")
                # Se falhar, tentar converter para string
                data['result'] = str(data['result']) if data['result'] is not None else None
        
        return data
    
    def _serialize_result(self, obj):
        """Serialização básica local para tipos comuns"""
        import pandas as pd
        from datetime import datetime, date, timedelta
        
        if obj is None:
            return None
        elif isinstance(obj, (datetime, date)):
            return obj.isoformat()
        elif isinstance(obj, timedelta):
            return obj.total_seconds()
        elif isinstance(obj, pd.Timestamp):
            return obj.isoformat()
        elif isinstance(obj, dict):
            return {k: self._serialize_result(v) for k, v in obj.items()}
        elif isinstance(obj, (list, tuple)):
            return [self._serialize_result(item) for item in obj]
        elif isinstance(obj, pd.Series):
            return self._serialize_result(obj.to_dict())
        elif isinstance(obj, pd.DataFrame):
            return self._serialize_result(obj.to_dict('records'))
        else:
            # Tentar converter tipos NumPy
            if hasattr(obj, '__class__'):
                class_name = obj.__class__.__name__
                class_module = getattr(obj.__class__, '__module__', '')
                
                if class_module == 'numpy':
                    if class_name in ('int8', 'int16', 'int32', 'int64', 'int_', 'intc', 'intp',
                                      'uint8', 'uint16', 'uint32', 'uint64', 'integer'):
                        try:
                            return int(obj)
                        except (ValueError, OverflowError):
                            return str(obj)
                    elif class_name in ('float16', 'float32', 'float64', 'float_', 'floating'):
                        try:
                            val = float(obj)
                            if pd.isna(val) or not (val == val):
                                return None
                            return val
                        except (ValueError, OverflowError):
                            return str(obj)
                    elif class_name == 'bool_':
                        return bool(obj)
                    elif class_name == 'ndarray':
                        return [self._serialize_result(item) for item in obj.tolist()]
            
            # Tipos nativos Python
            if isinstance(obj, (str, int, float, bool)):
                return obj
            
            # Fallback: converter para string
            try:
                if hasattr(obj, '__int__'):
                    return int(obj)
                elif hasattr(obj, '__float__'):
                    val = float(obj)
                    if pd.isna(val) or not (val == val):
                        return None
                    return val
                else:
                    return str(obj)
            except (TypeError, ValueError, OverflowError):
                return None


class AnalysisStatusManager:
    """Gerencia status de análises"""
    
    def __init__(self, max_jobs: int = 1000):
        """
        Inicializa o gerenciador de status
        
        Args:
            max_jobs: Número máximo de jobs a manter em memória
        """
        self.jobs: Dict[str, AnalysisJob] = {}
        self._dataframes: Dict[str, Any] = {}  # Armazena DataFrames para relatórios
        self.max_jobs = max_jobs
        self.logger = logging.getLogger(__name__)
    
    def create_job(
        self,
        endpoint: Optional[str] = None,
        files_count: int = 0,
        client_id: Optional[str] = None,
        message: Optional[str] = None
    ) -> str:
        """
        Cria um novo job de análise
        
        Args:
            endpoint: Nome do endpoint que criou o job
            files_count: Número de arquivos a processar
            message: Mensagem descritiva
            
        Returns:
            ID único do job
        """
        job_id = str(uuid.uuid4())
        
        job = AnalysisJob(
            job_id=job_id,
            status=AnalysisStatus.PENDING,
            created_at=datetime.now(),
            endpoint=endpoint,
            files_count=files_count,
            client_id=client_id,
            message=message or "Análise criada"
        )
        
        self.jobs[job_id] = job
        
        # Limpar jobs antigos se exceder o limite
        self._cleanup_old_jobs()
        
        self.logger.info(f"Criado job {job_id} - Endpoint: {endpoint}, Arquivos: {files_count}")
        
        return job_id
    
    def start_job(self, job_id: str) -> bool:
        """
        Marca um job como iniciado
        
        Args:
            job_id: ID do job
            
        Returns:
            True se o job foi iniciado, False se não encontrado
        """
        if job_id not in self.jobs:
            self.logger.warning(f"Job {job_id} não encontrado para iniciar")
            return False
        
        job = self.jobs[job_id]
        job.status = AnalysisStatus.PROCESSING
        job.started_at = datetime.now()
        job.progress = 0.0
        job.message = "Processamento iniciado"
        
        self.logger.info(f"Job {job_id} iniciado")
        return True
    
    def update_progress(self, job_id: str, progress: float, message: Optional[str] = None) -> bool:
        """
        Atualiza o progresso de um job
        
        Args:
            job_id: ID do job
            progress: Progresso (0.0 a 1.0)
            message: Mensagem opcional
            
        Returns:
            True se atualizado, False se não encontrado
        """
        if job_id not in self.jobs:
            return False
        
        job = self.jobs[job_id]
        job.progress = max(0.0, min(1.0, progress))  # Garantir entre 0 e 1
        if message:
            job.message = message
        
        return True
    
    def complete_job(self, job_id: str, result: Dict[str, Any], message: Optional[str] = None) -> bool:
        """
        Marca um job como concluído
        
        Args:
            job_id: ID do job
            result: Resultado da análise
            message: Mensagem opcional
            
        Returns:
            True se concluído, False se não encontrado
        """
        if job_id not in self.jobs:
            self.logger.warning(f"Job {job_id} não encontrado para concluir")
            return False
        
        job = self.jobs[job_id]
        job.status = AnalysisStatus.COMPLETED
        job.completed_at = datetime.now()
        job.progress = 1.0
        job.result = result
        job.message = message or "Análise concluída com sucesso"
        
        self.logger.info(f"Job {job_id} concluído com sucesso")
        return True
    
    def fail_job(self, job_id: str, error: str, message: Optional[str] = None) -> bool:
        """
        Marca um job como falhou
        
        Args:
            job_id: ID do job
            error: Mensagem de erro
            message: Mensagem opcional
            
        Returns:
            True se marcado como falha, False se não encontrado
        """
        if job_id not in self.jobs:
            self.logger.warning(f"Job {job_id} não encontrado para falhar")
            return False
        
        job = self.jobs[job_id]
        job.status = AnalysisStatus.FAILED
        job.completed_at = datetime.now()
        job.error = error
        job.message = message or f"Análise falhou: {error}"
        
        self.logger.error(f"Job {job_id} falhou: {error}")
        return True
    
    def cancel_job(self, job_id: str, message: Optional[str] = None) -> bool:
        """
        Cancela um job
        
        Args:
            job_id: ID do job
            message: Mensagem opcional
            
        Returns:
            True se cancelado, False se não encontrado
        """
        if job_id not in self.jobs:
            return False
        
        job = self.jobs[job_id]
        job.status = AnalysisStatus.CANCELLED
        job.completed_at = datetime.now()
        job.message = message or "Análise cancelada"
        
        self.logger.info(f"Job {job_id} cancelado")
        return True
    
    def get_job(self, job_id: str) -> Optional[AnalysisJob]:
        """
        Obtém um job pelo ID
        
        Args:
            job_id: ID do job
            
        Returns:
            AnalysisJob ou None se não encontrado
        """
        return self.jobs.get(job_id)
    
    def get_job_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        """
        Obtém o status de um job (serializado)
        
        Args:
            job_id: ID do job
            
        Returns:
            Dict com status do job ou None se não encontrado
        """
        job = self.jobs.get(job_id)
        if not job:
            return None
        
        return job.to_dict()
    
    def list_jobs(
        self,
        status: Optional[AnalysisStatus] = None,
        limit: int = 50
    ) -> list[Dict[str, Any]]:
        """
        Lista jobs (úteis para debug/admin)
        
        Args:
            status: Filtrar por status (opcional)
            limit: Limite de resultados
            
        Returns:
            Lista de jobs (serializados)
        """
        jobs_list = list(self.jobs.values())
        
        # Filtrar por status se especificado
        if status:
            jobs_list = [j for j in jobs_list if j.status == status]
        
        # Ordenar por data de criação (mais recentes primeiro)
        jobs_list.sort(key=lambda j: j.created_at, reverse=True)
        
        # Limitar resultados
        jobs_list = jobs_list[:limit]
        
        return [job.to_dict() for job in jobs_list]
    
    def _cleanup_old_jobs(self):
        """Remove jobs antigos se exceder o limite"""
        if len(self.jobs) <= self.max_jobs:
            return
        
        # Ordenar por data de criação (mais antigos primeiro)
        sorted_jobs = sorted(
            self.jobs.items(),
            key=lambda x: x[1].created_at
        )
        
        # Remover 10% dos mais antigos
        to_remove = len(sorted_jobs) - int(self.max_jobs * 0.9)
        
        for job_id, _ in sorted_jobs[:to_remove]:
            del self.jobs[job_id]
            # Também remover DataFrame associado
            if job_id in self._dataframes:
                del self._dataframes[job_id]
            self.logger.debug(f"Removido job antigo: {job_id}")
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Obtém estatísticas dos jobs
        
        Returns:
            Dict com estatísticas
        """
        total = len(self.jobs)
        
        stats = {
            "total_jobs": total,
            "by_status": {}
        }
        
        # Contar por status
        for status in AnalysisStatus:
            count = sum(1 for j in self.jobs.values() if j.status == status)
            stats["by_status"][status.value] = count
        
        return stats
    
    def store_dataframe(self, job_id: str, dataframe: Any) -> bool:
        """
        Armazena um DataFrame para um job (para uso em relatórios)
        
        Args:
            job_id: ID do job
            dataframe: DataFrame a armazenar
            
        Returns:
            True se armazenado, False se job não encontrado
        """
        if job_id not in self.jobs:
            return False
        self._dataframes[job_id] = dataframe
        self.logger.debug(f"DataFrame armazenado para job {job_id}")
        return True
    
    def get_dataframe(self, job_id: str) -> Optional[Any]:
        """
        Obtém o DataFrame de um job
        
        Args:
            job_id: ID do job
            
        Returns:
            DataFrame ou None se não encontrado
        """
        return self._dataframes.get(job_id)
