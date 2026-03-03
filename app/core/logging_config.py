"""
Sistema de Logging para o Sistema de Auditoria de Condomínios com IA
"""
import logging
import os
from datetime import datetime
from typing import Optional

from .config import SystemConfig

class AuditLogger:
    """Logger especializado para auditoria de condomínios"""
    
    def __init__(self, config: SystemConfig, log_file: Optional[str] = None):
        self.config = config
        self.log_file = log_file or self._get_default_log_file()
        self.logger = self._setup_logger()
    
    def _get_default_log_file(self) -> str:
        """Gera nome do arquivo de log baseado na data"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_dir = os.path.join(self.config.output_directory, 'logs')
        os.makedirs(log_dir, exist_ok=True)
        return os.path.join(log_dir, f'auditoria_{timestamp}.log')
    
    def _setup_logger(self) -> logging.Logger:
        """Configura o logger com handlers para arquivo e console"""
        logger = logging.getLogger('AuditoriaCondominio')
        logger.setLevel(getattr(logging, self.config.log_level))
        
        # Limpar handlers existentes
        logger.handlers.clear()
        
        # Formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Handler para arquivo
        file_handler = logging.FileHandler(self.log_file, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        
        # Handler para console
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        return logger
    
    def info(self, message: str) -> None:
        """Log de informação"""
        self.logger.info(message)
    
    def warning(self, message: str) -> None:
        """Log de aviso"""
        self.logger.warning(message)
    
    def error(self, message: str) -> None:
        """Log de erro"""
        self.logger.error(message)
    
    def debug(self, message: str) -> None:
        """Log de debug"""
        self.logger.debug(message)
    
    def critical(self, message: str) -> None:
        """Log crítico"""
        self.logger.critical(message)
    
    def log_audit_start(self, file_path: str) -> None:
        """Log do início da auditoria"""
        self.info(f"Iniciando auditoria do arquivo: {file_path}")
    
    def log_audit_end(self, total_transactions: int, anomalies_found: int) -> None:
        """Log do fim da auditoria"""
        self.info(f"Auditoria concluída. Transações: {total_transactions}, Anomalias: {anomalies_found}")
    
    def log_anomaly_detected(self, transaction_id: str, anomaly_type: str, justification: str) -> None:
        """Log de anomalia detectada"""
        self.warning(f"Anomalia detectada - ID: {transaction_id}, Tipo: {anomaly_type}, Justificativa: {justification}")
    
    def log_data_processing(self, stage: str, details: str) -> None:
        """Log do processamento de dados"""
        self.info(f"Processamento de dados - {stage}: {details}")
    
    def log_error(self, operation: str, error: Exception) -> None:
        """Log de erro com contexto"""
        self.error(f"Erro em {operation}: {str(error)}")
    
    def get_log_file_path(self) -> str:
        """Retorna o caminho do arquivo de log"""
        return self.log_file
