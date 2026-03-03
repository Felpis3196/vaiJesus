"""
Configurações do Sistema de Auditoria de Condomínios com IA
"""
import os
from dataclasses import dataclass
from typing import Dict, List, Optional
import json

@dataclass
class AnomalyDetectionConfig:
    """Configurações para detecção de anomalias"""
    z_score_threshold: float = 3.0
    isolation_forest_contamination: float = 0.05
    max_salary_threshold: float = 5000.0
    min_value_threshold: float = 1.0
    max_value_threshold: float = 50000.0

@dataclass
class DataProcessingConfig:
    """Configurações para processamento de dados"""
    supported_formats: Optional[List[str]] = None
    encoding: str = 'utf-8'
    date_format: str = '%Y-%m-%d'
    required_columns: Optional[List[str]] = None
    max_file_size_mb: int = 100
    
    def __post_init__(self):
        if self.supported_formats is None:
            self.supported_formats = ['.csv', '.xlsx', '.xls', '.xlt', '.sxc', '.pdf', '.ods']
        if self.required_columns is None:
            self.required_columns = ['data', 'descricao', 'tipo', 'valor']

@dataclass
class ReportConfig:
    """Configurações para geração de relatórios"""
    output_format: str = 'markdown'
    include_charts: bool = True
    language: str = 'pt-BR'
    currency_symbol: str = 'R$'
    decimal_places: int = 2

@dataclass
class SystemConfig:
    """Configuração principal do sistema"""
    anomaly_detection: Optional[AnomalyDetectionConfig] = None
    data_processing: Optional[DataProcessingConfig] = None
    report: Optional[ReportConfig] = None
    log_level: str = 'INFO'
    output_directory: str = './reports'
    
    def __post_init__(self):
        if self.anomaly_detection is None:
            self.anomaly_detection = AnomalyDetectionConfig()
        if self.data_processing is None:
            self.data_processing = DataProcessingConfig()
        if self.report is None:
            self.report = ReportConfig()

class ConfigManager:
    """Gerenciador de configurações do sistema"""
    
    def __init__(self, config_file: Optional[str] = None):
        self.config_file = config_file or 'auditoria_config.json'
        self.config = self.load_config()
    
    def load_config(self) -> SystemConfig:
        """Carrega configuração do arquivo ou usa padrões"""
        if os.path.exists(self.config_file):
            try:
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    config_data = json.load(f)
                return self._dict_to_config(config_data)
            except Exception as e:
                print(f"Aviso: Erro ao carregar configuração: {e}. Usando configurações padrão.")
        
        return SystemConfig()
    
    def save_config(self) -> None:
        """Salva configuração atual em arquivo"""
        try:
            config_dict = self._config_to_dict(self.config)
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(config_dict, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"Erro ao salvar configuração: {e}")
    
    def _config_to_dict(self, config: SystemConfig) -> Dict:
        """Converte configuração para dicionário"""
        result: Dict = {}
        
        # Anomaly Detection
        if config.anomaly_detection is not None:
            result['anomaly_detection'] = {
                'z_score_threshold': config.anomaly_detection.z_score_threshold,
                'isolation_forest_contamination': config.anomaly_detection.isolation_forest_contamination,
                'max_salary_threshold': config.anomaly_detection.max_salary_threshold,
                'min_value_threshold': config.anomaly_detection.min_value_threshold,
                'max_value_threshold': config.anomaly_detection.max_value_threshold
            }
        
        # Data Processing
        if config.data_processing is not None:
            result['data_processing'] = {
                'supported_formats': config.data_processing.supported_formats or ['.csv', '.xlsx', '.xls', '.xlt', '.sxc', '.pdf'],
                'encoding': config.data_processing.encoding,
                'date_format': config.data_processing.date_format,
                'required_columns': config.data_processing.required_columns or ['data', 'descricao', 'tipo', 'valor'],
                'max_file_size_mb': config.data_processing.max_file_size_mb
            }
        
        # Report
        if config.report is not None:
            result['report'] = {
                'output_format': config.report.output_format,
                'include_charts': config.report.include_charts,
                'language': config.report.language,
                'currency_symbol': config.report.currency_symbol,
                'decimal_places': config.report.decimal_places
            }
        
        # System
        result['system'] = {
            'log_level': config.log_level,
            'output_directory': config.output_directory
        }
        
        return result
    
    def _dict_to_config(self, config_dict: Dict) -> SystemConfig:
        """Converte dicionário para configuração"""
        config = SystemConfig()
        
        if 'anomaly_detection' in config_dict:
            ad_config = config_dict['anomaly_detection']
            config.anomaly_detection = AnomalyDetectionConfig(
                z_score_threshold=ad_config.get('z_score_threshold', 3.0),
                isolation_forest_contamination=ad_config.get('isolation_forest_contamination', 0.05),
                max_salary_threshold=ad_config.get('max_salary_threshold', 5000.0),
                min_value_threshold=ad_config.get('min_value_threshold', 1.0),
                max_value_threshold=ad_config.get('max_value_threshold', 50000.0)
            )
        
        if 'data_processing' in config_dict:
            dp_config = config_dict['data_processing']
            config.data_processing = DataProcessingConfig(
                supported_formats=dp_config.get('supported_formats', ['.csv', '.xlsx', '.xls', '.xlt', '.sxc', '.pdf']),
                encoding=dp_config.get('encoding', 'utf-8'),
                date_format=dp_config.get('date_format', '%Y-%m-%d'),
                required_columns=dp_config.get('required_columns', ['data', 'descricao', 'tipo', 'valor']),
                max_file_size_mb=dp_config.get('max_file_size_mb', 100)
            )
        
        if 'report' in config_dict:
            r_config = config_dict['report']
            config.report = ReportConfig(
                output_format=r_config.get('output_format', 'markdown'),
                include_charts=r_config.get('include_charts', True),
                language=r_config.get('language', 'pt-BR'),
                currency_symbol=r_config.get('currency_symbol', 'R$'),
                decimal_places=r_config.get('decimal_places', 2)
            )
        
        if 'system' in config_dict:
            s_config = config_dict['system']
            config.log_level = s_config.get('log_level', 'INFO')
            config.output_directory = s_config.get('output_directory', './reports')
        
        return config

# Configuração padrão do sistema
DEFAULT_CONFIG = SystemConfig()
