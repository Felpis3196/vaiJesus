"""
Sistema Principal de Auditoria de Condomínios com IA - Versão Profissional
"""
import os
import sys
from datetime import datetime
from typing import Optional, Dict, Any
from pathlib import Path

from app.core import ConfigManager, SystemConfig, AuditLogger
from app.data_input_manager import DataInputManager, DataInputValidator
from app.extraction.legacy import categorize_transactions
from app.analysis import run_anomaly_detection
from app.reporting.report_generator import generate_full_report
from .audit_structures import error_from_exception

class AuditSystem:
    """Sistema principal de auditoria de condomínios com IA"""
    
    def __init__(self, config_file: Optional[str] = None):
        """Inicializa o sistema de auditoria"""
        self.config_manager = ConfigManager(config_file)
        self.config = self.config_manager.config
        self.logger = AuditLogger(self.config)
        self.data_manager = DataInputManager(self.config, self.logger)
        self.validator = DataInputValidator()
        
        # Criar diretório de saída se não existir
        os.makedirs(self.config.output_directory, exist_ok=True)
        
        self.logger.info("Sistema de Auditoria de Condomínios inicializado")
    
    def run_audit(self, file_path: str, output_dir: Optional[str] = None) -> Dict[str, Any]:
        """
        Executa auditoria completa do arquivo especificado
        
        Args:
            file_path: Caminho para o arquivo de dados
            output_dir: Diretório de saída (opcional)
            
        Returns:
            Dict com resultados da auditoria
        """
        audit_results = {
            'success': False,
            'file_path': file_path,
            'start_time': datetime.now(),
            'end_time': None,
            'total_transactions': 0,
            'anomalies_detected': 0,
            'report_file': None,
            'errors': [],
            'warnings': [],
            'summary': {}
        }
        
        try:
            self.logger.log_audit_start(file_path)
            
            # 1. Validação e carregamento de dados
            self.logger.info("Fase 1: Validação e carregamento de dados")
            df = self._load_and_validate_data(file_path)
            audit_results['total_transactions'] = len(df)
            
            # 2. Processamento de dados
            self.logger.info("Fase 2: Processamento e categorização")
            df_processed = self._process_data(df)
            
            # 3. Detecção de anomalias
            self.logger.info("Fase 3: Detecção de anomalias com IA")
            df_audited = self._detect_anomalies(df_processed)
            audit_results['anomalies_detected'] = len(df_audited[df_audited['anomalia_detectada'] == True])
            
            # 4. Geração de relatório
            self.logger.info("Fase 4: Geração de relatório")
            report_file = self._generate_report(df_audited, output_dir)
            audit_results['report_file'] = report_file
            
            # 5. Resumo final
            audit_results['summary'] = self._generate_summary(df_audited)
            audit_results['success'] = True
            
            self.logger.log_audit_end(
                audit_results['total_transactions'],
                audit_results['anomalies_detected']
            )
            
        except Exception as e:
            self.logger.log_error("auditoria completa", e)
            audit_results['errors'].append(error_from_exception(e))
            
        finally:
            audit_results['end_time'] = datetime.now()
            audit_results['duration'] = audit_results['end_time'] - audit_results['start_time']
        
        return audit_results
    
    def _load_and_validate_data(self, file_path: str):
        """Carrega dados via DataInputManager (extração 100% LLM) e valida."""
        df = self.data_manager.load_data(file_path)
        
        # Validar dados
        validation_errors = self.validator.validate_transaction_data(df)
        
        # Processar erros críticos
        if validation_errors['critical']:
            raise ValueError(f"Erros críticos encontrados: {'; '.join(validation_errors['critical'])}")
        
        # Log de avisos e sugestões
        for warning in validation_errors['warnings']:
            self.logger.warning(f"Validação: {warning}")
        
        for suggestion in validation_errors['suggestions']:
            self.logger.info(f"Sugestão: {suggestion}")
        
        return df
    
    def _process_data(self, df):
        """Categoriza os dados. O df já vem normalizado pela LLM (colunas data, descricao, tipo, valor)."""
        self.logger.log_data_processing("categorização", "Iniciando categorização")
        df_categorized = categorize_transactions(df)
        self.logger.log_data_processing("processamento", f"Dados processados: {len(df_categorized)} transações")
        return df_categorized
    
    def _detect_anomalies(self, df):
        """Executa detecção de anomalias"""
        df_audited = run_anomaly_detection(df.copy())
        
        # Log de anomalias detectadas
        anomalies = df_audited[df_audited['anomalia_detectada'] == True]
        for _, anomaly in anomalies.iterrows():
            # Garantir que justificativa seja sempre uma string
            justification = anomaly.get('justificativa_anomalia', 'N/A')
            if justification is None:
                justification = 'N/A'
            justification_str = str(justification) if justification else 'N/A'
            
            self.logger.log_anomaly_detected(
                str(anomaly.get('id', 'N/A')),
                'various',
                justification_str
            )
        
        return df_audited
    
    def _generate_report(self, df_audited, output_dir: Optional[str] = None):
        """Gera relatório de auditoria"""
        report_content = generate_full_report(df_audited)
        
        # Determinar diretório de saída
        if output_dir is None:
            output_dir = self.config.output_directory
        
        # Gerar nome do arquivo
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_filename = f"relatorio_auditoria_{timestamp}.md"
        report_path = os.path.join(output_dir, report_filename)
        
        # Salvar relatório
        with open(report_path, "w", encoding="utf-8") as f:
            f.write(report_content)
        
        self.logger.info(f"Relatório salvo em: {report_path}")
        return report_path
    
    def _generate_summary(self, df_audited):
        """Gera resumo dos resultados"""
        total_receitas = df_audited[df_audited["tipo"].str.lower() == "receita"]["valor"].sum()
        total_despesas = df_audited[df_audited["tipo"].str.lower() == "despesa"]["valor"].sum()
        saldo = total_receitas - total_despesas
        
        anomalies = df_audited[df_audited['anomalia_detectada'] == True]
        
        return {
            'total_receitas': total_receitas,
            'total_despesas': total_despesas,
            'saldo': saldo,
            'total_transactions': len(df_audited),
            'anomalies_count': len(anomalies),
            'anomaly_types': anomalies['justificativa_anomalia'].value_counts().to_dict()
        }
    
    def get_system_info(self) -> Dict[str, Any]:
        """Retorna informações do sistema"""
        # Garantir que data_processing não é None (sempre inicializado no __post_init__)
        data_processing = self.config.data_processing
        if data_processing is None:
            data_processing = self.config_manager.config.data_processing
        
        return {
            'version': '2.0.0',
            'config_file': self.config_manager.config_file,
            'log_file': self.logger.get_log_file_path(),
            'output_directory': self.config.output_directory,
            'supported_formats': data_processing.supported_formats if data_processing else ['.csv', '.xlsx', '.xls', '.xlt', '.sxc'],
            'max_file_size_mb': data_processing.max_file_size_mb if data_processing else 100
        }
    
    def update_config(self, new_config: Dict[str, Any]) -> bool:
        """Atualiza configuração do sistema"""
        try:
            # Atualizar configurações específicas
            if 'anomaly_detection' in new_config:
                for key, value in new_config['anomaly_detection'].items():
                    if hasattr(self.config.anomaly_detection, key):
                        setattr(self.config.anomaly_detection, key, value)
            
            if 'data_processing' in new_config:
                for key, value in new_config['data_processing'].items():
                    if hasattr(self.config.data_processing, key):
                        setattr(self.config.data_processing, key, value)
            
            # Salvar configuração
            self.config_manager.save_config()
            self.logger.info("Configuração atualizada com sucesso")
            return True
            
        except Exception as e:
            self.logger.log_error("atualização de configuração", e)
            return False

def main():
    """Função principal para uso via linha de comando"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Sistema de Auditoria de Condomínios com IA')
    parser.add_argument('file_path', nargs='?', help='Caminho para o arquivo de dados')
    parser.add_argument('--config', help='Arquivo de configuração personalizado')
    parser.add_argument('--output-dir', help='Diretório de saída para relatórios')
    parser.add_argument('--info', action='store_true', help='Mostrar informações do sistema')
    
    args = parser.parse_args()
    
    try:
        # Inicializar sistema
        audit_system = AuditSystem(args.config)
        
        if args.info:
            # Mostrar informações do sistema
            info = audit_system.get_system_info()
            print("\n=== INFORMAÇÕES DO SISTEMA ===")
            for key, value in info.items():
                print(f"{key}: {value}")
            return
        
        if not args.file_path:
            parser.print_help()
            return
        
        # Executar auditoria
        print(f"Iniciando auditoria do arquivo: {args.file_path}")
        results = audit_system.run_audit(args.file_path, args.output_dir)
        
        # Mostrar resultados
        if results['success']:
            print(f"\n✅ Auditoria concluída com sucesso!")
            print(f"📊 Total de transações: {results['total_transactions']}")
            print(f"⚠️  Anomalias detectadas: {results['anomalies_detected']}")
            print(f"📄 Relatório: {results['report_file']}")
            print(f"⏱️  Duração: {results['duration']}")
            
            # Resumo financeiro (total_receitas/saldo can be None when no receita)
            summary = results['summary']
            _v = lambda k: summary.get(k) or 0
            print(f"\n💰 RESUMO FINANCEIRO:")
            print(f"   Receitas: R$ {_v('total_receitas'):,.2f}")
            print(f"   Despesas: R$ {_v('total_despesas'):,.2f}")
            print(f"   Saldo: R$ {_v('saldo'):,.2f}")
        else:
            print(f"\n❌ Auditoria falhou!")
            for error in results['errors']:
                print(f"   Erro: {error}")
    
    except Exception as e:
        print(f"Erro fatal: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
