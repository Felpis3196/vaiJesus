"""
Gerenciador de Entrada de Dados para Sistema de Auditoria de Condomínios.
Extração 100% via LLM: validação e carregamento usam apenas "arquivo -> texto" e LLM.
"""
import os
import pandas as pd
from typing import List, Dict, Optional, Union
from pathlib import Path
from app.core import SystemConfig, DataProcessingConfig, AuditLogger
from app.extraction.legacy import load_document, dataframe_to_text_br

# Extensões suportadas para extração (texto para LLM)
_SUPPORTED_EXTENSIONS = (".csv", ".xlsx", ".xls", ".xlt", ".sxc", ".pdf", ".ods", ".xml")


def _get_document_text(file_path: str, filename: Optional[str] = None, file_extension: Optional[str] = None) -> Optional[str]:
    """Obtém texto do arquivo para extração via LLM. Não usa clean_data."""
    ext = (file_extension or Path(file_path).suffix).lower()
    name = filename or Path(file_path).name
    try:
        if ext == ".xml":
            with open(file_path, "rb") as f:
                raw = f.read()
            return raw.decode("utf-8", errors="ignore") or raw.decode("latin-1", errors="ignore")
        if ext not in (".csv", ".xlsx", ".xls", ".xlt", ".sxc", ".pdf", ".ods"):
            return None
        result = load_document(file_path)
        df = result[0] if isinstance(result, tuple) else result
        if df is None or df.empty:
            return None
        if ext == ".pdf" and isinstance(result, tuple) and len(result) >= 2 and result[1]:
            return result[1] or None
        text = dataframe_to_text_br(df)
        if text:
            return text
        try:
            return df.head(200).to_string(index=False)[:150000]
        except Exception:
            return None
    except Exception:
        return None


class DataInputManager:
    """Gerenciador profissional para entrada de dados financeiros"""
    
    def __init__(self, config: SystemConfig, logger: AuditLogger):
        self.config = config
        self.logger = logger
        # Garantir que data_processing não é None (sempre inicializado no __post_init__)
        self.data_config = config.data_processing
        if self.data_config is None:
            # Fallback para valores padrão se None (não deveria acontecer, mas type safety)
            from app.core import DataProcessingConfig
            self.data_config = DataProcessingConfig()
    
    def validate_file(self, file_path: str) -> Dict[str, Union[bool, str, List[str]]]:
        """
        Valida arquivo antes do processamento (existência, tamanho, extensão, e se é possível obter texto para LLM).
        Não usa clean_data nem checagem de colunas normalizadas.
        """
        validation_result = {
            'valid': False,
            'message': '',
            'warnings': [],
            'file_info': {}
        }
        try:
            if not os.path.exists(file_path):
                validation_result['message'] = f"Arquivo não encontrado: {file_path}"
                return validation_result
            file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
            max_size = self.data_config.max_file_size_mb if self.data_config is not None else 100
            if file_size_mb > max_size:
                validation_result['message'] = f"Arquivo muito grande: {file_size_mb:.2f}MB (máximo: {max_size}MB)"
                return validation_result
            file_extension = Path(file_path).suffix.lower()
            if file_extension not in _SUPPORTED_EXTENSIONS:
                validation_result['message'] = f"Formato não suportado: {file_extension}. Suportados: {', '.join(_SUPPORTED_EXTENSIONS)}"
                return validation_result
            text = _get_document_text(file_path, Path(file_path).name, file_extension)
            if not text or not text.strip():
                validation_result['message'] = "Não foi possível obter texto do arquivo para extração (vazio ou formato não legível)."
                return validation_result
            validation_result['file_info'] = {
                'size_mb': file_size_mb,
                'format': file_extension,
                'has_text': True,
            }
            validation_result['valid'] = True
            validation_result['message'] = "Arquivo válido"
            if file_size_mb > 10:
                validation_result['warnings'].append(f"Arquivo grande ({file_size_mb:.2f}MB) - processamento pode demorar")
        except Exception as e:
            validation_result['message'] = f"Erro na validação: {str(e)}"
        return validation_result

    def _check_required_columns(self, df: pd.DataFrame) -> List[str]:
        """Verifica se as colunas esperadas (data, descricao, tipo, valor) estão presentes (para pós-LLM)."""
        if df is None or df.empty:
            return ['data', 'descricao', 'tipo', 'valor']
        df_columns_lower = [str(c).lower().strip().replace(' ', '_') for c in df.columns]
        missing = []
        for col in ['data', 'descricao', 'tipo', 'valor']:
            if col not in df_columns_lower:
                missing.append(col)
        return missing
    
    def load_data(self, file_path: str, metadata: Optional[dict] = None) -> pd.DataFrame:
        """
        Carrega dados do arquivo via LLM: obtém texto do arquivo, chama extração LLM e build_dataframe_and_context.
        Retorna o DataFrame normalizado (colunas data, descricao, tipo, valor). Requer LLM disponível.
        """
        try:
            self.logger.log_data_processing("carregamento", f"Iniciando carregamento de {file_path}")
            validation = self.validate_file(file_path)
            if not validation.get('valid', False):
                raise ValueError(f"Arquivo inválido: {validation.get('message', 'Erro desconhecido')}")
            for w in validation.get('warnings') or []:
                self.logger.warning(w)

            from app.extraction.llm import is_llm_available, extract_document_llm, build_dataframe_and_context
            if not is_llm_available():
                raise RuntimeError(
                    "Extração requer LLM. Configure LLM_BASE_URL ou OPENAI_API_KEY e garanta que o serviço esteja disponível."
                )
            filename = Path(file_path).name
            ext = Path(file_path).suffix.lower()
            text = _get_document_text(file_path, filename, ext)
            if not text or not text.strip():
                raise ValueError("Não foi possível obter texto do arquivo para extração.")
            document_texts = [{"filename": filename, "text": text}]
            extraction = extract_document_llm(document_texts)
            combined_df, document_context = build_dataframe_and_context(extraction, document_texts)
            if metadata is not None and isinstance(document_context, dict):
                if document_context.get("condominio_name"):
                    metadata["condominio_name"] = document_context["condominio_name"]
            self.logger.log_data_processing("carregamento", f"Carregadas {len(combined_df)} transações via LLM")
            return combined_df
        except Exception as e:
            self.logger.log_error("carregamento de dados", e)
            raise
    
    def get_file_info(self, file_path: str) -> Dict:
        """Retorna informações sobre o arquivo (tamanho, formato, se há texto para LLM)."""
        if not os.path.exists(file_path):
            return {'error': 'Arquivo não encontrado'}
        try:
            file_stat = os.stat(file_path)
            file_extension = Path(file_path).suffix.lower()
            text = _get_document_text(file_path, Path(file_path).name, file_extension)
            info = {
                'name': Path(file_path).name,
                'size_mb': file_stat.st_size / (1024 * 1024),
                'format': file_extension,
                'last_modified': pd.Timestamp(file_stat.st_mtime, unit='s'),
                'has_text': bool(text and text.strip()),
            }
            if text and len(text) > 0:
                info['text_length'] = len(text)
            return info
        except Exception as e:
            return {'error': f'Erro ao analisar arquivo: {str(e)}'}

class DataInputValidator:
    """Validador de dados de entrada"""
    
    @staticmethod
    def validate_transaction_data(df: pd.DataFrame) -> Dict[str, List[str]]:
        """
        Valida dados de transações
        
        Returns:
            Dict com erros encontrados por categoria
        """
        errors = {
            'critical': [],
            'warnings': [],
            'suggestions': []
        }
        
        # Validações críticas
        if df.empty:
            errors['critical'].append("DataFrame está vazio")
            return errors
        
        # Verificar valores nulos em colunas críticas
        critical_columns = ['data', 'descricao', 'tipo', 'valor']
        for col in critical_columns:
            if col in df.columns:
                null_count = df[col].isnull().sum()
                if null_count > 0:
                    errors['critical'].append(f"Coluna '{col}' tem {null_count} valores nulos")
        
        # Verificar tipos de transação
        if 'tipo' in df.columns:
            valid_types = ['receita', 'despesa']
            tipo_series = df['tipo'].str.lower()
            invalid_mask = ~tipo_series.isin(valid_types)
            invalid_types_series = df.loc[invalid_mask, 'tipo']
            if len(invalid_types_series) > 0:
                invalid_types_list = invalid_types_series.unique().tolist()
                errors['warnings'].append(f"Tipos inválidos encontrados: {invalid_types_list}")
        
        # Verificar valores monetários
        if 'valor' in df.columns:
            try:
                valor_series = df['valor']
                valor_numeric = pd.to_numeric(valor_series, errors='coerce')
                if not isinstance(valor_numeric, pd.Series):
                    valor_numeric = pd.Series(valor_numeric)
                valid_mask = pd.notna(valor_numeric)
                negative_values = valor_numeric[valid_mask & (valor_numeric < 0)]
                negative_count = len(negative_values) if isinstance(negative_values, pd.Series) else 0
                if negative_count > 0:
                    errors['suggestions'].append(f"{negative_count} transações com valores negativos")
            except Exception:
                errors['critical'].append("Coluna 'valor' não pode ser convertida para numérico")
        
        # Verificar datas
        if 'data' in df.columns:
            try:
                pd.to_datetime(df['data'], errors='coerce')
                invalid_dates = df[pd.to_datetime(df['data'], errors='coerce').isnull()]
                if len(invalid_dates) > 0:
                    errors['warnings'].append(f"{len(invalid_dates)} datas inválidas encontradas")
            except Exception:
                errors['critical'].append("Erro ao processar coluna de datas")
        
        return errors

class DataInputExamples:
    """Exemplos de como usar o sistema em diferentes cenários"""
    
    @staticmethod
    def create_sample_bank_statement() -> pd.DataFrame:
        """Cria exemplo de extrato bancário"""
        data = {
            'Data': ['2025-01-01', '2025-01-02', '2025-01-03'],
            'Descrição': ['TRANSFERÊNCIA RECEBIDA', 'PAGAMENTO CONTA ÁGUA', 'TRANSFERÊNCIA ENVIADA'],
            'Tipo': ['Receita', 'Despesa', 'Despesa'],
            'Valor': [1500.00, 180.50, 1200.00]
        }
        return pd.DataFrame(data)
    
    @staticmethod
    def create_sample_accounting_export() -> pd.DataFrame:
        """Cria exemplo de exportação de sistema contábil"""
        data = {
            'data': ['01/01/2025', '02/01/2025', '03/01/2025'],
            'descricao': ['Taxa condominial apto 101', 'Salário zelador', 'Manutenção elevador'],
            'tipo': ['receita', 'despesa', 'despesa'],
            'valor': [500.00, 1200.00, 450.00]
        }
        return pd.DataFrame(data)
    
    @staticmethod
    def save_sample_files(output_dir: str = './sample_data'):
        """Salva arquivos de exemplo para demonstração"""
        os.makedirs(output_dir, exist_ok=True)
        bank_df = DataInputExamples.create_sample_bank_statement()
        bank_df.to_csv(os.path.join(output_dir, 'extrato_bancario.csv'), index=False)
        accounting_df = DataInputExamples.create_sample_accounting_export()
        accounting_df.to_csv(os.path.join(output_dir, 'exportacao_contabil.csv'), index=False)
        print(f"Arquivos de exemplo salvos em: {output_dir}")
