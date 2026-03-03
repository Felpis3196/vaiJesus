"""
Categorizador de Arquivos
Categoriza arquivos internamente por tipo e função para processamento otimizado
"""
import os
import logging
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)


class FileCategory(Enum):
    """Categorias de arquivos"""
    FINANCIAL_DATA = "financial_data"  # Dados financeiros (CSV, Excel)
    FISCAL_DOCUMENT = "fiscal_document"  # Documentos fiscais (PDF, XML)
    TRANSACTION_DATA = "transaction_data"  # Dados de transações
    UNKNOWN = "unknown"  # Tipo desconhecido


class FileType(Enum):
    """Tipos de arquivo por extensão"""
    CSV = "csv"
    EXCEL = "excel"  # .xlsx, .xls
    PDF = "pdf"
    ODS = "ods"
    XML = "xml"
    TXT = "txt"
    UNKNOWN = "unknown"


@dataclass
class CategorizedFile:
    """Arquivo categorizado"""
    filename: str
    file_path: Optional[str] = None
    file_content: Optional[bytes] = None
    file_size: int = 0
    extension: str = ""
    file_type: FileType = FileType.UNKNOWN
    category: FileCategory = FileCategory.UNKNOWN
    is_fiscal_document: bool = False
    is_financial_data: bool = False
    metadata: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}


class FileCategorizer:
    """Categoriza arquivos por tipo e função"""
    
    # Mapeamento de extensões para tipos
    EXTENSION_TO_TYPE = {
        '.csv': FileType.CSV,
        '.xlsx': FileType.EXCEL,
        '.xls': FileType.EXCEL,
        '.xlt': FileType.EXCEL,
        '.sxc': FileType.EXCEL,
        '.pdf': FileType.PDF,
        '.ods': FileType.ODS,
        '.xml': FileType.XML,
        '.txt': FileType.TXT,
    }
    
    # Extensões de documentos fiscais
    FISCAL_EXTENSIONS = {'.pdf', '.xml', '.ods'}
    
    # Extensões de dados financeiros
    FINANCIAL_EXTENSIONS = {'.csv', '.xlsx', '.xls', '.xlt', '.sxc', '.ods'}
    
    # Palavras-chave para identificar documentos fiscais
    FISCAL_KEYWORDS = [
        'nota fiscal', 'nf-e', 'nfse', 'nfe', 'nfse',
        'chave acesso', 'danfe', 'xml fiscal',
        'icms', 'iss', 'ipi', 'imposto'
    ]
    
    def __init__(self):
        """Inicializa o categorizador"""
        self.logger = logging.getLogger(__name__)
    
    def categorize_file(
        self,
        filename: str,
        file_path: Optional[str] = None,
        file_content: Optional[bytes] = None,
        file_size: Optional[int] = None
    ) -> CategorizedFile:
        """
        Categoriza um arquivo individual
        
        Args:
            filename: Nome do arquivo
            file_path: Caminho do arquivo (opcional)
            file_content: Conteúdo do arquivo em bytes (opcional)
            file_size: Tamanho do arquivo em bytes (opcional)
            
        Returns:
            CategorizedFile com informações do arquivo categorizado
        """
        # Obter extensão
        extension = os.path.splitext(filename)[1].lower()
        
        # Determinar tipo de arquivo
        file_type = self.EXTENSION_TO_TYPE.get(extension, FileType.UNKNOWN)
        
        # Obter tamanho se não fornecido
        if file_size is None:
            if file_content:
                file_size = len(file_content)
            elif file_path and os.path.exists(file_path):
                file_size = os.path.getsize(file_path)
            else:
                file_size = 0
        
        # Determinar categoria baseada em extensão
        is_fiscal = extension in self.FISCAL_EXTENSIONS
        is_financial = extension in self.FINANCIAL_EXTENSIONS
        
        # Tentar detectar categoria mais específica pelo conteúdo
        category = self._detect_category(
            extension=extension,
            filename=filename,
            file_content=file_content,
            is_fiscal=is_fiscal,
            is_financial=is_financial
        )
        
        # Criar objeto categorizado
        categorized = CategorizedFile(
            filename=filename,
            file_path=file_path,
            file_content=file_content,
            file_size=file_size,
            extension=extension,
            file_type=file_type,
            category=category,
            is_fiscal_document=is_fiscal or category == FileCategory.FISCAL_DOCUMENT,
            is_financial_data=is_financial or category == FileCategory.FINANCIAL_DATA,
            metadata={
                'extension': extension,
                'file_type': file_type.value,
                'category': category.value
            }
        )
        
        self.logger.debug(
            f"Arquivo categorizado: {filename} -> "
            f"Tipo: {file_type.value}, Categoria: {category.value}, "
            f"Fiscal: {categorized.is_fiscal_document}, "
            f"Financeiro: {categorized.is_financial_data}"
        )
        
        return categorized
    
    def _detect_category(
        self,
        extension: str,
        filename: str,
        file_content: Optional[bytes],
        is_fiscal: bool,
        is_financial: bool
    ) -> FileCategory:
        """
        Detecta a categoria do arquivo baseado em múltiplos critérios
        
        Args:
            extension: Extensão do arquivo
            filename: Nome do arquivo
            file_content: Conteúdo do arquivo (opcional)
            is_fiscal: Se é extensão de documento fiscal
            is_financial: Se é extensão de dado financeiro
            
        Returns:
            FileCategory detectada
        """
        filename_lower = filename.lower()
        
        # Verificar palavras-chave no nome do arquivo
        has_fiscal_keyword = any(
            keyword in filename_lower for keyword in self.FISCAL_KEYWORDS
        )
        
        # Verificar conteúdo se disponível (primeiros 1000 bytes)
        has_fiscal_content = False
        if file_content:
            try:
                # Tentar decodificar como texto (primeiros 1000 bytes)
                preview = file_content[:1000].decode('utf-8', errors='ignore').lower()
                has_fiscal_content = any(
                    keyword in preview for keyword in self.FISCAL_KEYWORDS
                )
            except:
                pass
        
        # Priorizar detecção de documento fiscal
        if is_fiscal or has_fiscal_keyword or has_fiscal_content:
            return FileCategory.FISCAL_DOCUMENT
        
        # Dados financeiros
        if is_financial:
            return FileCategory.FINANCIAL_DATA
        
        # XML pode ser fiscal ou genérico
        if extension == '.xml':
            if has_fiscal_keyword or has_fiscal_content:
                return FileCategory.FISCAL_DOCUMENT
            return FileCategory.UNKNOWN
        
        # CSV e Excel são geralmente dados financeiros
        if extension in ['.csv', '.xlsx', '.xls', '.xlt', '.sxc']:
            return FileCategory.FINANCIAL_DATA
        
        return FileCategory.UNKNOWN
    
    def categorize_files(
        self,
        files: List[Any],
        file_paths: Optional[List[str]] = None
    ) -> Dict[FileCategory, List[CategorizedFile]]:
        """
        Categoriza múltiplos arquivos e agrupa por categoria
        
        Args:
            files: Lista de arquivos (UploadFile ou similar)
            file_paths: Lista de caminhos de arquivos (opcional)
            
        Returns:
            Dict agrupando arquivos por categoria
        """
        categorized_files: Dict[FileCategory, List[CategorizedFile]] = {
            category: [] for category in FileCategory
        }
        
        # Processar arquivos via upload
        if files:
            for file in files:
                if not hasattr(file, 'filename') or not file.filename:
                    continue
                
                # Ler conteúdo se disponível
                file_content = None
                if hasattr(file, 'read'):
                    try:
                        # Se já foi lido, pode estar em file.file
                        if hasattr(file, 'file'):
                            file_content = file.file.read()
                            file.file.seek(0)  # Resetar posição
                        elif hasattr(file, 'read'):
                            # Tentar ler (pode não funcionar se já foi consumido)
                            pass
                    except:
                        pass
                
                categorized = self.categorize_file(
                    filename=file.filename,
                    file_content=file_content,
                    file_size=getattr(file, 'size', None)
                )
                
                categorized_files[categorized.category].append(categorized)
        
        # Processar arquivos via caminhos
        if file_paths:
            for file_path in file_paths:
                if not os.path.exists(file_path):
                    self.logger.warning(f"Arquivo não encontrado: {file_path}")
                    continue
                
                filename = os.path.basename(file_path)
                file_size = os.path.getsize(file_path)
                
                categorized = self.categorize_file(
                    filename=filename,
                    file_path=file_path,
                    file_size=file_size
                )
                
                categorized_files[categorized.category].append(categorized)
        
        # Log de resumo
        total = sum(len(files) for files in categorized_files.values())
        self.logger.info(
            f"Categorização concluída: {total} arquivo(s) processado(s). "
            f"Financeiros: {len(categorized_files[FileCategory.FINANCIAL_DATA])}, "
            f"Fiscais: {len(categorized_files[FileCategory.FISCAL_DOCUMENT])}, "
            f"Outros: {len(categorized_files[FileCategory.UNKNOWN])}"
        )
        
        return categorized_files
    
    def get_files_by_category(
        self,
        categorized_files: Dict[FileCategory, List[CategorizedFile]],
        category: FileCategory
    ) -> List[CategorizedFile]:
        """
        Obtém arquivos de uma categoria específica
        
        Args:
            categorized_files: Dict de arquivos categorizados
            category: Categoria desejada
            
        Returns:
            Lista de arquivos da categoria
        """
        return categorized_files.get(category, [])
    
    def get_statistics(
        self,
        categorized_files: Dict[FileCategory, List[CategorizedFile]]
    ) -> Dict[str, Any]:
        """
        Obtém estatísticas da categorização
        
        Args:
            categorized_files: Dict de arquivos categorizados
            
        Returns:
            Dict com estatísticas
        """
        stats = {
            'total_files': sum(len(files) for files in categorized_files.values()),
            'by_category': {
                category.value: len(files)
                for category, files in categorized_files.items()
            },
            'by_type': {},
            'total_size': 0
        }
        
        # Estatísticas por tipo
        type_counts = {}
        for files in categorized_files.values():
            for file in files:
                file_type = file.file_type.value
                type_counts[file_type] = type_counts.get(file_type, 0) + 1
                stats['total_size'] += file.file_size
        
        stats['by_type'] = type_counts
        
        return stats

