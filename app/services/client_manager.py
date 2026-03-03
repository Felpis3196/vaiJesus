"""
Gerenciador de Clientes
Gerencia identificação e histórico de dados por cliente
"""
import os
import json
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime
from pathlib import Path
from dataclasses import dataclass, asdict
from collections import defaultdict

logger = logging.getLogger(__name__)


@dataclass
class ClientData:
    """Dados de um cliente"""
    client_id: str
    created_at: datetime
    last_activity: datetime
    total_analyses: int = 0
    total_files: int = 0
    months_processed: List[str] = None  # Ex: ["2024-12", "2025-01"]
    metadata: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.months_processed is None:
            self.months_processed = []
        if self.metadata is None:
            self.metadata = {}


class ClientManager:
    """Gerencia clientes e seus dados históricos"""
    
    def __init__(self, data_dir: str = "client_data"):
        """
        Inicializa o gerenciador de clientes
        
        Args:
            data_dir: Diretório para armazenar dados de clientes
        """
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)
        
        # Cache em memória para acesso rápido
        self.clients: Dict[str, ClientData] = {}
        
        self.logger = logging.getLogger(__name__)
        self._load_clients()
    
    def _get_client_file(self, client_id: str) -> Path:
        """Retorna o caminho do arquivo de dados do cliente"""
        return self.data_dir / f"{client_id}.json"
    
    def _load_clients(self):
        """Carrega clientes do disco"""
        try:
            for file_path in self.data_dir.glob("*.json"):
                client_id = file_path.stem
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    
                    client = ClientData(
                        client_id=client_id,
                        created_at=datetime.fromisoformat(data['created_at']),
                        last_activity=datetime.fromisoformat(data['last_activity']),
                        total_analyses=data.get('total_analyses', 0),
                        total_files=data.get('total_files', 0),
                        months_processed=data.get('months_processed', []),
                        metadata=data.get('metadata', {})
                    )
                    self.clients[client_id] = client
                except Exception as e:
                    self.logger.warning(f"Erro ao carregar cliente {client_id}: {e}")
        except Exception as e:
            self.logger.error(f"Erro ao carregar clientes: {e}")
    
    def _save_client(self, client_id: str):
        """Salva dados do cliente no disco"""
        if client_id not in self.clients:
            return
        
        client = self.clients[client_id]
        file_path = self._get_client_file(client_id)
        
        try:
            data = {
                'client_id': client.client_id,
                'created_at': client.created_at.isoformat(),
                'last_activity': client.last_activity.isoformat(),
                'total_analyses': client.total_analyses,
                'total_files': client.total_files,
                'months_processed': client.months_processed,
                'metadata': client.metadata
            }
            
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            self.logger.error(f"Erro ao salvar cliente {client_id}: {e}")
    
    def get_or_create_client(self, client_id: str, metadata: Optional[Dict[str, Any]] = None) -> ClientData:
        """
        Obtém ou cria um cliente
        
        Args:
            client_id: ID único do cliente
            metadata: Metadados adicionais (opcional)
            
        Returns:
            ClientData do cliente
        """
        if client_id not in self.clients:
            # Criar novo cliente
            client = ClientData(
                client_id=client_id,
                created_at=datetime.now(),
                last_activity=datetime.now(),
                metadata=metadata or {}
            )
            self.clients[client_id] = client
            self._save_client(client_id)
            self.logger.info(f"Novo cliente criado: {client_id}")
        else:
            # Atualizar última atividade
            client = self.clients[client_id]
            client.last_activity = datetime.now()
            if metadata:
                client.metadata.update(metadata)
            self._save_client(client_id)
        
        return client
    
    def register_analysis(
        self,
        client_id: str,
        files_count: int,
        month: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ):
        """
        Registra uma análise para um cliente
        
        Args:
            client_id: ID do cliente
            files_count: Número de arquivos processados
            month: Mês da análise (formato: YYYY-MM, opcional)
            metadata: Metadados adicionais (opcional)
        """
        client = self.get_or_create_client(client_id, metadata)
        
        # Atualizar estatísticas
        client.total_analyses += 1
        client.total_files += files_count
        
        # Adicionar mês se fornecido
        if month and month not in client.months_processed:
            client.months_processed.append(month)
            client.months_processed.sort()  # Manter ordenado
        
        client.last_activity = datetime.now()
        self._save_client(client_id)
        
        self.logger.info(
            f"Análise registrada para cliente {client_id}: "
            f"{files_count} arquivo(s), mês: {month or 'N/A'}"
        )
    
    def get_client(self, client_id: str) -> Optional[ClientData]:
        """
        Obtém dados de um cliente
        
        Args:
            client_id: ID do cliente
            
        Returns:
            ClientData ou None se não encontrado
        """
        return self.clients.get(client_id)
    
    def get_client_history(self, client_id: str) -> Dict[str, Any]:
        """
        Obtém histórico de um cliente
        
        Args:
            client_id: ID do cliente
            
        Returns:
            Dict com histórico do cliente
        """
        client = self.get_client(client_id)
        
        if not client:
            return {
                "client_id": client_id,
                "exists": False,
                "message": "Cliente não encontrado"
            }
        
        return {
            "client_id": client.client_id,
            "exists": True,
            "created_at": client.created_at.isoformat(),
            "last_activity": client.last_activity.isoformat(),
            "total_analyses": client.total_analyses,
            "total_files": client.total_files,
            "months_processed": client.months_processed,
            "metadata": client.metadata
        }
    
    def list_clients(self, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Lista todos os clientes
        
        Args:
            limit: Limite de resultados
            
        Returns:
            Lista de dados de clientes
        """
        clients_list = list(self.clients.values())
        
        # Ordenar por última atividade (mais recentes primeiro)
        clients_list.sort(key=lambda c: c.last_activity, reverse=True)
        
        # Limitar resultados
        clients_list = clients_list[:limit]
        
        return [
            {
                "client_id": c.client_id,
                "created_at": c.created_at.isoformat(),
                "last_activity": c.last_activity.isoformat(),
                "total_analyses": c.total_analyses,
                "total_files": c.total_files,
                "months_processed": c.months_processed
            }
            for c in clients_list
        ]
    
    def extract_month_from_data(self, data: Dict[str, Any]) -> Optional[str]:
        """
        Extrai o mês dos dados processados (para uso futuro)
        
        Args:
            data: Dados processados (pode conter datas)
            
        Returns:
            String no formato YYYY-MM ou None
        """
        # Tentar extrair mês de diferentes campos
        # Ex: processed_at, data_emissao, etc.
        if 'processed_at' in data:
            try:
                dt = datetime.fromisoformat(data['processed_at'].replace('Z', '+00:00'))
                return dt.strftime('%Y-%m')
            except:
                pass
        
        # Se não conseguir extrair, usar mês atual
        return datetime.now().strftime('%Y-%m')
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Obtém estatísticas gerais
        
        Returns:
            Dict com estatísticas
        """
        total_clients = len(self.clients)
        total_analyses = sum(c.total_analyses for c in self.clients.values())
        total_files = sum(c.total_files for c in self.clients.values())
        
        # Clientes por mês
        clients_by_month = defaultdict(int)
        for client in self.clients.values():
            for month in client.months_processed:
                clients_by_month[month] += 1
        
        return {
            "total_clients": total_clients,
            "total_analyses": total_analyses,
            "total_files": total_files,
            "average_analyses_per_client": round(total_analyses / max(total_clients, 1), 2),
            "clients_by_month": dict(clients_by_month)
        }

