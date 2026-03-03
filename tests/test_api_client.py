#!/usr/bin/env python3
"""
Cliente de teste para a API de Auditoria IA
NOTA: A API não utiliza autenticação JWT. O acesso é controlado por IP localhost.
"""
import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import requests
import json
import time
from typing import Dict, Any, List, Optional

class AuditoriaAPIClient:
    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url
        self.session = requests.Session()
    
    def health_check(self) -> Dict[str, Any]:
        """Verificar saúde da API"""
        try:
            response = self.session.get(f"{self.base_url}/health")
            return response.json()
        except Exception as e:
            print(f"❌ Erro no health check: {e}")
            return {}
    
    def analyze_file(self, file_path: str) -> Dict[str, Any]:
        """Analisar arquivo financeiro"""
        return self.analyze_files([file_path])
    
    def analyze_files(self, file_paths: List[str]) -> Dict[str, Any]:
        """Analisar múltiplos arquivos financeiros"""
        try:
            files = []
            file_handles = []
            
            for file_path in file_paths:
                file_handle = open(file_path, 'rb')
                file_handles.append(file_handle)
                filename = os.path.basename(file_path)
                files.append(('files', (filename, file_handle, 'application/octet-stream')))
            
            response = self.session.post(
                f"{self.base_url}/api/v1/analyze",
                files=files
            )
            
            for file_handle in file_handles:
                file_handle.close()
            
            if response.status_code == 200:
                return response.json()
            else:
                print(f"❌ Erro na análise: {response.status_code}")
                print(f"   Resposta: {response.text}")
                return {}
        except Exception as e:
            print(f"❌ Erro na análise: {e}")
            for file_handle in file_handles:
                try:
                    file_handle.close()
                except Exception:
                    pass
            return {}
    
    def analyze_documents(self, file_paths: List[str]) -> Dict[str, Any]:
        """Analisar documentos fiscais (NF-e, NFS-e, XML)"""
        try:
            files = []
            file_handles = []
            
            for file_path in file_paths:
                file_handle = open(file_path, 'rb')
                file_handles.append(file_handle)
                filename = os.path.basename(file_path)
                files.append(('files', (filename, file_handle, 'application/octet-stream')))
            
            response = self.session.post(
                f"{self.base_url}/api/v1/analyze/documents",
                files=files
            )
            
            for file_handle in file_handles:
                file_handle.close()
            
            if response.status_code == 200:
                return response.json()
            else:
                print(f"❌ Erro na análise de documentos: {response.status_code}")
                print(f"   Resposta: {response.text}")
                return {}
        except Exception as e:
            print(f"❌ Erro na análise de documentos: {e}")
            for file_handle in file_handles:
                try:
                    file_handle.close()
                except Exception:
                    pass
            return {}
    
    def correlate_documents(self, document_paths: List[str], transaction_path: str) -> Dict[str, Any]:
        """Correlacionar documentos fiscais com lançamentos"""
        try:
            files = []
            file_handles = []
            
            for doc_path in document_paths:
                file_handle = open(doc_path, 'rb')
                file_handles.append(file_handle)
                filename = os.path.basename(doc_path)
                files.append(('documents_files', (filename, file_handle, 'application/octet-stream')))
            
            trans_handle = open(transaction_path, 'rb')
            file_handles.append(trans_handle)
            trans_filename = os.path.basename(transaction_path)
            files.append(('transactions_file', (trans_filename, trans_handle, 'application/octet-stream')))
            
            response = self.session.post(
                f"{self.base_url}/api/v1/correlate",
                files=files
            )
            
            for file_handle in file_handles:
                file_handle.close()
            
            if response.status_code == 200:
                return response.json()
            else:
                print(f"❌ Erro na correlação: {response.status_code}")
                print(f"   Resposta: {response.text}")
                return {}
        except Exception as e:
            print(f"❌ Erro na correlação: {e}")
            for file_handle in file_handles:
                try:
                    file_handle.close()
                except Exception:
                    pass
            return {}
    
    def get_config(self) -> Dict[str, Any]:
        """Obter configurações"""
        try:
            response = self.session.get(f"{self.base_url}/api/v1/config")
            return response.json()
        except Exception as e:
            print(f"❌ Erro ao obter config: {e}")
            return {}
    
    def get_stats(self) -> Dict[str, Any]:
        """Obter estatísticas"""
        try:
            response = self.session.get(f"{self.base_url}/api/v1/stats")
            return response.json()
        except Exception as e:
            print(f"❌ Erro ao obter stats: {e}")
            return {}
    
    def get_system_info(self) -> Dict[str, Any]:
        """Obter informações do sistema"""
        try:
            response = self.session.get(f"{self.base_url}/api/v1/system")
            return response.json()
        except Exception as e:
            print(f"❌ Erro ao obter system info: {e}")
            return {}
    
    def test_analysis(self) -> Dict[str, Any]:
        """Testar análise com dados de exemplo"""
        try:
            response = self.session.post(f"{self.base_url}/api/v1/test")
            return response.json()
        except Exception as e:
            print(f"❌ Erro no teste: {e}")
            return {}

def main():
    """Função principal de teste"""
    print("🧪 Iniciando testes da API de Auditoria IA")
    print("=" * 50)
    
    client = AuditoriaAPIClient()
    
    print("\n1. Testando Health Check...")
    health = client.health_check()
    if health:
        print(f"✅ Status: {health.get('status', 'unknown')}")
        print(f"✅ Uptime: {health.get('uptime', 'unknown')}")
    else:
        print("❌ Health check falhou")
        return
    
    print("\n2. Verificando acesso à API...")
    print("   ℹ️  A API não utiliza autenticação JWT")
    print("   ℹ️  Acesso controlado por IP localhost")
    
    print("\n3. Testando Configurações...")
    config = client.get_config()
    if config.get('success'):
        print("✅ Configurações obtidas com sucesso")
    else:
        print("❌ Erro ao obter configurações")
    
    print("\n4. Testando Estatísticas...")
    stats = client.get_stats()
    if stats.get('success'):
        print("✅ Estatísticas obtidas com sucesso")
        print(f"   Total de requests: {stats['stats']['total_requests']}")
        print(f"   Análises bem-sucedidas: {stats['stats']['successful_analyses']}")
    else:
        print("❌ Erro ao obter estatísticas")
    
    print("\n5. Testando Informações do Sistema...")
    system_info = client.get_system_info()
    if system_info.get('success'):
        print("✅ Informações do sistema obtidas com sucesso")
        print(f"   CPU: {system_info['system']['cpu_percent']}%")
        print(f"   Memória: {system_info['system']['memory']['percent']}%")
    else:
        print("❌ Erro ao obter informações do sistema")
    
    print("\n6. Testando Análise de Dados...")
    test_result = client.test_analysis()
    if test_result.get('success'):
        print("✅ Análise de teste realizada com sucesso")
        data = test_result['data']
        print(f"   Total de transações: {data['total_transactions']}")
        print(f"   Anomalias detectadas: {data['anomalies_detected']}")
        print(f"   Tempo de processamento: {data['processing_time']:.3f}s")
    else:
        print("❌ Erro na análise de teste")
    
    print("\n7. Testando Análise de Arquivo Real...")
    test_files = [
        os.path.join(PROJECT_ROOT, "data", "sample_condominium_accounts.csv"),
        os.path.join(PROJECT_ROOT, "data", "teste_completo.csv"),
        os.path.join(PROJECT_ROOT, "test_data.csv"),
    ]
    
    for test_file in test_files:
        try:
            with open(test_file, 'r') as f:
                print(f"✅ Arquivo {test_file} encontrado")
                result = client.analyze_file(test_file)
                if result.get('success'):
                    data = result['data']
                    print(f"   ✅ Análise concluída: {data['anomalies_detected']} anomalias")
                    break
                else:
                    print(f"   ❌ Erro na análise do arquivo {test_file}")
        except FileNotFoundError:
            continue
    else:
        print("⚠️ Nenhum arquivo de teste encontrado")
    
    print("\n" + "=" * 50)
    print("🎉 Testes concluídos!")

if __name__ == "__main__":
    main()
