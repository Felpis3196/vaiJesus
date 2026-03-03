"""
Script de teste para validar o sistema com arquivo PDF real antes de produção
Testa o arquivo: Docs/pasta de maio 2025 imperador pdf.pdf
"""
import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import requests
import json
from pathlib import Path
from datetime import datetime

API_BASE_URL = "http://localhost:8000"
PDF_FILE_PATH = Path(PROJECT_ROOT) / "Docs" / "pasta de maio 2025 imperador pdf.pdf"

def print_section(title: str):
    """Imprime uma seção formatada"""
    print("\n" + "="*80)
    print(f"  {title}")
    print("="*80)

def print_result(success: bool, message: str, data: dict = None):
    """Imprime resultado formatado"""
    status = "✅ SUCESSO" if success else "❌ ERRO"
    print(f"\n{status}: {message}")
    if data:
        print(f"\nDados retornados:")
        print(json.dumps(data, indent=2, ensure_ascii=False))

def test_health_check():
    """Testa o health check da API"""
    print_section("1. HEALTH CHECK")
    try:
        response = requests.get(f"{API_BASE_URL}/health")
        if response.status_code == 200:
            print_result(True, "API está online", response.json())
            return True
        else:
            print_result(False, f"API retornou status {response.status_code}")
            return False
    except Exception as e:
        print_result(False, f"Erro ao conectar com a API: {str(e)}")
        return False

def test_analyze_documents():
    """Testa análise de documentos fiscais com o PDF real"""
    print_section("2. ANÁLISE DE DOCUMENTOS FISCAIS (PDF)")
    
    if not PDF_FILE_PATH.exists():
        print_result(False, f"Arquivo não encontrado: {PDF_FILE_PATH}")
        return False
    
    file_size_mb = PDF_FILE_PATH.stat().st_size / (1024 * 1024)
    print(f"📄 Arquivo: {PDF_FILE_PATH.name}")
    print(f"📊 Tamanho: {file_size_mb:.2f} MB")
    
    try:
        with open(PDF_FILE_PATH, 'rb') as f:
            files = {'files': (PDF_FILE_PATH.name, f, 'application/pdf')}
            params = {'client_id': 'teste_producao_maio_2025'}
            
            print(f"\n🔄 Enviando arquivo para análise...")
            response = requests.post(
                f"{API_BASE_URL}/api/v1/analyze/documents",
                files=files,
                params=params,
                timeout=300
            )
        
        if response.status_code == 200:
            result = response.json()
            print_result(True, "Análise de documentos concluída", result)
            
            if 'documents' in result:
                print(f"\n📊 ESTATÍSTICAS:")
                print(f"   Documentos processados: {result.get('documents_processed', 0)}")
                
                documents = result.get('documents', [])
                if documents:
                    print(f"\n📋 DADOS EXTRAÍDOS:")
                    for i, doc in enumerate(documents[:3], 1):
                        print(f"\n   Documento {i}:")
                        print(f"      Tipo: {doc.get('document_type', 'N/A')}")
                        print(f"      CNPJ Emissor: {doc.get('cnpj_emissor', 'N/A')}")
                        print(f"      Valor Total: R$ {doc.get('valor_total', 0):.2f}" if doc.get('valor_total') else "      Valor Total: N/A")
                        print(f"      ICMS: R$ {doc.get('icms', 0):.2f}" if doc.get('icms') else "      ICMS: N/A")
                        print(f"      ISS: R$ {doc.get('iss', 0):.2f}" if doc.get('iss') else "      ISS: N/A")
                        print(f"      IPI: R$ {doc.get('ipi', 0):.2f}" if doc.get('ipi') else "      IPI: N/A")
                    
                    if len(documents) > 3:
                        print(f"\n   ... e mais {len(documents) - 3} documento(s)")
                
                if 'file_categorization' in result:
                    cat = result['file_categorization']
                    print(f"\n📁 CATEGORIZAÇÃO:")
                    print(f"   Total de arquivos: {cat.get('total_files', 0)}")
                    print(f"   Por categoria: {cat.get('by_category', {})}")
                    print(f"   Por tipo: {cat.get('by_type', {})}")
            
            return True
        else:
            print_result(False, f"Erro HTTP {response.status_code}: {response.text}")
            return False
            
    except requests.exceptions.Timeout:
        print_result(False, "Timeout: O arquivo é muito grande ou a API demorou muito para responder")
        return False
    except Exception as e:
        print_result(False, f"Erro ao processar arquivo: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def test_analyze_financial_data():
    """Testa análise de dados financeiros com o PDF"""
    print_section("3. ANÁLISE DE DADOS FINANCEIROS (PDF)")
    
    if not PDF_FILE_PATH.exists():
        print_result(False, f"Arquivo não encontrado: {PDF_FILE_PATH}")
        return False
    
    try:
        with open(PDF_FILE_PATH, 'rb') as f:
            files = {'files': (PDF_FILE_PATH.name, f, 'application/pdf')}
            params = {'client_id': 'teste_producao_maio_2025'}
            
            print(f"\n🔄 Enviando arquivo para análise financeira...")
            response = requests.post(
                f"{API_BASE_URL}/api/v1/analyze",
                files=files,
                params=params,
                timeout=300
            )
        
        if response.status_code == 200:
            result = response.json()
            print_result(True, "Análise financeira concluída", result)
            
            if 'data' in result:
                data = result['data']
                print(f"\n📊 ESTATÍSTICAS:")
                print(f"   Total de transações: {data.get('total_transactions', 0)}")
                print(f"   Anomalias detectadas: {data.get('anomalies_detected', 0)}")
                print(f"   Arquivos processados: {data.get('files_processed', 0)}")
                print(f"   Tempo de processamento: {data.get('processing_time', 0):.2f}s")
                
                if 'summary' in data and 'financial_summary' in data['summary']:
                    financial = data['summary']['financial_summary']
                    print(f"\n💰 RESUMO FINANCEIRO:")
                    print(f"   Receitas: R$ {financial.get('total_receitas', 0):,.2f}")
                    print(f"   Despesas: R$ {financial.get('total_despesas', 0):,.2f}")
                    print(f"   Saldo: R$ {financial.get('saldo', 0):,.2f}")
            
            if 'job_id' in result:
                print(f"\n🆔 Job ID: {result['job_id']}")
                print(f"   Use este ID para consultar o status: GET /api/v1/analysis/status/{result['job_id']}")
            
            return True
        else:
            print_result(False, f"Erro HTTP {response.status_code}: {response.text}")
            return False
            
    except requests.exceptions.Timeout:
        print_result(False, "Timeout: O arquivo é muito grande ou a API demorou muito para responder")
        return False
    except Exception as e:
        print_result(False, f"Erro ao processar arquivo: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def test_system_info():
    """Testa informações do sistema"""
    print_section("4. INFORMAÇÕES DO SISTEMA")
    try:
        response = requests.get(f"{API_BASE_URL}/api/v1/system")
        if response.status_code == 200:
            result = response.json()
            print_result(True, "Informações do sistema obtidas", result)
            
            if 'system' in result:
                sys_info = result['system']
                print(f"\n💻 RECURSOS DO SISTEMA:")
                print(f"   CPU: {sys_info.get('cpu_percent', 0):.1f}%")
                print(f"   Memória: {sys_info.get('memory', {}).get('percent', 0):.1f}%")
                print(f"   Disco: {sys_info.get('disk', {}).get('percent', 0):.1f}%")
            
            return True
        else:
            print_result(False, f"Erro HTTP {response.status_code}")
            return False
    except Exception as e:
        print_result(False, f"Erro: {str(e)}")
        return False

def main():
    """Executa todos os testes"""
    print("\n" + "="*80)
    print("  🧪 TESTE DE PRODUÇÃO - ARQUIVO PDF REAL")
    print("  Arquivo: pasta de maio 2025 imperador pdf.pdf")
    print("  Data: " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    print("="*80)
    
    results = {
        'health_check': False,
        'analyze_documents': False,
        'analyze_financial': False,
        'system_info': False
    }
    
    results['health_check'] = test_health_check()
    if not results['health_check']:
        print("\n❌ API não está disponível. Verifique se o servidor está rodando.")
        return
    
    results['analyze_documents'] = test_analyze_documents()
    results['analyze_financial'] = test_analyze_financial_data()
    results['system_info'] = test_system_info()
    
    print_section("RESUMO DOS TESTES")
    total = len(results)
    passed = sum(1 for v in results.values() if v)
    
    print(f"\n📊 RESULTADOS:")
    for test_name, result in results.items():
        status = "✅ PASSOU" if result else "❌ FALHOU"
        print(f"   {test_name.replace('_', ' ').title()}: {status}")
    
    print(f"\n🎯 TOTAL: {passed}/{total} testes passaram")
    
    if passed == total:
        print("\n✅ TODOS OS TESTES PASSARAM! Sistema pronto para produção.")
    else:
        print(f"\n⚠️  {total - passed} teste(s) falharam. Revise antes de ir para produção.")
    
    print("\n" + "="*80)

if __name__ == "__main__":
    main()
