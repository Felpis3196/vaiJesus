# 📚 Documentação Completa da API - Auditoria IA

**Versão:** 1.0.0  
**Data:**  13 de Dezembro de 2025  
**Última Atualização:** 26 de Janeiro de 2026  
**Audience:** Desenvolvedores Fullstack

---

## 📋 Índice

1. [Visão Geral](#visão-geral)
2. [Configuração e Instalação](#configuração-e-instalação)
3. [Autenticação e Segurança](#autenticação-e-segurança)
4. [Endpoints da API](#endpoints-da-api)
5. [Estrutura de Dados](#estrutura-de-dados)
6. [Exemplos de Uso](#exemplos-de-uso)
7. [Tratamento de Erros](#tratamento-de-erros)
8. [Boas Práticas](#boas-práticas)
9. [Troubleshooting](#troubleshooting)
10. [Debug e Logs](#debug-e-logs)

---

## 🎯 Visão Geral

A API de Auditoria IA é um sistema completo para análise de dados financeiros de condomínios utilizando Inteligência Artificial. A API oferece:

- ✅ Análise financeira com IA avançada
- ✅ Detecção de anomalias
- ✅ Análise de documentos fiscais (NF-e, NFS-e, XML)
- ✅ Análise de impostos (ICMS, ISS, IPI)
- ✅ Correlação entre documentos e lançamentos
- ✅ Suporte a múltiplos formatos (CSV, Excel, PDF, ODS, XML)
- ✅ Extração automática de dados fiscais
- ✅ Detecção automática de tipo de documento

**Base URL:** `http://localhost:8000`  
**Versão da API:** `1.0.0`  
**Formato de Resposta:** JSON

---

## ⚙️ Configuração e Instalação

### **Pré-requisitos**

- Python 3.9+
- Ambiente virtual (recomendado)

### **Instalação**

```bash
# 1. Clonar repositório
git clone https://github.com/confiropasta/confiropasta_aiapp.git
cd confiropasta_aiapp

# 2. Criar ambiente virtual
python -m venv .venv

# 3. Ativar ambiente virtual
# Windows:
.\.venv\Scripts\activate
# Linux/Mac:
source .venv/bin/activate

# 4. Instalar dependências
pip install -r requirements.txt

# 5. Configurar variáveis de ambiente
cp config.env.example config.env
# Editar config.env com suas configurações

# 6. Iniciar servidor
python api_server.py
# Ou usar hot-reload:
python start_server.py
```

### **Verificar Instalação**

```bash
# Health check
curl http://localhost:8000/health

# Resposta esperada:
{
    "status": "healthy",
    "ai_module": {...},
    "system_resources": {...}
}
```

---

## 🔐 Autenticação e Segurança

### **⚠️ IMPORTANTE: Acesso Apenas Localhost**

A API **não utiliza autenticação JWT**. Em vez disso, o acesso é controlado por **IP localhost**.

**Bloqueio Automático:**
- ✅ Acesso de `localhost`, `127.0.0.1` → **PERMITIDO**
- ❌ Acesso de IPs externos → **BLOQUEADO (403 Forbidden)**

### **Endpoints Públicos (sem bloqueio)**
- `GET /` - Raiz da API
- `GET /health` - Health check
- `GET /docs` - Documentação Swagger
- `GET /redoc` - Documentação ReDoc
- `GET /openapi.json` - Esquema OpenAPI

### **Endpoints Protegidos (apenas localhost)**
Todos os endpoints `/api/v1/*` requerem acesso localhost.

### **Exemplo de Resposta de Bloqueio**

```json
{
    "error": "Forbidden",
    "message": "Acesso permitido apenas de localhost",
    "client_ip": "192.168.1.100"
}
```

**Status Code:** `403 Forbidden`

---

## 🔌 Endpoints da API

### **1. Health Check**

**Endpoint:** `GET /health`  
**Descrição:** Verifica status da API e recursos do sistema  
**Acesso:** Público

**Resposta:**
```json
{
    "status": "healthy",
    "ai_module": {
        "version": "3.0.0 - Advanced AI",
        "status": "operational"
    },
    "system_resources": {
        "cpu_percent": 15.5,
        "memory_percent": 45.2,
        "disk_percent": 60.0
    },
    "timestamp": "2024-11-24T10:30:00",
    "uptime": "2:30:15"
}
```

**Status Codes:**
- `200` - Sistema saudável
- `503` - Sistema degradado ou indisponível

---

### **2. Análise Financeira**

**Endpoint:** `POST /api/v1/analyze`  
**Descrição:** Analisa dados financeiros com IA avançada  
**Acesso:** Apenas localhost

**Parâmetros:**

| Parâmetro | Tipo | Obrigatório | Descrição |
|-----------|------|-------------|-----------|
| `files` | `List[File]` | Não* | Lista de arquivos via upload (multipart/form-data) |
| `file_paths` | `string` | Não* | Caminhos de arquivos separados por vírgula |

\* Um dos dois é obrigatório

**Formatos Suportados:**
- `.csv` - Arquivo CSV
- `.xlsx`, `.xls` - Planilhas Excel
- `.pdf` - Documentos PDF (extração de tabelas e texto)
- `.ods` - Planilhas OpenDocument (LibreOffice, OpenOffice)
- `.xml` - Arquivos XML (dados estruturados)

**Exemplo de Requisição (Upload):**

```bash
curl -X POST "http://localhost:8000/api/v1/analyze" \
  -F "files=@dados1.csv" \
  -F "files=@dados2.xlsx" \
  -F "files=@relatorio.pdf"
```

**Exemplo de Requisição (Caminhos Físicos):**

```bash
curl -X POST "http://localhost:8000/api/v1/analyze" \
  -d "file_paths=/caminho/dados1.csv,/caminho/dados2.xlsx"
```

**Resposta de Sucesso:**

```json
{
    "success": true,
    "data": {
        "files_processed": 3,
        "file_metadata": [
            {
                "filename": "dados1.csv",
                "file_size": 1024,
                "rows": 100
            }
        ],
        "total_rows": 250,
        "anomalies_detected": 5,
        "total_transactions": 250,
        "ai_analysis": {...},
        "nlp_analysis": {...},
        "predictive_analysis": {...},
        "processed_at": "2024-11-24T10:30:00",
        "api_version": "1.0.0",
        "processing_time": 2.5
    },
    "message": "Analysis completed successfully for 3 file(s)"
}
```

**Status Codes:**
- `200` - Análise concluída com sucesso
- `400` - Erro na requisição (arquivo inválido, formato não suportado)
- `500` - Erro interno do servidor

---

### **3. Análise de Documentos Fiscais**

**Endpoint:** `POST /api/v1/analyze/documents`  
**Descrição:** Extrai e analisa dados de documentos fiscais (NF-e, NFS-e, XML)  
**Acesso:** Apenas localhost

**Parâmetros:**

| Parâmetro | Tipo | Obrigatório | Descrição |
|-----------|------|-------------|-----------|
| `files` | `List[File]` | Não* | Documentos fiscais via upload |
| `file_paths` | `string` | Não* | Caminhos de documentos separados por vírgula |

\* Um dos dois é obrigatório

**Formatos de Arquivo Suportados:**
- `.pdf` - Documentos PDF (extração de texto)
- `.ods` - Planilhas OpenDocument
- `.xml` - Arquivos XML fiscais
- `.csv`, `.txt` - Arquivos de texto

**Tipos de Documentos Suportados:**
- **NF-e** (Nota Fiscal Eletrônica) - Detecção automática por XML ou palavras-chave
- **NFS-e** (Nota Fiscal de Serviços) - Detecção automática por XML ou palavras-chave
- **XML Fiscal** - Parsing estruturado com múltiplos XPath
- **Documentos genéricos** - Extração genérica de CNPJ, valores e datas

**Detecção Automática:**
O sistema detecta automaticamente o tipo de documento baseado em:
- Estrutura XML (`<?xml`, `<nfe`, `<nfse`)
- Palavras-chave no conteúdo
- Padrões de dados fiscais

**Exemplo de Requisição:**

```bash
# Upload de múltiplos documentos
curl -X POST "http://localhost:8000/api/v1/analyze/documents" \
  -F "files=@nota_fiscal.xml" \
  -F "files=@prestacao_contas.ods" \
  -F "files=@documento.pdf"

# Ou usando caminhos físicos
curl -X POST "http://localhost:8000/api/v1/analyze/documents" \
  -d "file_paths=/caminho/nota_fiscal.xml,/caminho/documento.pdf"
```

**Resposta de Sucesso:**

```json
{
    "success": true,
    "documents_processed": 2,
    "documents": [
        {
            "document_type": "NF-e",
            "cnpj_emissor": "12.345.678/0001-90",
            "cnpj_destinatario": "98.765.432/0001-10",
            "numero_nf": "123456",
            "serie": "1",
            "data_emissao": "2024-01-15",
            "valor_total": 5000.00,
            "valor_icms": 900.00,
            "valor_ipi": 200.00,
            "chave_acesso": "35200112345678901234567890123456789012345678",
            "itens": [],
            "filename": "nota_fiscal.xml",
            "file_size": 2048
        },
        {
            "document_type": "Generic",
            "cnpj_cpf": "12.345.678/0001-90",
            "valor": 1500.00,
            "data": "2024-01-20",
            "descricao": "Prestação de serviços",
            "filename": "documento.pdf",
            "file_size": 77996
        }
    ],
    "message": "Processed 2 document(s)"
}
```

**Notas Importantes:**
- Campos podem retornar `null` se não forem encontrados no documento
- O tipo de documento é detectado automaticamente
- PDFs escaneados (imagens) não terão texto extraído (limitação conhecida)
- Logs detalhados estão disponíveis em `api.log` para debug

---

### **4. Análise de Impostos**

**Endpoint:** `POST /api/v1/analyze/taxes`  
**Descrição:** Analisa impostos de documentos fiscais e compara com lançamentos  
**Acesso:** Apenas localhost

**Parâmetros:**

| Parâmetro | Tipo | Obrigatório | Descrição |
|-----------|------|-------------|-----------|
| `files` | `List[File]` | Não* | Documentos fiscais via upload |
| `file_paths` | `string` | Não* | Caminhos de documentos |
| `transactions_file` | `File` | Não** | Arquivo com lançamentos (upload) |
| `transactions_path` | `string` | Não** | Caminho do arquivo com lançamentos |

\* Um dos dois é obrigatório para documentos  
\** Um dos dois é obrigatório para lançamentos

**Exemplo de Requisição:**

```bash
# Com upload de arquivos
curl -X POST "http://localhost:8000/api/v1/analyze/taxes" \
  -F "files=@nota_fiscal.xml" \
  -F "files=@nfse.pdf" \
  -F "transactions_file=@lancamentos.csv"

# Ou usando caminhos físicos
curl -X POST "http://localhost:8000/api/v1/analyze/taxes" \
  -d "file_paths=/caminho/nota_fiscal.xml,/caminho/nfse.pdf" \
  -d "transactions_path=/caminho/lancamentos.csv"
```

**Resposta de Sucesso:**

```json
{
    "success": true,
    "tax_analysis": {
        "total_icms": 1800.00,
        "total_iss": 500.00,
        "total_ipi": 200.00,
        "total_impostos": 2500.00,
        "tax_by_document": [
            {
                "document_id": "35200112345678901234567890123456789012345678",
                "icms": 900.00,
                "iss": 0.00,
                "ipi": 200.00
            },
            {
                "document_id": "123456",
                "icms": 900.00,
                "iss": 500.00,
                "ipi": 0.00
            }
        ],
        "discrepancies": []
    },
    "documents_processed": 2,
    "message": "Tax analysis completed"
}
```

**Notas Importantes:**
- ✅ **Correção Implementada:** Valores `None` são convertidos para `0.0` automaticamente
- ✅ **Múltiplos Documentos:** Suporta análise de múltiplos documentos em uma única requisição
- ✅ **Logs Detalhados:** Logs indicam quando impostos são encontrados ou não
- ⚠️ **Se todos os valores forem `0`:** Verifique se a extração de dados está funcionando (veja logs)
- ⚠️ **`document_id`:** Pode ser `chave_acesso`, `numero_nf` ou `numero_nfse` (o primeiro disponível)

---

### **5. Correlação Documentos ↔ Lançamentos**

**Endpoint:** `POST /api/v1/correlate`  
**Descrição:** Correlaciona documentos fiscais com lançamentos financeiros  
**Acesso:** Apenas localhost

**Parâmetros:**

| Parâmetro | Tipo | Obrigatório | Descrição |
|-----------|------|-------------|-----------|
| `documents_files` | `List[File]` | Não* | Documentos fiscais via upload |
| `documents_paths` | `string` | Não* | Caminhos de documentos |
| `transactions_file` | `File` | Não** | Arquivo com lançamentos (upload) |
| `transactions_path` | `string` | Não** | Caminho do arquivo com lançamentos |

\* Um dos dois é obrigatório para documentos  
\** Um dos dois é obrigatório para lançamentos

**Exemplo de Requisição:**

```bash
curl -X POST "http://localhost:8000/api/v1/correlate" \
  -F "documents_files=@nfe1.xml" \
  -F "documents_files=@nfe2.xml" \
  -F "transactions_file=@lancamentos.csv"
```

**Resposta de Sucesso:**

```json
{
    "success": true,
    "correlations": [
        {
            "document_id": "35200112345678901234567890123456789012345678",
            "document_type": "NF-e",
            "document_value": 5000.00,
            "transaction_id": 0,
            "transaction_value": 5000.00,
            "transaction_date": "2024-01-15",
            "match_confidence": 0.95,
            "status": "matched"
        }
    ],
    "statistics": {
        "total_documents": 2,
        "total_transactions": 10,
        "matched": 2,
        "unmatched_documents": 0,
        "unmatched_transactions": 8,
        "match_rate": 100.0
    },
    "message": "Correlation analysis completed"
}
```

**Algoritmo de Matching:**
- **Valor** (peso 40%): Tolerância de 1%
- **Data** (peso 30%): Tolerância de 30 dias
- **CNPJ/CPF** (peso 30%): Comparação exata

**Confiança:**
- `match_confidence >= 0.5` → `status: "matched"`
- `match_confidence < 0.5` → `status: "unmatched"`

---

### **6. Configurações**

**Endpoint:** `GET /api/v1/config`  
**Descrição:** Obter configurações da IA  
**Acesso:** Apenas localhost

**Resposta:**
```json
{
    "success": true,
    "config": {
        "anomaly_detection": {...},
        "data_processing": {...},
        "report": {...}
    }
}
```

**Endpoint:** `POST /api/v1/config`  
**Descrição:** Atualizar configurações da IA  
**Acesso:** Apenas localhost

**Body:**
```json
{
    "anomaly_detection": {
        "z_score_threshold": 3.0
    }
}
```

---

### **7. Estatísticas**

**Endpoint:** `GET /api/v1/stats`  
**Descrição:** Obter estatísticas da API  
**Acesso:** Apenas localhost

**Resposta:**
```json
{
    "success": true,
    "stats": {
        "total_requests": 150,
        "successful_analyses": 145,
        "failed_analyses": 5,
        "average_processing_time": 2.3,
        "uptime": "5:30:00",
        "success_rate": 96.67
    }
}
```

---

### **8. Informações do Sistema**

**Endpoint:** `GET /api/v1/system`  
**Descrição:** Obter informações do sistema  
**Acesso:** Apenas localhost

**Resposta:**
```json
{
    "success": true,
    "system": {
        "cpu_percent": 15.5,
        "memory": {
            "total": 8589934592,
            "available": 4294967296,
            "percent": 50.0
        },
        "disk": {
            "total": 107374182400,
            "used": 64424509440,
            "free": 42949672960,
            "percent": 60.0
        },
        "processes": 150,
        "boot_time": "2024-11-24T05:00:00"
    }
}
```

---

### **9. Teste de Análise**

**Endpoint:** `POST /api/v1/test`  
**Descrição:** Testa funcionamento da IA com dados de exemplo  
**Acesso:** Apenas localhost

**Resposta:**
```json
{
    "success": true,
    "message": "Test analysis completed",
    "data": {
        "anomalies_detected": 1,
        "total_transactions": 3,
        ...
    }
}
```

---

## 📊 Estrutura de Dados

### **Formato de Arquivo CSV/Excel**

**Colunas Obrigatórias:**
- `Data` ou `data` - Data da transação (formato: YYYY-MM-DD ou DD/MM/YYYY)
- `Descricao` ou `descricao` - Descrição da transação
- `Tipo` ou `tipo` - Tipo: "Receita" ou "Despesa"
- `Valor` ou `valor` - Valor numérico da transação

**Colunas Opcionais:**
- `Categoria` ou `categoria` - Categoria da transação
- `Fornecedor` ou `fornecedor` - Nome do fornecedor
- `Nota Fiscal` ou `nota_fiscal` - Número da nota fiscal

**Nota:** O sistema normaliza automaticamente os nomes das colunas para minúsculas internamente, então tanto `Data` quanto `data` funcionam.

**Exemplo de CSV:**
```csv
Data,Descricao,Tipo,Valor,Fornecedor
2024-01-01,Taxa de condomínio,Receita,5000.00,
2024-01-02,Manutenção elevador,Despesa,1500.00,Empresa XYZ
2024-01-03,Despesa suspeita,Despesa,50000.00,Fornecedor ABC
```

---

## 💻 Exemplos de Uso

### **JavaScript/TypeScript (Fetch API)**

```javascript
// Análise financeira
async function analyzeFinancialData(files) {
    const formData = new FormData();
    files.forEach(file => {
        formData.append('files', file);
    });
    
    const response = await fetch('http://localhost:8000/api/v1/analyze', {
        method: 'POST',
        body: formData
    });
    
    if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
    }
    
    const result = await response.json();
    return result;
}

// Análise de documentos
async function analyzeDocuments(documentFiles) {
    const formData = new FormData();
    documentFiles.forEach(file => {
        formData.append('files', file);
    });
    
    const response = await fetch('http://localhost:8000/api/v1/analyze/documents', {
        method: 'POST',
        body: formData
    });
    
    return await response.json();
}

// Correlação
async function correlateDocuments(docFiles, transactionFile) {
    const formData = new FormData();
    docFiles.forEach(file => formData.append('documents_files', file));
    formData.append('transactions_file', transactionFile);
    
    const response = await fetch('http://localhost:8000/api/v1/correlate', {
        method: 'POST',
        body: formData
    });
    
    return await response.json();
}
```

---

### **Python (requests)**

```python
import requests

# Análise financeira
def analyze_financial_data(file_paths):
    """Analisa dados financeiros"""
    url = "http://localhost:8000/api/v1/analyze"
    
    # Opção 1: Upload de arquivos
    files = [('files', open(path, 'rb')) for path in file_paths]
    response = requests.post(url, files=files)
    
    # Opção 2: Caminhos físicos
    # data = {'file_paths': ','.join(file_paths)}
    # response = requests.post(url, data=data)
    
    return response.json()

# Análise de documentos
def analyze_documents(document_paths):
    """Analisa documentos fiscais"""
    url = "http://localhost:8000/api/v1/analyze/documents"
    
    files = [('files', open(path, 'rb')) for path in document_paths]
    response = requests.post(url, files=files)
    result = response.json()
    
    # Verificar se dados foram extraídos
    if result.get('success'):
        for doc in result.get('documents', []):
            if not any([doc.get('cnpj_emissor'), doc.get('valor_total'), 
                       doc.get('cnpj_cpf'), doc.get('valor')]):
                print(f"⚠️ Aviso: Nenhum dado extraído de {doc.get('filename')}")
                print("   Verifique logs do servidor para mais detalhes")
    
    return result

# Análise de impostos
def analyze_taxes(document_paths, transaction_path=None):
    """Analisa impostos de documentos fiscais"""
    url = "http://localhost:8000/api/v1/analyze/taxes"
    
    files = [('files', open(path, 'rb')) for path in document_paths]
    if transaction_path:
        files.append(('transactions_file', open(transaction_path, 'rb')))
    
    response = requests.post(url, files=files)
    result = response.json()
    
    # Verificar se impostos foram encontrados
    if result.get('success'):
        tax_analysis = result.get('tax_analysis', {})
        if tax_analysis.get('total_impostos', 0) == 0:
            print("⚠️ Aviso: Nenhum imposto encontrado")
            print("   Verifique se os documentos contêm dados fiscais")
            print("   Verifique logs do servidor para mais detalhes")
    
    return result

# Correlação
def correlate(doc_paths, transaction_path):
    """Correlaciona documentos com lançamentos"""
    url = "http://localhost:8000/api/v1/correlate"
    
    files = [
        ('documents_files', open(path, 'rb')) for path in doc_paths
    ]
    files.append(('transactions_file', open(transaction_path, 'rb')))
    
    response = requests.post(url, files=files)
    return response.json()
```

---

### **React/Next.js (Exemplo Completo)**

```typescript
import { useState } from 'react';

interface AnalysisResult {
    success: boolean;
    data: any;
    message: string;
}

export function useAuditAPI() {
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState<string | null>(null);
    
    const analyzeFiles = async (files: File[]): Promise<AnalysisResult> => {
        setLoading(true);
        setError(null);
        
        try {
            const formData = new FormData();
            files.forEach(file => {
                formData.append('files', file);
            });
            
            const response = await fetch('http://localhost:8000/api/v1/analyze', {
                method: 'POST',
                body: formData
            });
            
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            
            const result = await response.json();
            return result;
            
        } catch (err) {
            const errorMessage = err instanceof Error ? err.message : 'Unknown error';
            setError(errorMessage);
            throw err;
        } finally {
            setLoading(false);
        }
    };
    
    const analyzeDocuments = async (docFiles: File[]): Promise<any> => {
        setLoading(true);
        try {
            const formData = new FormData();
            docFiles.forEach(file => {
                formData.append('files', file);
            });
            
            const response = await fetch('http://localhost:8000/api/v1/analyze/documents', {
                method: 'POST',
                body: formData
            });
            
            return await response.json();
        } finally {
            setLoading(false);
        }
    };
    
    return {
        analyzeFiles,
        analyzeDocuments,
        loading,
        error
    };
}
```

---

## ⚠️ Tratamento de Erros

### **Códigos de Status HTTP**

| Código | Significado | Quando Ocorre |
|--------|-------------|---------------|
| `200` | OK | Requisição bem-sucedida |
| `400` | Bad Request | Dados inválidos, formato não suportado |
| `403` | Forbidden | Acesso bloqueado (IP não local) |
| `500` | Internal Server Error | Erro interno do servidor |
| `503` | Service Unavailable | Serviço indisponível |

### **Estrutura de Erro**

```json
{
    "error": "Error type",
    "message": "Descrição detalhada do erro",
    "detail": "Informações adicionais (em desenvolvimento)"
}
```

### **Exemplos de Erros**

**Erro 400 - Arquivo Inválido:**
```json
{
    "detail": "File type not supported. Allowed: ['.csv', '.xlsx', '.xls', '.pdf']"
}
```

**Erro 400 - Nenhum Arquivo:**
```json
{
    "detail": "No files provided. Use 'files' (upload) or 'file_paths' (comma-separated paths)"
}
```

**Erro 403 - Acesso Bloqueado:**
```json
{
    "error": "Forbidden",
    "message": "Acesso permitido apenas de localhost",
    "client_ip": "192.168.1.100"
}
```

**Erro 500 - Erro Interno:**
```json
{
    "detail": "Internal server error: [mensagem de erro]"
}
```

---

## ✅ Boas Práticas

### **1. Validação de Arquivos**

```javascript
// Validar antes de enviar
function validateFile(file) {
    const allowedTypes = ['.csv', '.xlsx', '.xls', '.pdf'];
    const extension = file.name.toLowerCase().substring(file.name.lastIndexOf('.'));
    
    if (!allowedTypes.includes(extension)) {
        throw new Error(`Formato não suportado: ${extension}`);
    }
    
    const maxSize = 50 * 1024 * 1024; // 50MB
    if (file.size > maxSize) {
        throw new Error(`Arquivo muito grande: ${(file.size / 1024 / 1024).toFixed(2)}MB`);
    }
    
    return true;
}
```

### **2. Tratamento de Erros**

```javascript
async function safeApiCall(apiFunction) {
    try {
        const result = await apiFunction();
        return { success: true, data: result };
    } catch (error) {
        if (error.response) {
            // Erro da API
            return {
                success: false,
                error: error.response.data.detail || error.response.data.message,
                status: error.response.status
            };
        } else {
            // Erro de rede
            return {
                success: false,
                error: 'Erro de conexão com a API',
                status: 0
            };
        }
    }
}
```

### **3. Loading States**

```javascript
const [loading, setLoading] = useState(false);
const [progress, setProgress] = useState(0);

async function uploadWithProgress(files) {
    setLoading(true);
    
    const formData = new FormData();
    files.forEach(file => formData.append('files', file));
    
    try {
        const response = await fetch('http://localhost:8000/api/v1/analyze', {
            method: 'POST',
            body: formData,
            // Nota: Fetch API não suporta progress nativamente
            // Use XMLHttpRequest ou biblioteca como axios para progress
        });
        
        return await response.json();
    } finally {
        setLoading(false);
    }
}
```

### **4. Cache de Resultados**

```javascript
// Cache simples em memória
const cache = new Map();

async function analyzeWithCache(files) {
    const cacheKey = files.map(f => `${f.name}-${f.size}`).join('|');
    
    if (cache.has(cacheKey)) {
        return cache.get(cacheKey);
    }
    
    const result = await analyzeFiles(files);
    cache.set(cacheKey, result);
    
    return result;
}
```

---

## 🔧 Troubleshooting

### **Problema: "Acesso bloqueado (403)"**

**Causa:** Requisição vinda de IP não localhost

**Solução:**
- Verificar se está acessando de `localhost` ou `127.0.0.1`
- Se estiver em rede local, adicionar IP ao `ALLOWED_IPS` em `api_server.py`
- Verificar se não há proxy redirecionando requisições

---

### **Problema: "File type not supported"**

**Causa:** Formato de arquivo não suportado

**Formatos Suportados:**
- **Análise Financeira (`/api/v1/analyze`):** `.csv`, `.xlsx`, `.xls`, `.pdf`, `.ods`, `.xml`
- **Análise de Documentos (`/api/v1/analyze/documents`):** `.pdf`, `.ods`, `.xml`, `.csv`, `.txt`

**Solução:**
- Verificar extensão do arquivo
- Converter arquivo para formato suportado
- Verificar se extensão está correta (não apenas renomear)

---

### **Problema: "PDF processing requires pdfplumber"**

**Causa:** Biblioteca `pdfplumber` não instalada

**Solução:**
```bash
pip install pdfplumber>=0.9.0
```

---

### **Problema: "No valid files could be loaded"**

**Causa:** Nenhum arquivo válido foi processado

**Possíveis Motivos:**
- Arquivos com formato inválido
- Arquivos muito grandes
- Erro ao ler arquivo
- Estrutura de dados incorreta

**Solução:**
- Verificar logs do servidor (`api.log`)
- Validar estrutura dos arquivos antes de enviar
- Verificar tamanho dos arquivos

---

### **Problema: "Document analyzer not available"**

**Causa:** Módulo `DocumentAnalyzer` não pôde ser importado

**Solução:**
- Verificar se `services/document_analyzer.py` existe
- Verificar se todas as dependências estão instaladas:
  ```bash
  pip install pdfplumber lxml odfpy beautifulsoup4
  ```
- Verificar logs do servidor para erros de importação

---

### **Problema: "Nenhum dado extraído" (campos retornando null)**

**Causa:** Extração de dados não está funcionando

**Possíveis Motivos:**
1. **PDF escaneado:** PDF é uma imagem, não tem texto extraível
2. **Formato diferente:** Padrões de regex não estão encontrando os dados
3. **Tipo de documento não detectado:** Documento classificado como "Generic"
4. **Encoding incorreto:** Caracteres corrompidos na extração

**Solução:**
1. **Verificar logs do servidor:**
   ```bash
   Get-Content api.log -Tail 100
   ```
   Procure por:
   - "Extraído X caracteres" - Verifica se texto foi extraído
   - "Tipo de documento detectado" - Verifica detecção
   - "Campos extraídos: X/Y" - Verifica quantos campos foram encontrados

2. **Verificar se texto foi extraído:**
   - Se log mostra "0 caracteres" → PDF pode ser escaneado
   - Se log mostra texto mas "0 campos extraídos" → Padrões não estão encontrando dados

3. **Testar com arquivo XML real:**
   - XMLs fiscais devem ter melhor taxa de sucesso
   - Verificar se XML está bem formado

4. **Ajustar padrões (se necessário):**
   - Verificar formato real dos dados no arquivo
   - Ajustar padrões de regex em `services/document_analyzer.py`

---

### **Problema: "Todos os valores de impostos são 0"**

**Causa:** Extração de dados não está funcionando ou valores não foram encontrados

**Solução:**
1. **Verificar se documentos foram processados:**
   - Verificar `documents_processed` na resposta
   - Se for 0, nenhum documento foi processado

2. **Verificar logs:**
   - Procure por "Nenhum imposto extraído"
   - Verifique quais campos estão disponíveis no documento

3. **Verificar se extração de dados está funcionando:**
   - Testar endpoint `/api/v1/analyze/documents` primeiro
   - Se campos estão `null` lá, o problema é na extração, não no cálculo

4. **Verificar formato do documento:**
   - PDFs escaneados não funcionam
   - Verificar se documento tem dados fiscais estruturados

---

## 🔍 Debug e Logs

### **Logs do Servidor**

Os logs são salvos em `api.log` e também exibidos no console. Use os logs para:

- **Verificar extração de texto:** Procure por "Extraído X caracteres do PDF/ODS"
- **Verificar detecção de tipo:** Procure por "Tipo de documento detectado"
- **Verificar extração de dados:** Procure por "Campos extraídos: X/Y"
- **Identificar erros:** Procure por "ERRO" ou "WARNING"

**Exemplo de Logs:**

```
INFO - Extraído 15234 caracteres do PDF documento.pdf
DEBUG - Tipo de documento detectado automaticamente: NF-e
INFO - Extração concluída. Tipo: NF-e, Campos extraídos: 8/11
INFO - Documento 352001123456...: ICMS=900.0, ISS=0.0, IPI=200.0
WARNING - Documento null: Nenhum imposto extraído. Tipo: Generic
```

### **Verificar Logs em Tempo Real**

```bash
# Windows PowerShell
Get-Content api.log -Tail 50 -Wait

# Linux/Mac
tail -f api.log
```

### **Níveis de Log**

Configure o nível de log via variável de ambiente:
```bash
AI_AUDIT_LOG_LEVEL=DEBUG  # DEBUG, INFO, WARNING, ERROR
```

---

## 📝 Notas Importantes

### **Limitações Conhecidas:**

1. **PDF Escaneado:** PDFs escaneados (imagens) não terão texto extraído
   - **Solução Futura:** Implementar OCR (pytesseract ou serviço externo)
   - **Workaround:** Converter PDF escaneado para PDF com texto usando ferramentas externas

2. **PDF:** Extração limitada a texto e tabelas estruturadas
   - PDFs com texto extraível: ✅ Funciona
   - PDFs escaneados: ❌ Não funciona (requer OCR)

3. **XML:** Suporta XMLs fiscais padrão SEFAZ
   - NF-e, NFS-e: ✅ Suportado
   - XMLs genéricos: ✅ Suportado (extração genérica)

4. **ODS:** Limitado a 1000 linhas para conversão em texto
   - Arquivos muito grandes podem ter dados truncados
   - **Solução:** Processar em lotes menores

5. **Correlação:** Matching baseado em heurísticas   
    - Algoritmo: Valor (40%), Data (30%), CNPJ (30%)
    - Pode precisar de ajustes para casos específicos

6. **Tamanho:** Arquivos muito grandes podem causar timeout
   - **Recomendação:** Dividir arquivos grandes em lotes menores
   - **Limite padrão:** 50MB (configurável via `DATA_PROCESSING_MAX_FILE_SIZE_MB`)

7. **Encoding:** Suporta UTF-8 e Latin-1
   - Outros encodings podem não ser detectados automaticamente
   - **Solução:** Converter arquivos para UTF-8 antes de enviar

### **Recomendações:**

1. **Arquivos Grandes:** Dividir em lotes menores
2. **Múltiplos Arquivos:** Processar em paralelo quando possível
3. **Cache:** Implementar cache de resultados para arquivos idênticos
4. **Validação:** Sempre validar arquivos antes de enviar
5. **Logs:** Sempre verificar logs quando houver problemas de extração
6. **Testes:** Testar com arquivos reais antes de integrar em produção

---

## 🔄 **Atualizações Recentes (17/12/2025)**

### **✅ Melhorias Implementadas:**

1. **Suporte a ODS e XML:**
   - ✅ Extração de texto de arquivos ODS usando `pandas` com engine `odf`
   - ✅ Suporte a XML fiscal com parsing melhorado usando `lxml`
   - ✅ Detecção automática de tipo de documento

2. **Extração de Texto Melhorada:**
   - ✅ PDF: Extração página por página usando `pdfplumber`
   - ✅ ODS: Conversão de DataFrame para texto estruturado
   - ✅ XML: Decodificação com fallback UTF-8/Latin-1

3. **Padrões de Regex Melhorados:**
   - ✅ Múltiplos padrões para CNPJ (formatado e sem formatação)
   - ✅ Múltiplos padrões para valores (ICMS, ISS, IPI)
   - ✅ Múltiplos padrões para datas e chave de acesso

4. **Endpoint de Impostos Corrigido:**
   - ✅ Tratamento correto de valores `None` (converte para `0.0`)
   - ✅ Logs detalhados para debug
   - ✅ Suporte a múltiplos documentos

5. **Logs Detalhados:**
   - ✅ Logs de extração de texto
   - ✅ Logs de detecção de tipo
   - ✅ Logs de campos extraídos
   - ✅ Logs de análise de impostos

### **📦 Dependências Adicionadas:**

- `pdfplumber>=0.9.0` - Extração de texto de PDF
- `lxml>=4.9.0` - Parsing de XML
- `odfpy>=1.4.1` - Suporte a arquivos ODS
- `beautifulsoup4>=4.10.0` - Parsing de HTML/XML

### **⚠️ Limitações Conhecidas:**

- PDFs escaneados não terão texto extraído (requer OCR - não implementado)
- Padrões de regex podem não cobrir todos os formatos possíveis
- Detecção de tipo pode falhar em casos específicos

---

## 🚀 **Guia de Integração Rápida**

### **1. Verificar Instalação**

```bash
# Ativar ambiente virtual
.venv\Scripts\Activate.ps1  # Windows
source .venv/bin/activate   # Linux/Mac

# Verificar dependências
python -c "import pdfplumber, pandas, lxml, odf; print('✅ Todas OK')"

# Iniciar servidor
python start_server.py
```

### **2. Testar Endpoints**

```bash
# Health check
curl http://localhost:8000/health

# Teste de análise
curl -X POST http://localhost:8000/api/v1/test

# Análise de documentos
curl -X POST "http://localhost:8000/api/v1/analyze/documents" \
  -F "files=@documento.pdf"
```

### **3. Integração Frontend (React/Next.js)**

```typescript
// Hook personalizado
import { useState } from 'react';

export function useDocumentAnalysis() {
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState<string | null>(null);
    
    const analyzeDocuments = async (files: File[]) => {
        setLoading(true);
        setError(null);
        
        try {
            const formData = new FormData();
            files.forEach(file => formData.append('files', file));
            
            const response = await fetch('http://localhost:8000/api/v1/analyze/documents', {
                method: 'POST',
                body: formData
            });
            
            if (!response.ok) {
                const errorData = await response.json();
                throw new Error(errorData.detail || 'Erro ao analisar documentos');
            }
            
            const result = await response.json();
            
            // Verificar se dados foram extraídos
            const hasData = result.documents.some((doc: any) => 
                doc.cnpj_emissor || doc.valor_total || doc.cnpj_cpf
            );
            
            if (!hasData) {
                console.warn('Nenhum dado extraído. Verifique logs do servidor.');
            }
            
            return result;
        } catch (err) {
            const errorMessage = err instanceof Error ? err.message : 'Erro desconhecido';
            setError(errorMessage);
            throw err;
        } finally {
            setLoading(false);
        }
    };
    
    return { analyzeDocuments, loading, error };
}
```

### **4. Tratamento de Respostas**

```typescript
// Verificar se extração funcionou
function validateExtractionResult(result: any) {
    const issues: string[] = [];
    
    result.documents.forEach((doc: any, index: number) => {
        if (doc.document_type === 'Generic' && !doc.cnpj_cpf && !doc.valor) {
            issues.push(`Documento ${index + 1} (${doc.filename}): Nenhum dado extraído`);
        }
        
        if (doc.document_type === 'NF-e' && !doc.cnpj_emissor && !doc.valor_total) {
            issues.push(`NF-e ${index + 1} (${doc.filename}): Dados fiscais não extraídos`);
        }
    });
    
    return {
        isValid: issues.length === 0,
        issues
    };
}
```

### **5. Monitoramento e Debug**

```typescript
// Verificar logs do servidor via API (se implementado)
// Ou monitorar arquivo api.log diretamente

// Exemplo de tratamento de erros
async function analyzeWithRetry(files: File[], maxRetries = 3) {
    for (let i = 0; i < maxRetries; i++) {
        try {
            const result = await analyzeDocuments(files);
            const validation = validateExtractionResult(result);
            
            if (!validation.isValid) {
                console.warn('Extração parcial:', validation.issues);
                // Decidir se aceita resultado parcial ou tenta novamente
            }
            
            return result;
        } catch (error) {
            if (i === maxRetries - 1) throw error;
            await new Promise(resolve => setTimeout(resolve, 1000 * (i + 1)));
        }
    }
}
```



**Última atualização:** 18 de Dezembro de 2025

