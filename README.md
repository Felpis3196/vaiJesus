# 🏢 Sistema de Auditoria de Condomínios com IA

[![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)](https://python.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.68+-green.svg)](https://fastapi.tiangolo.com)
[![Docker](https://img.shields.io/badge/Docker-Ready-blue.svg)](https://docker.com)

**Versão:** 1.0.0  
**Última Atualização:** 27 de Dezembro de 2025

## 📋 Visão Geral

Sistema avançado de auditoria de contas de condomínios utilizando inteligência artificial para detecção de anomalias, análise preditiva e processamento de linguagem natural. O sistema oferece uma API REST completa, pronta para produção com deploy em VPS.

**Principais Recursos:**
- 🤖 IA Avançada com múltiplos algoritmos de detecção
- 📄 Análise de documentos fiscais (NF-e, NFS-e, XML)
- 💰 Análise de impostos (ICMS, ISS, IPI)
- 🔄 Correlação automática entre documentos e lançamentos
- 👤 Identificação de cliente para rastreamento histórico
- 📊 Status de análises em tempo real (WebSocket + Polling)
- 🗂️ Categorização automática de arquivos
- ⚡ Processamento assíncrono (sem bloqueio)
- 🔔 Notificações via webhook quando análises completam

## ✨ Funcionalidades Principais

### 🤖 Inteligência Artificial
- **Detecção de Anomalias**: Algoritmos avançados (Isolation Forest, Z-Score, Ensemble Models)
- **IA Preditiva**: Análise de tendências e previsão de riscos futuros
- **Processamento de Linguagem Natural**: Análise inteligente de descrições de transações
- **Explainable AI**: Explicações detalhadas das decisões da IA

### 📊 Análise de Dados
- **Processamento Avançado**: Carregamento, limpeza e normalização de dados
- **Categorização Inteligente**: Classificação automática de transações e arquivos
- **Relatórios Detalhados**: Geração de relatórios completos de auditoria
- **Visualizações**: Gráficos e dashboards interativos
- **Análise de Documentos Fiscais**: Extração de dados de NF-e, NFS-e, XML
- **Análise de Impostos**: Cálculo e validação de ICMS, ISS, IPI
- **Correlação Automática**: Matching entre documentos fiscais e lançamentos

### 🚀 API REST
- **FastAPI**: API moderna e performática
- **Acesso Controlado**: Proteção por IP localhost
- **Documentação Automática**: Swagger/OpenAPI integrado
- **Deploy Ready**: Configuração completa para VPS
- **Múltiplos Formatos**: Suporte a CSV, Excel, PDF, ODS e XML
- **Identificação de Cliente**: Rastreamento histórico por cliente/usuário
- **Status de Análises**: Sistema de acompanhamento de progresso (job_id)
- **Categorização Automática**: Classificação inteligente de arquivos
- **Processamento Assíncrono**: Retorna job_id imediatamente, processa em background
- **WebSockets**: Atualização em tempo real sem polling
- **Webhooks**: Notificações automáticas quando análises completam

## 🏗️ Arquitetura

O backend está organizado no pacote `app/` com camadas bem definidas:

```
app/
├── core/                 # Configuração e logging (config, logging_config)
├── extraction/           # Extração de dados
│   ├── legacy/           # Regras (loader, data_processor, financial_extractor, document_analyzer, etc.)
│   └── llm/              # Extração via LLM (client, document_extractor, labor_extractor)
├── analysis/             # IA: anomaly_detector, predictive_ai, nlp_analyzer, advanced_ai_engine
├── audit/                # Auditoria: audit_system, advanced_audit_system, labor_analyzer, financial_consolidator, etc.
├── reporting/            # Relatórios: report_generator, report_formatter, alert_generator
└── services/             # Serviços transversais: client_manager, analysis_status, fgts_link_fetcher
```

Na raiz permanecem os pontos de entrada: `api_server.py`, `main.py`, e opcionalmente `config.py` / `logger_config.py` (wrappers que re-exportam de `app.core`). Os módulos na raiz (`data_processor`, `report_generator`, `audit_system`, `advanced_audit_system`, etc.) são wrappers de compatibilidade; prefira importar de `app.*`.

```
auditoriaIA/
├── 🤖 Core AI (em app/analysis e app/audit)
├── 🔧 Infrastructure
│   ├── api_server.py              # Servidor FastAPI
│   ├── config.py                  # Wrapper → app.core
│   ├── logger_config.py           # Wrapper → app.core
│   └── data_input_manager.py      # Gerenciamento de dados
├── 🛠️ Services (em app/services e app/extraction)
├── 🐳 Deployment
│   ├── Dockerfile                 # Container Docker
│   ├── docker-compose.yml         # Orquestração de serviços
│   ├── nginx.conf                 # Configuração do Nginx
│   └── deploy.sh                  # Script de deploy
└── 📚 Documentation
    ├── Docs/API_DOCUMENTATION.md          # Documentação completa da API

```

## 🚀 Quick Start

### 1. Clone o Repositório
```bash
git clone https://github.com/coderlucianasena/auditoria_IA.git
cd auditoria_IA
```

### 2. Configuração Local
```bash
# Instalar dependências
pip install -r requirements.txt

# Executar sistema básico
python main.py

# Executar API
python api_server.py
```

### 3. Deploy com Docker
```bash
# Build e execução
docker-compose up --build

# Acesso à API
curl http://localhost:8000/health
```

### 4. Deploy em VPS
```bash
# Preparar VPS
chmod +x setup_vps.sh
./setup_vps.sh

# Deploy da aplicação
chmod +x deploy.sh
./deploy.sh
```

## 📡 API Endpoints

| Endpoint | Método | Descrição |
|----------|--------|-----------|
| `/health` | GET | Status da API |
| `/api/v1/analyze` | POST | Análise de dados financeiros (múltiplos arquivos, PDF, ODS, XML) |
| `/api/v1/analyze/documents` | POST | Análise de documentos fiscais (NF-e, NFS-e, XML) |
| `/api/v1/analyze/taxes` | POST | Análise de impostos (ICMS, ISS, IPI) |
| `/api/v1/correlate` | POST | Correlação documentos ↔ lançamentos |
| `/api/v1/config` | GET/POST | Configurações do sistema |
| `/api/v1/stats` | GET | Estatísticas do sistema |
| `/api/v1/system` | GET | Informações do sistema |
| `/api/v1/test` | POST | Teste de análise |
| `/api/v1/analysis/status/{job_id}` | GET | Status de uma análise específica |
| `/api/v1/analysis/status` | GET | Lista de análises recentes |
| `/api/v1/client/{client_id}` | GET | Informações e histórico de um cliente |
| `/api/v1/clients` | GET | Lista todos os clientes |
| `/ws/analysis/{job_id}` | WebSocket | Atualização em tempo real de um job específico |
| `/ws/analysis` | WebSocket | Atualização em tempo real de todos os jobs |
| `/docs` | GET | Documentação Swagger |

### Exemplo de Uso da API

**⚠️ Nota:** A API não utiliza autenticação JWT. O acesso é controlado por IP localhost.

```python
import requests
import time

# Análise financeira com identificação de cliente
files = [('files', open('dados.csv', 'rb'))]
response = requests.post(
    "http://localhost:8000/api/v1/analyze",
    params={"client_id": "condominio_abc"},  # Identificação do cliente
    files=files
)
result = response.json()
print(f"Anomalias detectadas: {result['data']['anomalies_detected']}")
print(f"Job ID: {result.get('job_id')}")  # ID para acompanhar status
print(f"Client ID: {result.get('client_id')}")

# Consultar status da análise
job_id = result.get('job_id')
if job_id:
    status_response = requests.get(
        f"http://localhost:8000/api/v1/analysis/status/{job_id}"
    )
    status = status_response.json()
    print(f"Status: {status['status']['status']}")
    print(f"Progresso: {status['status']['progress']*100:.1f}%")

# Análise de documentos fiscais
doc_files = [('files', open('nota_fiscal.xml', 'rb'))]
response = requests.post(
    "http://localhost:8000/api/v1/analyze/documents",
    params={"client_id": "condominio_abc"},
    files=doc_files
)
result = response.json()
print(f"Documentos processados: {len(result['documents'])}")

# Consultar histórico do cliente
client_info = requests.get(
    "http://localhost:8000/api/v1/client/condominio_abc"
).json()
print(f"Total de análises: {client_info['client']['total_analyses']}")
print(f"Meses processados: {client_info['client']['months_processed']}")

# Correlação documentos ↔ lançamentos
files = [
    ('documents_files', open('nfe.xml', 'rb')),
    ('transactions_file', open('lancamentos.csv', 'rb'))
]
response = requests.post("http://localhost:8000/api/v1/correlate", files=files)
result = response.json()
print(f"Taxa de match: {result['statistics']['match_rate']}%")
```

## 📚 Documentação

### **Para Desenvolvedores Fullstack:**
- 📖 **[Documentação Completa da API](Docs/API_DOCUMENTATION.md)** - Guia completo com todos os endpoints, exemplos em JavaScript/Python/React, tratamento de erros e boas práticas


## 🔧 Configuração

### Variáveis de Ambiente
```bash
# Configurações da IA
AI_AUDIT_LOG_LEVEL=INFO
AI_AUDIT_OUTPUT_DIR=/app/data
AI_AUDIT_MAX_FILE_SIZE=50
AI_AUDIT_ENABLE_NLP=true
AI_AUDIT_ENABLE_PREDICTIVE=true

# Integração OpenAI (opcional)
OPENAI_API_KEY=sua-chave-openai-aqui
OPENAI_MODEL=gpt-4
```

**Extração via LLM local (sem token externo):**  
Defina `LLM_EXTRACTION_ENABLED=true` e `LLM_BASE_URL` (ex.: `http://localhost:11434/v1` para Ollama) e `LLM_MODEL` (ex.: `llama3.1`). Com LLM local **não é necessário** token de GPT-4 mini nem de qualquer IA externa. A extração de transações, saldos, holerites e encargos passa a ser feita pela LLM local. Veja `config.env.example` para todas as variáveis `LLM_EXTRACTION_*`.

### Estrutura de Dados Suportada

**Formatos de Arquivo:**
- CSV (`.csv`)
- Excel (`.xlsx`, `.xls`)
- PDF (`.pdf`) - Extração de tabelas e texto
- OpenDocument (`.ods`) - Planilhas LibreOffice/OpenOffice
- XML (`.xml`) - Documentos fiscais estruturados

**Estrutura CSV/Excel:**
```csv
Data,Descricao,Tipo,Valor,Categoria
2024-01-15,Manutenção elevador,Despesa,1500.00,Manutenção
2024-01-16,Taxa condomínio,Receita,2500.00,Receita
```

**Documentos Fiscais Suportados:**
- NF-e (Nota Fiscal Eletrônica)
- NFS-e (Nota Fiscal de Serviços)
- XML Fiscal
- Documentos genéricos (PDFs, recibos)

## 🧪 Testes

```bash
# Teste da API
python test_api_client.py

# Teste de funcionalidades
python -m pytest tests/
```

## 📊 Relatórios Gerados

- **Resumo Financeiro**: Receitas, despesas e saldo por categoria
- **Anomalias Detectadas**: Transações suspeitas com justificativas
- **Análise Preditiva**: Tendências e riscos futuros
- **Insights de NLP**: Padrões em descrições de transações
- **Análise de Documentos Fiscais**: Dados extraídos de NF-e, NFS-e, XML
- **Análise de Impostos**: Consolidação de ICMS, ISS, IPI
- **Correlação**: Matching entre documentos e lançamentos financeiros
- **Estatísticas de Cliente**: Histórico e métricas por cliente/usuário

## 🛡️ Segurança

- ✅ Controle de acesso por IP (apenas localhost)
- ✅ CORS configurado para localhost
- ✅ Validação de tipos de arquivo
- ✅ Limite de tamanho de arquivo (configurável)
- ✅ SSL/TLS com Certbot (produção)
- ✅ Firewall e Fail2ban (produção)
- ✅ Logs de auditoria
- ✅ Identificação de cliente (client_id opcional)
- ✅ Dados de clientes isolados por arquivo JSON

## 📈 Monitoramento

- **Health Checks**: Verificação automática de saúde
- **Métricas**: CPU, memória, disco
- **Logs**: Sistema de logging estruturado
- **Alertas**: Notificações automáticas
- **Status de Análises**: Acompanhamento em tempo real via job_id
- **Estatísticas de Clientes**: Histórico e métricas por cliente
- **Categorização de Arquivos**: Estatísticas por tipo e categoria