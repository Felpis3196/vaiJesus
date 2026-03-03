# Estrutura dos Arrays `errors` e `warnings`

**Data:** 26 de Janeiro de 2026  
**Versão:** 2.0.0  
**Autor:** Sistema de Auditoria de Condomínios com IA

---

## 📋 Visão Geral

Os arrays `errors` e `warnings` fazem parte da estrutura de resposta dos endpoints de análise (`/api/v1/analyze`, `/api/v1/analyze/documents`, `/api/v1/analyze/taxes`). A partir da **versão 2.0** eles utilizam **estrutura expandida** (objetos com `code`, `message`, `details`, `timestamp`, `severity`).

---

## 🔴 Array `errors`

### **Descrição**
Array que contém erros críticos que impediram ou afetaram o processamento da análise. Cada item é um **objeto estruturado**.

### **Tipo**
```typescript
errors: {
  code: string;
  message: string;
  details: Record<string, unknown>;
  timestamp: string;  // ISO 8601 (UTC)
  severity: string;
}[]
```

### **Estrutura de um Item**
| Campo      | Tipo   | Descrição |
|------------|--------|-----------|
| `code`     | string | Código do erro (ex.: `VALIDATION_ERROR`, `AUDIT_ERROR`) |
| `message`  | string | Mensagem legível para o usuário |
| `details`  | object | Dados adicionais (ex.: `exception_type`, colunas faltando) |
| `timestamp`| string | Data/hora do erro em ISO 8601 (UTC) |
| `severity` | string | Sempre `"critical"` para erros |

### **Códigos de erro**
| Código | Descrição |
|--------|-----------|
| `AUDIT_ERROR` | Erro genérico durante a auditoria |
| `VALIDATION_ERROR` | Falha de validação (ex.: ValueError) |
| `LOAD_ERROR` | Erro ao carregar arquivo (FileNotFoundError, OSError) |
| `PROCESSING_ERROR` | Erro no processamento de dados |
| `SERIALIZATION_ERROR` | Erro ao serializar resposta |

### **Exemplo de Item**
```json
{
  "code": "VALIDATION_ERROR",
  "message": "Erro durante auditoria avançada: Erros críticos: Colunas essenciais não encontradas.",
  "details": {
    "exception_type": "ValueError"
  },
  "timestamp": "2026-01-26T14:30:00.123456+00:00",
  "severity": "critical"
}
```

### **Quando é Populado**
- Quando uma exceção é capturada durante o processamento
- Quando a validação de dados falha criticamente
- Quando não é possível carregar arquivos

### **Comportamento**
- Se o processamento for bem-sucedido, o array estará **vazio** (`[]`)
- Cada erro é um objeto com os campos acima
- Para exibir ao usuário, use o campo `message`

### **Localização no Código**
- `services/audit_structures.error_from_exception()` – monta o objeto a partir da exceção
- `advanced_audit_system.run_comprehensive_audit` / `audit_system.run_audit` – fazem `errors.append(error_from_exception(e))`

---

## ⚠️ Array `warnings`

### **Descrição**
Array que contém avisos (warnings) sobre problemas não críticos que não impediram o processamento, mas que merecem atenção.

### **Tipo**
```typescript
warnings: string[]
```

### **Estrutura de um Item**
Cada item do array é uma **string** contendo a mensagem de aviso:

```json
"[contexto]: [descrição do aviso]"
```

### **Exemplos de Itens**

#### Exemplo 1: Aviso de Validação
```json
"Validação: Encontrados tipos inválidos: ['RECEITA', 'DESPESA_INVÁLIDA']"
```

#### Exemplo 2: Aviso de Dados Faltantes
```json
"Validação: Coluna 'categoria' tem 50 valores nulos"
```

#### Exemplo 3: Aviso de Arquivo Grande
```json
"PDF muito grande (72.04 MB). Processamento pode demorar..."
```

#### Exemplo 4: Aviso de Formato
```json
"Validação: Arquivo tem formato não padrão. Alguns dados podem não ser processados corretamente"
```

#### Exemplo 5: Aviso de Performance
```json
"Validação: Arquivo muito grande. Processamento pode demorar mais que o normal"
```

### **Quando é Populado**
- Quando há avisos de validação (não críticos)
- Quando arquivos são muito grandes
- Quando há dados faltantes que não impedem o processamento
- Quando há tipos de dados inválidos que foram corrigidos automaticamente
- Quando há problemas de performance esperados
- **Alertas automáticos:** documentos principais incompletos, guias/comprovantes pendentes, folhas/holerites pendentes, ausência de férias/13º no mês vigente, transações com anomalia (as mesmas mensagens também aparecem em `alerts` com código e severidade)

### **Comportamento**
- O array pode estar **vazio** (`[]`) quando não há avisos
- Avisos não impedem o processamento
- Múltiplos avisos podem ser adicionados ao mesmo array

### **Localização no Código**
- Validação: `advanced_audit_system._validate_dataframe` (warnings de validação)
- Alertas: `services/alert_generator.add_alerts_to_audit_result` (mensagens de alerta adicionadas também a `warnings`)

---

## 🔔 Array `alerts` (estruturado)

### **Descrição**
Array que contém alertas gerados por regras de negócio. Cada item tem o **mesmo formato** dos itens de `warnings` (code, message, details, timestamp, severity). Os mesmos objetos são referenciados em `alerts` e também incluídos em `warnings`.

### **Tipo**
```typescript
alerts: {
  code: string;
  message: string;
  details: Record<string, unknown>;
  timestamp: string;
  severity: string;
}[]
```

### **Severidades**
- `high`: atenção prioritária (ex.: documentos incompletos, anomalias a revisar)
- `medium`: atenção recomendada (ex.: guias/holerites pendentes)
- `low`: informativo (ex.: férias/13º não encontrados no mês)

### **Códigos de alerta**

| Código | Descrição |
|--------|-----------|
| `MAIN_DOCUMENTS_MISSING` | Nenhum documento foi enviado para análise. |
| `MAIN_DOCUMENTS_INCOMPLETE` | Nenhum dado financeiro estruturado nos arquivos. |
| `MAIN_DOCUMENTS_NO_FINANCIAL` | Nenhum arquivo de dados financeiros (CSV/Excel) identificado. |
| `GUIDES_RECEIPTS_PENDING` | Guias e comprovantes pendentes (há despesas que exigem comprovante, mas não há guia/comprovante nas descrições). |
| `PAYSLIPS_PENDING` | Folhas e holerites pendentes (há despesas com pessoal, mas não há folha/holerite nas descrições). |
| `VACATION_PAYMENT_MISSING` | Não foi identificada movimentação de pagamento de férias no mês vigente. |
| `THIRTEENTH_SALARY_MISSING` | Não foi identificada movimentação de 13º salário no mês vigente. |
| `ANOMALIES_REQUIRE_REVIEW` | Existem transações com anomalia que requerem revisão. |

### **Exemplo**
```json
"alerts": [
  {
    "code": "PAYSLIPS_PENDING",
    "message": "Folhas e holerites apresentados estão pendentes: há despesas com pessoal/salários, porém não foi identificada movimentação de folha de pagamento ou holerites nas descrições.",
    "details": {},
    "timestamp": "2026-01-26T14:30:00.123456+00:00",
    "severity": "medium"
  },
  {
    "code": "VACATION_PAYMENT_MISSING",
    "message": "Não foi identificada movimentação de pagamento de férias no mês vigente (01/2025). Verifique se há lançamentos de férias neste período.",
    "details": { "month": 1, "year": 2025 },
    "timestamp": "2026-01-26T14:30:00.123456+00:00",
    "severity": "low"
  }
]
```

---

## 📊 Estrutura Completa no Resultado da Análise

### **Exemplo Completo de Resposta**

```json
{
  "success": true,
  "file_path": "in_memory",
  "start_time": "2025-12-29T17:55:00.000000",
  "end_time": "2025-12-29T17:55:00.674419",
  "total_transactions": 1,
  "anomalies_detected": 0,
  "ai_analysis": {
    "total_anomalies": 0,
    "high_confidence_anomalies": 0,
    "model_agreement": {
      "agreement_rate": 0.0
    }
  },
  "nlp_analysis": {
    "high_suspicion_count": 0,
    "suspicious_patterns": {},
    "fraud_indicators": {}
  },
  "predictive_analysis": {
    "error": "Dados insuficientes para análise preditiva",
    "recommendations": []
  },
  "report_file": "./reports/relatorio_auditoria_2025-12-29.md",
  "errors": [],
  "warnings": [],
  "alerts": [],
  "summary": {
    "financial_summary": {
      "total_receitas": 0.0,
      "total_despesas": 0.0,
      "saldo": 0.0
    },
    "anomaly_summary": {
      "total_anomalies": 0,
      "anomaly_rate": 0.0,
      "high_risk_count": 0,
      "medium_risk_count": 0
    },
    "ai_performance": {},
    "risk_analysis": {}
  },
  "duration": 0.674419,
  "files_processed": 1,
  "file_metadata": [
    {
      "filename": "documento.pdf",
      "size": 75539734,
      "extension": ".pdf",
      "processed_at": "2025-12-29T17:55:00.000000"
    }
  ],
  "total_rows": 1,
  "processed_at": "2025-12-29T17:55:00.000000"
}
```

### **Exemplo com Erros**

```json
{
  "success": false,
  "errors": [
    {
      "code": "VALIDATION_ERROR",
      "message": "Erro durante auditoria avançada: Erros críticos: Colunas essenciais não encontradas: ['data', 'descricao', 'tipo', 'valor'].",
      "details": { "exception_type": "ValueError" },
      "timestamp": "2026-01-26T14:30:00.123456+00:00",
      "severity": "critical"
    }
  ],
  "warnings": [],
  "alerts": [],
  "total_transactions": 0,
  "anomalies_detected": 0
}
```

### **Exemplo com Avisos**

```json
{
  "success": true,
  "errors": [],
  "warnings": [
    {
      "code": "VALIDATION_WARNING",
      "message": "Validação: Encontrados tipos inválidos: ['RECEITA_INVÁLIDA']",
      "details": { "raw": "Encontrados tipos inválidos: ['RECEITA_INVÁLIDA']", "context": "validation" },
      "timestamp": "2026-01-26T14:30:00.123456+00:00",
      "severity": "warning"
    },
    {
      "code": "PAYSLIPS_PENDING",
      "message": "Folhas e holerites apresentados estão pendentes...",
      "details": {},
      "timestamp": "2026-01-26T14:30:00.234567+00:00",
      "severity": "medium"
    }
  ],
  "alerts": [ ... ],
  "total_transactions": 150,
  "anomalies_detected": 5
}
```

---

## 🔍 Detalhamento Técnico

### **Inicialização**

```python
# advanced_audit_system.py
audit_results = {
    'errors': [],      # Array de objetos estruturados
    'warnings': [],    # Array de objetos estruturados
    'alerts': [],      # Array de objetos estruturados (alertas de negócio)
    # ... outros campos
}
```

### **Adição de Erros**

```python
# services/audit_structures.error_from_exception
from services.audit_structures import error_from_exception

except Exception as e:
    self.logger.log_error("auditoria avançada", e)
    audit_results['errors'].append(error_from_exception(e))
```

### **Adição de Avisos**

```python
# Validação: make_warning com código VALIDATION_WARNING
from services.audit_structures import make_warning, WarningCode

warnings_list.append(
    make_warning(WarningCode.VALIDATION_WARNING, warning_msg, details={"raw": warning, "context": "validation"})
)

# Alertas: add_alerts_to_audit_result adiciona itens a warnings e alerts
add_alerts_to_audit_result(audit_result, df=df_final, document_context=document_context)
```

---

## 📝 Convenções de Nomenclatura

### **Formato de Mensagens de Erro**
```
"Erro durante auditoria avançada: [detalhes do erro]"
```

### **Formato de Mensagens de Aviso**
```
"[Contexto]: [descrição do aviso]"
```

**Contextos comuns:**
- `"Validação:"` - Avisos de validação de dados
- `"Processamento:"` - Avisos durante processamento
- `"Performance:"` - Avisos de performance
- `"Formato:"` - Avisos sobre formato de arquivo

---

## 🎯 Uso na Integração

### **Verificação de Erros**

```javascript
// JavaScript/TypeScript
const response = await fetch('/api/v1/analyze', { ... });
const data = await response.json();

if (data.errors && data.errors.length > 0) {
    console.error('Erros encontrados:', data.errors);
    // Tratar erros
    data.errors.forEach(error => {
        console.error('Erro:', error);
    });
}
```

### **Verificação de Avisos**

```javascript
// JavaScript/TypeScript
if (data.warnings && data.warnings.length > 0) {
    console.warn('Avisos encontrados:', data.warnings);
    // Exibir avisos ao usuário (não críticos)
    data.warnings.forEach(warning => {
        console.warn('Aviso:', warning);
    });
}
```

### **Python**

```python
# Python
response = requests.post('/api/v1/analyze', ...)
data = response.json()

if data.get('errors'):
    print("Erros encontrados:")
    for error in data['errors']:
        print(f"  - {error}")

if data.get('warnings'):
    print("Avisos encontrados:")
    for warning in data['warnings']:
        print(f"  - {warning}")
```

---

## 🔄 Fluxo de Processamento

```
Início da Análise
    ↓
Inicialização: errors = [], warnings = []
    ↓
Processamento...
    ↓
┌─────────────────────────┐
│ Erro Crítico?          │
│ Sim → errors.append()   │
│ Não → Continua          │
└─────────────────────────┘
    ↓
┌─────────────────────────┐
│ Aviso Não Crítico?     │
│ Sim → warnings.append() │
│ Não → Continua          │
└─────────────────────────┘
    ↓
Retorno com errors[] e warnings[]
```

---

## 📌 Notas Importantes

1. **Array Vazio vs. Não Presente:**
   - Se não houver erros/avisos, o array estará vazio (`[]`), não `null` ou `undefined`

2. **Ordem dos Itens:**
   - Os itens são adicionados na ordem em que ocorrem
   - O primeiro erro/aviso está no índice 0

3. **Múltiplos Itens:**
   - Pode haver múltiplos erros e avisos no mesmo processamento
   - Cada item é independente

4. **Serialização:**
   - Cada item de `errors` e `warnings` é um objeto com `code`, `message`, `details`, `timestamp`, `severity`
   - O campo `details` pode conter objetos aninhados (sempre serializáveis em JSON)

5. **Localização:**
   - `errors` e `warnings` estão no nível raiz do objeto de resposta
   - Estão presentes mesmo quando vazios

---

## ✅ Estrutura Expandida (Implementada v2.0)

A partir da **versão 2.0**, os arrays `errors` e `warnings` utilizam a estrutura expandida descrita neste documento: cada item é um objeto com `code`, `message`, `details`, `timestamp` e `severity`. A implementação está em `services/audit_structures.py` (`make_error`, `make_warning`, `error_from_exception`).

---

## 📚 Referências

- **Código Fonte:**
  - `services/audit_structures.py` – `make_error`, `make_warning`, `error_from_exception`, `ErrorCode`, `WarningCode`
  - `advanced_audit_system.py` – inicialização de `errors`/`warnings`/`alerts`, uso de `error_from_exception` e `make_warning`
  - `audit_system.py` – uso de `error_from_exception`
  - `services/alert_generator.py` – uso de `make_warning` para alertas

- **Documentação Relacionada:**
  - `Docs/API_DOCUMENTATION.md` - Documentação completa da API
  - `README.md` - Visão geral do sistema

---

**Última Atualização:** 26 de Janeiro de 2026

