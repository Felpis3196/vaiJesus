# Estrutura de um Item dos Arrays `errors` e `warnings`

**Data:** 26 de Janeiro de 2026  
**Versão:** 2.0.0

---

## 📋 Resumo Rápido (estrutura atual v2.0)

A partir da **versão 2.0**, cada item de `errors` e `warnings` é um **objeto** com os campos: `code`, `message`, `details`, `timestamp`, `severity`. Para a documentação completa, consulte `ESTRUTURA_ARRAYS_ERROS_AVISOS.md`.

### Array `errors`
- **Tipo do item:** `object`
- **Campos:** `code`, `message`, `details`, `timestamp`, `severity`
- **Exibir ao usuário:** use o campo `message`

### Array `warnings`
- **Tipo do item:** `object`
- **Campos:** `code`, `message`, `details`, `timestamp`, `severity`
- **Exibir ao usuário:** use o campo `message`

---

## 🔴 Estrutura de um Item do Array `errors`

### **Tipo**
```typescript
string
```

### **Estrutura Visual**
```
┌─────────────────────────────────────────────────────────────────┐
│ Item do Array errors                                             │
├─────────────────────────────────────────────────────────────────┤
│ Tipo: string                                                     │
│                                                                  │
│ "Erro durante auditoria avançada: [descrição detalhada do erro]"│
│                                                                  │
│ Prefixo fixo: "Erro durante auditoria avançada: "                │
│ + Descrição do erro (variável)                                  │
└─────────────────────────────────────────────────────────────────┘
```

### **Exemplo Real Completo**

```json
{
  "errors": [
    "Erro durante auditoria avançada: Erro ao limpar os dados: Colunas essenciais não encontradas: ['data', 'descricao', 'tipo', 'valor']. Colunas disponíveis: ['recebimento_no_mês_12.844', '56_55', '57%']"
  ]
}
```

### **Uso na integração**
- Exibir texto: `err.message`
- Filtrar por tipo: `err.code === "VALIDATION_ERROR"`
- Detalhes para log/debug: `err.details`, `err.timestamp`

### **Outros Exemplos de Códigos**

#### Item 1: Erro de carregamento (`LOAD_ERROR`)
- `code`: `"LOAD_ERROR"`, `message`: contém detalhes do arquivo; `details.exception_type`: `"FileNotFoundError"` ou similar.

#### Item 2: Erro de validação (`VALIDATION_ERROR`)
- `code`: `"VALIDATION_ERROR"`, `message`: texto do ValueError; `details.exception_type`: `"ValueError"`.

#### Item 3: Erro genérico (`AUDIT_ERROR`)
- `code`: `"AUDIT_ERROR"`, `message`: mensagem completa; `details`: pode incluir `exception_type`.

---

## ⚠️ Estrutura de um Item do Array `warnings` (v2.0)

### **Tipo**
```typescript
{
  code: string;
  message: string;
  details: Record<string, unknown>;
  timestamp: string;
  severity: string;  // "warning" | "high" | "medium" | "low"
}
```

### **Estrutura Visual**
```
┌─────────────────────────────────────────────────────────────────┐
│ Item do Array warnings (objeto)                                  │
├─────────────────────────────────────────────────────────────────┤
│ code      : "VALIDATION_WARNING" | "PAYSLIPS_PENDING" | ...      │
│ message   : "Validação: ..." ou mensagem de alerta               │
│ details   : { "raw": "...", "context": "validation", ... }       │
│ timestamp : "2026-01-26T14:30:00.123456+00:00"                   │
│ severity  : "warning" | "high" | "medium" | "low"                │
└─────────────────────────────────────────────────────────────────┘
```

### **Exemplo Real Completo**

```json
{
  "warnings": [
    "Validação: Encontrados tipos inválidos: ['RECEITA', 'DESPESA_INVÁLIDA']"
  ]
}
```

### **Análise do Item**

```
┌──────────────────────────────────────────────────────────────────────┐
│ ESTRUTURA DO ITEM                                                     │
├──────────────────────────────────────────────────────────────────────┤
│                                                                       │
│ "Validação: "                        ← Contexto (variável)          │
│ Encontrados tipos inválidos:         ← Tipo de aviso                │
│ ['RECEITA', 'DESPESA_INVÁLIDA']       ← Detalhes específicos         │
│                                                                       │
└──────────────────────────────────────────────────────────────────────┘
```

### **Outros Exemplos de Itens**

#### Item 1: Aviso de Dados Faltantes
```json
"Validação: Coluna 'categoria' tem 50 valores nulos"
```

**Estrutura:**
- Contexto: `"Validação: "`
- Descrição: `"Coluna 'categoria' tem 50 valores nulos"`

#### Item 2: Aviso de Arquivo Grande
```json
"PDF muito grande (72.04 MB). Processamento pode demorar..."
```

**Estrutura:**
- Contexto: (nenhum prefixo fixo neste caso)
- Descrição: `"PDF muito grande (72.04 MB). Processamento pode demorar..."`

#### Item 3: Aviso de Performance
```json
"Validação: Arquivo muito grande. Processamento pode demorar mais que o normal"
```

**Estrutura:**
- Contexto: `"Validação: "`
- Descrição: `"Arquivo muito grande. Processamento pode demorar mais que o normal"`

---

## 🔍 Comparação Visual

### **Array `errors`**

```
Array errors: string[]
│
├─ Item 0: "Erro durante auditoria avançada: [erro 1]"
├─ Item 1: "Erro durante auditoria avançada: [erro 2]"
└─ Item N: "Erro durante auditoria avançada: [erro N]"
```

**Características:**
- ✅ Sempre começa com `"Erro durante auditoria avançada: "`
- ✅ Descreve problemas críticos
- ✅ Pode conter múltiplas informações separadas por `;` ou `|`

### **Array `warnings`**

```
Array warnings: string[]
│
├─ Item 0: "[contexto]: [aviso 1]"
├─ Item 1: "[contexto]: [aviso 2]"
└─ Item N: "[contexto]: [aviso N]"
```

**Características:**
- ✅ Contexto variável (ex: `"Validação: "`, `"Processamento: "`)
- ✅ Descreve problemas não críticos
- ✅ Pode não ter contexto prefixo em alguns casos

---

## 📊 Exemplo Completo de Resposta JSON

### **Cenário 1: Arrays Vazios (Sucesso)**
```json
{
  "success": true,
  "errors": [],
  "warnings": [],
  "total_transactions": 150,
  "anomalies_detected": 5
}
```

**Análise:**
- `errors`: Array vazio `[]` - nenhum erro
- `warnings`: Array vazio `[]` - nenhum aviso

### **Cenário 2: Com Erros**
```json
{
  "success": false,
  "errors": [
    "Erro durante auditoria avançada: Erro ao limpar os dados: Colunas essenciais não encontradas: ['data', 'descricao', 'tipo', 'valor']",
    "Erro durante auditoria avançada: No valid files could be loaded"
  ],
  "warnings": [],
  "total_transactions": 0,
  "anomalies_detected": 0
}
```

**Análise:**
- `errors`: Array com 2 itens (strings)
  - Item 0: String com erro de colunas
  - Item 1: String com erro de arquivo
- `warnings`: Array vazio `[]`

### **Cenário 3: Com Avisos**
```json
{
  "success": true,
  "errors": [],
  "warnings": [
    "Validação: Encontrados tipos inválidos: ['RECEITA_INVÁLIDA']",
    "Validação: Coluna 'categoria' tem 25 valores nulos",
    "PDF muito grande (72.04 MB). Processamento pode demorar..."
  ],
  "total_transactions": 150,
  "anomalies_detected": 5
}
```

**Análise:**
- `errors`: Array vazio `[]`
- `warnings`: Array com 3 itens (strings)
  - Item 0: String com contexto `"Validação: "`
  - Item 1: String com contexto `"Validação: "`
  - Item 2: String sem contexto prefixo

### **Cenário 4: Com Erros e Avisos**
```json
{
  "success": false,
  "errors": [
    "Erro durante auditoria avançada: Erro ao limpar os dados: Colunas essenciais não encontradas"
  ],
  "warnings": [
    "Validação: Arquivo muito grande. Processamento pode demorar mais que o normal"
  ],
  "total_transactions": 0,
  "anomalies_detected": 0
}
```

**Análise:**
- `errors`: Array com 1 item (string)
- `warnings`: Array com 1 item (string)

---

## 💻 Como Acessar um Item nos Arrays

### **JavaScript/TypeScript**

```javascript
const response = await fetch('/api/v1/analyze', { ... });
const data = await response.json();

// Acessar primeiro item do array errors
if (data.errors && data.errors.length > 0) {
    const primeiroErro = data.errors[0];  // string
    console.log('Primeiro erro:', primeiroErro);
    // Output: "Erro durante auditoria avançada: [descrição]"
}

// Acessar primeiro item do array warnings
if (data.warnings && data.warnings.length > 0) {
    const primeiroAviso = data.warnings[0];  // string
    console.log('Primeiro aviso:', primeiroAviso);
    // Output: "Validação: [descrição]"
}

// Iterar sobre todos os itens
data.errors.forEach((erro, index) => {
    console.log(`Erro ${index}:`, erro);  // erro é uma string
});

data.warnings.forEach((aviso, index) => {
    console.log(`Aviso ${index}:`, aviso);  // aviso é uma string
});
```

### **Python**

```python
import requests

response = requests.post('/api/v1/analyze', ...)
data = response.json()

# Acessar primeiro item do array errors
if data.get('errors'):
    primeiro_erro = data['errors'][0]  # str
    print('Primeiro erro:', primeiro_erro)
    # Output: "Erro durante auditoria avançada: [descrição]"

# Acessar primeiro item do array warnings
if data.get('warnings'):
    primeiro_aviso = data['warnings'][0]  # str
    print('Primeiro aviso:', primeiro_aviso)
    # Output: "Validação: [descrição]"

# Iterar sobre todos os itens
for index, erro in enumerate(data.get('errors', [])):
    print(f'Erro {index}:', erro)  # erro é uma string

for index, aviso in enumerate(data.get('warnings', [])):
    print(f'Aviso {index}:', aviso)  # aviso é uma string
```

---

## 🎯 Resumo Final

### **Estrutura de um Item - Array `errors`**

| Propriedade | Valor |
|------------|-------|
| **Tipo** | `string` |
| **Formato** | `"Erro durante auditoria avançada: [descrição]"` |
| **Prefixo** | Sempre `"Erro durante auditoria avançada: "` |
| **Conteúdo** | Descrição detalhada do erro |
| **Tamanho** | Variável (pode ser muito longo) |
| **Exemplo** | `"Erro durante auditoria avançada: Erro ao limpar os dados: Colunas essenciais não encontradas"` |

### **Estrutura de um Item - Array `warnings`**

| Propriedade | Valor |
|------------|-------|
| **Tipo** | `string` |
| **Formato** | `"[contexto]: [descrição]"` ou `"[descrição]"` |
| **Prefixo** | Variável (ex: `"Validação: "`, `"Processamento: "`) ou nenhum |
| **Conteúdo** | Descrição do aviso |
| **Tamanho** | Variável |
| **Exemplo** | `"Validação: Encontrados tipos inválidos: ['RECEITA_INVÁLIDA']"` |

---

**Última Atualização:** 26 de Janeiro de 2026

