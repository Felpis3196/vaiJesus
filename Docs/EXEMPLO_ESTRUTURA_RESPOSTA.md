# Exemplo de Estrutura de Resposta dos Endpoints

## 📋 Visão Geral

Cada endpoint retorna uma seção específica do relatório em formato JSON estruturado, permitindo que o front-end monte o relatório completo.

---

## 🔌 Endpoint: `/api/v1/report/section/1`

**Seção:** O que foi conferido

### **Resposta:**

```json
{
  "success": true,
  "section": {
    "number": 1,
    "title": "1️⃣ O que foi conferido",
    "icon": "1️⃣"
  },
  "data": {
    "content": {
      "documents_analyzed": [
        "Prestação de contas / balancetes",
        "Folha de pagamento e adiantamentos salariais",
        "Guias e comprovantes de encargos (INSS, FGTS, IRRF, PIS, ISS, etc.)",
        "Extratos e comprovantes de pagamento"
      ],
      "note": "A análise considera exclusivamente os documentos entregues.",
      "statistics": {
        "files_processed": 3,
        "transactions_count": 150,
        "period_start": "2025-01-01",
        "period_end": "2025-01-31"
      }
    },
    "metadata": {
      "generated_at": "2025-12-30T10:00:00Z",
      "job_id": "550e8400-e29b-41d4-a716-446655440000",
      "processing_time_ms": 45
    }
  }
}
```

---

## 🔌 Endpoint: `/api/v1/report/section/2`

**Seção:** Situação dos documentos

### **Resposta:**

```json
{
  "success": true,
  "section": {
    "number": 2,
    "title": "2️⃣ Situação dos documentos",
    "icon": "2️⃣"
  },
  "data": {
    "content": {
      "resumo_simples": {
        "documentos_principais": {
          "status": "completos",
          "icon": "✅",
          "details": [
            "Prestação de contas encontrada",
            "Balancetes presentes"
          ]
        },
        "guias_comprovantes": {
          "status": "apresentados",
          "icon": "✅",
          "details": [
            "Guias de INSS encontradas",
            "Guias de FGTS encontradas",
            "Guias de IRRF encontradas"
          ]
        },
        "folha_holerites": {
          "status": "apresentados",
          "icon": "✅",
          "details": [
            "Folha de pagamento encontrada",
            "Holerites presentes"
          ]
        }
      },
      "observacao": "Não foram identificadas ausências relevantes que comprometam a conferência do período.",
      "missing_documents": []
    },
    "metadata": {
      "generated_at": "2025-12-30T10:00:05Z",
      "job_id": "550e8400-e29b-41d4-a716-446655440000",
      "processing_time_ms": 120
    }
  }
}
```

---

## 🔌 Endpoint: `/api/v1/report/section/3`

**Seção:** Resumo financeiro do período

### **Resposta:**

```json
{
  "success": true,
  "section": {
    "number": 3,
    "title": "3️⃣ Resumo financeiro do período",
    "icon": "3️⃣"
  },
  "data": {
    "content": {
      "receitas_despesas_status": "coerentes com os demonstrativos",
      "saldo_inicial_ordinaria": 31394.50,
      "recebimentos_totais": 23114.63,
      "despesas_totais": 22166.24,
      "saldo_final_calculado": 32342.89,
      "saldo_final_demonstrado": 32342.89,
      "saldo_match": true,
      "observacoes": [
        "Não há sinal de lançamentos duplicados",
        "Alguns gastos não têm NF associada no texto (ex: taxa administração, portaria virtual). Precisaria checar pasta física/digital."
      ],
      "checks": {
        "saldos_fecham_corretamente": true,
        "sem_erros_soma": true,
        "sem_lancamentos_duplicados": true
      }
    },
    "metadata": {
      "generated_at": "2025-12-30T10:00:10Z",
      "job_id": "550e8400-e29b-41d4-a716-446655440000",
      "processing_time_ms": 80
    }
  }
}
```

---

## 🔌 Endpoint: `/api/v1/report/section/4`

**Seção:** Encargos trabalhistas e tributos

### **Resposta:**

```json
{
  "success": true,
  "section": {
    "number": 4,
    "title": "4️⃣ Encargos trabalhistas e tributos",
    "icon": "4️⃣"
  },
  "data": {
    "content": {
      "base_calculo": {
        "folha_pagamento_total": 17400.88,
        "inclui_adiantamento": true,
        "periodo": "2025-01"
      },
      "encargos": {
        "fgts": {
          "percentual": 8,
          "valor_calculado": 1392.07,
          "valor_pago": 1392.07,
          "status": "correto",
          "icon": "✅",
          "detalhes": "Recolhimento correto"
        },
        "inss": {
          "tipo": "patronal, funcionários e Terceiros",
          "valor_calculado": 1835.52,
          "valor_pago": 1835.52,
          "status": "compativel",
          "icon": "✅",
          "detalhes": "Valor alto, mas possível. Recomendo comparar com GFIP/eSocial do mês.",
          "recomendacao": "Comparar com GFIP/eSocial do mês"
        },
        "irrf": {
          "valor_pago": 450.00,
          "status": "aplicado_conforme_tabela",
          "icon": "✅",
          "detalhes": "Aplicado conforme tabela vigente"
        }
      },
      "tributos": {
        "pis": {
          "codigo": "8301",
          "status": "recolhido",
          "icon": "✅",
          "detalhes": "Recolhido nos meses aplicáveis"
        },
        "iss": {
          "status": "recolhido_quando_devido",
          "icon": "✅",
          "detalhes": "Recolhidas quando devidas"
        }
      },
      "resumo": "Não foram identificadas diferenças entre valores calculados e valores pagos."
    },
    "metadata": {
      "generated_at": "2025-12-30T10:00:15Z",
      "job_id": "550e8400-e29b-41d4-a716-446655440000",
      "processing_time_ms": 200
    }
  }
}
```

---

## 🔌 Endpoint: `/api/v1/report/section/5`

**Seção:** Férias e 13º

### **Resposta:**

```json
{
  "success": true,
  "section": {
    "number": 5,
    "title": "5️⃣ Férias e 13º",
    "icon": "5️⃣"
  },
  "data": {
    "content": {
      "provisao": {
        "presente": true,
        "valor": 10992.09,
        "detalhes": "Há provisão separada (R$ 10.992,09 após recebimentos).",
        "icon": "✔️"
      },
      "pagamentos": {
        "ferias_no_periodo": false,
        "decimo_terceiro_no_periodo": false,
        "detalhes": "Não aparece movimentação de pagamento de férias nem 13º neste mês"
      }
    },
    "metadata": {
      "generated_at": "2025-12-30T10:00:20Z",
      "job_id": "550e8400-e29b-41d4-a716-446655440000",
      "processing_time_ms": 60
    }
  }
}
```

---

## 🔌 Endpoint: `/api/v1/report/section/6`

**Seção:** Pontos de alerta

### **Resposta (Sem Alertas):**

```json
{
  "success": true,
  "section": {
    "number": 6,
    "title": "6️⃣ Pontos de alerta",
    "icon": "6️⃣"
  },
  "data": {
    "content": {
      "has_alerts": false,
      "status": "Não foram identificados pontos críticos",
      "icon": "✔️",
      "alerts": []
    },
    "metadata": {
      "generated_at": "2025-12-30T10:00:25Z",
      "job_id": "550e8400-e29b-41d4-a716-446655440000",
      "processing_time_ms": 100
    }
  }
}
```

### **Resposta (Com Alertas):**

```json
{
  "success": true,
  "section": {
    "number": 6,
    "title": "6️⃣ Pontos de alerta",
    "icon": "6️⃣"
  },
  "data": {
    "content": {
      "has_alerts": true,
      "status": "Foram identificados os seguintes ajustes recomendados:",
      "icon": "⚠️",
      "alerts": [
        {
          "type": "gasto_fora_padrao",
          "description": "Elevadores R$ 2.842,50, incluindo troca de fórmica (R$ 2.000).",
          "valor": 2842.50,
          "categoria": "Manutenção",
          "detalhes": "Gasto extra fora da rotina.",
          "recomendacao": "Justificar em assembleia"
        },
        {
          "type": "documento_nao_localizado",
          "description": "Documento não localizado na pasta no período analisado",
          "detalhes": "Comprovante de pagamento de taxa de administração"
        }
      ]
    },
    "metadata": {
      "generated_at": "2025-12-30T10:00:25Z",
      "job_id": "550e8400-e29b-41d4-a716-446655440000",
      "processing_time_ms": 100
    }
  }
}
```

---

## 🔌 Endpoint: `/api/v1/report/section/7`

**Seção:** Conclusão geral

### **Resposta (Template):**

```json
{
  "success": true,
  "section": {
    "number": 7,
    "title": "7️⃣ Conclusão geral",
    "icon": "7️⃣"
  },
  "data": {
    "content": {
      "text": "Com base nos documentos analisados:\n\nAs contas do período estão organizadas e coerentes\nOs encargos trabalhistas e tributos estão regularmente recolhidos\nNão há indícios de erros relevantes ou pendências financeiras no período",
      "points": [
        {
          "type": "contas",
          "status": "organizadas_e_coerentes",
          "text": "As contas do período estão organizadas e coerentes"
        },
        {
          "type": "encargos",
          "status": "regularmente_recolhidos",
          "text": "Os encargos trabalhistas e tributos estão regularmente recolhidos"
        },
        {
          "type": "erros",
          "status": "sem_indicios",
          "text": "Não há indícios de erros relevantes ou pendências financeiras no período"
        }
      ],
      "note": "Este relatório tem caráter informativo e visa apoiar síndicos, conselheiros e moradores na compreensão das contas."
    },
    "metadata": {
      "generated_at": "2025-12-30T10:00:30Z",
      "job_id": "550e8400-e29b-41d4-a716-446655440000",
      "processing_time_ms": 50,
      "generation_method": "template"
    }
  }
}
```

### **Resposta (LLM):**

```json
{
  "success": true,
  "section": {
    "number": 7,
    "title": "7️⃣ Conclusão geral",
    "icon": "7️⃣"
  },
  "data": {
    "content": {
      "text": "Cálculos batem com saldos, nada errado na matemática.\nTributos: FGTS ok; INSS compatível; PIS/IRRF não localizados → checar.\nProvisão 13º e férias presente e alimentada.\nGastos fora do padrão: elevadores (R$ 2 mil) e cano de cobre (R$ 1,4 mil)",
      "points": [
        {
          "type": "calculos",
          "text": "Cálculos batem com saldos, nada errado na matemática."
        },
        {
          "type": "tributos",
          "text": "Tributos: FGTS ok; INSS compatível; PIS/IRRF não localizados → checar."
        },
        {
          "type": "provisoes",
          "text": "Provisão 13º e férias presente e alimentada."
        },
        {
          "type": "gastos",
          "text": "Gastos fora do padrão: elevadores (R$ 2 mil) e cano de cobre (R$ 1,4 mil)"
        }
      ],
      "note": "Este relatório tem caráter informativo e visa apoiar síndicos, conselheiros e moradores na compreensão das contas."
    },
    "metadata": {
      "generated_at": "2025-12-30T10:00:30Z",
      "job_id": "550e8400-e29b-41d4-a716-446655440000",
      "processing_time_ms": 2500,
      "generation_method": "llm",
      "llm_model": "gpt-4",
      "llm_tokens_used": 450
    }
  }
}
```

---

## 🔌 Endpoint: `/api/v1/report/section/8`

**Seção:** Parecer final

### **Resposta:**

```json
{
  "success": true,
  "section": {
    "number": 8,
    "title": "8️⃣ Parecer final",
    "icon": "8️⃣"
  },
  "data": {
    "content": {
      "situacao_periodo": "REGULAR",
      "status_color": "green",
      "status_icon": "🟢",
      "text": "Situação do período analisado: REGULAR\nAs contas fecham corretamente, mas a pasta precisa de comprovantes de tributos e documentos de suporte para validar de fato. Houve um gasto fora da rotina que devem ser justificados em assembleia (elevador). No geral, a gestão financeira está sob controle.",
      "summary": {
        "contas_fecham": true,
        "documentacao_completa": false,
        "gastos_fora_rotina": true,
        "gestao_financeira": "sob_controle"
      },
      "recomendacoes": [
        "Obter comprovantes de tributos faltantes",
        "Justificar gasto de elevador em assembleia"
      ]
    },
    "metadata": {
      "generated_at": "2025-12-30T10:00:35Z",
      "job_id": "550e8400-e29b-41d4-a716-446655440000",
      "processing_time_ms": 50,
      "generation_method": "template"
    }
  }
}
```

---

## 🔌 Endpoint: `/api/v1/report/full`

**Descrição:** Retorna todas as seções de uma vez

### **Resposta:**

```json
{
  "success": true,
  "report": {
    "condominio": "________",
    "periodo_analisado": {
      "inicio": "2025-01-01",
      "fim": "2025-01-31"
    },
    "data_relatorio": "2025-12-30",
    "sections": [
      {
        "number": 1,
        "title": "1️⃣ O que foi conferido",
        "data": { /* ... seção 1 ... */ }
      },
      {
        "number": 2,
        "title": "2️⃣ Situação dos documentos",
        "data": { /* ... seção 2 ... */ }
      },
      /* ... outras seções ... */
      {
        "number": 8,
        "title": "8️⃣ Parecer final",
        "data": { /* ... seção 8 ... */ }
      }
    ]
  },
  "metadata": {
    "generated_at": "2025-12-30T10:00:40Z",
    "job_id": "550e8400-e29b-41d4-a716-446655440000",
    "total_processing_time_ms": 3200,
    "sections_count": 8
  }
}
```

---

## 📝 Notas Importantes

### **1. Formato Consistente**

Todas as respostas seguem o mesmo formato:
- `success`: boolean
- `section`: informações da seção
- `data.content`: conteúdo específico da seção
- `data.metadata`: metadados (timestamps, job_id, etc.)

### **2. Estrutura de Dados**

- **JSON estruturado** (não apenas texto)
- Permite que o front-end formate como desejar
- Facilita internacionalização
- Facilita testes automatizados

### **3. Metadados**

Cada resposta inclui:
- `generated_at`: timestamp ISO 8601
- `job_id`: ID do job de análise
- `processing_time_ms`: tempo de processamento em milissegundos
- `generation_method`: "template" ou "llm" (quando aplicável)

### **4. Ícones e Status**

- Ícones emoji para identificação visual
- Status com cores (green, yellow, red)
- Ícones de status (✅, ⚠️, ✔️, etc.)

### **5. Flexibilidade**

- Cada seção pode ser chamada independentemente
- Endpoint `/full` para relatório completo
- Suporte a diferentes métodos de geração (template/LLM)

---

**Última Atualização:** 30 de Dezembro de 2025
