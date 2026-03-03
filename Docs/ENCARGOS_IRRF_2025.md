# IRRF – Regras 2025 e Condição de Auditabilidade

**Data:** Janeiro 2026  
**Versão:** 1.0

---

## Tabela IRRF 2025

Tabela vigente a partir de 2025 para cálculo do Imposto de Renda Retido na Fonte sobre folha:

| Faixa de rendimento mensal (R$) | Alíquota (%) | Parcela a deduzir (R$) |
|--------------------------------|--------------|------------------------|
| Até 5.000,00                    | Isento       | –                      |
| De 5.000,01 até 7.500,00       | 7,5%         | 375,00                 |
| De 7.500,01 até 10.000,00      | 15%          | 1.125,00               |
| De 10.000,01 até 15.000,00     | 22,5%        | 2.250,00               |
| Acima de 15.000,00             | 27,5%        | 3.375,00               |

Fórmula por faixa: `IRRF = (base × alíquota) - parcela_deduzir`

---

## Limitação Fundamental: IRRF por Salário

O IRRF é **recolhido pelo valor de cada salário**, não pelo total da folha. Por isso:

- **Na prestação de contas** há apenas o valor total de IRRF lançado no balancete.
- **Sem holerites individuais** (salário bruto por funcionário) **não é possível calcular ou validar** o IRRF.
- A validação correta exige aplicar a tabela progressiva **por funcionário** e somar os valores.

### Comportamento do Sistema

| Situação                          | Status retornado             | Detalhes                                                                 |
|-----------------------------------|-----------------------------|---------------------------------------------------------------------------|
| Sem holerites na pasta            | `nao_calculavel_sem_holerites` | Mensagem explicando que IRRF é por salário e que é necessário conferir com pasta que contenha holerites |
| Com holerites e valor no balancete| `aplicado_conforme_tabela`  | Valor lançado no balancete; conferir comprovantes (DARFs)                |
| Com holerites, valor zerado       | `nao_auditavel`             | Frase padrão de encargo não auditável                                    |

---

## Separação Prestação vs Folha

Os valores extraídos vêm da **prestação de contas**. Evita-se misturar:

- **Prestação**: linhas consolidadas (ex.: "Folha de pagamento", "FGTS", "INSS", "IRRF")
- **Folha detalhada**: linhas por funcionário (zelador, porteiro, etc.)

O sistema prioriza a **linha consolidada** "Folha de pagamento" / "Folha pagamento" quando existir, para não somar total + breakdown e gerar duplicação.
