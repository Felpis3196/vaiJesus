"""
Analisador de Processamento de Linguagem Natural para Auditoria de Condomínios
Analisa descrições de transações usando NLP para detectar padrões suspeitos
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional, Any
import re
from collections import Counter
import warnings
warnings.filterwarnings('ignore')

class NLPAnalyzer:
    """Analisador de linguagem natural para descrições de transações"""
    
    def __init__(self):
        self.suspicious_patterns = self._load_suspicious_patterns()
        self.category_keywords = self._load_category_keywords()
        self.fraud_indicators = self._load_fraud_indicators()
        
    def _load_suspicious_patterns(self) -> Dict[str, List[str]]:
        """Carrega padrões suspeitos em descrições"""
        return {
            'urgency_indicators': [
                'urgente', 'emergência', 'emergencia', 'imediato', 'asap',
                'rápido', 'rapido', 'pressa', 'urgência'
            ],
            'vague_descriptions': [
                'diversos', 'varios', 'vários', 'outros', 'miscelânea',
                'miscelanea', 'geral', 'administração', 'administracao'
            ],
            'amount_indicators': [
                'valor alto', 'quantia', 'montante', 'total geral',
                'soma', 'conjunto'
            ],
            'temporal_irregularities': [
                'retroativo', 'retroativo', 'complemento', 'adicional',
                'bonus', 'extra', 'extraordinário', 'extraordinario'
            ],
            'missing_details': [
                'sem nota', 'sem comprovante', 'sem detalhamento',
                'não especificado', 'nao especificado'
            ]
        }
    
    def _load_category_keywords(self) -> Dict[str, List[str]]:
        """Carrega palavras-chave para categorização inteligente"""
        return {
            'Taxas Condominiais': [
                'taxa', 'condominial', 'condomínio', 'condominio', 'cota',
                'mensalidade', 'rateio', 'apartamento', 'apto'
            ],
            'Água': [
                'água', 'agua', 'saneamento', 'sabesp', 'concessionária',
                'concessionaria', 'hidrômetro', 'hidrometro'
            ],
            'Eletricidade': [
                'luz', 'energia', 'eletricidade', 'enel', 'eletropaulo',
                'conta de luz', 'consumo'
            ],
            'Salários': [
                'salário', 'salario', 'zelador', 'porteiro', 'funcionário',
                'funcionario', 'folha', 'rh', 'recursos humanos'
            ],
            'Manutenção': [
                'manutenção', 'manutencao', 'reparo', 'conserto', 'jardim',
                'piscina', 'elevador', 'portão', 'portao'
            ],
            'Segurança': [
                'segurança', 'seguranca', 'vigilância', 'vigilancia',
                'alarme', 'câmera', 'camera', 'monitoramento'
            ],
            'Administração': [
                'administração', 'administracao', 'honorários', 'honorarios',
                'contabilidade', 'contador', 'advogado'
            ],
            'Obras': [
                'obra', 'reforma', 'construção', 'construcao', 'melhoria',
                'ampliação', 'ampliacao', 'pintura'
            ],
            'Impostos': [
                'imposto', 'iptu', 'taxa', 'tributo', 'fiscal', 'receita'
            ],
            'Multas': [
                'multa', 'infração', 'infracao', 'penalidade', 'sanção',
                'sancao', 'cobrança', 'cobranca'
            ]
        }
    
    def _load_fraud_indicators(self) -> Dict[str, List[str]]:
        """Carrega indicadores de possível fraude"""
        return {
            'duplicate_patterns': [
                'mesmo valor', 'valor idêntico', 'valor identico',
                'duplicado', 'repetido'
            ],
            'round_amounts': [
                'valor redondo', 'quantia exata', 'sem centavos'
            ],
            'personal_expenses': [
                'pessoal', 'particular', 'privado', 'pessoa física',
                'pessoa fisica'
            ],
            'unusual_timing': [
                'fim de semana', 'feriado', 'madrugada', 'noite',
                'horário não comercial', 'horario nao comercial'
            ],
            'cash_transactions': [
                'dinheiro', 'especie', 'efetivo', 'caixa', 'numerário',
                'numerario'
            ]
        }
    
    def analyze_descriptions(self, df: pd.DataFrame) -> pd.DataFrame:
        """Analisa descrições das transações usando NLP"""
        df_analyzed = df.copy()
        
        print("Analisando descricoes com NLP...")
        
        # 1. Análise de padrões suspeitos
        df_analyzed['suspicious_patterns'] = df_analyzed['descricao'].apply(
            self._detect_suspicious_patterns
        )
        
        # 2. Análise de indicadores de fraude
        df_analyzed['fraud_indicators'] = df_analyzed['descricao'].apply(
            self._detect_fraud_indicators
        )
        
        # 3. Categorização inteligente baseada em NLP
        df_analyzed['nlp_category'] = df_analyzed['descricao'].apply(
            self._categorize_with_nlp
        )
        
        # 4. Análise de sentimento (básica)
        df_analyzed['sentiment_score'] = df_analyzed['descricao'].apply(
            self._analyze_sentiment
        )
        
        # 5. Análise de complexidade
        df_analyzed['description_complexity'] = df_analyzed['descricao'].apply(
            self._analyze_complexity
        )
        
        # 6. Detecção de valores em texto
        df_analyzed['values_in_text'] = df_analyzed['descricao'].apply(
            self._extract_values_from_text
        )
        
        # 7. Análise de consistência
        df_analyzed['consistency_score'] = df_analyzed.apply(
            self._analyze_consistency, axis=1
        )
        
        # 8. Score de suspeição NLP
        df_analyzed['nlp_suspicion_score'] = df_analyzed.apply(
            self._calculate_nlp_suspicion_score, axis=1
        )
        
        print(f"NLP analisou {len(df_analyzed)} descricoes")
        return df_analyzed
    
    def _detect_suspicious_patterns(self, description: str) -> List[str]:
        """Detecta padrões suspeitos na descrição"""
        if pd.isna(description):
            return []
        
        description_lower = str(description).lower()
        detected_patterns = []
        
        for pattern_type, patterns in self.suspicious_patterns.items():
            for pattern in patterns:
                if pattern in description_lower:
                    detected_patterns.append(pattern_type)
        
        return list(set(detected_patterns))  # Remove duplicatas
    
    def _detect_fraud_indicators(self, description: str) -> List[str]:
        """Detecta indicadores de fraude na descrição"""
        if pd.isna(description):
            return []
        
        description_lower = str(description).lower()
        detected_indicators = []
        
        for indicator_type, indicators in self.fraud_indicators.items():
            for indicator in indicators:
                if indicator in description_lower:
                    detected_indicators.append(indicator_type)
        
        return list(set(detected_indicators))
    
    def _categorize_with_nlp(self, description: str) -> str:
        """Categoriza transação baseada em análise NLP da descrição"""
        if pd.isna(description):
            return 'Não Categorizado'
        
        description_lower = str(description).lower()
        category_scores = {}
        
        for category, keywords in self.category_keywords.items():
            score = 0
            for keyword in keywords:
                if keyword in description_lower:
                    score += 1
            category_scores[category] = score
        
        # Retornar categoria com maior score
        if category_scores:
            # Usar lambda para garantir type safety
            best_category = max(category_scores.items(), key=lambda x: x[1])[0]
            if category_scores[best_category] > 0:
                return best_category
        
        return 'Não Categorizado'
    
    def _analyze_sentiment(self, description: str) -> float:
        """Análise básica de sentimento (positivo/negativo/neutro)"""
        if pd.isna(description):
            return 0.0
        
        description_lower = str(description).lower()
        
        # Palavras positivas
        positive_words = ['aprovado', 'conforme', 'correto', 'válido', 'valido', 'ok']
        positive_count = sum(1 for word in positive_words if word in description_lower)
        
        # Palavras negativas
        negative_words = ['problema', 'erro', 'inconsistente', 'suspeito', 'anômalo', 'anomalo']
        negative_count = sum(1 for word in negative_words if word in description_lower)
        
        # Palavras neutras/administrativas
        neutral_words = ['pagamento', 'recebimento', 'transferência', 'transferencia', 'depósito', 'deposito']
        neutral_count = sum(1 for word in neutral_words if word in description_lower)
        
        total_words = len(description_lower.split())
        if total_words == 0:
            return 0.0
        
        # Score normalizado (-1 a 1)
        sentiment_score = (positive_count - negative_count) / total_words
        return max(-1, min(1, sentiment_score))
    
    def _analyze_complexity(self, description: str) -> Dict[str, float]:
        """Analisa complexidade da descrição"""
        if pd.isna(description):
            return {'length': 0, 'word_count': 0, 'sentence_count': 0, 'complexity_score': 0}
        
        description_str = str(description)
        
        # Métricas básicas
        length = len(description_str)
        words = description_str.split()
        word_count = len(words)
        sentence_count = len(re.split(r'[.!?]+', description_str))
        
        # Score de complexidade (0-1)
        avg_word_length = np.mean([len(word) for word in words]) if words else 0.0
        complexity_score = min(1.0, float((avg_word_length / 10) * (word_count / 20)))
        
        return {
            'length': length,
            'word_count': word_count,
            'sentence_count': sentence_count,
            'complexity_score': complexity_score
        }
    
    def _extract_values_from_text(self, description: str) -> List[float]:
        """Extrai valores monetários mencionados na descrição"""
        if pd.isna(description):
            return []
        
        # Padrões para valores monetários
        patterns = [
            r'R\$\s*(\d+(?:\.\d{3})*(?:,\d{2})?)',  # R$ 1.000,00
            r'(\d+(?:\.\d{3})*(?:,\d{2})?)\s*reais',  # 1000,00 reais
            r'valor\s*(\d+(?:\.\d{3})*(?:,\d{2})?)',  # valor 1000,00
        ]
        
        values = []
        for pattern in patterns:
            matches = re.findall(pattern, str(description), re.IGNORECASE)
            for match in matches:
                # Converter para float
                value_str = match.replace('.', '').replace(',', '.')
                try:
                    values.append(float(value_str))
                except ValueError:
                    continue
        
        return values
    
    def _analyze_consistency(self, row: pd.Series) -> float:
        """Analisa consistência entre descrição, categoria e valor"""
        descricao_val = row.get('descricao')
        categoria_val = row.get('categoria')
        
        # Verificar se valores não são None e não são NaN
        if descricao_val is None or categoria_val is None:
            return 0.0
        if pd.isna(descricao_val) or pd.isna(categoria_val):
            return 0.0
        
        # Garantir que são strings
        description = str(descricao_val).lower()
        category = str(categoria_val).lower()
        value = row.get('valor', 0)
        
        # Garantir que value é numérico
        if value is None or pd.isna(value):
            value = 0.0
        value = float(value) if isinstance(value, (int, float)) else 0.0
        
        consistency_score = 1.0
        
        # Verificar se descrição menciona categoria
        category_keywords = {
            'taxas condominiais': ['taxa', 'condominial', 'cota'],
            'água': ['água', 'agua', 'saneamento'],
            'salários': ['salário', 'salario', 'zelador', 'porteiro'],
            'manutenção': ['manutenção', 'manutencao', 'reparo'],
            'eletricidade': ['luz', 'energia', 'eletricidade']
        }
        
        if category in category_keywords:
            keywords = category_keywords[category]
            if not any(keyword in description for keyword in keywords):
                consistency_score -= 0.3
        
        # Verificar valores mencionados na descrição
        descricao_str = str(row.get('descricao', ''))
        values_in_text = self._extract_values_from_text(descricao_str)
        if values_in_text:
            # Verificar se algum valor na descrição é próximo ao valor da transação
            value_consistency = any(abs(float(val) - value) < 0.01 for val in values_in_text)
            if not value_consistency:
                consistency_score -= 0.2
        
        # Verificar complexidade vs valor
        complexity = self._analyze_complexity(descricao_str)
        complexity_score = complexity.get('complexity_score', 0.0) if isinstance(complexity, dict) else 0.0
        if value > 1000 and complexity_score < 0.1:
            consistency_score -= 0.1  # Valores altos deveriam ter descrições mais detalhadas
        
        return max(0, consistency_score)
    
    def _calculate_nlp_suspicion_score(self, row: pd.Series) -> float:
        """Calcula score de suspeição baseado em análise NLP"""
        score = 0.0
        
        # Padrões suspeitos (peso: 0.3)
        suspicious_patterns = row.get('suspicious_patterns', [])
        if suspicious_patterns is not None and isinstance(suspicious_patterns, (list, tuple)):
            score += len(suspicious_patterns) * 0.1
        
        # Indicadores de fraude (peso: 0.4)
        fraud_indicators = row.get('fraud_indicators', [])
        if fraud_indicators is not None and isinstance(fraud_indicators, (list, tuple)):
            score += len(fraud_indicators) * 0.15
        
        # Inconsistência (peso: 0.2)
        consistency_score = row.get('consistency_score', 1.0)
        if consistency_score is not None:
            consistency_val = float(consistency_score) if isinstance(consistency_score, (int, float)) else 1.0
            score += (1.0 - consistency_val) * 0.3
        
        # Sentimento negativo (peso: 0.1)
        sentiment_score = row.get('sentiment_score', 0.0)
        if sentiment_score is not None:
            sentiment_val = float(sentiment_score) if isinstance(sentiment_score, (int, float)) else 0.0
            if sentiment_val < 0:
                score += abs(sentiment_val) * 0.1
        
        return min(1.0, score)
    
    def generate_nlp_report(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Gera relatório de análise NLP"""
        report = {
            'summary': {},
            'suspicious_patterns': {},
            'fraud_indicators': {},
            'category_analysis': {},
            'recommendations': []
        }
        
        # Resumo
        total_transactions = len(df)
        # Garantir que nlp_suspicion_score existe e é Series antes de filtrar
        if 'nlp_suspicion_score' in df.columns:
            suspicion_series = df['nlp_suspicion_score']
            # Filtrar valores não nulos e maiores que 0.5
            high_suspicion_mask = suspicion_series.notna() & (suspicion_series > 0.5)
            high_suspicion = int(high_suspicion_mask.sum()) if isinstance(high_suspicion_mask, pd.Series) else 0
        else:
            high_suspicion = 0
        
        report['summary'] = {
            'total_transactions': total_transactions,
            'high_suspicion_count': high_suspicion,
            'high_suspicion_percentage': (high_suspicion / total_transactions * 100) if total_transactions > 0 else 0
        }
        
        # Análise de padrões suspeitos
        all_patterns = []
        if 'suspicious_patterns' in df.columns:
            for patterns in df['suspicious_patterns']:
                if patterns is not None and isinstance(patterns, (list, tuple)):
                    all_patterns.extend(patterns)
        
        report['suspicious_patterns'] = dict(Counter(all_patterns))
        
        # Análise de indicadores de fraude
        all_indicators = []
        if 'fraud_indicators' in df.columns:
            for indicators in df['fraud_indicators']:
                if indicators is not None and isinstance(indicators, (list, tuple)):
                    all_indicators.extend(indicators)
        
        report['fraud_indicators'] = dict(Counter(all_indicators))
        
        # Análise de categorias
        if 'nlp_category' in df.columns:
            category_counts = df['nlp_category'].value_counts()
            report['category_analysis'] = category_counts.to_dict()
        
        # Recomendações
        if high_suspicion > total_transactions * 0.1:
            report['recommendations'].append("Alta taxa de suspeição detectada - revisar transações prioritariamente")
        
        if 'vague_descriptions' in report['suspicious_patterns']:
            report['recommendations'].append("Muitas descrições vagas encontradas - solicitar mais detalhes")
        
        if 'urgency_indicators' in report['suspicious_patterns']:
            report['recommendations'].append("Indicadores de urgência detectados - verificar justificativas")
        
        return report

# Exemplo de uso
if __name__ == "__main__":
    # Dados de exemplo
    sample_data = {
        'descricao': [
            'Taxa condominial apartamento 101',
            'Pagamento urgente de conta de água',
            'Salário do zelador conforme contrato',
            'Despesa diversa sem comprovante',
            'Manutenção extraordinária do elevador',
            'Transferência para pessoa física'
        ],
        'categoria': ['Taxas Condominiais', 'Água', 'Salários', 'Outras', 'Manutenção', 'Outras'],
        'valor': [500.0, 180.0, 1200.0, 150.0, 450.0, 200.0]
    }
    
    df = pd.DataFrame(sample_data)
    
    # Analisar com NLP
    nlp_analyzer = NLPAnalyzer()
    df_analyzed = nlp_analyzer.analyze_descriptions(df)
    
    print("\n=== ANÁLISE NLP ===")
    transaction_num = 1
    for idx, row in df_analyzed.iterrows():
        print(f"\nTransação {transaction_num}:")
        transaction_num += 1
        print(f"  Descrição: {row['descricao']}")
        print(f"  Padrões suspeitos: {row['suspicious_patterns']}")
        print(f"  Indicadores de fraude: {row['fraud_indicators']}")
        print(f"  Score de suspeição NLP: {row['nlp_suspicion_score']:.3f}")
        print(f"  Categoria NLP: {row['nlp_category']}")
    
    # Gerar relatório
    report = nlp_analyzer.generate_nlp_report(df_analyzed)
    print(f"\n=== RELATÓRIO NLP ===")
    print(f"Transações de alta suspeição: {report['summary']['high_suspicion_count']}")
    print(f"Padrões suspeitos mais comuns: {report['suspicious_patterns']}")
    print(f"Recomendações: {report['recommendations']}")
