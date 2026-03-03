"""
Motor de IA Avançado para Auditoria de Condomínios
Implementa múltiplos algoritmos de Machine Learning e IA moderna
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional, Any
from sklearn.ensemble import IsolationForest, RandomForestClassifier, GradientBoostingClassifier
from sklearn.svm import OneClassSVM
from sklearn.neural_network import MLPClassifier
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, confusion_matrix
from sklearn.cluster import DBSCAN
import warnings
warnings.filterwarnings('ignore')

class AdvancedAIEngine:
    """Motor de IA avançado com múltiplos algoritmos de machine learning"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config if config is not None else self._get_default_config()
        self.models = {}
        self.scalers = {}
        self.encoders = {}
        self.feature_importance = {}
        self.performance_metrics = {}
        
        # Inicializar modelos
        self._initialize_models()
        
    def _get_default_config(self) -> Dict[str, Any]:
        """Configuração padrão para os modelos de IA"""
        return {
            'isolation_forest': {
                'contamination': 0.05,
                'random_state': 42,
                'n_estimators': 100
            },
            'one_class_svm': {
                'nu': 0.05,
                'kernel': 'rbf',
                'gamma': 'scale'
            },
            'random_forest': {
                'n_estimators': 100,
                'random_state': 42,
                'max_depth': 10
            },
            'gradient_boosting': {
                'n_estimators': 100,
                'learning_rate': 0.1,
                'random_state': 42
            },
            'neural_network': {
                'hidden_layer_sizes': (100, 50),
                'random_state': 42,
                'max_iter': 1000
            },
            'dbscan': {
                'eps': 0.5,
                'min_samples': 5
            }
        }
    
    def _initialize_models(self):
        """Inicializa todos os modelos de IA"""
        self.models = {
            'isolation_forest': IsolationForest(**self.config['isolation_forest']),
            'one_class_svm': OneClassSVM(**self.config['one_class_svm']),
            'random_forest': RandomForestClassifier(**self.config['random_forest']),
            'gradient_boosting': GradientBoostingClassifier(**self.config['gradient_boosting']),
            'neural_network': MLPClassifier(**self.config['neural_network']),
            'dbscan': DBSCAN(**self.config['dbscan'])
        }
        
        # Inicializar scalers e encoders
        self.scalers = {
            'standard': StandardScaler(),
            'robust': StandardScaler()
        }
        
        self.encoders = {
            'label': LabelEncoder()
        }
    
    def prepare_features(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
        """Prepara features para os modelos de IA"""
        df_features = df.copy()
        feature_columns = []
        
        # 1. Features numéricas básicas
        numeric_features = ['valor']
        for col in numeric_features:
            if col in df_features.columns:
                feature_columns.append(col)
        
        # 2. Features temporais
        if 'data' in df_features.columns:
            df_features['ano'] = pd.to_datetime(df_features['data']).dt.year
            df_features['mes'] = pd.to_datetime(df_features['data']).dt.month
            df_features['dia_semana'] = pd.to_datetime(df_features['data']).dt.dayofweek
            df_features['dia_mes'] = pd.to_datetime(df_features['data']).dt.day
            
            feature_columns.extend(['ano', 'mes', 'dia_semana', 'dia_mes'])
        
        # 3. Features categóricas
        if 'tipo' in df_features.columns:
            df_features['tipo_encoded'] = self.encoders['label'].fit_transform(df_features['tipo'])
            feature_columns.append('tipo_encoded')
        
        if 'categoria' in df_features.columns:
            # One-hot encoding para categorias
            categoria_dummies = pd.get_dummies(df_features['categoria'], prefix='cat')
            df_features = pd.concat([df_features, categoria_dummies], axis=1)
            feature_columns.extend(categoria_dummies.columns.tolist())
        
        # 4. Features estatísticas por categoria
        if 'categoria' in df_features.columns:
            # Valor médio por categoria
            categoria_stats = df_features.groupby('categoria')['valor'].agg(['mean', 'std', 'count']).reset_index()
            categoria_stats.columns = ['categoria', 'cat_mean', 'cat_std', 'cat_count']
            df_features = df_features.merge(categoria_stats, on='categoria', how='left')
            
            # Features relativas
            df_features['valor_vs_cat_mean'] = df_features['valor'] / (df_features['cat_mean'] + 1e-8)
            df_features['valor_vs_cat_std'] = (df_features['valor'] - df_features['cat_mean']) / (df_features['cat_std'] + 1e-8)
            
            feature_columns.extend(['cat_mean', 'cat_std', 'cat_count', 'valor_vs_cat_mean', 'valor_vs_cat_std'])
        
        # 5. Features de texto (descrição)
        if 'descricao' in df_features.columns:
            # Comprimento da descrição
            df_features['desc_length'] = df_features['descricao'].str.len()
            # Número de palavras
            df_features['desc_words'] = df_features['descricao'].str.split().str.len()
            # Palavras-chave suspeitas
            suspicious_keywords = ['urgente', 'emergencia', 'extraordinario', 'adicional', 'bonus', 'complemento']
            for keyword in suspicious_keywords:
                df_features[f'has_{keyword}'] = df_features['descricao'].str.lower().str.contains(keyword, na=False).astype(int)
                feature_columns.append(f'has_{keyword}')
            
            feature_columns.extend(['desc_length', 'desc_words'])
        
        # 6. Features de agrupamento temporal
        if 'data' in df_features.columns:
            df_features['data'] = pd.to_datetime(df_features['data'])
            # Rolling statistics
            df_features = df_features.sort_values('data')
            df_features['valor_rolling_mean_7d'] = df_features['valor'].rolling(window=7, min_periods=1).mean()
            df_features['valor_rolling_std_7d'] = df_features['valor'].rolling(window=7, min_periods=1).std()
            
            feature_columns.extend(['valor_rolling_mean_7d', 'valor_rolling_std_7d'])
        
        # Remover colunas com valores nulos
        df_features = df_features.fillna(0)
        
        return df_features, feature_columns
    
    def train_ensemble_models(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Treina ensemble de modelos de IA"""
        print("Iniciando treinamento de ensemble de modelos de IA...")
        
        # Preparar features
        df_features, feature_columns = self.prepare_features(df)
        
        # Separar features e target (se disponível)
        X = df_features[feature_columns]
        
        # Normalizar features
        X_scaled = self.scalers['standard'].fit_transform(X)
        
        # Criar target sintético baseado em regras (para modelos supervisionados)
        y_synthetic = self._create_synthetic_target(df_features)
        
        results = {}
        
        # 1. Modelos não supervisionados (anomaly detection)
        print("   Treinando modelos de deteccao de anomalias...")
        
        # Isolation Forest
        iso_scores = self.models['isolation_forest'].fit_predict(X_scaled)
        results['isolation_forest'] = {
            'scores': iso_scores,
            'anomalies': iso_scores == -1
        }
        
        # One-Class SVM
        svm_scores = self.models['one_class_svm'].fit_predict(X_scaled)
        results['one_class_svm'] = {
            'scores': svm_scores,
            'anomalies': svm_scores == -1
        }
        
        # 2. Modelos supervisionados (stratify so quando ha amostras suficientes para train/test)
        unique, counts = np.unique(y_synthetic, return_counts=True)
        has_two_classes = len(unique) > 1
        n_samples = len(y_synthetic)
        min_test = max(2, len(unique))  # teste precisa ter pelo menos 1 por classe
        can_stratify = (
            has_two_classes
            and (counts >= 2).all()
            and (n_samples * 0.2 >= min_test)
        )
        if has_two_classes:
            print("   Treinando modelos supervisionados...")
            if can_stratify:
                X_train, X_test, y_train, y_test = train_test_split(
                    X_scaled, y_synthetic, test_size=0.2, random_state=42, stratify=y_synthetic
                )
            else:
                X_train, X_test, y_train, y_test = train_test_split(
                    X_scaled, y_synthetic, test_size=0.2, random_state=42
                )
            
            # Random Forest
            self.models['random_forest'].fit(X_train, y_train)
            rf_pred = self.models['random_forest'].predict(X_test)
            results['random_forest'] = {
                'predictions': self.models['random_forest'].predict(X_scaled),
                'probabilities': self.models['random_forest'].predict_proba(X_scaled),
                'feature_importance': dict(zip(feature_columns, self.models['random_forest'].feature_importances_)),
                'performance': classification_report(y_test, rf_pred, output_dict=True)
            }
            
            # Gradient Boosting
            self.models['gradient_boosting'].fit(X_train, y_train)
            gb_pred = self.models['gradient_boosting'].predict(X_test)
            results['gradient_boosting'] = {
                'predictions': self.models['gradient_boosting'].predict(X_scaled),
                'probabilities': self.models['gradient_boosting'].predict_proba(X_scaled),
                'feature_importance': dict(zip(feature_columns, self.models['gradient_boosting'].feature_importances_)),
                'performance': classification_report(y_test, gb_pred, output_dict=True)
            }
            
            # Neural Network
            self.models['neural_network'].fit(X_train, y_train)
            nn_pred = self.models['neural_network'].predict(X_test)
            results['neural_network'] = {
                'predictions': self.models['neural_network'].predict(X_scaled),
                'probabilities': self.models['neural_network'].predict_proba(X_scaled),
                'performance': classification_report(y_test, nn_pred, output_dict=True)
            }
        
        # 3. Clustering
        print("   Executando analise de clusters...")
        cluster_labels = self.models['dbscan'].fit_predict(X_scaled)
        results['clustering'] = {
            'labels': cluster_labels,
            'n_clusters': len(set(cluster_labels)) - (1 if -1 in cluster_labels else 0),
            'n_noise': list(cluster_labels).count(-1)
        }
        
        # 4. Ensemble final
        print("   Criando ensemble final...")
        ensemble_scores = self._create_ensemble_scores(results, len(X_scaled))
        results['ensemble'] = {
            'scores': ensemble_scores,
            'anomalies': ensemble_scores > 0.5
        }
        
        # Salvar feature importance
        self.feature_importance = results.get('random_forest', {}).get('feature_importance', {})
        
        print("Treinamento de ensemble concluido.")
        return results
    
    def _create_synthetic_target(self, df: pd.DataFrame) -> np.ndarray:
        """Cria target sintético baseado em regras para treinar modelos supervisionados"""
        y = np.zeros(len(df))
        
        # Regras para criar target sintético
        # 1. Valores muito altos
        y[df['valor'] > df['valor'].quantile(0.95)] = 1
        
        # 2. Valores muito baixos
        y[df['valor'] < df['valor'].quantile(0.05)] = 1
        
        # 3. Receitas negativas
        if 'tipo' in df.columns:
            y[(df['tipo'].str.lower() == 'receita') & (df['valor'] < 0)] = 1
        
        # 4. Salários muito altos
        if 'categoria' in df.columns:
            y[(df['categoria'].str.contains('Salário', case=False, na=False)) & (df['valor'] > 5000)] = 1
        
        return y
    
    def _create_ensemble_scores(self, results: Dict, n_samples: int) -> np.ndarray:
        """Cria scores de ensemble combinando múltiplos modelos"""
        ensemble_scores = np.zeros(n_samples)
        weights = {
            'isolation_forest': 0.3,
            'one_class_svm': 0.2,
            'random_forest': 0.25,
            'gradient_boosting': 0.25
        }
        
        # Isolation Forest scores
        if 'isolation_forest' in results:
            iso_scores = (results['isolation_forest']['scores'] == -1).astype(float)
            ensemble_scores += weights['isolation_forest'] * iso_scores
        
        # One-Class SVM scores
        if 'one_class_svm' in results:
            svm_scores = (results['one_class_svm']['scores'] == -1).astype(float)
            ensemble_scores += weights['one_class_svm'] * svm_scores
        
        # Random Forest scores
        if 'random_forest' in results:
            rf_scores = results['random_forest']['probabilities'][:, 1]  # Probabilidade da classe positiva
            ensemble_scores += weights['random_forest'] * rf_scores
        
        # Gradient Boosting scores
        if 'gradient_boosting' in results:
            gb_scores = results['gradient_boosting']['probabilities'][:, 1]
            ensemble_scores += weights['gradient_boosting'] * gb_scores
        
        return ensemble_scores
    
    def detect_anomalies(self, df: pd.DataFrame) -> pd.DataFrame:
        """Detecta anomalias usando ensemble de modelos de IA"""
        print("Iniciando deteccao de anomalias com IA avancada...")
        
        # Treinar modelos
        ai_results = self.train_ensemble_models(df)
        
        # Preparar features
        df_features, feature_columns = self.prepare_features(df)
        
        # Adicionar colunas de anomalia
        df_result = df.copy()
        df_result['ai_anomaly_score'] = ai_results['ensemble']['scores']
        df_result['ai_anomaly_detected'] = ai_results['ensemble']['anomalies']
        df_result['cluster_label'] = ai_results['clustering']['labels']
        
        # Adicionar scores individuais dos modelos
        df_result['iso_forest_score'] = (ai_results['isolation_forest']['scores'] == -1).astype(int)
        df_result['svm_score'] = (ai_results['one_class_svm']['scores'] == -1).astype(int)
        
        if 'random_forest' in ai_results:
            df_result['rf_anomaly_prob'] = ai_results['random_forest']['probabilities'][:, 1]
        
        if 'gradient_boosting' in ai_results:
            df_result['gb_anomaly_prob'] = ai_results['gradient_boosting']['probabilities'][:, 1]
        
        # Criar justificativas baseadas nos scores
        df_result['ai_justification'] = self._create_ai_justifications(df_result, ai_results)
        
        # Atualizar coluna principal de anomalia
        df_result['anomalia_detectada'] = df_result['ai_anomaly_detected']
        
        # Combinar com justificativas existentes
        if 'justificativa_anomalia' in df_result.columns:
            df_result['justificativa_anomalia'] = df_result.apply(
                lambda row: f"{row['justificativa_anomalia']}; {row['ai_justification']}" 
                if row['justificativa_anomalia'] and row['ai_justification'] 
                else row['ai_justification'] if row['ai_justification'] else row['justificativa_anomalia'],
                axis=1
            )
        else:
            df_result['justificativa_anomalia'] = df_result['ai_justification']
        
        print(f"IA detectou {df_result['ai_anomaly_detected'].sum()} anomalias")
        return df_result
    
    def _create_ai_justifications(self, df: pd.DataFrame, ai_results: Dict) -> List[str]:
        """Cria justificativas inteligentes baseadas nos resultados da IA"""
        justifications = []
        
        for idx, row in df.iterrows():
            justification_parts = []
            
            # Score do ensemble
            if row['ai_anomaly_score'] > 0.7:
                justification_parts.append("Alta probabilidade de anomalia (IA)")
            elif row['ai_anomaly_score'] > 0.5:
                justification_parts.append("Probabilidade moderada de anomalia (IA)")
            
            # Isolation Forest
            if row['iso_forest_score'] == 1:
                justification_parts.append("Detectado por Isolation Forest")
            
            # One-Class SVM
            if row['svm_score'] == 1:
                justification_parts.append("Detectado por One-Class SVM")
            
            # Random Forest
            if 'rf_anomaly_prob' in row and row['rf_anomaly_prob'] > 0.7:
                justification_parts.append("Alta probabilidade (Random Forest)")
            
            # Gradient Boosting
            if 'gb_anomaly_prob' in row and row['gb_anomaly_prob'] > 0.7:
                justification_parts.append("Alta probabilidade (Gradient Boosting)")
            
            # Clustering
            if row['cluster_label'] == -1:
                justification_parts.append("Outlier em análise de clusters")
            
            # Features específicas
            if 'valor_vs_cat_std' in row and abs(row['valor_vs_cat_std']) > 3:
                justification_parts.append("Desvio significativo da categoria")
            
            if 'valor_vs_cat_mean' in row and row['valor_vs_cat_mean'] > 2:
                justification_parts.append("Valor muito acima da média da categoria")
            
            justifications.append("; ".join(justification_parts) if justification_parts else "")
        
        return justifications
    
    def get_feature_importance(self) -> Dict[str, float]:
        """Retorna importância das features"""
        return self.feature_importance
    
    def get_model_performance(self) -> Dict[str, Any]:
        """Retorna performance dos modelos"""
        return self.performance_metrics
    
    def explain_anomaly(self, df: pd.DataFrame, anomaly_idx: int) -> Dict[str, Any]:
        """Explica uma anomalia específica usando IA explicável"""
        if anomaly_idx >= len(df):
            return {"error": "Índice de anomalia inválido"}
        
        anomaly_row = df.iloc[anomaly_idx]
        explanation = {
            'transaction_id': anomaly_idx,
            'ai_anomaly_score': anomaly_row.get('ai_anomaly_score', 0),
            'main_features': {},
            'model_contributions': {},
            'recommendations': []
        }
        
        # Contribuições dos modelos
        if 'iso_forest_score' in anomaly_row:
            explanation['model_contributions']['isolation_forest'] = anomaly_row['iso_forest_score']
        
        if 'svm_score' in anomaly_row:
            explanation['model_contributions']['one_class_svm'] = anomaly_row['svm_score']
        
        if 'rf_anomaly_prob' in anomaly_row:
            explanation['model_contributions']['random_forest'] = anomaly_row['rf_anomaly_prob']
        
        if 'gb_anomaly_prob' in anomaly_row:
            explanation['model_contributions']['gradient_boosting'] = anomaly_row['gb_anomaly_prob']
        
        # Features principais
        if 'valor_vs_cat_std' in anomaly_row:
            explanation['main_features']['desvio_categoria'] = anomaly_row['valor_vs_cat_std']
        
        if 'valor_vs_cat_mean' in anomaly_row:
            explanation['main_features']['vs_media_categoria'] = anomaly_row['valor_vs_cat_mean']
        
        # Recomendações
        if anomaly_row.get('ai_anomaly_score', 0) > 0.8:
            explanation['recommendations'].append("Prioridade alta: Investigar imediatamente")
        
        if anomaly_row.get('valor_vs_cat_std', 0) > 3:
            explanation['recommendations'].append("Verificar se valor está correto para categoria")
        
        return explanation

# Exemplo de uso
if __name__ == "__main__":
    # Criar dados de exemplo
    sample_data = {
        'data': pd.date_range('2025-01-01', periods=100, freq='D'),
        'descricao': ['Transação normal'] * 90 + ['Transação suspeita'] * 10,
        'tipo': ['receita'] * 50 + ['despesa'] * 50,
        'valor': np.random.normal(1000, 200, 100),
        'categoria': ['Normal'] * 90 + ['Suspeita'] * 10
    }
    
    # Adicionar algumas anomalias
    sample_data['valor'][-10:] = np.random.normal(5000, 1000, 10)  # Valores altos
    
    df = pd.DataFrame(sample_data)
    
    # Inicializar IA
    ai_engine = AdvancedAIEngine()
    
    # Detectar anomalias
    df_with_anomalies = ai_engine.detect_anomalies(df)
    
    print("\n=== RESULTADOS DA IA AVANÇADA ===")
    print(f"Total de transações: {len(df_with_anomalies)}")
    print(f"Anomalias detectadas: {df_with_anomalies['ai_anomaly_detected'].sum()}")
    
    # Mostrar anomalias
    anomalies = df_with_anomalies[df_with_anomalies['ai_anomaly_detected']]
    print(f"\nAnomalias detectadas:")
    for idx, row in anomalies.iterrows():
        print(f"  - {row['descricao']}: R$ {row['valor']:.2f} (Score: {row['ai_anomaly_score']:.3f})")
        print(f"    Justificativa: {row['ai_justification']}")
