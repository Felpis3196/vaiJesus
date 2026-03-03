"""
Sistema de IA Preditiva para Auditoria de Condomínios
Prevê riscos de fraude e anomalias antes que aconteçam
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional, Any
from datetime import datetime, timedelta
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score
import warnings
warnings.filterwarnings('ignore')

class PredictiveAI:
    """Sistema de IA preditiva para auditoria de condomínios"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config if config is not None else self._get_default_config()
        self.models = {}
        self.scalers = {}
        self.encoders = {}
        self.feature_importance = {}
        self.risk_thresholds = {
            'low': 0.3,
            'medium': 0.6,
            'high': 0.8
        }
        
        self._initialize_models()
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Configuração padrão para modelos preditivos"""
        return {
            'random_forest': {
                'n_estimators': 100,
                'max_depth': 10,
                'random_state': 42
            },
            'gradient_boosting': {
                'n_estimators': 100,
                'learning_rate': 0.1,
                'max_depth': 6,
                'random_state': 42
            },
            'linear_regression': {
                'fit_intercept': True
            },
            'prediction_horizon_days': 30,
            'min_training_samples': 50
        }
    
    def _initialize_models(self):
        """Inicializa modelos preditivos"""
        self.models = {
            'fraud_risk': RandomForestRegressor(**self.config['random_forest']),
            'anomaly_probability': GradientBoostingRegressor(**self.config['gradient_boosting']),
            'budget_deviation': LinearRegression(**self.config['linear_regression'])
        }
        
        self.scalers = {
            'features': StandardScaler(),
            'targets': StandardScaler()
        }
        
        self.encoders = {
            'categoria': LabelEncoder(),
            'tipo': LabelEncoder()
        }
    
    def prepare_training_data(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, np.ndarray]]:
        """Prepara dados para treinamento dos modelos preditivos"""
        print("Preparando dados para IA preditiva...")
        
        df_features = df.copy()
        df_features['data'] = pd.to_datetime(df_features['data'])
        df_features = df_features.sort_values('data')
        
        # Features temporais
        df_features['ano'] = df_features['data'].dt.year
        df_features['mes'] = df_features['data'].dt.month
        df_features['dia_semana'] = df_features['data'].dt.dayofweek
        df_features['dia_mes'] = df_features['data'].dt.day
        df_features['trimestre'] = df_features['data'].dt.quarter
        
        # Features de categoria
        if 'categoria' in df_features.columns:
            df_features['categoria_encoded'] = self.encoders['categoria'].fit_transform(
                df_features['categoria'].fillna('Não Categorizado')
            )
        
        if 'tipo' in df_features.columns:
            tipo_series = df_features['tipo'].astype(str).fillna('Não informado')
            df_features['tipo_encoded'] = self.encoders['tipo'].fit_transform(tipo_series)
        
        # Features estatísticas por categoria
        df_features = self._add_category_statistics(df_features)
        
        # Features de tendência
        df_features = self._add_trend_features(df_features)
        
        # Features de sazonalidade
        df_features = self._add_seasonality_features(df_features)
        
        # Targets para predição
        targets = self._create_prediction_targets(df_features)
        
        # Features finais
        feature_columns = [
            'ano', 'mes', 'dia_semana', 'dia_mes', 'trimestre',
            'categoria_encoded', 'tipo_encoded',
            'valor_mean_cat', 'valor_std_cat', 'valor_count_cat',
            'trend_7d', 'trend_30d', 'seasonality_score',
            'budget_vs_actual', 'category_volatility'
        ]
        
        # Filtrar colunas existentes
        feature_columns = [col for col in feature_columns if col in df_features.columns]
        
        # Garantir que retorna DataFrame mesmo se houver apenas uma coluna
        if len(feature_columns) == 0:
            # Se não houver colunas, criar DataFrame vazio com índice
            features_df = pd.DataFrame(index=df_features.index)
        else:
            # Selecionar colunas - sempre retorna DataFrame
            selected = df_features[feature_columns]
            # Garantir que é DataFrame (não Series)
            if isinstance(selected, pd.Series):
                features_df = selected.to_frame()
            elif isinstance(selected, pd.DataFrame):
                features_df = selected
            else:
                # Fallback: criar DataFrame vazio
                features_df = pd.DataFrame(index=df_features.index)
        
        # Imputar NaN: GradientBoostingRegressor (e outros) não aceitam missing values
        if not features_df.empty:
            features_df = features_df.fillna(0)
        
        return features_df, targets
    
    def _add_category_statistics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Adiciona estatísticas por categoria"""
        if 'categoria' not in df.columns:
            return df
        
        # Estatísticas por categoria
        cat_stats = df.groupby('categoria')['valor'].agg(['mean', 'std', 'count']).reset_index()
        cat_stats.columns = ['categoria', 'valor_mean_cat', 'valor_std_cat', 'valor_count_cat']
        
        # Garantir que cat_stats é DataFrame antes de merge
        if isinstance(cat_stats, pd.DataFrame):
            df = df.merge(cat_stats, on='categoria', how='left')
        else:
            # Se não for DataFrame, criar colunas vazias
            df['valor_mean_cat'] = 0.0
            df['valor_std_cat'] = 0.0
            df['valor_count_cat'] = 0
        
        # Volatilidade da categoria
        df['category_volatility'] = df['valor_std_cat'] / (df['valor_mean_cat'] + 1e-8)
        
        return df
    
    def _add_trend_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Adiciona features de tendência"""
        # Tendência de 7 dias
        df['trend_7d'] = df['valor'].rolling(window=7, min_periods=1).mean()
        
        # Tendência de 30 dias
        df['trend_30d'] = df['valor'].rolling(window=30, min_periods=1).mean()
        
        # Diferença entre tendências
        df['trend_diff'] = df['trend_7d'] - df['trend_30d']
        
        return df
    
    def _add_seasonality_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Adiciona features de sazonalidade"""
        # Score de sazonalidade baseado no mês
        monthly_means_series = df.groupby('mes')['valor'].mean()
        
        # Converter para dict para garantir type safety no map()
        # Garantir que monthly_means_series é Series antes de converter
        monthly_means_dict: Dict[int, float] = {}
        if isinstance(monthly_means_series, pd.Series):
            for key, value in monthly_means_series.items():
                if pd.notna(value):
                    try:
                        # Converter key para int de forma segura
                        if isinstance(key, (int, float)):
                            key_int = int(key)
                        elif isinstance(key, str) and key.isdigit():
                            key_int = int(key)
                        else:
                            continue
                        
                        # Converter value para float
                        if isinstance(value, (int, float)):
                            value_float = float(value)
                        else:
                            continue
                        
                        monthly_means_dict[key_int] = value_float
                    except (ValueError, TypeError, AttributeError):
                        continue
        
        # Aplicar map usando dict - usar apply() como alternativa se map() falhar
        def get_monthly_mean(mes_val: Any) -> float:
            """Função auxiliar para mapear mês para média"""
            try:
                if pd.isna(mes_val):
                    return 0.0
                if isinstance(mes_val, (int, float)):
                    mes_int = int(mes_val)
                elif isinstance(mes_val, str) and mes_val.isdigit():
                    mes_int = int(mes_val)
                else:
                    return 0.0
                return monthly_means_dict.get(mes_int, 0.0)
            except (ValueError, TypeError, AttributeError):
                return 0.0
        
        df['seasonality_score'] = df['mes'].apply(get_monthly_mean)
        
        # Normalizar sazonalidade
        seasonality_mean = float(df['seasonality_score'].mean()) if hasattr(df['seasonality_score'], 'mean') else 0.0
        seasonality_std_val = df['seasonality_score'].std()
        # Verificar se seasonality_std_val não é None e não é NaN antes de converter
        seasonality_std = 0.0
        if seasonality_std_val is not None:
            try:
                # Verificar se não é NaN de forma explícita
                is_not_na = not (isinstance(seasonality_std_val, float) and np.isnan(seasonality_std_val))
                if is_not_na and isinstance(seasonality_std_val, (int, float)):
                    seasonality_std = float(seasonality_std_val)
            except (ValueError, TypeError, AttributeError):
                seasonality_std = 0.0
        
        # Comparar com 0 usando valor numérico
        if isinstance(seasonality_std, (int, float)) and seasonality_std > 0:
            df['seasonality_score'] = (df['seasonality_score'] - seasonality_mean) / (seasonality_std + 1e-8)
        else:
            df['seasonality_score'] = 0.0
        
        return df
    
    def _create_prediction_targets(self, df: pd.DataFrame) -> Dict[str, np.ndarray]:
        """Cria targets para predição"""
        targets = {}
        
        # 1. Risco de fraude (baseado em anomalias futuras)
        future_anomaly_risk = self._calculate_future_anomaly_risk(df)
        targets['fraud_risk'] = future_anomaly_risk
        
        # 2. Probabilidade de anomalia
        anomaly_probability = self._calculate_anomaly_probability(df)
        targets['anomaly_probability'] = anomaly_probability
        
        # 3. Desvio do orçamento
        budget_deviation = self._calculate_budget_deviation(df)
        targets['budget_deviation'] = budget_deviation
        
        return targets
    
    def _calculate_future_anomaly_risk(self, df: pd.DataFrame) -> np.ndarray:
        """Calcula risco de anomalias futuras"""
        risk_scores = np.zeros(len(df))
        
        # Baseado em padrões históricos
        for i in range(1, len(df)):
            # Verificar se próximas transações serão anômalas
            future_window = df.iloc[i:min(i+5, len(df))]
            
            # Score baseado em:
            # 1. Variação de valores
            value_variation = future_window['valor'].std() / (future_window['valor'].mean() + 1e-8)
            
            # 2. Frequência de transações
            transaction_frequency = len(future_window) / 5  # 5 dias
            
            # 3. Padrões temporais
            temporal_pattern = abs(df.iloc[i]['dia_semana'] - df.iloc[i-1]['dia_semana'])
            
            risk_scores[i] = min(1.0, (value_variation * 0.4 + transaction_frequency * 0.3 + temporal_pattern * 0.3))
        
        return risk_scores
    
    def _calculate_anomaly_probability(self, df: pd.DataFrame) -> np.ndarray:
        """Calcula probabilidade de anomalia"""
        probabilities = np.zeros(len(df))
        
        for i in range(len(df)):
            current_value = df.iloc[i]['valor']
            
            # Comparar com histórico da categoria
            if 'categoria' in df.columns:
                categoria = df.iloc[i]['categoria']
                cat_history = df[df['categoria'] == categoria]['valor']
                
                if len(cat_history) > 1:
                    cat_mean = cat_history.mean()
                    cat_std = cat_history.std()
                    
                    # Z-score
                    z_score = abs(current_value - cat_mean) / (cat_std + 1e-8)
                    
                    # Converter para probabilidade
                    probabilities[i] = min(1.0, z_score / 3.0)
        
        return probabilities
    
    def _calculate_budget_deviation(self, df: pd.DataFrame) -> np.ndarray:
        """Calcula desvio do orçamento"""
        deviations = np.zeros(len(df))
        
        # Orçamento estimado baseado em médias históricas
        for i in range(len(df)):
            if i > 0:
                # Calcular orçamento esperado baseado no histórico
                historical_data = df.iloc[:i]
                
                if 'categoria' in historical_data.columns:
                    categoria = df.iloc[i]['categoria']
                    cat_data = historical_data[historical_data['categoria'] == categoria]
                    
                    if len(cat_data) > 0:
                        expected_budget = cat_data['valor'].mean()
                        actual_value = df.iloc[i]['valor']
                        
                        deviations[i] = (actual_value - expected_budget) / (expected_budget + 1e-8)
        
        return deviations
    
    def train_predictive_models(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Treina modelos preditivos"""
        print("🧠 Treinando modelos de IA preditiva...")
        
        # Preparar dados
        X, targets = self.prepare_training_data(df)
        
        # Verificar se temos dados suficientes
        if len(X) < self.config['min_training_samples']:
            print(f"AVISO: Dados insuficientes para treinamento ({len(X)} < {self.config['min_training_samples']})")
            return {'error': 'Dados insuficientes para treinamento'}
        
        # Normalizar features
        X_scaled = self.scalers['features'].fit_transform(X)
        
        results = {}
        
        # Treinar cada modelo
        for target_name, target_values in targets.items():
            if target_name in self.models:
                print(f"   📈 Treinando modelo: {target_name}")
                
                # Dividir dados
                X_train, X_test, y_train, y_test = train_test_split(
                    X_scaled, target_values, test_size=0.2, random_state=42
                )
                
                # Treinar modelo
                model = self.models[target_name]
                model.fit(X_train, y_train)
                
                # Predições
                y_pred = model.predict(X_test)
                
                # Métricas
                mse = mean_squared_error(y_test, y_pred)
                r2 = r2_score(y_test, y_pred)
                
                results[target_name] = {
                    'model': model,
                    'mse': mse,
                    'r2': r2,
                    'predictions': model.predict(X_scaled)
                }
                
                # Feature importance (se disponível)
                if hasattr(model, 'feature_importances_'):
                    feature_names = X.columns.tolist()
                    importance_dict = dict(zip(feature_names, model.feature_importances_))
                    results[target_name]['feature_importance'] = importance_dict
        
        print("Modelos preditivos treinados.")
        return results
    
    def predict_future_risks(self, df: pd.DataFrame, horizon_days: int = 30) -> Dict[str, Any]:
        """Prediz riscos futuros"""
        print(f"Predizendo riscos para os proximos {horizon_days} dias...")
        
        # Preparar dados
        X, _ = self.prepare_training_data(df)
        
        # Normalizar features
        X_scaled = self.scalers['features'].transform(X)
        
        predictions = {}
        
        # Predições para cada modelo
        for model_name, model in self.models.items():
            if hasattr(model, 'predict'):
                pred = model.predict(X_scaled)
                predictions[model_name] = pred
        
        # Calcular riscos agregados
        risk_assessment = self._assess_aggregate_risks(predictions, df)
        
        # Recomendações preditivas
        recommendations = self._generate_predictive_recommendations(risk_assessment, df)
        
        return {
            'predictions': predictions,
            'risk_assessment': risk_assessment,
            'recommendations': recommendations,
            'horizon_days': horizon_days
        }
    
    def _assess_aggregate_risks(self, predictions: Dict, df: pd.DataFrame) -> Dict[str, Any]:
        """Avalia riscos agregados"""
        risk_assessment = {
            'overall_risk_level': 'low',
            'high_risk_transactions': [],
            'risk_factors': {},
            'trend_analysis': {}
        }
        
        # Calcular risco geral
        if 'fraud_risk' in predictions:
            avg_fraud_risk = np.mean(predictions['fraud_risk'])
            
            if avg_fraud_risk > self.risk_thresholds['high']:
                risk_assessment['overall_risk_level'] = 'high'
            elif avg_fraud_risk > self.risk_thresholds['medium']:
                risk_assessment['overall_risk_level'] = 'medium'
        
        # Identificar transações de alto risco
        if 'fraud_risk' in predictions:
            high_risk_indices = np.where(predictions['fraud_risk'] > self.risk_thresholds['high'])[0]
            
            for idx in high_risk_indices:
                risk_assessment['high_risk_transactions'].append({
                    'index': idx,
                    'fraud_risk': predictions['fraud_risk'][idx],
                    'description': df.iloc[idx]['descricao'] if 'descricao' in df.columns else 'N/A',
                    'value': df.iloc[idx]['valor'] if 'valor' in df.columns else 0
                })
        
        # Análise de tendências
        if 'budget_deviation' in predictions:
            trend = np.polyfit(range(len(predictions['budget_deviation'])), predictions['budget_deviation'], 1)[0]
            risk_assessment['trend_analysis']['budget_trend'] = 'increasing' if trend > 0 else 'decreasing'
        
        return risk_assessment
    
    def _generate_predictive_recommendations(self, risk_assessment: Dict, df: pd.DataFrame) -> List[str]:
        """Gera recomendações preditivas"""
        recommendations = []
        
        # Baseado no nível de risco geral
        overall_risk = risk_assessment['overall_risk_level']
        
        if overall_risk == 'high':
            recommendations.append("ALERTA: Alto risco de fraude detectado - intensificar monitoramento")
            recommendations.append("Revisar todas as transacoes dos ultimos 30 dias")
            recommendations.append("Implementar controles adicionais de aprovacao")
        
        elif overall_risk == 'medium':
            recommendations.append("Risco moderado detectado - monitorar de perto")
            recommendations.append("Aumentar frequencia de auditorias")
        
        # Baseado em transações de alto risco
        high_risk_count = len(risk_assessment['high_risk_transactions'])
        if high_risk_count > 0:
            recommendations.append(f"{high_risk_count} transacoes identificadas como alto risco")
            recommendations.append("Investigar transacoes de alto risco prioritariamente")
        
        # Baseado em tendências
        if 'budget_trend' in risk_assessment['trend_analysis']:
            trend = risk_assessment['trend_analysis']['budget_trend']
            if trend == 'increasing':
                recommendations.append("Tendencia de aumento de despesas - revisar orcamento")
        
        # Recomendações específicas por categoria
        if 'categoria' in df.columns:
            category_risks = df.groupby('categoria').apply(
                lambda x: len(x) if len(x) > 0 else 0
            )
            
            high_volume_categories = category_risks[category_risks > category_risks.quantile(0.8)]
            if len(high_volume_categories) > 0:
                recommendations.append(f"Categorias com alto volume: {', '.join(high_volume_categories.index)}")
        
        return recommendations
    
    def get_model_insights(self) -> Dict[str, Any]:
        """Retorna insights dos modelos"""
        insights = {
            'feature_importance': {},
            'model_performance': {},
            'risk_thresholds': self.risk_thresholds
        }
        
        # Feature importance
        for model_name, model in self.models.items():
            if hasattr(model, 'feature_importances_'):
                insights['feature_importance'][model_name] = model.feature_importances_.tolist()
        
        return insights

# Exemplo de uso
if __name__ == "__main__":
    # Criar dados de exemplo com padrões temporais
    dates = pd.date_range('2024-01-01', periods=365, freq='D')
    
    sample_data = {
        'data': dates,
        'descricao': [f'Transação {i}' for i in range(365)],
        'tipo': ['receita'] * 100 + ['despesa'] * 265,
        'valor': np.random.normal(1000, 300, 365),
        'categoria': ['Taxas Condominiais'] * 100 + ['Manutenção'] * 100 + ['Salários'] * 165
    }
    
    # Adicionar algumas anomalias
    sample_data['valor'][50:60] = np.random.normal(5000, 1000, 10)  # Anomalias em fevereiro
    sample_data['valor'][200:210] = np.random.normal(5000, 1000, 10)  # Anomalias em julho
    
    df = pd.DataFrame(sample_data)
    
    # Inicializar IA preditiva
    predictive_ai = PredictiveAI()
    
    # Treinar modelos
    training_results = predictive_ai.train_predictive_models(df)
    
    if 'error' not in training_results:
        # Fazer predições
        predictions = predictive_ai.predict_future_risks(df)
        
        print("\n=== IA PREDITIVA ===")
        print(f"Nível de risco geral: {predictions['risk_assessment']['overall_risk_level']}")
        print(f"Transações de alto risco: {len(predictions['risk_assessment']['high_risk_transactions'])}")
        
        print("\nRecomendações:")
        for rec in predictions['recommendations']:
            print(f"  - {rec}")
    else:
        print(f"Erro: {training_results['error']}")
