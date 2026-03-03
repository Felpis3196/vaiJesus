import pandas as pd
from sklearn.ensemble import IsolationForest
import numpy as np


def get_duplicate_mask(df: pd.DataFrame) -> pd.Series:
    """Retorna máscara de transações duplicadas com regra padrão."""
    if df is None or df.empty:
        return pd.Series([], dtype=bool)
    cols = [c for c in ["data", "valor", "descricao"] if c in df.columns]
    if not cols:
        return pd.Series([False] * len(df))
    df_sorted = df.sort_values(cols)
    return df_sorted.duplicated(subset=cols, keep=False)

def detect_statistical_anomalies(df: pd.DataFrame, column: str, threshold: float = 3.0) -> pd.DataFrame:
    """Detecta anomalias estatísticas usando o método Z-score."""
    if not pd.api.types.is_numeric_dtype(df[column]):
        print(f"A coluna \'{column}\' não é numérica. Pulando detecção estatística.")
        return df

    mean = df[column].mean()
    std = df[column].std()
    if std == 0:
        print(f"Desvio padrão da coluna \'{column}\' é zero. Não é possível calcular Z-score.")
        return df

    df["z_score"] = np.abs((df[column] - mean) / std)
    anomalies = df["z_score"] > threshold

    df.loc[anomalies, "anomalia_detectada"] = True
    df.loc[anomalies, "justificativa_anomalia"] = df.loc[anomalies, "justificativa_anomalia"] + "; Desvio estatístico (Z-score)" if df.loc[anomalies, "justificativa_anomalia"].any() else "Desvio estatístico (Z-score)"
    df.drop(columns=["z_score"], inplace=True)
    return df

def detect_isolation_forest_anomalies(df: pd.DataFrame, columns_to_analyze: list[str], contamination: float = 0.05) -> pd.DataFrame:
    """Detecta anomalias usando o algoritmo Isolation Forest."""
    # Filtrar apenas colunas numéricas para o Isolation Forest
    numeric_cols = [col for col in columns_to_analyze if pd.api.types.is_numeric_dtype(df[col])]
    if not numeric_cols:
        print("Nenhuma coluna numérica para análise com Isolation Forest.")
        return df

    data_for_model = df[numeric_cols]

    # Treinar o modelo Isolation Forest
    model = IsolationForest(contamination=contamination, random_state=42)
    df["anomaly_score"] = model.fit_predict(data_for_model)

    # -1 indica anomalia, 1 indica normal
    anomalies = df["anomaly_score"] == -1

    df.loc[anomalies, "anomalia_detectada"] = True
    df.loc[anomalies, "justificativa_anomalia"] = df.loc[anomalies, "justificativa_anomalia"] + "; Anomalia detectada por Isolation Forest" if df.loc[anomalies, "justificativa_anomalia"].any() else "Anomalia detectada por Isolation Forest"
    df.drop(columns=["anomaly_score"], inplace=True)
    return df

def detect_rule_based_anomalies(df: pd.DataFrame) -> pd.DataFrame:
    """Detecta anomalias baseadas em regras específicas para condomínios."""
    
    # Regra 1: Receitas com valor negativo
    anomalies_revenue = (df["tipo"].str.lower() == "receita") & (df["valor"] < 0)
    df.loc[anomalies_revenue, "anomalia_detectada"] = True
    df.loc[anomalies_revenue, "justificativa_anomalia"] = df.loc[anomalies_revenue, "justificativa_anomalia"] + "; Receita com valor negativo" if df.loc[anomalies_revenue, "justificativa_anomalia"].any() else "Receita com valor negativo"

    # Regra 2: Taxas condominiais com valores muito baixos ou altos
    taxa_condominial = df["categoria"].str.contains("Taxas Condominiais", case=False, na=False)
    if taxa_condominial.any():
        taxa_values = df[taxa_condominial]["valor"]
        taxa_mean = taxa_values.mean()
        taxa_std = taxa_values.std()
        
        # Taxa muito baixa (menor que média - 2*desvio)
        taxa_baixa = taxa_condominial & (df["valor"] < taxa_mean - 2 * taxa_std)
        df.loc[taxa_baixa, "anomalia_detectada"] = True
        df.loc[taxa_baixa, "justificativa_anomalia"] = df.loc[taxa_baixa, "justificativa_anomalia"] + "; Taxa condominial muito baixa" if df.loc[taxa_baixa, "justificativa_anomalia"].any() else "Taxa condominial muito baixa"
        
        # Taxa muito alta (maior que média + 2*desvio)
        taxa_alta = taxa_condominial & (df["valor"] > taxa_mean + 2 * taxa_std)
        df.loc[taxa_alta, "anomalia_detectada"] = True
        df.loc[taxa_alta, "justificativa_anomalia"] = df.loc[taxa_alta, "justificativa_anomalia"] + "; Taxa condominial muito alta" if df.loc[taxa_alta, "justificativa_anomalia"].any() else "Taxa condominial muito alta"

    # Regra 3: Salários com valores suspeitos
    salarios = df["categoria"].str.contains("Salários", case=False, na=False)
    if salarios.any():
        salario_values = df[salarios]["valor"]
        # Salário muito alto (acima de R$ 5.000 pode ser suspeito para zelador/porteiro)
        salario_alto = salarios & (df["valor"] > 5000)
        df.loc[salario_alto, "anomalia_detectada"] = True
        df.loc[salario_alto, "justificativa_anomalia"] = df.loc[salario_alto, "justificativa_anomalia"] + "; Salário muito alto para função" if df.loc[salario_alto, "justificativa_anomalia"].any() else "Salário muito alto para função"

    # Regra 4: Despesas com valores muito altos em relação à média geral
    despesas = df["tipo"].str.lower() == "despesa"
    if despesas.any():
        despesa_values = df[despesas]["valor"]
        despesa_mean = despesa_values.mean()
        despesa_std = despesa_values.std()
        
        # Despesa excepcionalmente alta (maior que média + 3*desvio)
        despesa_alta = despesas & (df["valor"] > despesa_mean + 3 * despesa_std)
        df.loc[despesa_alta, "anomalia_detectada"] = True
        df.loc[despesa_alta, "justificativa_anomalia"] = df.loc[despesa_alta, "justificativa_anomalia"] + "; Despesa excepcionalmente alta" if df.loc[despesa_alta, "justificativa_anomalia"].any() else "Despesa excepcionalmente alta"

    # Regra 5: Transações duplicadas (mesmo valor, mesma data, mesma descrição)
    duplicated_mask = get_duplicate_mask(df)
    if not duplicated_mask.empty and duplicated_mask.any():
        df.loc[duplicated_mask, "anomalia_detectada"] = True
        df.loc[duplicated_mask, "justificativa_anomalia"] = df.loc[duplicated_mask, "justificativa_anomalia"] + "; Transação possivelmente duplicada" if df.loc[duplicated_mask, "justificativa_anomalia"].any() else "Transação possivelmente duplicada"

    # Regra 6: Valores muito pequenos (possível erro de digitação)
    valores_pequenos = df["valor"].between(0.01, 1.00)
    df.loc[valores_pequenos, "anomalia_detectada"] = True
    df.loc[valores_pequenos, "justificativa_anomalia"] = df.loc[valores_pequenos, "justificativa_anomalia"] + "; Valor muito pequeno (possível erro)" if df.loc[valores_pequenos, "justificativa_anomalia"].any() else "Valor muito pequeno (possível erro)"

    return df

def run_anomaly_detection(df: pd.DataFrame) -> pd.DataFrame:
    """Executa todos os métodos de detecção de anomalias no DataFrame."""
    print("Iniciando detecção de anomalias...")

    # Garantir que as colunas de anomalia existam e estejam limpas para cada execução
    df["anomalia_detectada"] = False
    df["justificativa_anomalia"] = ""

    # Detecção baseada em regras
    df = detect_rule_based_anomalies(df.copy())

    # Detecção estatística (ex: no valor)
    df = detect_statistical_anomalies(df.copy(), column="valor")

    # Detecção com Isolation Forest (usando colunas numéricas relevantes)
    # Para uma detecção mais robusta, pode-se incluir outras features como 'mês', 'dia da semana', etc.
    df = detect_isolation_forest_anomalies(df.copy(), columns_to_analyze=["valor"])

    print("Detecção de anomalias concluída.")
    return df

# Exemplo de uso (para testes)
if __name__ == "__main__":
    # Criar um DataFrame de exemplo (simulando a saída do data_processor)
    sample_data = {
        'data': pd.to_datetime(['2025-01-01', '2025-01-05', '2025-01-10', '2025-01-15', '2025-01-20', '2025-01-25', '2025-01-26', '2025-01-27']),
        'descricao': [
            'Recebimento taxa condominial apto 101',
            'Pagamento conta de agua',
            'Salario do zelador',
            'Compra de material de limpeza',
            'Recebimento aluguel salao de festas',
            'Manutencao elevador',
            'Despesa suspeita - valor muito alto',
            'Receita com valor negativo'
        ],
        'tipo': ['receita', 'despesa', 'despesa', 'despesa', 'receita', 'despesa', 'despesa', 'receita'],
        'valor': [500.00, 150.00, 1200.00, 80.00, 200.00, 300.00, 5000.00, -100.00],
        'categoria': ['Taxas Condominiais', 'Água', 'Salários', 'Material de Limpeza', 'Outras Receitas', 'Manutenção', 'Outras Despesas', 'Outras Receitas'],
        'anomalia_detectada': [False, False, False, False, False, False, False, False],
        'justificativa_anomalia': ['', '', '', '', '', '', '', '']
    }
    df_test = pd.DataFrame(sample_data)
    df_test['valor'] = df_test['valor'].astype(float)

    print("\n--- DataFrame Original ---")
    print(df_test)

    df_anomalies = run_anomaly_detection(df_test.copy())

    print("\n--- DataFrame com Anomalias Detectadas ---")
    print(df_anomalies)

    print("\n--- Transações Anômalas ---")
    print(df_anomalies[df_anomalies["anomalia_detectada"] == True])


