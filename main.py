"""
Ponto de entrada para auditoria via linha de comando.
Extração 100% via LLM: usa AdvancedAuditSystem que carrega dados via DataInputManager (LLM).
"""
import os
from app.audit import AdvancedAuditSystem


def run_audit(file_path: str, output_dir: str = ".") -> None:
    """Executa auditoria completa (carregamento via LLM, detecção de anomalias, relatório)."""
    try:
        print(f"Iniciando auditoria para o arquivo: {file_path}")
        audit_system = AdvancedAuditSystem()
        audit_results = audit_system.run_comprehensive_audit(file_path=file_path, output_dir=output_dir)
        if not audit_results.get("success"):
            errors = audit_results.get("errors", [])
            print(f"Auditoria concluída com falhas: {errors}")
            return
        print("\nAuditoria concluída com sucesso!")
        total = audit_results.get("total_transactions", 0)
        anomalies = audit_results.get("anomalies_detected", 0)
        report_file = audit_results.get("report_file")
        if report_file:
            print(f"Relatório gerado: {report_file}")
        print(f"Total de transações: {total}")
        print(f"Anomalias detectadas: {anomalies}")
    except RuntimeError as e:
        if "LLM" in str(e) or "Extração requer" in str(e):
            print("Erro: Extração requer LLM. Configure LLM_BASE_URL ou OPENAI_API_KEY e garanta que o serviço esteja disponível.")
        else:
            raise
    except FileNotFoundError as e:
        print(f"Erro: {e}")
    except ValueError as e:
        print(f"Erro de validação: {e}")
    except Exception as e:
        print(f"Erro inesperado durante a auditoria: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    sample_file = "data/sample_condominium_accounts.csv"
    output_directory = "."

    if not os.path.exists(sample_file):
        print(f"Erro: Arquivo de exemplo '{sample_file}' não encontrado.")
        print("Crie um arquivo de exemplo ou use outro caminho. A extração é feita via LLM.")
    else:
        run_audit(sample_file, output_directory)
