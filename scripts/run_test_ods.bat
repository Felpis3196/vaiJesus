@echo off
REM Teste com Ed Led prestacao_contas_8_2025.ods
REM Execute na raiz do projeto (auditoriaIA), com o venv ativado.

cd /d "%~dp0.."
if exist .venv\Scripts\activate.bat (
    call .venv\Scripts\activate.bat
)
python scripts\test_analysis_docs.py "Docs/Ed Led prestacao_contas_8_2025.ods"
pause
