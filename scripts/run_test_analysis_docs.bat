@echo off
REM Teste de analise da IA com documentos em Docs/
REM Execute na raiz do projeto (auditoriaIA), com o venv ativado.

cd /d "%~dp0.."
if exist .venv\Scripts\activate.bat (
    call .venv\Scripts\activate.bat
)
python scripts\test_analysis_docs.py
pause
