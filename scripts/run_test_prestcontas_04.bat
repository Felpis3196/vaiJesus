@echo off
REM Teste com PrestContas 04.2025.pdf
REM Execute na raiz do projeto (auditoriaIA), com o venv ativado.

cd /d "%~dp0.."
if exist .venv\Scripts\activate.bat (
    call .venv\Scripts\activate.bat
)
python scripts\test_analysis_docs.py "Docs/PrestContas 04.2025.pdf"
pause
