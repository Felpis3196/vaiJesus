@echo off
REM Script para configurar ambiente virtual no Windows

echo ========================================
echo Configurando Ambiente Virtual
echo ========================================
echo.

REM Verificar se Python está instalado
python --version >nul 2>&1
if errorlevel 1 (
    echo [ERRO] Python nao encontrado!
    echo Instale Python 3.9+ e tente novamente.
    pause
    exit /b 1
)

echo [1/4] Criando ambiente virtual...
if exist .venv (
    echo Ambiente virtual ja existe. Removendo...
    rmdir /s /q .venv
)
python -m venv .venv
if errorlevel 1 (
    echo [ERRO] Falha ao criar ambiente virtual!
    pause
    exit /b 1
)

echo [2/4] Ativando ambiente virtual...
call .venv\Scripts\activate.bat

echo [3/4] Atualizando pip...
python -m pip install --upgrade pip

echo [4/4] Instalando dependencias...
pip install -r requirements.txt
if errorlevel 1 (
    echo [ERRO] Falha ao instalar dependencias!
    pause
    exit /b 1
)

echo.
echo ========================================
echo Ambiente virtual configurado com sucesso!
echo ========================================
echo.
echo Para ativar o ambiente virtual:
echo   .\.venv\Scripts\activate
echo.
echo Para testar:
echo   python test_api_client.py
echo.
pause

