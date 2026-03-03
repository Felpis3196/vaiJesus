FROM python:3.9-slim

# Instalar dependências do sistema incluindo Tesseract OCR e idioma português
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    curl \
    tesseract-ocr \
    tesseract-ocr-por \
    libtesseract-dev \
    && rm -rf /var/lib/apt/lists/*

# Criar usuário não-root
RUN useradd --create-home --shell /bin/bash app

# Definir diretório de trabalho
WORKDIR /app

# Copiar requirements e instalar dependências
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código da aplicação
COPY . .

# Criar diretórios necessários
RUN mkdir -p /app/logs /app/data /app/backups && \
    chown -R app:app /app

# Mudar para usuário não-root
USER app

# Expor porta
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Comando para executar
CMD ["python", "api_server.py"]
