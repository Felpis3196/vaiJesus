#!/usr/bin/env python3
"""
Script para iniciar o servidor FastAPI com hot-reload para desenvolvimento.
Execute a partir da raiz do projeto: python scripts/start_server.py
"""
import uvicorn

if __name__ == "__main__":
    uvicorn.run(
        "api_server:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
