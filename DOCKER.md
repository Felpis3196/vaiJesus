# Rodar o sistema no Docker

## Testar só a API (recomendado para validar endpoints)

1. **Subir apenas o serviço da API:**
   ```bash
   docker compose up --build ai-api
   ```
2. Acesse:
   - **Swagger UI:** http://localhost:8000/docs  
   - **Health:** http://localhost:8000/health  
   - **OpenAPI:** http://localhost:8000/openapi.json  

3. **Configurar a LLM (obrigatório para análise):**  
   A extração é 100% via LLM. Defina **uma** das opções:

   - **LLM local (Ollama no seu PC):**
     - Crie um `.env` na raiz com:
       ```
       LLM_BASE_URL=http://host.docker.internal:11434/v1
       LLM_MODEL=llama3.1
       ```
     - **Importante:** por padrão o Ollama escuta só em `127.0.0.1`. Para o container acessar o Ollama no host, o Ollama precisa escutar em todas as interfaces. No **Windows** (PowerShell, antes de abrir o Ollama):
       ```powershell
       $env:OLLAMA_HOST="0.0.0.0"
       ollama serve
       ```
       Ou defina a variável de ambiente do sistema `OLLAMA_HOST=0.0.0.0` e reinicie o Ollama. No **Mac/Linux**: `OLLAMA_HOST=0.0.0.0 ollama serve`.
     - Confirme que o modelo está instalado: `ollama pull llama3.1`.
     - Suba a API: `docker compose up ai-api` (o compose lê o `.env` automaticamente).

   - **OpenAI (nuvem):**
     ```
     OPENAI_API_KEY=sk-...
     ```

   Sem `LLM_BASE_URL` nem `OPENAI_API_KEY`, o endpoint de análise retornará **503** (Extração requer LLM).

## Stack completa (API + Nginx + Redis)

```bash
docker compose up --build
```

- **Requisitos:** pasta `deploy/` com `nginx.conf` e, para HTTPS, certificados em `deploy/ssl/` (cert.pem, key.pem).
- API exposta na porta **8000**; Nginx em **80** e **443**.

## Variáveis de ambiente (ai-api)

| Variável | Descrição |
|----------|-----------|
| `LLM_BASE_URL` | URL do servidor LLM local (ex.: `http://host.docker.internal:11434/v1` para Ollama). |
| `LLM_MODEL` | Nome do modelo (ex.: `llama3.1`, `mistral`). |
| `OPENAI_API_KEY` | Chave OpenAI (alternativa à LLM local). |
| `LLM_EXTRACTION_ENABLED` | `true` para ativar extração via LLM (já é o padrão no compose). |
| `AI_AUDIT_OUTPUT_DIR` | Diretório de saída (ex.: `/app/data`). |

## Health check

O container usa `GET /health` para saúde. O endpoint retorna 200 quando a API e o sistema de auditoria estão ok; não verifica se a LLM está configurada (a análise é que retorna 503 nesse caso).

## "Connection error" ou "Extração LLM não retornou dados"

Se o job falhar com erro de conexão à LLM, confira:

1. **Ollama em execução no host**  
   No PC (fora do Docker): `ollama list` deve listar modelos. Se não, inicie o Ollama ou rode `ollama serve`.

2. **Ollama escutando em 0.0.0.0**  
   Por padrão o Ollama usa só `127.0.0.1`; do container não dá para acessar. Defina `OLLAMA_HOST=0.0.0.0` e reinicie o Ollama (veja seção "LLM local" acima).

3. **.env na raiz do projeto**  
   O `docker compose` carrega o `.env` da pasta onde está o `docker-compose.yml`. Deve conter `LLM_BASE_URL=http://host.docker.internal:11434/v1` (Windows/Mac). No **Linux** pode ser necessário em `docker-compose.yml` no serviço `ai-api`:
   ```yaml
   extra_hosts:
     - "host.docker.internal:host-gateway"
   ```

4. **Teste direto no host**  
   No host: `curl http://localhost:11434/api/tags` (ou abra no navegador). Se responder, o Ollama está ativo; falta só o passo 2 para o container alcançar.
