#!/bin/bash

# Script de deploy para VPS
set -e

echo "🚀 Iniciando deploy da IA API..."

# Configurações
APP_NAME="auditoria-ia-api"
APP_DIR="/opt/$APP_NAME"
BACKUP_DIR="/opt/backups"
LOG_FILE="/var/log/$APP_NAME-deploy.log"

# Função de logging
log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" | tee -a $LOG_FILE
}

# Função de backup
backup() {
    log "📦 Criando backup..."
    if [ -d "$APP_DIR" ]; then
        BACKUP_NAME="backup-$(date +%Y%m%d-%H%M%S)"
        tar -czf "$BACKUP_DIR/$BACKUP_NAME.tar.gz" -C "$APP_DIR" .
        log "✅ Backup criado: $BACKUP_NAME.tar.gz"
    fi
}

# Função de deploy
deploy() {
    log "🔄 Iniciando deploy..."
    
    # Parar serviços
    log "⏹️ Parando serviços..."
    docker-compose down || true
    
    # Backup
    backup
    
    # Atualizar código
    log "📥 Atualizando código..."
    git pull origin main
    
    # Instalar dependências
    log "📦 Instalando dependências..."
    pip install -r requirements.txt
    
    # Construir e iniciar containers
    log "🐳 Construindo containers..."
    docker-compose build --no-cache
    
    log "🚀 Iniciando serviços..."
    docker-compose up -d
    
    # Aguardar serviços ficarem prontos
    log "⏳ Aguardando serviços..."
    sleep 30
    
    # Verificar saúde
    log "🏥 Verificando saúde dos serviços..."
    if curl -f http://localhost:8000/health; then
        log "✅ Deploy concluído com sucesso!"
    else
        log "❌ Erro no deploy - serviços não estão respondendo"
        exit 1
    fi
}

# Função de rollback
rollback() {
    log "🔄 Executando rollback..."
    
    # Parar serviços
    docker-compose down
    
    # Restaurar backup mais recente
    LATEST_BACKUP=$(ls -t $BACKUP_DIR/backup-*.tar.gz | head -n1)
    if [ -n "$LATEST_BACKUP" ]; then
        log "📦 Restaurando backup: $LATEST_BACKUP"
        tar -xzf "$LATEST_BACKUP" -C "$APP_DIR"
        
        # Reiniciar serviços
        docker-compose up -d
        
        log "✅ Rollback concluído"
    else
        log "❌ Nenhum backup encontrado"
        exit 1
    fi
}

# Menu principal
case "$1" in
    deploy)
        deploy
        ;;
    rollback)
        rollback
        ;;
    backup)
        backup
        ;;
    *)
        echo "Uso: $0 {deploy|rollback|backup}"
        exit 1
        ;;
esac
