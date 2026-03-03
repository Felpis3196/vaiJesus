#!/bin/bash

# Script de backup automático
set -e

# Configurações
APP_DIR="/opt/auditoria-ia-api"
BACKUP_DIR="/opt/backups"
RETENTION_DAYS=7
LOG_FILE="/var/log/backup.log"

log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" | tee -a $LOG_FILE
}

# Criar backup
create_backup() {
    log "📦 Criando backup..."
    
    BACKUP_NAME="backup-$(date +%Y%m%d-%H%M%S)"
    BACKUP_PATH="$BACKUP_DIR/$BACKUP_NAME.tar.gz"
    
    # Backup dos dados
    tar -czf "$BACKUP_PATH" \
        -C "$APP_DIR" \
        --exclude="logs/*" \
        --exclude="__pycache__/*" \
        --exclude="*.pyc" \
        .
    
    log "✅ Backup criado: $BACKUP_PATH"
    
    # Limpar backups antigos
    find "$BACKUP_DIR" -name "backup-*.tar.gz" -mtime +$RETENTION_DAYS -delete
    log "🗑️ Backups antigos removidos"
}

# Restaurar backup
restore_backup() {
    BACKUP_FILE="$1"
    
    if [ -z "$BACKUP_FILE" ]; then
        echo "Uso: $0 restore <arquivo_backup>"
        exit 1
    fi
    
    log "📦 Restaurando backup: $BACKUP_FILE"
    
    # Parar serviços (a partir do diretório da aplicação)
    cd "$APP_DIR" && docker-compose down
    
    # Restaurar dados
    tar -xzf "$BACKUP_FILE" -C "$APP_DIR"
    
    # Reiniciar serviços
    cd "$APP_DIR" && docker-compose up -d
    
    log "✅ Backup restaurado"
}

# Menu principal
case "$1" in
    create)
        create_backup
        ;;
    restore)
        restore_backup "$2"
        ;;
    *)
        echo "Uso: $0 {create|restore <arquivo>}"
        exit 1
        ;;
esac
