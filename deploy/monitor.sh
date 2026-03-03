#!/bin/bash

# Script de monitoramento
set -e

# Configurações
API_URL="http://localhost:8000"
LOG_FILE="/var/log/monitor.log"
ALERT_EMAIL="admin@seu-dominio.com"

log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" | tee -a $LOG_FILE
}

# Verificar saúde da API
check_health() {
    if curl -f -s "$API_URL/health" > /dev/null; then
        log "✅ API está funcionando"
        return 0
    else
        log "❌ API não está respondendo"
        return 1
    fi
}

# Verificar uso de CPU
check_cpu() {
    CPU_USAGE=$(top -bn1 | grep "Cpu(s)" | awk '{print $2}' | cut -d'%' -f1)
    if (( $(echo "$CPU_USAGE > 80" | bc -l) )); then
        log "⚠️ CPU usage alto: ${CPU_USAGE}%"
        return 1
    fi
    return 0
}

# Verificar uso de memória
check_memory() {
    MEMORY_USAGE=$(free | grep Mem | awk '{printf "%.2f", $3/$2 * 100.0}')
    if (( $(echo "$MEMORY_USAGE > 80" | bc -l) )); then
        log "⚠️ Memory usage alto: ${MEMORY_USAGE}%"
        return 1
    fi
    return 0
}

# Verificar espaço em disco
check_disk() {
    DISK_USAGE=$(df / | tail -1 | awk '{print $5}' | cut -d'%' -f1)
    if [ "$DISK_USAGE" -gt 80 ]; then
        log "⚠️ Disk usage alto: ${DISK_USAGE}%"
        return 1
    fi
    return 0
}

# Enviar alerta
send_alert() {
    local message="$1"
    log "🚨 ALERTA: $message"
    
    # Enviar email (requer configuração)
    # echo "$message" | mail -s "Alerta IA API" "$ALERT_EMAIL"
    
    # Log do alerta
    echo "$(date '+%Y-%m-%d %H:%M:%S') - ALERTA: $message" >> /var/log/alerts.log
}

# Monitoramento principal
main() {
    log "🔍 Iniciando monitoramento..."
    
    local alerts=0
    
    # Verificar saúde da API
    if ! check_health; then
        send_alert "API não está respondendo"
        ((alerts++))
    fi
    
    # Verificar recursos do sistema
    if ! check_cpu; then
        send_alert "CPU usage alto"
        ((alerts++))
    fi
    
    if ! check_memory; then
        send_alert "Memory usage alto"
        ((alerts++))
    fi
    
    if ! check_disk; then
        send_alert "Disk usage alto"
        ((alerts++))
    fi
    
    if [ $alerts -eq 0 ]; then
        log "✅ Todos os checks passaram"
    else
        log "⚠️ $alerts alertas detectados"
    fi
}

# Executar monitoramento
main
