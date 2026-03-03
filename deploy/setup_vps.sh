#!/bin/bash

# Script de configuração da VPS para produção
set -e

echo "🔧 Configurando VPS para produção..."

# Atualizar sistema
echo "📦 Atualizando sistema..."
sudo apt update && sudo apt upgrade -y

# Instalar dependências
echo "📦 Instalando dependências..."
sudo apt install -y \
    curl \
    wget \
    git \
    docker.io \
    docker-compose \
    nginx \
    certbot \
    python3-certbot-nginx \
    htop \
    iotop \
    nethogs \
    bc \
    fail2ban \
    ufw

# Configurar Docker
echo "🐳 Configurando Docker..."
sudo systemctl enable docker
sudo systemctl start docker
sudo usermod -aG docker $USER

# Criar diretórios
echo "📁 Criando diretórios..."
sudo mkdir -p /opt/auditoria-ia-api
sudo mkdir -p /opt/backups
sudo mkdir -p /var/log/auditoria-ia-api
sudo mkdir -p /etc/nginx/ssl

# Configurar firewall
echo "🔥 Configurando firewall..."
sudo ufw allow 22
sudo ufw allow 80
sudo ufw allow 443
sudo ufw --force enable

# Configurar fail2ban
echo "🛡️ Configurando fail2ban..."
sudo systemctl enable fail2ban
sudo systemctl start fail2ban

# Configurar logrotate
echo "📝 Configurando logrotate..."
sudo cat > /etc/logrotate.d/auditoria-ia-api << EOF
/var/log/auditoria-ia-api/*.log {
    daily
    missingok
    rotate 7
    compress
    delaycompress
    notifempty
    create 644 root root
}
EOF

# Configurar backup automático (scripts em deploy/)
echo "💾 Configurando backup automático..."
sudo cat > /etc/cron.daily/backup-auditoria-ia << EOF
#!/bin/bash
/opt/auditoria-ia-api/deploy/backup.sh create
EOF
sudo chmod +x /etc/cron.daily/backup-auditoria-ia

# Configurar monitoramento (scripts em deploy/)
echo "📊 Configurando monitoramento..."
sudo cat > /etc/cron.d/auditoria-ia-monitor << EOF
# Monitoramento a cada 5 minutos
*/5 * * * * /opt/auditoria-ia-api/deploy/monitor.sh
EOF

echo "✅ Configuração da VPS concluída!"
echo ""
echo "Próximos passos:"
echo "1. Clonar o repositório: git clone <repo> /opt/auditoria-ia-api"
echo "2. Configurar variáveis de ambiente"
echo "3. Executar deploy: ./deploy/deploy.sh deploy"
echo "4. Configurar SSL: sudo certbot --nginx -d seu-dominio.com"
