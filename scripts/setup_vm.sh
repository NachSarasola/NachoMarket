#!/bin/bash
# Setup del VPS Hetzner Cloud CPX11 para NachoMarket
# Compatible con cualquier VPS Ubuntu 22.04+
set -e

# Crear usuario 'ubuntu' si corremos como root (Hetzner default)
if [ "$(id -u)" = "0" ]; then
    if ! id ubuntu &>/dev/null; then
        echo "👤 Creando usuario 'ubuntu' con sudo..."
        adduser --disabled-password --gecos "" ubuntu
        usermod -aG sudo ubuntu
        echo "ubuntu ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/ubuntu-nopasswd
        mkdir -p /home/ubuntu/.ssh
        cp /root/.ssh/authorized_keys /home/ubuntu/.ssh/
        chown -R ubuntu:ubuntu /home/ubuntu/.ssh
        chmod 700 /home/ubuntu/.ssh
        chmod 600 /home/ubuntu/.ssh/authorized_keys
        echo "✅ Usuario 'ubuntu' creado. Re-conectá con: ssh ubuntu@<IP>"
        echo "   Después corré: bash scripts/setup_vm.sh"
        exit 0
    fi
fi

echo "═══════════════════════════════════════════════"
echo "  NachoMarket — VPS Setup"
echo "═══════════════════════════════════════════════"
echo ""

# Actualizar sistema
echo "📦 Actualizando sistema..."
sudo apt update && sudo apt upgrade -y

# Instalar Python 3.11
echo "🐍 Instalando Python 3.11..."
sudo apt install -y python3.11 python3.11-venv python3-pip git curl

# Crear directorio del proyecto (si deploy.sh no lo hizo ya)
mkdir -p ~/nachomarket
cd ~/nachomarket

# Crear venv
echo "🔧 Creando virtualenv..."
python3.11 -m venv venv
source venv/bin/activate

# Instalar dependencias
echo "📥 Instalando dependencias Python..."
pip install --upgrade pip
pip install -r requirements.txt

# Crear directorios de data
mkdir -p data/reviews

# Ajustar limites de SSH para evitar bloqueos por MaxStartups
echo "🔐 Ajustando límites de SSH..."
bash scripts/fix_ssh_limits.sh

# Configurar systemd service
echo "⚙️  Configurando systemd service..."
sudo cp scripts/polymarket-bot.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable polymarket-bot

# Verificar acceso geográfico a Polymarket CLOB
echo ""
echo "🔐 Verificando credenciales de entorno..."
if python scripts/check_env.py; then
    echo "✅ Env-check OK"
else
    echo ""
    echo "🚫 ALERTA: faltan variables obligatorias en .env para modo LIVE."
    echo "   Completá las credenciales antes de arrancar el servicio."
    echo ""
    exit 1
fi

echo ""
echo "🌍 Verificando acceso geográfico a Polymarket..."
if python scripts/check_geo.py; then
    echo "✅ Geo-check OK"
else
    echo ""
    echo "🚫 ALERTA: Esta IP está geobloqueada por Polymarket."
    echo "   El bot NO podrá operar desde este VPS."
    echo "   → Elegí un VPS en una región permitida (US, UK, etc.)"
    echo ""
    exit 1
fi

# Verificar .env
echo ""
if [ -f .env ]; then
    echo "✅ .env encontrado"
else
    echo "⚠️  .env NO encontrado — copialo antes de arrancar el bot"
fi

echo ""
echo "═══════════════════════════════════════════════"
echo "  Setup completo ✅"
echo "═══════════════════════════════════════════════"
echo ""
echo "Próximos pasos:"
echo "  1. nano .env                           # Pegar API keys"
echo "  2. python scripts/check_env.py         # Validar credenciales LIVE"
echo "  3. python scripts/check_geo.py         # Re-verificar acceso"
echo "  4. sudo systemctl start polymarket-bot # Arrancar bot LIVE"
echo "  5. sudo journalctl -u polymarket-bot -f # Ver logs en vivo"
echo ""
echo "Control via Telegram: /status /pause /resume /kill"
