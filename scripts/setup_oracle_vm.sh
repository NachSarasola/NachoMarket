#!/bin/bash
# Setup del VPS Oracle Cloud Free Tier para NachoMarket
# Compatible con cualquier VPS Ubuntu 22.04+
set -e

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

# Configurar systemd service
echo "⚙️  Configurando systemd service..."
sudo cp scripts/polymarket-bot.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable polymarket-bot

# Verificar acceso geográfico a Polymarket CLOB
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
echo "  2. python scripts/check_geo.py         # Re-verificar acceso"
echo "  3. sudo systemctl start polymarket-bot # Arrancar bot"
echo "  4. sudo journalctl -u polymarket-bot -f # Ver logs en vivo"
echo ""
echo "Control via Telegram: /status /pause /resume /kill"
