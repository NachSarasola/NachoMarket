#!/bin/bash
# Setup del VPS Oracle Cloud Free Tier para NachoMarket
set -e

echo "=== NachoMarket Oracle VM Setup ==="

# Actualizar sistema
sudo apt update && sudo apt upgrade -y

# Instalar Python 3.11
sudo apt install -y python3.11 python3.11-venv python3-pip git

# Crear directorio del proyecto
mkdir -p ~/nachomarket
cd ~/nachomarket

# Crear venv
python3.11 -m venv venv
source venv/bin/activate

# Instalar dependencias
pip install --upgrade pip
pip install -r requirements.txt

# Crear directorios de data
mkdir -p data/reviews

# Configurar systemd service
sudo cp scripts/polymarket-bot.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable polymarket-bot

echo "=== Setup complete ==="
echo "1. Copia tu .env con las API keys"
echo "2. Edita config/settings.yaml (mode: paper para empezar)"
echo "3. sudo systemctl start polymarket-bot"
echo "4. sudo journalctl -u polymarket-bot -f  (para ver logs)"
