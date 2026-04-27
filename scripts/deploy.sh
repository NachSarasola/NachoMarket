#!/bin/bash
# Deploy NachoMarket al VPS (Contabo, Lightsail, OVH o cualquier servidor Linux)
#
# Uso:
#   ./scripts/deploy.sh ubuntu@<IP-del-VPS>
#   ./scripts/deploy.sh ubuntu@<IP-del-VPS> --setup   # deploy + setup completo
#
# Requisitos locales: rsync, ssh
# El VPS debe tener tu SSH key pública configurada.

set -e

VPS=$1
SETUP_FLAG=$2
REMOTE_DIR="~/nachomarket"

if [ -z "$VPS" ]; then
    echo "❌ Uso: $0 usuario@ip-del-vps [--setup]"
    echo ""
    echo "Ejemplos:"
    echo "  $0 root@<VPS-IP>                  # Primera vez (Contabo arranca como root)"
    echo "  $0 ubuntu@<VPS-IP>                # Solo sync de archivos"
    echo "  $0 ubuntu@<VPS-IP> --setup        # Sync + setup completo + arrancar bot"
    exit 1
fi

echo "🚀 Desplegando NachoMarket a $VPS:$REMOTE_DIR ..."

# Sync del proyecto excluyendo archivos innecesarios
rsync -avz --progress \
    --exclude='venv/' \
    --exclude='__pycache__/' \
    --exclude='.git/' \
    --exclude='*.pyc' \
    --exclude='*.pyo' \
    --exclude='data/trades.jsonl' \
    --exclude='data/nachomarket.log' \
    --exclude='data/state.json' \
    --exclude='data/reviews/' \
    --exclude='.pytest_cache/' \
    --exclude='.claude/' \
    --exclude='plan.pdf' \
    --exclude='CUsersUsuarioDesktopNachoMarketscripts/' \
    ./ "$VPS:$REMOTE_DIR/"

echo ""
echo "✅ Archivos sincronizados."

# Si se pidió setup completo
if [ "$SETUP_FLAG" = "--setup" ]; then
    echo ""
    echo "🔧 Ejecutando setup completo en el VPS..."
    ssh "$VPS" "cd $REMOTE_DIR && bash scripts/setup_vm.sh"
    echo ""
    echo "✅ Setup completo."
fi

echo ""
echo "═══════════════════════════════════════════════"
echo "  Deploy exitoso a $VPS"
echo "═══════════════════════════════════════════════"
echo ""
echo "Próximos pasos en el VPS:"
echo "  1. ssh $VPS"
echo "  2. cd nachomarket"

if [ "$SETUP_FLAG" != "--setup" ]; then
    echo "  3. bash scripts/setup_vm.sh            # Solo la primera vez"
fi

echo "  4. nano .env                           # Pegar API keys"
echo "  5. python scripts/check_env.py         # Validar credenciales LIVE"
echo "  6. python scripts/check_geo.py         # Verificar acceso geo"
echo "  7. bash scripts/fix_ssh_limits.sh      # Si SSH empieza a rechazar con MaxStartups"
echo "  8. sudo systemctl start polymarket-bot # Arrancar bot LIVE"
echo "  9. sudo journalctl -u polymarket-bot -f # Ver logs"
echo ""
echo "Control remoto via Telegram: /status /pause /resume /kill"
