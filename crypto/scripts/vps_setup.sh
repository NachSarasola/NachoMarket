#!/usr/bin/env bash
# Setup AISLADO del bot de crypto SMC en el VPS — NO toca el bot existente.
#
# Diseño de seguridad (el usuario tiene OTRO bot corriendo en ~/nachomarket que NO hay que
# tocar):
#   - Corre en un directorio SEPARADO (por defecto el que contiene a crypto/), nunca en
#     ~/nachomarket. Aborta si detecta que está dentro de ese árbol.
#   - venv propio (.venv-crypto). NO usa el venv del otro bot.
#   - CERO apt / sudo / systemd / rm. No instala nada a nivel sistema. No borra nada.
#   - Solo: crea un venv, instala numpy/pandas/pytest (deps ligeras) y corre los tests.
#
# Uso (en el VPS, en un dir aislado que contenga la carpeta crypto/):
#   bash crypto/scripts/vps_setup.sh            # setup + tests
#   bash crypto/scripts/vps_setup.sh --ccxt     # + ccxt (para bajar datos reales)
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"   # padre de crypto/
WITH_CCXT=0
[ "${1:-}" = "--ccxt" ] && WITH_CCXT=1

echo "== Setup aislado del bot crypto SMC =="
echo "   REPO_ROOT: $REPO_ROOT"

# --- Guarda 1: no correr como root (evita cambios a nivel sistema / permisos raros) ---
if [ "$(id -u)" = "0" ]; then
    echo "❌ No corras esto como root. Usá el usuario 'ubuntu'." >&2
    exit 1
fi

# --- Guarda 2: NUNCA dentro del árbol del otro bot ---
case "$REPO_ROOT" in
    *nachomarket*|*/nachomarket|*/nachomarket/*)
        echo "❌ ABORTO: REPO_ROOT parece el árbol del OTRO bot ('$REPO_ROOT')." >&2
        echo "   Cloná/copiá el código en un directorio AISLADO, p.ej. ~/nacho-crypto," >&2
        echo "   y corré este script desde ahí. Este script no debe tocar ~/nachomarket." >&2
        exit 1
        ;;
esac

# --- Guarda 3: la carpeta crypto/ tiene que existir bajo REPO_ROOT ---
if [ ! -d "$REPO_ROOT/crypto/smc" ]; then
    echo "❌ No encuentro $REPO_ROOT/crypto/smc. ¿Copiaste la carpeta crypto/ completa?" >&2
    exit 1
fi

cd "$REPO_ROOT"

# --- Interprete de Python (preferir 3.11; si no, python3) ---
PY=python3
command -v python3.11 >/dev/null 2>&1 && PY=python3.11
echo "   Python: $($PY --version 2>&1)"

# --- venv AISLADO ---
VENV="$REPO_ROOT/.venv-crypto"
if [ ! -d "$VENV" ]; then
    echo "🔧 Creando venv aislado en $VENV ..."
    if ! "$PY" -m venv "$VENV" 2>/dev/null; then
        echo "❌ Falta el módulo venv. Instalá (una sola vez):  sudo apt install -y python3-venv" >&2
        echo "   (Eso es lo ÚNICO que podría requerir sudo; no lo hago automáticamente.)" >&2
        exit 1
    fi
fi
# shellcheck disable=SC1091
source "$VENV/bin/activate"

echo "📥 Instalando dependencias ligeras (numpy, pandas, pytest)..."
pip install --quiet --upgrade pip
pip install --quiet numpy pandas pytest
if [ "$WITH_CCXT" = "1" ]; then
    echo "📥 Instalando ccxt (para fetch_data.py)..."
    pip install --quiet ccxt || echo "⚠️  ccxt falló; podés bajar datos con freqtrade download-data en su lugar."
fi

echo "🧪 Corriendo tests..."
python -m pytest crypto/tests -q

echo ""
echo "═══════════════════════════════════════════════════════"
echo "  ✅ Setup aislado OK. El otro bot NO fue tocado."
echo "═══════════════════════════════════════════════════════"
echo "Activá el venv en cada sesión con:  source $VENV/bin/activate"
echo ""
echo "Próximos pasos (datos reales + validación):"
echo "  1. python crypto/scripts/fetch_data.py --symbol BTC/USDT --timeframe 4h \\"
echo "         --since 2019-01-01 --out crypto/data/BTC_USDT-4h.csv"
echo "  2. python crypto/scripts/validate.py --data crypto/data/BTC_USDT-4h.csv \\"
echo "         --strategy sweep --compare --deflated-sharpe 108"
echo "  3. Repetir con --strategy donchian (el sweep DEBE batirlo)."
echo ""
echo "Para freqtrade (dry-run) ver crypto/DESPLIEGUE_VPS.md — instalación aparte, aislada."
