#!/usr/bin/env bash
# Review TRIMESTRAL (TESIS.md): re-corre las validaciones congeladas con datos extendidos
# y saca el snapshot de carry. Es OBSERVACIONAL: vigila si el regimen cambio (p.ej. trend
# en majors reviviendo). NINGUN resultado habilita live sin re-pasar el pipeline completo.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
[ "$(id -u)" = "0" ] && { echo "❌ No corras como root." >&2; exit 1; }
case "$REPO_ROOT" in
    *nachomarket*|*/nachomarket|*/nachomarket/*)
        echo "❌ ABORTO: '$REPO_ROOT' parece el arbol del OTRO bot." >&2; exit 1;;
esac
cd "$REPO_ROOT"

echo "═══════════════════════════════════════════════════════"
echo "  REVIEW TRIMESTRAL — $(date -u +%F)"
echo "  1) GATE 1 (sweep/donchian) con datos extendidos"
echo "  2) H1/H2 (ma_timing/flow) con datos extendidos"
echo "  3) Snapshot de carry + plan de capital"
echo "  Comparar cada veredicto contra las tablas de REGLAS_CONGELADAS.md:"
echo "  si algo PASA ahora, el regimen cambio -> abrir ventana trimestral"
echo "  (cuenta trials nuevos; nada pasa a live sin pipeline completo)."
echo "═══════════════════════════════════════════════════════"

bash crypto/scripts/vps_validate_all.sh || echo "⚠️ validate_all fallo"
bash crypto/scripts/vps_run_hipotesis.sh || echo "⚠️ hipotesis fallo"
# shellcheck disable=SC1091
source "$REPO_ROOT/.venv-crypto/bin/activate"
python crypto/scripts/carry_monitor.py --snapshot --capital "${CAPITAL:-500}" || true
python crypto/scripts/carry_monitor.py --plan --start "${CAPITAL:-500}" \
    --monthly "${AHORRO_MENSUAL:-200}" || true
[ -f crypto/data/riesgo_vivo.csv ] && python crypto/scripts/budget_review.py \
    --journal crypto/data/riesgo_vivo.csv || true

echo "Registrar conclusiones como fila trimestral en REGLAS_CONGELADAS.md."
