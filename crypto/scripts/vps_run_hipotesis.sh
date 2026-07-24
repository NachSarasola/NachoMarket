#!/usr/bin/env bash
# One-shot del programa de hipotesis: corre H1 (ma_timing 1d) y H2 (flow 4h/1d) con las
# specs CONGELADAS de HIPOTESIS.md, aplica el veredicto mecanico y empaqueta reportes.
# Aislado del otro bot (mismas guardas que vps_setup.sh).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

[ "$(id -u)" = "0" ] && { echo "❌ No corras como root." >&2; exit 1; }
case "$REPO_ROOT" in
    *nachomarket*|*/nachomarket|*/nachomarket/*)
        echo "❌ ABORTO: '$REPO_ROOT' parece el arbol del OTRO bot." >&2; exit 1;;
esac

cd "$REPO_ROOT"
VENV="$REPO_ROOT/.venv-crypto"
[ -d "$VENV" ] || { echo "❌ Falta $VENV — corre: bash crypto/scripts/vps_setup.sh --ccxt" >&2; exit 1; }
# shellcheck disable=SC1091
source "$VENV/bin/activate"

echo "== Actualizando codigo =="
git pull --ff-only 2>/dev/null || echo "  (git pull fallo/no-ff — sigo con lo local)"

echo "== Tests primero (deben pasar) =="
python -m pytest crypto/tests -q

STAMP="$(date +%Y%m%d_%H%M)"
DATA="$REPO_ROOT/crypto/data"
OUT="$DATA/hipotesis_$STAMP"
mkdir -p "$OUT"
LOG="$OUT/run.log"
: > "$LOG"

echo "== Bajando datos con flujo (taker_buy_volume) =="
fetch() {  # symbol tf out
    if [ -f "$3" ] && [ "$(wc -l < "$3")" -gt 500 ]; then echo "  (cache) $3"; return 0; fi
    echo "  $1 $2 ..."
    python crypto/scripts/fetch_data.py --with-flow --symbol "$1" --timeframe "$2" \
        --since 2019-01-01 --out "$3" >>"$LOG" 2>&1 && echo "  ✅ $3" || echo "  ⚠️ fallo $1 $2 (ver $LOG)"
}
fetch "BTC/USDT" 1d "$DATA/BTC_USDT-1d.csv"
fetch "ETH/USDT" 1d "$DATA/ETH_USDT-1d.csv"
fetch "BTC/USDT" 4h "$DATA/BTC_USDT-4h-flow.csv"
fetch "ETH/USDT" 4h "$DATA/ETH_USDT-4h-flow.csv"

echo "== H1: ma_timing (1d, vol-target 0.30) =="
for d in "$DATA/BTC_USDT-1d.csv" "$DATA/ETH_USDT-1d.csv"; do
    [ -f "$d" ] || continue
    b="$(basename "${d%.csv}")"
    python crypto/scripts/validate.py --data "$d" --strategy ma_timing --vol-target 0.30 \
        --compare --deflated-sharpe 120 \
        --out "$OUT/report_${b}_ma_timing.json" \
        --trades-out "$OUT/journal_${b}_ma_timing.csv" >>"$LOG" 2>&1 \
        && echo "  ✅ $b/ma_timing" || echo "  ⚠️ $b/ma_timing fallo"
done

echo "== H2: flow (4h y 1d) =="
for d in "$DATA/BTC_USDT-4h-flow.csv" "$DATA/ETH_USDT-4h-flow.csv" \
         "$DATA/BTC_USDT-1d.csv" "$DATA/ETH_USDT-1d.csv"; do
    [ -f "$d" ] || continue
    b="$(basename "${d%.csv}")"
    python crypto/scripts/validate.py --data "$d" --strategy flow \
        --compare --deflated-sharpe 120 \
        --out "$OUT/report_${b}_flow.json" \
        --trades-out "$OUT/journal_${b}_flow.csv" >>"$LOG" 2>&1 \
        && echo "  ✅ $b/flow" || echo "  ⚠️ $b/flow fallo"
done

echo ""
echo "== VEREDICTO (decide.py) =="
python crypto/scripts/decide.py "$OUT"/report_*.json 2>>"$LOG" | tee "$OUT/veredicto.txt" || true

TARBALL="$DATA/hipotesis_$STAMP.tar.gz"
tar czf "$TARBALL" -C "$DATA" "hipotesis_$STAMP"
echo ""
echo "═══════════════════════════════════════════════════════"
echo "  ✅ H1/H2 corridas. Trae: $TARBALL (o pega veredicto.txt)"
echo "  H3 (funding) NO se corre aca: se dispara SOLO tras registrar"
echo "  este veredicto (una familia a la vez — HIPOTESIS.md)."
echo "═══════════════════════════════════════════════════════"
