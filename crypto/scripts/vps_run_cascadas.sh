#!/usr/bin/env bash
# One-shot H9 (cascadas de liquidacion, spec CONGELADA): baja klines+funding+OI 5m de los
# 10 majors del universo fijo, extrae eventos de purga y corre el event-study con gates.
# Aislado del otro bot (mismas guardas de siempre).
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

# Universo FIJO de la spec H9 (REGLAS_CONGELADAS.md) — NO editar sin registrar.
UNIVERSE="BTCUSDT ETHUSDT SOLUSDT BNBUSDT XRPUSDT DOGEUSDT ADAUSDT LINKUSDT AVAXUSDT LTCUSDT"

STAMP="$(date +%Y%m%d_%H%M)"
DATA="$REPO_ROOT/crypto/data"
CAS="$DATA/cascade"
OUT="$DATA/cascadas_$STAMP"
mkdir -p "$CAS" "$OUT"
LOG="$OUT/run.log"
: > "$LOG"

echo "== 1/3 Datos: klines 4h + funding + OI 5m desde 2021-12 (10 majors) =="
for S in $UNIVERSE; do
    [ -f "$CAS/${S}-4h.csv" ] || python crypto/scripts/fetch_data.py --futures --symbol "$S" \
        --timeframe 4h --since 2021-12-01 --out "$CAS/${S}-4h.csv" >>"$LOG" 2>&1 \
        || echo "  ⚠️ klines $S fallaron"
    [ -f "$CAS/${S}-funding.csv" ] || python crypto/scripts/fetch_funding.py --symbol "$S" \
        --since 2021-12-01 --out "$CAS/${S}-funding.csv" >>"$LOG" 2>&1 \
        || echo "  ⚠️ funding $S fallo"
    [ -f "$CAS/${S}-oi.csv" ] || python crypto/scripts/fetch_metrics.py --symbol "$S" \
        --since 2021-12 --out "$CAS/${S}-oi.csv" >>"$LOG" 2>&1 \
        || echo "  ⚠️ OI $S fallo (data.binance.vision)"
    echo "  ✓ $S"
done

echo "== 2/3 Eventos de purga (spec congelada: oi_drop 3%, vol_z 2) =="
python crypto/scripts/make_cascade_events.py --data-dir "$CAS" \
    --symbols "$(echo "$UNIVERSE" | tr ' ' ',')" --oi-drop 3.0 --vol-z 2.0 \
    --out "$CAS/events.csv" 2>&1 | tee -a "$LOG" | tail -12

echo "== 3/3 Event study H9 (--deflated-sharpe 132) =="
python crypto/scripts/event_validate.py --strategy h9_cascade \
    --events "$CAS/events.csv" --data-dir "$CAS" --funding-dir "$CAS" \
    --is-end 2024-12-31 --deflated-sharpe 132 --out "$OUT/rep_h9_cascade.json" \
    --trades-out "$OUT/journal_h9_cascade.csv" | tee "$OUT/veredicto_h9.txt" || true

TARBALL="$DATA/cascadas_$STAMP.tar.gz"
tar czf "$TARBALL" -C "$DATA" "cascadas_$STAMP"
echo ""
echo "═══════════════════════════════════════════════════════"
echo "  ✅ H9 corrida. Trae: $TARBALL (o pega veredicto_h9.txt)"
echo "  La VARIANTE (eventos con --oi-drop 2.0) SOLO tras registrar este"
echo "  veredicto, y cuenta como trial (REGLAS_CONGELADAS.md)."
echo "═══════════════════════════════════════════════════════"
