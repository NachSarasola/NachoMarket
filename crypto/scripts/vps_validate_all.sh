#!/usr/bin/env bash
# One-shot de validacion en el VPS: baja datos, corre TODOS los gates y empaqueta reportes.
# Aislado del otro bot (mismas guardas que vps_setup.sh). El usuario corre UN comando y trae
# UN archivo (crypto/data/reportes_<fecha>.tar.gz) para el veredicto con decide.py.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# --- Guardas (idénticas a vps_setup.sh): no root, nunca dentro de ~/nachomarket ---
[ "$(id -u)" = "0" ] && { echo "❌ No corras como root. Usá 'ubuntu'." >&2; exit 1; }
case "$REPO_ROOT" in
    *nachomarket*|*/nachomarket|*/nachomarket/*)
        echo "❌ ABORTO: '$REPO_ROOT' parece el árbol del OTRO bot. Corré desde ~/nacho-crypto." >&2
        exit 1;;
esac

cd "$REPO_ROOT"
VENV="$REPO_ROOT/.venv-crypto"
if [ ! -d "$VENV" ]; then
    echo "❌ Falta $VENV. Corré primero:  bash crypto/scripts/vps_setup.sh --ccxt" >&2
    exit 1
fi
# shellcheck disable=SC1091
source "$VENV/bin/activate"

STAMP="$(date +%Y%m%d_%H%M)"
DATA="$REPO_ROOT/crypto/data"
OUTDIR="$DATA/reportes_$STAMP"
mkdir -p "$OUTDIR"
LOG="$OUTDIR/run.log"
: > "$LOG"

echo "== Validación completa ($STAMP) — logs en $LOG =="

# --- Descarga con fallback de exchange ---
fetch_pair() {
    local sym="$1" out="$2" ex
    for ex in binance okx kraken; do
        echo "  bajando $sym via $ex ..."
        if python crypto/scripts/fetch_data.py --exchange "$ex" --symbol "$sym" \
                --timeframe 4h --since 2019-01-01 --out "$out" >>"$LOG" 2>&1; then
            local n; n=$(wc -l < "$out" 2>/dev/null || echo 0)
            if [ "$n" -gt 1000 ]; then echo "  ✅ $sym: $n velas ($ex)"; return 0; fi
        fi
        echo "  ⚠️  $sym via $ex falló/pocos datos; siguiente..."
    done
    echo "  ❌ no pude bajar $sym de ningún exchange (ver $LOG)"; return 1
}

BTC="$DATA/BTC_USDT-4h.csv"; ETH="$DATA/ETH_USDT-4h.csv"
fetch_pair "BTC/USDT" "$BTC" || true
fetch_pair "ETH/USDT" "$ETH" || true

# --- Validación por par y estrategia ---
run_validate() {
    local csv="$1" pair="$2" strat="$3"
    [ -f "$csv" ] || { echo "  (skip $pair/$strat: sin CSV)"; return 0; }
    echo "  validate $pair/$strat ..."
    python crypto/scripts/validate.py --data "$csv" --strategy "$strat" --compare \
        --deflated-sharpe 109 \
        --out "$OUTDIR/report_${pair}_${strat}.json" \
        --trades-out "$OUTDIR/journal_${pair}_${strat}.csv" >>"$LOG" 2>&1 \
        && echo "  ✅ $pair/$strat" || echo "  ⚠️  $pair/$strat falló (ver $LOG)"
}

for pair_csv in "BTC:$BTC" "ETH:$ETH"; do
    pair="${pair_csv%%:*}"; csv="${pair_csv#*:}"
    run_validate "$csv" "$pair" sweep
    run_validate "$csv" "$pair" donchian
done

# --- Robustez de parametros (meseta) sobre BTC sweep ---
if [ -f "$BTC" ]; then
    echo "  param_sweep BTC/sweep ..."
    python crypto/scripts/param_sweep.py --data "$BTC" >"$OUTDIR/param_sweep_BTC.txt" 2>>"$LOG" \
        && echo "  ✅ param_sweep" || echo "  ⚠️  param_sweep falló"
fi

# --- Veredicto mecánico ---
echo ""
echo "== VEREDICTO (decide.py) =="
python crypto/scripts/decide.py "$OUTDIR"/report_*.json 2>>"$LOG" | tee "$OUTDIR/veredicto.txt" || \
    echo "⚠️  decide.py no pudo correr (¿faltan reportes? ver $LOG)"

# --- Empaquetar ---
TARBALL="$DATA/reportes_$STAMP.tar.gz"
tar czf "$TARBALL" -C "$DATA" "reportes_$STAMP"
echo ""
echo "═══════════════════════════════════════════════════════"
echo "  ✅ Listo. Traé este archivo para el análisis:"
echo "     $TARBALL"
echo "  (contiene reports JSON, journals CSV, param_sweep y veredicto)"
echo "═══════════════════════════════════════════════════════"
