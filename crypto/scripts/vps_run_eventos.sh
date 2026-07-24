#!/usr/bin/env bash
# One-shot del programa EVENT-DRIVEN (tesis invertida): H7 short-unlocks + H8 listing-fade
# con las specs CONGELADAS de REGLAS_CONGELADAS.md. Baja eventos (DeFiLlama + Binance),
# klines/funding de los PERPS, corre event_validate y empaqueta. Aislado del otro bot.
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
EV="$DATA/events"
OUT="$DATA/eventos_$STAMP"
mkdir -p "$EV" "$OUT"
LOG="$OUT/run.log"
: > "$LOG"

echo "== 1/4 Listings spot (mecanico: primera vela) + exchangeInfo de perps =="
if [ ! -f "$EV/listings.csv" ]; then
    python crypto/scripts/fetch_listings.py --since 2023-01-01 \
        --out "$EV/listings.csv" --perps-out "$EV/perps.json" >>"$LOG" 2>&1 \
        && echo "  ✅ listings.csv" || echo "  ⚠️ fetch_listings fallo (ver $LOG)"
else echo "  (cache) $EV/listings.csv"; fi

echo "== 2/4 Calendario historico de unlocks (repo open-source emissions-adapters) =="
ADP="$EV/emissions-adapters"
if [ ! -f "$EV/unlocks.csv" ]; then
    if [ ! -d "$ADP/protocols" ]; then
        echo "  clonando emissions-adapters (fuente de verdad de DefiLlama)..."
        git clone --depth 1 https://github.com/DefiLlama/emissions-adapters "$ADP" \
            >>"$LOG" 2>&1 || echo "  ⚠️ clone fallo (ver $LOG)"
    fi
    if python crypto/scripts/fetch_unlocks.py --source adapters --adapters-dir "$ADP" \
        --perps-json "$EV/perps.json" --min-pct 1.0 --out "$EV/unlocks.csv" \
        2>&1 | tee -a "$LOG" | tail -5 && [ -s "$EV/unlocks.csv" ]; then
        echo "  ✅ unlocks.csv ($(($(wc -l < "$EV/unlocks.csv") - 1)) eventos)"
    else
        echo "  ⚠️ 0 eventos del parser — pegar estas muestras para ajustarlo:"
        ls "$ADP/protocols" 2>/dev/null | head -15
        for f in aptos arbitrum celestia; do
            [ -f "$ADP/protocols/$f.ts" ] && { echo "----- $f.ts"; sed -n '1,60p' "$ADP/protocols/$f.ts"; break; }
        done
    fi
else echo "  (cache) $EV/unlocks.csv"; fi

echo "== 3/4 Klines 4h + funding de los perps de los eventos =="
SYMS="$(python - <<'PY'
import csv
syms = set()
for p in ("crypto/data/events/unlocks.csv", "crypto/data/events/listings.csv"):
    try:
        with open(p) as f:
            for row in csv.DictReader(f):
                s = (row.get("symbol") or "").strip()
                if s and row.get("has_perp", "1") != "0":
                    syms.add(s)
    except FileNotFoundError:
        pass
print(" ".join(sorted(syms)))
PY
)"
N=$(echo "$SYMS" | wc -w)
echo "  simbolos con eventos y perp: $N"
i=0
for S in $SYMS; do
    i=$((i+1))
    K="$EV/${S}-4h.csv"; F="$EV/${S}-funding.csv"
    if [ ! -f "$K" ]; then
        python crypto/scripts/fetch_data.py --futures --symbol "$S" --timeframe 4h \
            --since 2022-12-01 --out "$K" >>"$LOG" 2>&1 || echo "  ⚠️ klines $S fallaron"
    fi
    if [ ! -f "$F" ]; then
        python crypto/scripts/fetch_funding.py --symbol "$S" --since 2022-12-01 \
            --out "$F" >>"$LOG" 2>&1 || echo "  ⚠️ funding $S fallo"
    fi
    [ $((i % 20)) -eq 0 ] && echo "  ... $i/$N"
done

echo "== 4/4 Event studies (specs congeladas; --deflated-sharpe 130) =="
if [ -f "$EV/unlocks.csv" ]; then
    python crypto/scripts/event_validate.py --strategy h7_unlock \
        --events "$EV/unlocks.csv" --data-dir "$EV" --funding-dir "$EV" \
        --deflated-sharpe 130 --out "$OUT/rep_h7_unlock.json" \
        --trades-out "$OUT/journal_h7_unlock.csv" | tee "$OUT/veredicto_h7.txt" || true
else
    echo "⚠️ SIN unlocks.csv: H7 no corre. Diagnostico:" | tee "$OUT/veredicto_h7.txt"
    echo "   tail -30 $LOG   y   python crypto/scripts/fetch_unlocks.py --dump-raw aptos --out /tmp/llama_aptos.json" | tee -a "$OUT/veredicto_h7.txt"
fi
echo ""
python crypto/scripts/event_validate.py --strategy h8_listing \
    --events "$EV/listings.csv" --data-dir "$EV" --funding-dir "$EV" \
    --deflated-sharpe 130 --out "$OUT/rep_h8_listing.json" \
    --trades-out "$OUT/journal_h8_listing.csv" | tee "$OUT/veredicto_h8.txt" || true

TARBALL="$DATA/eventos_$STAMP.tar.gz"
tar czf "$TARBALL" -C "$DATA" "eventos_$STAMP" \
    -C "$DATA" "events/unlocks.csv" "events/listings.csv" 2>/dev/null \
    || tar czf "$TARBALL" -C "$DATA" "eventos_$STAMP"
echo ""
echo "═══════════════════════════════════════════════════════"
echo "  ✅ H7/H8 corridas. Trae: $TARBALL (o pega veredicto_h7.txt y veredicto_h8.txt)"
echo "  La VARIANTE unica (--variant) NO se corre aca: solo tras registrar"
echo "  este veredicto, y cuenta como trial (REGLAS_CONGELADAS.md)."
echo "═══════════════════════════════════════════════════════"
