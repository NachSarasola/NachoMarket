#!/usr/bin/env python3
"""Barrido de parametros con deteccion de MESETA vs PICO (anti curve-fitting).

Un optimo aislado (pico) casi siempre es overfit: sus vecinos rinden mucho peor. Un optimo
robusto (meseta) tiene vecinos que tambien rinden bien. Este script corre una grilla sobre
los parametros libres del sweep y reporta si el mejor esta en meseta o en pico, ademas del
conteo de variantes (que alimenta el Deflated Sharpe: mas variantes => mas exigencia).

Uso:
    python crypto/scripts/param_sweep.py --data crypto/data/BTC_USDT-4h.csv
    python crypto/scripts/param_sweep.py --synthetic   # usa el control POSITIVO (hay cresta)
"""

from __future__ import annotations

import argparse
import itertools
import sys

import numpy as np

sys.path.insert(0, __file__.rsplit("/crypto/", 1)[0])

from crypto.scripts.validate import BARS_PER_YEAR_4H, load_csv  # noqa: E402
from crypto.smc.backtest import run_backtest  # noqa: E402
from crypto.smc.signals import smc_sweep_signals  # noqa: E402
from crypto.smc.synthetic import sweep_market_ohlcv  # noqa: E402

# Grilla de parametros libres (valores ordenados por dimension -> define vecindad).
GRID = {
    "lookback": [15, 20, 30, 40],
    "sl_buffer_atr": [0.3, 0.5, 0.8],
    "pda_lookback": [40, 60, 90],
    "time_stop_bars": [8, 12, 18],
}
BASE = {"use_pda_filter": True, "require_equal": False, "sl_cap_pct": 0.04, "single_target": True}


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    src = p.add_mutually_exclusive_group(required=True)
    src.add_argument("--data")
    src.add_argument("--synthetic", action="store_true")
    p.add_argument("--fee-bps", type=float, default=10.0)
    p.add_argument("--slippage-bps", type=float, default=5.0)
    p.add_argument("--risk-pct", type=float, default=0.01)
    p.add_argument("--initial", type=float, default=500.0)
    p.add_argument("--top", type=int, default=10)
    p.add_argument("--plateau-frac", type=float, default=0.6)
    args = p.parse_args()

    if args.synthetic:
        df = sweep_market_ohlcv(seed=3)
        print("### SINTETICO (control POSITIVO) — demuestra la deteccion de meseta ###\n")
    else:
        df = load_csv(args.data)

    bt = dict(fee_bps=args.fee_bps, slippage_bps=args.slippage_bps, risk_pct=args.risk_pct,
              initial_equity=args.initial, direction="long", bars_per_year=BARS_PER_YEAR_4H)

    dims = list(GRID.keys())
    value_lists = [GRID[d] for d in dims]
    results: dict[tuple, dict] = {}
    for combo_idx in itertools.product(*[range(len(v)) for v in value_lists]):
        params = dict(BASE)
        for d, vi in zip(dims, combo_idx):
            params[d] = GRID[d][vi]
        sig = smc_sweep_signals(df, **params)
        m = run_backtest(df, sig, **bt).metrics()
        results[combo_idx] = {"params": params, "metrics": m,
                              "sharpe": m.get("sharpe"), "trades": m.get("trades")}

    n_variants = len(results)
    valid = [(k, v) for k, v in results.items() if v["sharpe"] is not None and v["trades"] >= 20]
    if not valid:
        print("Sin combos con suficientes trades. Ampliar datos.")
        return 1

    valid.sort(key=lambda kv: kv[1]["sharpe"], reverse=True)
    print(f"Variantes probadas: {n_variants} (todas cuentan para el multiple-testing / DSR)\n")
    print(f"Top {args.top} por Sharpe:")
    print(f"  {'lookback':>8} {'sl_buf':>6} {'pda':>4} {'tstop':>5} | "
          f"{'sharpe':>7} {'pf':>6} {'trades':>6} {'ret':>7} {'maxDD':>7}")
    for k, v in valid[: args.top]:
        pr, m = v["params"], v["metrics"]
        print(f"  {pr['lookback']:>8} {pr['sl_buffer_atr']:>6} {pr['pda_lookback']:>4} "
              f"{pr['time_stop_bars']:>5} | {m['sharpe']:>7} {str(m['profit_factor']):>6} "
              f"{m['trades']:>6} {str(m['total_return']):>7} {str(m['max_drawdown']):>7}")

    # --- Deteccion de meseta: vecinos del mejor (distancia L1 = 1 en indices de grilla) ---
    best_idx, best = valid[0]
    neigh = []
    for k, v in results.items():
        if v["sharpe"] is None:
            continue
        if sum(abs(a - b) for a, b in zip(k, best_idx)) == 1:
            neigh.append(v["sharpe"])
    print(f"\nMejor combo: {best['params']}  (Sharpe={best['sharpe']})")
    if neigh:
        med = float(np.median(neigh))
        frac = med / best["sharpe"] if best["sharpe"] else 0.0
        verdict = "MESETA (robusto)" if (best["sharpe"] > 0 and frac >= args.plateau_frac) else \
                  "PICO (sospechoso de overfit)"
        print(f"Vecinos: n={len(neigh)} Sharpe mediano={med:.3f}  "
              f"({med:.3f}/{best['sharpe']:.3f} = {frac:.2f})  ->  {verdict}")
        print(f"Regla: se acepta el optimo solo si los vecinos rinden >= {args.plateau_frac:.0%} "
              f"del pico. Un pico aislado es curve-fitting.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
