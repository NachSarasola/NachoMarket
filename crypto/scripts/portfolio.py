#!/usr/bin/env python3
"""Validacion multi-par + correlacion: ¿agregar un par DIVERSIFICA o solo suma fees?

Implementa el criterio de TESIS.md: en crypto los pares correlacionan altisimo, asi que mas
pares no diversifica. Corre la estrategia en cada par, y ademas de las metricas por par calcula
la CORRELACION de los retornos diarios de las curvas de equity. Si dos pares corr > 0.7,
agregar el segundo aporta poca diversificacion real y el reporte lo dice.

Uso:
    python crypto/scripts/portfolio.py --data crypto/data/BTC_USDT-4h.csv crypto/data/ETH_USDT-4h.csv
    python crypto/scripts/portfolio.py --data-dir crypto/data --strategy sweep
"""

from __future__ import annotations

import argparse
import glob
import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, __file__.rsplit("/crypto/", 1)[0])

from crypto.scripts.validate import BARS_PER_YEAR_4H, DEFAULT_PARAMS, gen_signals, load_csv  # noqa: E402
from crypto.smc.backtest import compute_metrics, run_backtest  # noqa: E402

CORR_HIGH = 0.7  # umbral de TESIS: por encima, agregar el par no diversifica


def daily_returns(equity: pd.Series) -> pd.Series:
    """Retornos diarios de una curva de equity (indice temporal, cualquier frecuencia)."""
    daily = equity.resample("1D").last().ffill()
    return daily.pct_change().dropna()


def correlation_matrix(equities: dict[str, pd.Series]) -> pd.DataFrame:
    """Matriz de correlacion de los retornos diarios de varias curvas de equity."""
    cols = {name: daily_returns(eq) for name, eq in equities.items()}
    df = pd.DataFrame(cols).dropna()
    return df.corr()


def combined_equity(equities: dict[str, pd.Series], initial: float) -> pd.Series:
    """Equity de una cartera equal-weight (riesgo independiente por par) — diario."""
    rets = pd.DataFrame({name: daily_returns(eq) for name, eq in equities.items()}).dropna()
    if rets.empty:
        return pd.Series(dtype=float)
    port = rets.mean(axis=1)  # equal weight
    return initial * (1 + port).cumprod()


def high_corr_pairs(corr: pd.DataFrame, threshold: float = CORR_HIGH) -> list[tuple[str, str, float]]:
    """Pares con correlacion > threshold (candidatos a NO aportar diversificacion)."""
    out = []
    names = list(corr.columns)
    for i in range(len(names)):
        for j in range(i + 1, len(names)):
            c = corr.iloc[i, j]
            if c > threshold:
                out.append((names[i], names[j], round(float(c), 3)))
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--data", nargs="+", help="uno o mas CSV OHLCV")
    src.add_argument("--data-dir", help="carpeta con *-4h.csv")
    ap.add_argument("--strategy", choices=["sweep", "donchian"], default="sweep")
    ap.add_argument("--initial", type=float, default=500.0)
    ap.add_argument("--fee-bps", type=float, default=10.0)
    ap.add_argument("--slippage-bps", type=float, default=5.0)
    args = ap.parse_args()

    files = args.data if args.data else sorted(glob.glob(os.path.join(args.data_dir, "*-4h.csv")))
    if len(files) < 1:
        print("Sin CSVs.", file=sys.stderr)
        return 1

    params = DEFAULT_PARAMS[args.strategy]
    bt = dict(fee_bps=args.fee_bps, slippage_bps=args.slippage_bps, initial_equity=args.initial,
              direction="long", bars_per_year=BARS_PER_YEAR_4H)

    equities: dict[str, pd.Series] = {}
    print(f"Estrategia: {args.strategy} | pares: {len(files)}\n")
    print(f"  {'par':>14} | {'sharpe':>7} {'pf':>6} {'trades':>6} {'ret':>7} {'maxDD':>7}")
    for path in files:
        name = os.path.basename(path).replace("-4h.csv", "")
        df = load_csv(path)
        sig = gen_signals(df, args.strategy, params)
        res = run_backtest(df, sig, **bt)
        m = res.metrics()
        equities[name] = res.equity
        print(f"  {name:>14} | {str(m['sharpe']):>7} {str(m['profit_factor']):>6} "
              f"{m['trades']:>6} {str(m['total_return']):>7} {str(m['max_drawdown']):>7}")

    if len(equities) >= 2:
        corr = correlation_matrix(equities)
        print("\nCorrelacion de retornos diarios (equity):")
        print(corr.round(2).to_string())
        highs = high_corr_pairs(corr)
        if highs:
            print(f"\n⚠️  Pares con corr > {CORR_HIGH} (agregar el 2do NO diversifica, solo suma fees):")
            for a, b, c in highs:
                print(f"    {a} ~ {b}: {c}")
        else:
            print(f"\n✅ Ningun par supera corr {CORR_HIGH}: hay algo de diversificacion real.")

        comb = combined_equity(equities, args.initial)
        if not comb.empty:
            cm = compute_metrics(comb, [], 365, args.initial)
            print(f"\nCartera equal-weight (diaria): sharpe={cm['sharpe']} "
                  f"maxDD={cm['max_drawdown']} ret={cm['total_return']}")
            print("  (comparar el maxDD de la cartera vs el de cada par: si no baja, no diversifica)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
