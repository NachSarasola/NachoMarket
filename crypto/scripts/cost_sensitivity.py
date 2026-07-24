#!/usr/bin/env python3
"""Sensibilidad a costos: ¿a partir de que fee+slippage MUERE el edge?

El costo es el killer #1 del capital chico. Un edge que solo existe con costos optimistas
es un espejismo. Este script corre la MISMA estrategia (mismas senales) sobre una grilla de
supuestos de costo y muestra donde se cae el retorno a cero -> el "acantilado de costos".

Uso:
    python crypto/scripts/cost_sensitivity.py --data crypto/data/BTC_USDT-4h.csv
    python crypto/scripts/cost_sensitivity.py --synthetic-positive
"""

from __future__ import annotations

import argparse
import sys

sys.path.insert(0, __file__.rsplit("/crypto/", 1)[0])

from crypto.scripts.validate import BARS_PER_YEAR_4H, DEFAULT_PARAMS, gen_signals, load_csv  # noqa: E402
from crypto.smc.backtest import run_backtest  # noqa: E402

# Escenarios de costo round-trip (por LADO en bps). Van de optimista a catastrofico.
# Referencia: spot maker Binance ~7.5-10 bps/lado; taker ~10; slippage 2-6 en BTC/ETH.
SCENARIOS = [
    ("optimista",     5.0,  2.0),
    ("base",          10.0, 5.0),
    ("realista",      15.0, 7.0),
    ("pesimista",     20.0, 10.0),
    ("muy_pesimista", 30.0, 15.0),
]


def run_cost_grid(df, strategy: str, params: dict, scenarios: list, initial: float,
                  direction: str = "long") -> list[dict]:
    """Corre el backtest en cada escenario de costo. Las senales NO cambian: solo el PnL.

    Devuelve una fila por escenario con trades (constante), sharpe y total_return.
    """
    sig = gen_signals(df, strategy, params)  # una sola vez: las senales no dependen del costo
    out = []
    for label, fee, slip in scenarios:
        res = run_backtest(df, sig, fee_bps=fee, slippage_bps=slip, initial_equity=initial,
                           direction=direction, bars_per_year=BARS_PER_YEAR_4H)
        m = res.metrics()
        out.append({
            "escenario": label,
            "fee_bps": fee,
            "slippage_bps": slip,
            "roundtrip_bps": round(2 * (fee + slip), 1),  # entrada+salida, fee+slip
            "trades": m["trades"],
            "sharpe": m["sharpe"],
            "total_return": m["total_return"],
            "profit_factor": m["profit_factor"],
        })
    return out


def cost_cliff(results: list[dict]) -> dict | None:
    """Primer escenario (de menor a mayor costo) donde el retorno deja de ser positivo."""
    for r in results:
        tr = r["total_return"]
        if tr is None or tr <= 0:
            return r
    return None


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--data")
    src.add_argument("--synthetic-positive", action="store_true")
    ap.add_argument("--strategy", choices=["sweep", "donchian"], default="sweep")
    ap.add_argument("--direction", choices=["long", "short", "both"], default="long")
    ap.add_argument("--initial", type=float, default=500.0)
    args = ap.parse_args()

    if args.synthetic_positive:
        from crypto.smc.synthetic import sweep_market_ohlcv
        df = sweep_market_ohlcv(n_events=340, seed=7)
        print("### SINTETICO CON EDGE — demuestra la lectura, no es dato real ###\n")
    else:
        df = load_csv(args.data)

    params = DEFAULT_PARAMS[args.strategy]
    results = run_cost_grid(df, args.strategy, params, SCENARIOS, args.initial, args.direction)

    print(f"Sensibilidad a costos — {args.strategy} ({args.direction})\n")
    print(f"  {'escenario':>14} {'RT bps':>7} {'trades':>7} {'sharpe':>8} {'retorno':>9} {'PF':>6}")
    for r in results:
        print(f"  {r['escenario']:>14} {r['roundtrip_bps']:>7} {r['trades']:>7} "
              f"{str(r['sharpe']):>8} {str(r['total_return']):>9} {str(r['profit_factor']):>6}")

    cliff = cost_cliff(results)
    print()
    if cliff is None:
        print("✅ El edge sobrevive hasta el escenario mas pesimista. Robusto a costos.")
    elif cliff["escenario"] == "optimista":
        print("🛑 El edge NO existe ni con costos optimistas. No hay nada que operar.")
    else:
        print(f"⚠️  El edge MUERE en el escenario '{cliff['escenario']}' "
              f"({cliff['roundtrip_bps']} bps round-trip). Tu costo REAL debe estar MUY por "
              f"debajo de eso para que sobreviva. Si tu round-trip real se acerca, no operar.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
