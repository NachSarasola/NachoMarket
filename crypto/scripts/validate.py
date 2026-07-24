#!/usr/bin/env python3
"""Orquestador de validacion anti-overfitting para las estrategias SMC.

Implementa los gates del plan (REGLAS_CONGELADAS.md), en orden:
  2. Backtest in-sample con costos pesimistas.
  3. Exigir un minimo de trades (si no, no es validable).
  4. Walk-forward (folds) + reporte de estabilidad del Sharpe.
  5. OOS puro en una sola pasada (aviso explicito: mirar UNA vez).
  6. Comparacion contra benchmarks: buy&hold y cruce de media movil.

Uso:
    # con datos reales (CSV de fetch_data.py / freqtrade):
    python crypto/scripts/validate.py --data crypto/data/BTC_USDT-4h.csv --strategy sweep

    # smoke test del pipeline sobre datos SINTETICOS (no es evidencia de edge):
    python crypto/scripts/validate.py --synthetic --strategy sweep

IMPORTANTE: el modo --synthetic sirve SOLO para verificar que el pipeline corre y que
los costos muerden. NUNCA interpretar sus numeros como validacion de la estrategia.
"""

from __future__ import annotations

import argparse
import json
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, __file__.rsplit("/crypto/", 1)[0])  # raiz del repo en path

from crypto.smc.backtest import BacktestResult, compute_metrics, run_backtest  # noqa: E402
from crypto.smc.report import export_trades_csv, slice_report  # noqa: E402
from crypto.smc.signals import donchian_bms_signals, smc_sweep_signals  # noqa: E402
from crypto.smc.stats import deflated_sharpe_ratio, monte_carlo_ruin  # noqa: E402

BARS_PER_YEAR_4H = 6 * 365  # 2190


# --------------------------------------------------------------------------- #
# Carga de datos
# --------------------------------------------------------------------------- #

def load_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    tcol = "timestamp" if "timestamp" in df.columns else ("date" if "date" in df.columns else None)
    if tcol is None:
        raise ValueError("CSV sin columna 'timestamp' ni 'date'")
    # Numerico -> epoch ms (formato de ccxt/fetch_data); si no, parsear como fecha ISO.
    # (pd.api.types maneja StringDtype de pandas 3.0, que rompe np.issubdtype.)
    if pd.api.types.is_numeric_dtype(df[tcol]):
        df.index = pd.to_datetime(df[tcol], unit="ms", utc=True)
    else:
        df.index = pd.to_datetime(df[tcol], utc=True)
    df = df[["open", "high", "low", "close", "volume"]].astype(float).sort_index()
    return df


def synthetic_ohlcv(n: int = 15000, seed: int = 42) -> pd.DataFrame:
    """GBM con cambios de regimen (bull/bear/rango) — SOLO para smoke test."""
    rng = np.random.default_rng(seed)
    idx = pd.date_range("2019-01-01", periods=n, freq="4h", tz="UTC")
    # regimenes: drift y vol cambiantes por tramos.
    drift = np.zeros(n)
    vol = np.full(n, 0.01)
    t = 0
    while t < n:
        seg = rng.integers(300, 1200)
        regime = rng.choice(["bull", "bear", "range"], p=[0.4, 0.3, 0.3])
        d = {"bull": 0.0006, "bear": -0.0006, "range": 0.0}[regime]
        v = {"bull": 0.012, "bear": 0.018, "range": 0.008}[regime]
        drift[t : t + seg] = d
        vol[t : t + seg] = v
        t += seg
    rets = rng.normal(drift, vol)
    close = 4000 * np.exp(np.cumsum(rets))
    # construir OHLC coherente alrededor del close
    open_ = np.empty(n)
    open_[0] = close[0]
    open_[1:] = close[:-1]
    wick = vol * close
    high = np.maximum(open_, close) + rng.uniform(0.1, 1.0, n) * wick
    low = np.minimum(open_, close) - rng.uniform(0.1, 1.0, n) * wick
    return pd.DataFrame(
        {"open": open_, "high": high, "low": low, "close": close, "volume": np.ones(n)},
        index=idx,
    )


# --------------------------------------------------------------------------- #
# Benchmarks
# --------------------------------------------------------------------------- #

def bench_buy_hold(df: pd.DataFrame, initial: float) -> dict:
    eq = initial * df["close"] / df["close"].iloc[0]
    return compute_metrics(eq, [], BARS_PER_YEAR_4H, initial)


def bench_ma_cross(df: pd.DataFrame, initial: float, window: int = 200, fee_bps: float = 10.0) -> dict:
    """Long-only: mantiene BTC cuando close>SMA(window), si no cash. Con fee por flip."""
    close = df["close"].astype(float)
    sma = close.rolling(window).mean()
    long = (close > sma).fillna(False).to_numpy()
    ret = close.pct_change().fillna(0.0).to_numpy()
    fee = fee_bps / 1e4
    eq = np.full(len(df), initial, dtype=float)
    pos_prev = False
    for i in range(1, len(df)):
        pos = long[i - 1]  # decidido con la barra previa (causal)
        r = ret[i] if pos else 0.0
        cost = fee if pos != pos_prev else 0.0
        eq[i] = eq[i - 1] * (1 + r) * (1 - cost)
        pos_prev = pos
    return compute_metrics(pd.Series(eq, index=df.index), [], BARS_PER_YEAR_4H, initial)


# --------------------------------------------------------------------------- #
# Estrategias
# --------------------------------------------------------------------------- #

def gen_signals(df: pd.DataFrame, strategy: str, params: dict) -> pd.DataFrame:
    if strategy == "sweep":
        return smc_sweep_signals(df, **params)
    if strategy == "donchian":
        return donchian_bms_signals(df, **params)
    raise ValueError(f"estrategia desconocida: {strategy}")


def run_segment_full(df: pd.DataFrame, strategy: str, params: dict, bt_kwargs: dict) -> tuple[dict, BacktestResult]:
    sig = gen_signals(df, strategy, params)
    res = run_backtest(df, sig, **bt_kwargs)
    m = res.metrics()
    m["signals_long"] = int(sig["enter_long"].sum())
    m["signals_short"] = int(sig["enter_short"].sum())
    return m, res


def run_segment(df: pd.DataFrame, strategy: str, params: dict, bt_kwargs: dict) -> dict:
    return run_segment_full(df, strategy, params, bt_kwargs)[0]


def walk_forward(df: pd.DataFrame, strategy: str, params: dict, bt_kwargs: dict, folds: int) -> list[dict]:
    out = []
    bounds = np.linspace(0, len(df), folds + 1, dtype=int)
    for k in range(folds):
        seg = df.iloc[bounds[k] : bounds[k + 1]]
        if len(seg) < 300:
            continue
        m = run_segment(seg, strategy, params, bt_kwargs)
        m["fold"] = k
        m["from"] = str(seg.index[0].date())
        m["to"] = str(seg.index[-1].date())
        out.append(m)
    return out


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #

DEFAULT_PARAMS = {
    "sweep": {
        "lookback": 20,
        "sl_buffer_atr": 0.5,
        "sl_cap_pct": 0.04,
        "use_pda_filter": True,
        "require_equal": False,
        "pda_lookback": 60,
        "time_stop_bars": 12,
    },
    "donchian": {
        "lookback": 20,
        "sl_buffer_atr": 1.5,
        "sl_cap_pct": 0.06,
        "tp_r_multiple": 2.0,
        "time_stop_bars": 18,
    },
}


def _fmt(m: dict) -> str:
    return (
        f"trades={m.get('trades')} winrate={m.get('win_rate')} pf={m.get('profit_factor')} "
        f"expR={m.get('expectancy_r')} sharpe={m.get('sharpe')} maxDD={m.get('max_drawdown')} "
        f"CAGR={m.get('cagr')} finalEq={m.get('final_equity')} maxLossStreak={m.get('max_loss_streak')}"
    )


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    src = p.add_mutually_exclusive_group(required=True)
    src.add_argument("--data", help="CSV OHLCV (timestamp,open,high,low,close,volume)")
    src.add_argument("--synthetic", action="store_true",
                     help="datos sinteticos SIN edge (random-walk multi-regimen) — smoke negativo")
    src.add_argument("--synthetic-positive", action="store_true",
                     help="datos sinteticos CON edge (sweeps inyectados) — demuestra el pipeline en verde")
    p.add_argument("--strategy", choices=["sweep", "donchian"], default="sweep")
    p.add_argument("--direction", choices=["long", "short", "both"], default="long")
    p.add_argument("--is-end", default="2023-12-31", help="fin del in-sample (OOS empieza al dia sig.)")
    p.add_argument("--initial", type=float, default=500.0)
    p.add_argument("--fee-bps", type=float, default=10.0)
    p.add_argument("--slippage-bps", type=float, default=5.0)
    p.add_argument("--risk-pct", type=float, default=0.01)
    p.add_argument("--folds", type=int, default=4)
    p.add_argument("--min-trades", type=int, default=100)
    p.add_argument("--deflated-sharpe", type=int, default=1, metavar="N_TRIALS",
                   help="nº de variantes probadas para el Deflated Sharpe (multiple-testing)")
    p.add_argument("--mc-sims", type=int, default=5000, help="simulaciones Monte Carlo de ruina")
    p.add_argument("--compare", action="store_true",
                   help="tabla comparativa sweep vs donchian vs benchmarks (IS y OOS)")
    p.add_argument("--out", default="", help="ruta opcional para el reporte JSON")
    p.add_argument("--trades-out", default="",
                   help="exporta el journal de trades del in-sample a CSV (regimen incluido)")
    args = p.parse_args()

    if args.synthetic:
        df = synthetic_ohlcv()
        print("### SINTETICO SIN EDGE (random-walk) — debe dar NEGATIVO; es el control ###\n")
    elif args.synthetic_positive:
        from crypto.smc.synthetic import sweep_market_ohlcv
        # Muchos eventos para abarcar IS (hasta 2023) y dejar barras reales para el OOS.
        df = sweep_market_ohlcv(n_events=480, seed=7)
        print("### SINTETICO CON EDGE (sweeps inyectados) — demuestra el pipeline en verde. "
              "NO es dato real, solo muestra que detecta el fenomeno cuando existe ###\n")
    else:
        df = load_csv(args.data)

    params = DEFAULT_PARAMS[args.strategy]
    bt_kwargs = dict(
        fee_bps=args.fee_bps,
        slippage_bps=args.slippage_bps,
        risk_pct=args.risk_pct,
        initial_equity=args.initial,
        direction=args.direction,
        bars_per_year=BARS_PER_YEAR_4H,
    )

    is_df = df.loc[: args.is_end]
    oos_df = df.loc[args.is_end :].iloc[1:]

    report: dict = {
        "strategy": args.strategy,
        "direction": args.direction,
        "params": params,
        "costs": {"fee_bps": args.fee_bps, "slippage_bps": args.slippage_bps},
        "data": {
            "source": args.data if args.data else ("synthetic-positive" if args.synthetic_positive else "synthetic"),
            "bars": len(df),
            "from": str(df.index[0]),
            "to": str(df.index[-1]),
            "is_bars": len(is_df),
            "oos_bars": len(oos_df),
        },
    }

    print(f"Datos: {len(df)} velas {df.index[0].date()} -> {df.index[-1].date()}")
    print(f"Estrategia: {args.strategy} ({args.direction}) | params: {params}\n")

    # --- Gate 2: in-sample ---
    is_m, is_res = run_segment_full(is_df, args.strategy, params, bt_kwargs)
    report["in_sample"] = is_m
    print("== IN-SAMPLE ==")
    print(" ", _fmt(is_m))

    # --- Slice por regimen y por tipo de salida (journal del review semanal) ---
    if is_res.trades:
        slices = slice_report(is_res.trades)
        report["slices"] = slices
        print("\n== SLICES (in-sample) — donde gana y donde pierde ==")
        for titulo, key in (("por regimen", "por_regimen"), ("por salida", "por_salida")):
            print(f"  {titulo}:")
            for k, v in slices[key].items():
                print(f"    {k:>10}: n={v['n']:>4} winrate={v['winrate']:<6} "
                      f"pnl={v['pnl']:<10} avgR={v['avg_r']}")
    if args.trades_out and is_res.trades:
        export_trades_csv(is_res.trades, args.trades_out)
        print(f"\nJournal de trades (IS) -> {args.trades_out}")

    # --- Gate 3: minimo de trades ---
    enough = (is_m.get("trades") or 0) >= args.min_trades
    report["gate3_min_trades_ok"] = enough
    print(f"\n[Gate 3] trades={is_m.get('trades')} (min {args.min_trades}) -> "
          f"{'OK' if enough else 'INSUFICIENTE: no validable'}")

    # --- Gate 4: walk-forward ---
    wf = walk_forward(is_df, args.strategy, params, bt_kwargs, args.folds)
    report["walk_forward"] = wf
    sharpes = [f["sharpe"] for f in wf if f.get("sharpe") is not None]
    print("\n== WALK-FORWARD (in-sample) ==")
    for f in wf:
        print(f"  fold {f['fold']} [{f['from']}..{f['to']}]  {_fmt(f)}")
    if sharpes:
        print(f"  Sharpe folds: mean={np.mean(sharpes):.3f} std={np.std(sharpes):.3f} "
              f"min={np.min(sharpes):.3f}  (buscar MESETA, no picos)")
        report["wf_sharpe_mean"] = float(np.mean(sharpes))
        report["wf_sharpe_std"] = float(np.std(sharpes))

    # --- Benchmarks (in-sample) ---
    bh = bench_buy_hold(is_df, args.initial)
    ma = bench_ma_cross(is_df, args.initial, fee_bps=args.fee_bps)
    report["benchmarks_in_sample"] = {"buy_hold": bh, "ma_cross": ma}
    print("\n== BENCHMARKS (in-sample) — la estrategia debe batirlos neto de costos ==")
    print("  buy&hold:", _fmt(bh))
    print("  ma_cross:", _fmt(ma))

    # --- Gate 5: OOS (una sola pasada) ---
    print("\n== OOS (2024+) — MIRAR UNA SOLA VEZ ==")
    if len(oos_df) > 300:
        oos_m = run_segment(oos_df, args.strategy, params, bt_kwargs)
        report["oos"] = oos_m
        print(" ", _fmt(oos_m))
        oos_bh = bench_buy_hold(oos_df, args.initial)
        report["oos_buy_hold"] = oos_bh
        print("  oos buy&hold:", _fmt(oos_bh))
        is_sharpe = is_m.get("sharpe")
        oos_sharpe = oos_m.get("sharpe")
        if is_sharpe is None or is_sharpe <= 0:
            # El ratio OOS/IS solo tiene sentido si el in-sample fue rentable.
            print("\n[Gate 5] IN-SAMPLE NO RENTABLE (Sharpe<=0): la estrategia no pasa; "
                  "el OOS es informativo pero no hay nada que validar.")
            report["gate5_verdict"] = "in_sample_not_profitable"
        elif oos_sharpe is not None:
            ratio = oos_sharpe / is_sharpe
            verdict = "SOSPECHA DE OVERFIT" if ratio < 0.5 else "consistente"
            print(f"\n[Gate 5] Sharpe OOS/IS = {ratio:.2f} -> {verdict} "
                  f"(<0.5 = descartar sin negociar)")
            report["gate5_oos_is_ratio"] = float(ratio)
            report["gate5_verdict"] = verdict
    else:
        print("  (OOS con muy pocas barras — extender el rango de datos)")

    # --- Deflated Sharpe Ratio (multiple-testing) sobre el in-sample ---
    is_rets = is_res.equity.pct_change().dropna().to_numpy()
    dsr = deflated_sharpe_ratio(is_rets, n_trials=args.deflated_sharpe)
    report["deflated_sharpe"] = dsr
    print(f"\n== DEFLATED SHARPE (n_trials={args.deflated_sharpe}) ==")
    print(f"  PSR vs 0 = {dsr.get('psr_vs_0')}  |  DSR = {dsr.get('dsr')}  "
          f"(Sharpe/periodo={dsr.get('sr_per_period')}, SR0={dsr.get('sr0_per_period')})")
    print("  Regla: DSR < 0.95 => el Sharpe NO se distingue del mejor de N intentos al azar.")

    # --- Monte Carlo de ruina sobre los trades del in-sample ---
    r_mults = [t.r_multiple for t in is_res.trades]
    if r_mults:
        mc = monte_carlo_ruin(r_mults, n_sims=args.mc_sims, risk_pct=args.risk_pct,
                              initial=args.initial, ruin_frac=0.5, seed=0)
        report["monte_carlo_ruin"] = mc
        print(f"\n== MONTE CARLO DE RUINA ({mc['sims']} sims, riesgo {args.risk_pct:.0%}/trade) ==")
        print(f"  P(equity cae a <= 50% del inicial) = {mc['prob_ruin']:.1%}")
        print(f"  retorno final  p5/p50/p95 = {mc['final_return_p5']} / {mc['final_return_p50']} "
              f"/ {mc['final_return_p95']}")
        print(f"  peor maxDD (p95) = {mc['max_drawdown_p95']}  |  maxDD mediano = {mc['max_drawdown_p50']}")

    # --- Tabla comparativa opcional ---
    if args.compare:
        print("\n== COMPARATIVA (Sharpe / PF / trades / retorno) ==")
        rows = []
        for strat in ("sweep", "donchian"):
            sp = DEFAULT_PARAMS[strat]
            mi = run_segment(is_df, strat, sp, bt_kwargs)
            mo = run_segment(oos_df, strat, sp, bt_kwargs) if len(oos_df) > 300 else {}
            rows.append((strat, mi, mo))
        print(f"  {'estrategia':>12} | {'IS sharpe':>9} {'IS pf':>6} {'IS ret':>7} | "
              f"{'OOS sharpe':>10} {'OOS ret':>8}")
        for name, mi, mo in rows:
            print(f"  {name:>12} | {str(mi.get('sharpe')):>9} {str(mi.get('profit_factor')):>6} "
                  f"{str(mi.get('total_return')):>7} | {str(mo.get('sharpe')):>10} "
                  f"{str(mo.get('total_return')):>8}")
        print(f"  {'buy&hold':>12} | {str(bh.get('sharpe')):>9} {'-':>6} "
              f"{str(bh.get('total_return')):>7} | {'-':>10} {'-':>8}")
        print(f"  {'ma_cross':>12} | {str(ma.get('sharpe')):>9} {'-':>6} "
              f"{str(ma.get('total_return')):>7} | {'-':>10} {'-':>8}")
        report["compare"] = {name: {"is": mi, "oos": mo} for name, mi, mo in rows}

    print("\nRECORDATORIO: cada variante de parametros probada cuenta para el multiple-testing.")
    print("Anotarla en crypto/REGLAS_CONGELADAS.md antes de mirar el OOS.")

    if args.out:
        with open(args.out, "w") as f:
            json.dump(report, f, indent=2, default=str)
        print(f"\nReporte JSON -> {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
