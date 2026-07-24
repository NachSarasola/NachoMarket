#!/usr/bin/env python3
"""Revision semanal automatizada (el ritual de TESIS.md, sin depender de disciplina).

Compara el journal de trades VIVO contra el cono del backtest (Monte Carlo + métricas de
baseline) y emite un veredicto: TODO_NORMAL / ALERTA(...) / KILL(...). Los criterios de KILL
son los "criterios de muerte" de TESIS.md, escritos de antemano.

Uso:
    # journal propio (formato de validate --trades-out o del backtester):
    python crypto/scripts/weekly_review.py --journal journal.csv --baseline report.json
    # export de freqtrade (mapea columnas al vuelo):
    python crypto/scripts/weekly_review.py --freqtrade-csv trades.csv --baseline report.json

No toca parámetros ni datos: solo lee el journal y el baseline. La decisión es determinista.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys

sys.path.insert(0, __file__.rsplit("/crypto/", 1)[0])

from crypto.smc.report import TradeRow, load_trades_csv, loss_streaks, slice_report  # noqa: E402

# Umbrales (coinciden con los criterios de muerte de TESIS.md).
DD_KILL_MULT = 2.0        # maxDD realizado peor que 2x el p95 del Monte Carlo -> KILL
WINRATE_ALERT_FRAC = 0.7  # winrate vivo < 70% del backtest -> ALERTA (edge decayendo)
STREAK_KILL_EXTRA = 3     # racha de perdidas > baseline + 3 (y > 1.5x) -> KILL


def _trailing_loss_streak(trades: list) -> int:
    """Racha de perdidas consecutivas contando desde el ultimo trade hacia atras."""
    n = 0
    for t in reversed(trades):
        if t.pnl < 0:
            n += 1
        else:
            break
    return n


def _equity_maxdd(trades: list, initial: float) -> tuple[float, float]:
    """Reconstruye equity aplicando pnl en orden; devuelve (equity_final, maxDD)."""
    eq = initial
    peak = initial
    max_dd = 0.0
    for t in trades:
        eq += t.pnl
        peak = max(peak, eq)
        if peak > 0:
            max_dd = min(max_dd, (eq - peak) / peak)
    return eq, max_dd


def from_freqtrade_csv(path: str) -> list[TradeRow]:
    """Mapea un export de trades de freqtrade a nuestro TradeRow (best-effort).

    Columnas de freqtrade tipicas: pair, open_date, close_date, open_rate, close_rate,
    amount, profit_abs, profit_ratio, exit_reason, is_short, enter_tag. r_multiple se
    aproxima con profit_ratio (no es R real; suficiente para tendencia). regime queda vacio.
    """
    rows: list[TradeRow] = []
    with open(path, newline="") as f:
        for d in csv.DictReader(f):
            rows.append(TradeRow(
                entry_time=d.get("open_date", d.get("open_timestamp", "")),
                exit_time=d.get("close_date", d.get("close_timestamp", "")),
                side="short" if str(d.get("is_short", "")).lower() in ("true", "1") else "long",
                tag=d.get("enter_tag", d.get("enter_reason", "")),
                regime="",
                entry_price=float(d.get("open_rate", 0) or 0),
                exit_price=float(d.get("close_rate", 0) or 0),
                qty=float(d.get("amount", 0) or 0),
                pnl=float(d.get("profit_abs", 0) or 0),
                r_multiple=float(d.get("profit_ratio", 0) or 0),
                exit_reason=d.get("exit_reason", ""),
            ))
    return rows


def review(trades: list, baseline: dict | None, initial: float) -> dict:
    """Nucleo determinista de la revision. Devuelve stats + status + reasons."""
    n = len(trades)
    wins = sum(1 for t in trades if t.pnl > 0)
    winrate = wins / n if n else 0.0
    streaks = loss_streaks(trades)
    cur_streak = _trailing_loss_streak(trades)
    max_streak = max(streaks) if streaks else 0
    eq_final, max_dd = _equity_maxdd(trades, initial)

    alerts: list[str] = []
    kills: list[str] = []

    ins = (baseline or {}).get("in_sample", {}) or {}
    mc = (baseline or {}).get("monte_carlo_ruin", {}) or {}
    base_winrate = ins.get("win_rate")
    base_max_streak = ins.get("max_loss_streak")
    mc_dd_p95 = mc.get("max_drawdown_p95")  # p95 (peor 5%) del Monte Carlo, negativo

    # 1) Drawdown vs cono del Monte Carlo.
    if mc_dd_p95 is not None:
        if max_dd < DD_KILL_MULT * mc_dd_p95:
            kills.append(f"maxDD {max_dd:.2%} peor que {DD_KILL_MULT}x el p95 MC ({mc_dd_p95:.2%})")
        elif max_dd < mc_dd_p95:
            alerts.append(f"maxDD {max_dd:.2%} peor que el p95 MC ({mc_dd_p95:.2%})")

    # 2) Racha de perdidas vs backtest.
    if base_max_streak is not None:
        if max_streak > base_max_streak + STREAK_KILL_EXTRA and max_streak > 1.5 * base_max_streak:
            kills.append(f"racha {max_streak} >> backtest ({base_max_streak})")
        elif max_streak > base_max_streak:
            alerts.append(f"racha {max_streak} > backtest ({base_max_streak})")

    # 3) Winrate vs backtest (edge decayendo).
    if base_winrate:
        if winrate < WINRATE_ALERT_FRAC * base_winrate:
            alerts.append(f"winrate {winrate:.2f} < 70% del backtest ({base_winrate:.2f})")

    status = "KILL" if kills else ("ALERTA" if alerts else "TODO_NORMAL")
    return {
        "trades": n,
        "winrate": round(winrate, 3),
        "cur_loss_streak": cur_streak,
        "max_loss_streak": max_streak,
        "equity_final": round(eq_final, 2),
        "max_drawdown": round(max_dd, 4),
        "slices": slice_report(trades) if trades else {},
        "status": status,
        "alerts": alerts,
        "kills": kills,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--journal", help="CSV formato validate --trades-out")
    src.add_argument("--freqtrade-csv", help="export de trades de freqtrade")
    ap.add_argument("--baseline", help="report.json de validate (con Monte Carlo)")
    ap.add_argument("--initial", type=float, default=500.0)
    args = ap.parse_args()

    trades = load_trades_csv(args.journal) if args.journal else from_freqtrade_csv(args.freqtrade_csv)
    baseline = None
    if args.baseline:
        with open(args.baseline) as f:
            baseline = json.load(f)

    r = review(trades, baseline, args.initial)
    print(f"Trades: {r['trades']} | winrate: {r['winrate']} | racha actual: {r['cur_loss_streak']} "
          f"(max {r['max_loss_streak']}) | equity: {r['equity_final']} | maxDD: {r['max_drawdown']:.2%}")
    if r["slices"]:
        print("\nPor regimen:")
        for k, v in r["slices"].get("por_regimen", {}).items():
            print(f"  {k:>10}: n={v['n']:>4} winrate={v['winrate']:<6} pnl={v['pnl']:<10} avgR={v['avg_r']}")
        print("Por salida:")
        for k, v in r["slices"].get("por_salida", {}).items():
            print(f"  {k:>10}: n={v['n']:>4} winrate={v['winrate']:<6} pnl={v['pnl']:<10} avgR={v['avg_r']}")
    for a in r["alerts"]:
        print(f"  ⚠️  ALERTA: {a}")
    for k in r["kills"]:
        print(f"  🛑 KILL: {k}")
    print(f"\n>>> {r['status']} <<<")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
