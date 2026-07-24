"""Tests del review semanal (determinista): TODO_NORMAL / ALERTA / KILL."""

from __future__ import annotations

import importlib.util
import pathlib

from crypto.smc.report import TradeRow

_W = pathlib.Path(__file__).resolve().parents[1] / "scripts" / "weekly_review.py"
_spec = importlib.util.spec_from_file_location("weekly_review", _W)
wr = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(wr)


def _tr(pnl: float, regime: str = "up_lo", reason: str = "tp2") -> TradeRow:
    return TradeRow("t0", "t1", "long", "smc_sweep_long", regime, 100.0, 101.0, 1.0,
                    pnl, pnl / 5.0, reason)


def _baseline(winrate=0.6, max_streak=8, mc_dd_p95=-0.5) -> dict:
    return {
        "in_sample": {"win_rate": winrate, "max_loss_streak": max_streak},
        "monte_carlo_ruin": {"max_drawdown_p95": mc_dd_p95},
    }


def test_all_normal_within_cone() -> None:
    # 14 wins / 6 losses, sin rachas largas, drawdown chico vs p95 generoso.
    trades = []
    for _ in range(7):
        trades += [_tr(10), _tr(10), _tr(-5)]  # 21 trades, streaks de 1
    r = wr.review(trades, _baseline(), 500.0)
    assert r["status"] == "TODO_NORMAL", (r["alerts"], r["kills"])


def test_kill_on_drawdown() -> None:
    # 40 perdidas -> equity 500->300 = -40% maxDD, peor que 2x el p95 (-0.10).
    trades = [_tr(-5) for _ in range(40)]
    r = wr.review(trades, _baseline(max_streak=100, mc_dd_p95=-0.10), 500.0)
    assert r["status"] == "KILL"
    assert any("maxDD" in k for k in r["kills"])


def test_kill_on_streak() -> None:
    # Racha de 12 perdidas, backtest esperaba 5 -> KILL por racha. p95 generoso.
    trades = [_tr(10)] * 5 + [_tr(-1)] * 12
    r = wr.review(trades, _baseline(max_streak=5, mc_dd_p95=-0.9), 100000.0)
    assert r["status"] == "KILL"
    assert any("racha" in k for k in r["kills"])


def test_alert_on_winrate_decay() -> None:
    # Winrate 0.3 < 70% del backtest (0.6), pero sin racha larga ni drawdown fuerte.
    trades = []
    for _ in range(10):
        trades += [_tr(10), _tr(-3), _tr(-3)]  # 30 trades, winrate 1/3, streaks de 2
    r = wr.review(trades, _baseline(winrate=0.6, max_streak=8, mc_dd_p95=-0.5), 100000.0)
    assert r["status"] == "ALERTA"
    assert any("winrate" in a for a in r["alerts"])


def test_no_baseline_is_lenient() -> None:
    trades = [_tr(10), _tr(-5), _tr(10)]
    r = wr.review(trades, None, 500.0)
    assert r["status"] == "TODO_NORMAL"
    assert r["trades"] == 3
