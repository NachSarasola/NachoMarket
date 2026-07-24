"""Tests de H1 (MA-timing) y H2 (flow momentum) + infra nueva del backtester.

Cubre: causalidad, mecánica de cruce/histéresis, exit-por-señal (fill al open siguiente),
vol-targeting (solo reduce), y controles positivo/negativo de cada hipótesis.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from crypto.smc.backtest import run_backtest
from crypto.smc.signals import flow_momentum_signals, ma_timing_signals
from crypto.smc.synthetic import flow_market_ohlcv, random_walk_ohlcv, trend_market_ohlcv

BT0 = dict(fee_bps=10, slippage_bps=5, risk_pct=0.01, initial_equity=500, direction="long")


def _mk(rows):
    idx = pd.date_range("2021-01-01", periods=len(rows), freq="1D", tz="UTC")
    a = np.array(rows, dtype=float)
    return pd.DataFrame({"open": a[:, 0], "high": a[:, 1], "low": a[:, 2],
                         "close": a[:, 3], "volume": np.ones(len(rows))}, index=idx)


# ---------------------------------------------------------------- H1 mecánica

def test_ma_timing_cross_up_and_down() -> None:
    # 30 barras descendiendo suave (close < SMA sin ambiguedad), salto a 120 (cross up),
    # caida a 85 (cross down) -> exactamente un enter y al menos un exit.
    rows = [(110 - i * 0.3, 111 - i * 0.3, 108 - i * 0.3, 110 - i * 0.3) for i in range(30)]
    rows += [(120, 121, 119, 120)] * 5 + [(85, 86, 84, 85)] * 5
    df = _mk(rows)
    sig = ma_timing_signals(df, window=20, sl_atr=10.0)
    assert sig["enter_long"].sum() == 1
    assert sig["exit_long"].sum() >= 1
    # el enter ocurre en la primera barra que cruza la SMA
    assert sig.index[sig["enter_long"]][0] == df.index[30]


def test_ma_timing_is_causal() -> None:
    df = trend_market_ohlcv(n_legs=4, seed=9)
    full = ma_timing_signals(df, window=100)
    for t in (500, 900, len(df) - 1):
        trunc = ma_timing_signals(df.iloc[: t + 1], window=100)
        assert bool(full["enter_long"].iloc[t]) == bool(trunc["enter_long"].iloc[t])
        assert bool(full["exit_long"].iloc[t]) == bool(trunc["exit_long"].iloc[t])


def test_ma_timing_positive_control_trend_market() -> None:
    df = trend_market_ohlcv(seed=5)
    sig = ma_timing_signals(df, window=100)
    res = run_backtest(df, sig, bars_per_year=2190, **BT0)
    m = res.metrics()
    assert m["trades"] >= 5
    assert m["total_return"] > 0, f"MA-timing debe capturar tendencias largas: {m}"


# ---------------------------------------------------------------- H2 mecánica

def test_flow_no_column_no_signals() -> None:
    df = random_walk_ohlcv(n=800, seed=2)
    sig = flow_momentum_signals(df)
    assert sig["enter_long"].sum() == 0


def test_flow_is_causal() -> None:
    df = flow_market_ohlcv(n=2000, seed=6)
    full = flow_momentum_signals(df)
    for t in (900, 1500, 1999):
        trunc = flow_momentum_signals(df.iloc[: t + 1])
        assert bool(full["enter_long"].iloc[t]) == bool(trunc["enter_long"].iloc[t])
        assert bool(full["exit_long"].iloc[t]) == bool(trunc["exit_long"].iloc[t])


def test_flow_positive_vs_uninformative_control() -> None:
    bt = dict(BT0, bars_per_year=2190)
    pos = flow_market_ohlcv(n=6000, seed=6, informative=True)
    neg = flow_market_ohlcv(n=6000, seed=6, informative=False)
    m_pos = run_backtest(pos, flow_momentum_signals(pos), **bt).metrics()
    m_neg = run_backtest(neg, flow_momentum_signals(neg), **bt).metrics()
    assert m_pos["trades"] >= 10
    assert m_pos["total_return"] > 0, f"flow informativo debe ganar: {m_pos}"
    # El control no-informativo debe rendir claramente peor que el informativo.
    assert m_pos["total_return"] > m_neg["total_return"], (m_pos, m_neg)


# ------------------------------------------------------- infra: exit por señal

def test_signal_exit_fills_next_open() -> None:
    # enter señal en barra 1 (fill open b2=100); exit señal en barra 4 -> fill open b5=111.
    rows = [
        (100, 101, 99, 100),   # 0
        (100, 101, 99, 100),   # 1 <- enter señal
        (100, 106, 99, 105),   # 2 fill entrada a 100
        (105, 109, 104, 108),  # 3
        (108, 112, 107, 110),  # 4 <- exit señal (al cierre)
        (111, 113, 110, 112),  # 5 fill salida al open=111
        (112, 113, 111, 112),  # 6
    ]
    df = _mk(rows)
    n = len(df)
    sig = pd.DataFrame({
        "enter_long": [False, True, False, False, False, False, False],
        "enter_short": [False] * n,
        "exit_long": [False, False, False, False, True, False, False],
        "exit_short": [False] * n,
        "sl_price": [np.nan, 90.0, np.nan, np.nan, np.nan, np.nan, np.nan],
        "tp1_price": [np.nan] * n,
        "tp2_price": [np.nan] * n,
        "entry_price": df["close"],
        "enter_tag": [""] * n,
        "time_stop_bars": [np.inf] * n,
    }, index=df.index)
    res = run_backtest(df, sig, fee_bps=0, slippage_bps=0, risk_pct=0.02, initial_equity=1000)
    assert len(res.trades) == 1
    t = res.trades[0]
    assert t.exit_reason == "signal"
    assert np.isclose(t.entry_price, 100.0)
    assert np.isclose(t.exit_price, 111.0), f"debe salir al open siguiente: {t.exit_price}"
    assert t.pnl > 0


# ------------------------------------------------------- infra: vol targeting

def test_vol_target_only_reduces_size() -> None:
    df = trend_market_ohlcv(n_legs=4, seed=5)
    sig = ma_timing_signals(df, window=100)
    base = run_backtest(df, sig, bars_per_year=2190, **BT0)
    tgt = run_backtest(df, sig, bars_per_year=2190, vol_target_annual=0.05, **BT0)
    assert len(base.trades) == len(tgt.trades), "el overlay no debe cambiar las señales"
    # Con target de vol muy bajo, las cantidades son menores o iguales en todos los trades.
    for b, t in zip(base.trades, tgt.trades):
        assert t.qty <= b.qty + 1e-12
    assert any(t.qty < b.qty for b, t in zip(base.trades, tgt.trades)), "debe achicar en vol alta"
