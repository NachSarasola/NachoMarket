"""Tests de H3a (funding extremo contrarian): causalidad, histéresis y controles."""

from __future__ import annotations

import numpy as np

from crypto.smc.backtest import run_backtest
from crypto.smc.signals import funding_extreme_signals
from crypto.smc.synthetic import funding_market_ohlcv, random_walk_ohlcv

BT = dict(fee_bps=10, slippage_bps=5, risk_pct=0.01, initial_equity=500,
          direction="long", bars_per_year=2190)
PARAMS = dict(enter_pct=0.02, exit_pct=0.50, lookback=1095, sl_atr=3.0)


def test_no_funding_column_no_signals() -> None:
    df = random_walk_ohlcv(n=2000, seed=3)
    sig = funding_extreme_signals(df, **PARAMS)
    assert sig["enter_long"].sum() == 0


def test_funding_signals_are_causal() -> None:
    df = funding_market_ohlcv(n=4000, seed=8)
    full = funding_extreme_signals(df, **PARAMS)
    for t in (2000, 3000, 3999):
        trunc = funding_extreme_signals(df.iloc[: t + 1], **PARAMS)
        assert bool(full["enter_long"].iloc[t]) == bool(trunc["enter_long"].iloc[t])
        assert bool(full["exit_long"].iloc[t]) == bool(trunc["exit_long"].iloc[t])


def test_hysteresis_one_entry_per_episode() -> None:
    df = funding_market_ohlcv(n=8000, seed=8)
    sig = funding_extreme_signals(df, **PARAMS)
    n_entries = int(sig["enter_long"].sum())
    # ~10 episodios de capitulacion en 8000 barras: las entradas deben ser del mismo
    # orden (una por episodio detectable), no cientos (churn).
    assert 3 <= n_entries <= 30, f"entradas={n_entries}"


def test_positive_control_informative_funding() -> None:
    df = funding_market_ohlcv(n=8000, seed=8, informative=True)
    m = run_backtest(df, funding_extreme_signals(df, **PARAMS), **BT).metrics()
    assert m["trades"] >= 3
    assert m["total_return"] > 0, f"con rebote post-capitulacion debe ganar: {m}"


def test_uninformative_control_underperforms() -> None:
    pos = funding_market_ohlcv(n=8000, seed=8, informative=True)
    neg = funding_market_ohlcv(n=8000, seed=8, informative=False)
    m_pos = run_backtest(pos, funding_extreme_signals(pos, **PARAMS), **BT).metrics()
    m_neg = run_backtest(neg, funding_extreme_signals(neg, **PARAMS), **BT).metrics()
    assert m_pos["total_return"] > m_neg["total_return"], (m_pos, m_neg)
