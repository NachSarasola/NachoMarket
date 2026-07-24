"""Tests de la etiqueta de regimen (observacional: nunca filtra senales)."""

from __future__ import annotations

import numpy as np
import pandas as pd

from crypto.smc.backtest import run_backtest
from crypto.smc.signals import regime_labels, smc_sweep_signals
from crypto.smc.synthetic import random_walk_ohlcv, sweep_market_ohlcv


def test_regime_labels_values_and_shape() -> None:
    df = random_walk_ohlcv(n=1500, seed=2)
    labels = regime_labels(df)
    assert len(labels) == len(df)
    valid = {"", "up_hi", "up_lo", "dn_hi", "dn_lo"}
    assert set(labels.unique()).issubset(valid)
    # Tras la ventana de warm-up tiene que haber etiquetas no vacias.
    assert (labels.iloc[400:] != "").any()


def test_regime_labels_are_causal() -> None:
    df = random_walk_ohlcv(n=800, seed=9)
    full = regime_labels(df)
    for t in (300, 500, 799):
        trunc = regime_labels(df.iloc[: t + 1])
        assert full.iloc[t] == trunc.iloc[t], f"look-ahead en regime barra {t}"


def test_regime_does_not_gate_signals() -> None:
    # La columna regime NO debe alterar enter_long/enter_short (es observacional).
    df = sweep_market_ohlcv(seed=3)
    sig = smc_sweep_signals(df)
    assert "regime" in sig.columns
    sig_no_regime = sig.drop(columns=["regime"])
    res_a = run_backtest(df, sig, fee_bps=0, slippage_bps=0)
    res_b = run_backtest(df, sig_no_regime, fee_bps=0, slippage_bps=0)
    assert len(res_a.trades) == len(res_b.trades)
    assert np.isclose(res_a.equity.iloc[-1], res_b.equity.iloc[-1])


def test_trades_carry_regime_tag() -> None:
    df = sweep_market_ohlcv(seed=4)
    sig = smc_sweep_signals(df)
    res = run_backtest(df, sig, fee_bps=0, slippage_bps=0)
    assert res.trades, "el control positivo debe generar trades"
    # Todo trade tiene un regime valido (posiblemente vacio al inicio de la serie).
    valid = {"", "up_hi", "up_lo", "dn_hi", "dn_lo"}
    assert all(t.regime in valid for t in res.trades)
    # Y al menos algunos con etiqueta no vacia si la serie es larga.
    if len(df) > 1300:
        assert any(t.regime for t in res.trades)
