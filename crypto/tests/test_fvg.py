"""Tests de la primitiva Fair Value Gap (causal, no cableada al baseline v1)."""

from __future__ import annotations

import numpy as np
import pandas as pd

from crypto.smc.signals import fair_value_gap


def _mk(rows: list[tuple[float, float, float, float]]) -> pd.DataFrame:
    idx = pd.date_range("2021-01-01", periods=len(rows), freq="4h", tz="UTC")
    a = np.array(rows, dtype=float)
    return pd.DataFrame(
        {"open": a[:, 0], "high": a[:, 1], "low": a[:, 2], "close": a[:, 3],
         "volume": np.ones(len(rows))},
        index=idx,
    )


def test_bullish_fvg_detected() -> None:
    # high[t-2]=101 < low[t]=103 => FVG alcista en la 3ra vela.
    df = _mk([
        (100, 101, 99, 100),   # t-2: high 101
        (101, 105, 100, 104),  # t-1: vela de impulso
        (104, 106, 103, 105),  # t:   low 103 > 101 => gap
    ])
    fvg = fair_value_gap(df)
    assert bool(fvg["bull_fvg"].iloc[-1])
    assert not bool(fvg["bear_fvg"].iloc[-1])
    assert np.isclose(fvg["fvg_bottom"].iloc[-1], 101.0)
    assert np.isclose(fvg["fvg_top"].iloc[-1], 103.0)


def test_bearish_fvg_detected() -> None:
    # low[t-2]=99 > high[t]=97 => FVG bajista.
    df = _mk([
        (100, 101, 99, 100),   # t-2: low 99
        (99, 100, 95, 96),     # t-1: impulso bajista
        (96, 97, 94, 95),      # t:   high 97 < 99 => gap
    ])
    fvg = fair_value_gap(df)
    assert bool(fvg["bear_fvg"].iloc[-1])
    assert not bool(fvg["bull_fvg"].iloc[-1])
    assert np.isclose(fvg["fvg_top"].iloc[-1], 99.0)
    assert np.isclose(fvg["fvg_bottom"].iloc[-1], 97.0)


def test_no_fvg_when_no_gap() -> None:
    df = _mk([
        (100, 102, 99, 101),
        (101, 103, 100, 102),
        (102, 104, 101, 103),  # low 101 < high[t-2] 102 => sin gap alcista
    ])
    fvg = fair_value_gap(df)
    assert not bool(fvg["bull_fvg"].iloc[-1])
    assert not bool(fvg["bear_fvg"].iloc[-1])


def test_fvg_is_causal() -> None:
    rng = np.random.default_rng(5)
    n = 200
    base = 100 + rng.normal(0, 1, n).cumsum()
    df = _mk([(b, b + 1.5, b - 1.5, b + 0.3) for b in base])
    full = fair_value_gap(df)
    for t in (50, 120, 199):
        trunc = fair_value_gap(df.iloc[: t + 1])
        assert bool(full["bull_fvg"].iloc[t]) == bool(trunc["bull_fvg"].iloc[t])
        assert bool(full["bear_fvg"].iloc[t]) == bool(trunc["bear_fvg"].iloc[t])
