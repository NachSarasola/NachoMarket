"""Tests de las features de flujo (F0) y la inferencia de timeframe."""

from __future__ import annotations

import numpy as np
import pandas as pd

from crypto.scripts.validate import infer_bars_per_year
from crypto.smc.signals import taker_buy_ratio, volume_zscore


def _df(n: int = 300, freq: str = "4h", vol: list | None = None,
        taker: list | None = None) -> pd.DataFrame:
    idx = pd.date_range("2021-01-01", periods=n, freq=freq, tz="UTC")
    base = 100 + np.arange(n) * 0.01
    d = pd.DataFrame({
        "open": base, "high": base + 1, "low": base - 1, "close": base,
        "volume": vol if vol is not None else np.full(n, 100.0),
    }, index=idx)
    if taker is not None:
        d["taker_buy_volume"] = taker
    return d


def test_volume_zscore_detects_climax() -> None:
    rng = np.random.default_rng(7)
    vol = rng.normal(100, 10, 299).clip(50).tolist() + [1000.0]  # clímax en la última barra
    z = volume_zscore(_df(vol=vol), window=90)
    assert z.iloc[-1] > 5, f"clímax no detectado: z={z.iloc[-1]}"
    assert abs(z.iloc[150]) < 3, "una barra normal no debe marcar clímax"


def test_volume_zscore_is_causal() -> None:
    rng = np.random.default_rng(4)
    vol = rng.uniform(50, 200, 400).tolist()
    df = _df(n=400, vol=vol)
    full = volume_zscore(df, window=90)
    for t in (150, 250, 399):
        trunc = volume_zscore(df.iloc[: t + 1], window=90)
        a, b = full.iloc[t], trunc.iloc[t]
        assert (np.isnan(a) and np.isnan(b)) or np.isclose(a, b)


def test_taker_buy_ratio_basic() -> None:
    vol = [100.0] * 10
    taker = [80.0] * 5 + [20.0] * 5  # compra agresiva -> venta agresiva
    r = taker_buy_ratio(_df(n=10, vol=vol, taker=taker))
    assert np.isclose(r.iloc[0], 0.8)
    assert np.isclose(r.iloc[-1], 0.2)


def test_taker_buy_ratio_missing_column_is_nan() -> None:
    r = taker_buy_ratio(_df(n=10))
    assert r.isna().all()


def test_infer_bars_per_year() -> None:
    assert abs(infer_bars_per_year(_df(freq="4h")) - 2190) < 1
    assert abs(infer_bars_per_year(_df(freq="1h")) - 8760) < 1
    assert abs(infer_bars_per_year(_df(freq="1D")) - 365) < 1
