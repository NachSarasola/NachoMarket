"""Tests de la deteccion SMC — foco en CAUSALIDAD (anti look-ahead) y sweep."""

from __future__ import annotations

import numpy as np
import pandas as pd

from crypto.smc.signals import (
    add_atr,
    confirmed_swings,
    donchian_bms_signals,
    smc_sweep_signals,
    swing_mask,
)


def _mk_df(rows: list[tuple[float, float, float, float]]) -> pd.DataFrame:
    """rows = [(open, high, low, close), ...] con indice horario 4h UTC."""
    idx = pd.date_range("2021-01-01", periods=len(rows), freq="4h", tz="UTC")
    arr = np.array(rows, dtype=float)
    return pd.DataFrame(
        {
            "open": arr[:, 0],
            "high": arr[:, 1],
            "low": arr[:, 2],
            "close": arr[:, 3],
            "volume": np.ones(len(rows)),
        },
        index=idx,
    )


def _random_walk(n: int, seed: int = 7) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    steps = rng.normal(0, 1, n).cumsum()
    base = 100 + steps
    idx = pd.date_range("2021-01-01", periods=n, freq="4h", tz="UTC")
    high = base + rng.uniform(0.1, 1.5, n)
    low = base - rng.uniform(0.1, 1.5, n)
    open_ = base + rng.uniform(-0.5, 0.5, n)
    close = base + rng.uniform(-0.5, 0.5, n)
    high = np.maximum.reduce([high, open_, close])
    low = np.minimum.reduce([low, open_, close])
    return pd.DataFrame(
        {"open": open_, "high": high, "low": low, "close": close, "volume": np.ones(n)},
        index=idx,
    )


# --------------------------------------------------------------------------- #
# Causalidad: la piedra angular. Un signal en la barra t NO puede cambiar
# cuando se agregan barras futuras.
# --------------------------------------------------------------------------- #

def test_signals_are_causal_no_lookahead() -> None:
    df = _random_walk(400)
    full = smc_sweep_signals(df, lookback=20, use_pda_filter=True, require_equal=False)

    # Para varios cortes t, recomputar sobre df[:t+1] y exigir igualdad en la barra t.
    for t in (60, 120, 200, 333, 399):
        trunc = smc_sweep_signals(
            df.iloc[: t + 1], lookback=20, use_pda_filter=True, require_equal=False
        )
        for col in ("enter_long", "enter_short"):
            assert bool(full[col].iloc[t]) == bool(trunc[col].iloc[t]), (
                f"look-ahead en {col} barra {t}"
            )
        # sl_price idem (NaN==NaN se maneja aparte)
        a, b = full["sl_price"].iloc[t], trunc["sl_price"].iloc[t]
        assert (np.isnan(a) and np.isnan(b)) or np.isclose(a, b), f"sl_price look-ahead barra {t}"


def test_confirmed_swings_are_causal() -> None:
    df = _random_walk(300)
    ch_full, cl_full = confirmed_swings(df, left=2, right=2)
    for t in (50, 150, 250, 299):
        ch_t, cl_t = confirmed_swings(df.iloc[: t + 1], left=2, right=2)
        av, bv = ch_full.iloc[t], ch_t.iloc[t]
        assert (np.isnan(av) and np.isnan(bv)) or np.isclose(av, bv)
        av, bv = cl_full.iloc[t], cl_t.iloc[t]
        assert (np.isnan(av) and np.isnan(bv)) or np.isclose(av, bv)


def test_require_equal_variant_is_causal() -> None:
    df = _random_walk(300, seed=11)
    full = smc_sweep_signals(df, lookback=15, require_equal=True, equal_tol_atr=0.6)
    for t in (80, 180, 299):
        trunc = smc_sweep_signals(
            df.iloc[: t + 1], lookback=15, require_equal=True, equal_tol_atr=0.6
        )
        assert bool(full["enter_long"].iloc[t]) == bool(trunc["enter_long"].iloc[t])
        assert bool(full["enter_short"].iloc[t]) == bool(trunc["enter_short"].iloc[t])


# --------------------------------------------------------------------------- #
# swing_mask / fractales
# --------------------------------------------------------------------------- #

def test_swing_mask_finds_pivot() -> None:
    # high con un pico claro en el indice 4.
    highs = pd.Series([10, 11, 12, 13, 20, 13, 12, 11, 10], dtype=float)
    mask = swing_mask(highs, left=2, right=2, is_high=True)
    assert mask[4]
    assert not mask.any().tolist() or mask.sum() == 1  # solo un swing high


def test_swing_mask_finds_low() -> None:
    lows = pd.Series([20, 19, 18, 17, 5, 17, 18, 19, 20], dtype=float)
    mask = swing_mask(lows, left=2, right=2, is_high=False)
    assert mask[4]


# --------------------------------------------------------------------------- #
# Sweep / turtle soup: deteccion positiva
# --------------------------------------------------------------------------- #

def test_bearish_sweep_short_triggers() -> None:
    # Construir un rango con techo ~110, luego una barra que barre 112 pero cierra en 108.
    rows = []
    # 25 barras oscilando bajo 110 para fijar el Donchian high en 110.
    for i in range(25):
        h = 110.0 if i % 5 == 0 else 108.0
        rows.append((107.0, h, 105.0, 106.0))
    # barra de sweep: high 112 (barre), close 107 (cuerpo debajo de 110)
    rows.append((108.0, 112.0, 106.5, 107.0))
    df = _mk_df(rows)
    sig = smc_sweep_signals(
        df, lookback=20, use_pda_filter=False, require_equal=False, sl_cap_pct=0.10
    )
    assert bool(sig["enter_short"].iloc[-1]), "deberia disparar short en el sweep"
    # stop por encima de la mecha (112) + buffer
    assert sig["sl_price"].iloc[-1] > 112.0


def test_bullish_sweep_long_triggers() -> None:
    rows = []
    for i in range(25):
        low_v = 90.0 if i % 5 == 0 else 92.0
        rows.append((93.0, 95.0, low_v, 94.0))
    # sweep: low 88 (barre el 90), close 93 (cuerpo por encima de 90)
    rows.append((92.0, 94.0, 88.0, 93.0))
    df = _mk_df(rows)
    sig = smc_sweep_signals(
        df, lookback=20, use_pda_filter=False, require_equal=False, sl_cap_pct=0.10
    )
    assert bool(sig["enter_long"].iloc[-1]), "deberia disparar long en el sweep"
    assert sig["sl_price"].iloc[-1] < 88.0  # stop debajo de la mecha


def test_no_signal_when_body_breaks_through() -> None:
    # Ruptura real (cuerpo cierra por encima del nivel) NO es sweep -> sin short.
    rows = []
    for i in range(25):
        h = 110.0 if i % 5 == 0 else 108.0
        rows.append((107.0, h, 105.0, 106.0))
    rows.append((108.0, 113.0, 107.0, 112.0))  # cierra 112 > 110 = breakout, no sweep
    df = _mk_df(rows)
    sig = smc_sweep_signals(df, lookback=20, use_pda_filter=False, require_equal=False)
    assert not bool(sig["enter_short"].iloc[-1])


def test_stop_cap_filters_wide_stops() -> None:
    # Si la mecha del sweep es enorme, el stop excede el cap y la senal se descarta.
    rows = []
    for i in range(25):
        h = 110.0 if i % 5 == 0 else 108.0
        rows.append((107.0, h, 105.0, 106.0))
    rows.append((108.0, 140.0, 106.0, 107.0))  # mecha gigantesca -> stop > 4%
    df = _mk_df(rows)
    sig = smc_sweep_signals(
        df, lookback=20, use_pda_filter=False, require_equal=False, sl_cap_pct=0.04
    )
    assert not bool(sig["enter_short"].iloc[-1]), "stop demasiado ancho debe filtrar la senal"


# --------------------------------------------------------------------------- #
# ATR y control Donchian
# --------------------------------------------------------------------------- #

def test_atr_positive_and_causal() -> None:
    df = _random_walk(100)
    atr = add_atr(df, 14)
    assert atr.dropna().gt(0).all()
    # atr en t no cambia al agregar barras futuras
    atr_full = add_atr(df, 14)
    atr_trunc = add_atr(df.iloc[:60], 14)
    assert np.isclose(atr_full.iloc[59], atr_trunc.iloc[59])


def test_donchian_bms_breakout() -> None:
    rows = []
    for _ in range(25):
        rows.append((100.0, 101.0, 99.0, 100.0))  # rango plano ~101 techo
    rows.append((100.5, 105.0, 100.0, 104.0))  # cierra 104 > 101 = breakout long
    df = _mk_df(rows)
    sig = donchian_bms_signals(df, lookback=20, sl_cap_pct=0.10)
    assert bool(sig["enter_long"].iloc[-1])
    assert sig["sl_price"].iloc[-1] < 104.0
