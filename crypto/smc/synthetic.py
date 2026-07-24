"""Generadores de OHLCV sinteticos para VERIFICAR la estrategia (no para evidencia de edge).

Dos controles complementarios:

- ``random_walk_ohlcv``: GBM sin microestructura. La estrategia de sweep NO tiene nada que
  capturar -> debe PERDER neto de costos. Control NEGATIVO (el backtester no miente).
- ``sweep_market_ohlcv``: inyecta el fenomeno que la estrategia persigue (barrido de
  liquidez bajo un minimo previo + reversion, i.e. clustering de stops de Osler). La
  estrategia DEBE ser rentable. Control POSITIVO (el detector no esta roto ni muerto).

Que la misma logica gane en uno y pierda en el otro es la prueba de que detecta la senal
correcta y no ruido — el complemento honesto de un backtest que "siempre gana".
"""

from __future__ import annotations

import numpy as np
import pandas as pd


def _index(n: int) -> pd.DatetimeIndex:
    return pd.date_range("2019-01-01", periods=n, freq="4h", tz="UTC")


def random_walk_ohlcv(n: int = 5000, seed: int = 1, vol: float = 0.01) -> pd.DataFrame:
    """GBM con velas OHLC coherentes. Control NEGATIVO (sin edge)."""
    rng = np.random.default_rng(seed)
    rets = rng.normal(0.0, vol, n)
    close = 100.0 * np.exp(np.cumsum(rets))
    open_ = np.empty(n)
    open_[0] = close[0]
    open_[1:] = close[:-1]
    wick = vol * close
    high = np.maximum(open_, close) + rng.uniform(0.05, 1.0, n) * wick
    low = np.minimum(open_, close) - rng.uniform(0.05, 1.0, n) * wick
    return pd.DataFrame(
        {"open": open_, "high": high, "low": low, "close": close, "volume": np.ones(n)},
        index=_index(n),
    )


def trend_market_ohlcv(n_legs: int = 12, seed: int = 5) -> pd.DataFrame:
    """Control POSITIVO de H1 (MA-timing): tramos de tendencia larga + chop intercalado.

    Piernas alcistas fuertes (drift positivo sostenido ~400 barras), bajistas y laterales.
    Un MA-timing lento DEBE capturar las alcistas y quedarse flat en lo demas.
    """
    rng = np.random.default_rng(seed)
    rets: list[float] = []
    kinds = ["up", "chop", "down", "chop"]
    for leg in range(n_legs):
        kind = kinds[leg % len(kinds)]
        length = int(rng.integers(300, 500))
        drift = {"up": 0.004, "down": -0.003, "chop": 0.0}[kind]
        vol = {"up": 0.012, "down": 0.015, "chop": 0.008}[kind]
        rets.extend(rng.normal(drift, vol, length).tolist())
    r = np.array(rets)
    close = 100.0 * np.exp(np.cumsum(r))
    n = len(close)
    open_ = np.empty(n)
    open_[0] = close[0]
    open_[1:] = close[:-1]
    wick = np.abs(r) * close + 0.2
    high = np.maximum(open_, close) + rng.uniform(0.1, 0.8, n) * wick
    low = np.minimum(open_, close) - rng.uniform(0.1, 0.8, n) * wick
    return pd.DataFrame(
        {"open": open_, "high": high, "low": low, "close": close, "volume": np.ones(n)},
        index=_index(n),
    )


def flow_market_ohlcv(n: int = 6000, seed: int = 6, informative: bool = True) -> pd.DataFrame:
    """Control de H2 (taker imbalance): episodios de flujo comprador que PRECEDEN drift.

    Con ``informative=True``, un estado latente 'bull' eleva el taker_buy_ratio (~0.62) y el
    drift de las barras SIGUIENTES: el flujo anticipa el retorno (como en el paper de JFM).
    Con ``informative=False``, el ratio es ruido sin relacion con el drift -> control negativo
    (la estrategia de flow no debe ganar).
    """
    rng = np.random.default_rng(seed)
    state = np.zeros(n, dtype=bool)
    s = False
    for i in range(n):
        # cambia de estado con prob baja -> episodios persistentes (~150 barras)
        if rng.random() < 1 / 150:
            s = not s
        state[i] = s

    drift = np.where(state, 0.0035, -0.0005)
    rets = rng.normal(drift, 0.010)
    close = 100.0 * np.exp(np.cumsum(rets))
    open_ = np.empty(n)
    open_[0] = close[0]
    open_[1:] = close[:-1]
    wick = 0.010 * close
    high = np.maximum(open_, close) + rng.uniform(0.1, 0.9, n) * wick
    low = np.minimum(open_, close) - rng.uniform(0.1, 0.9, n) * wick

    volume = rng.uniform(80, 120, n)
    if informative:
        ratio = np.where(state, rng.normal(0.62, 0.04, n), rng.normal(0.50, 0.04, n))
    else:
        ratio = rng.normal(0.50, 0.06, n)
    taker = np.clip(ratio, 0.0, 1.0) * volume

    return pd.DataFrame(
        {"open": open_, "high": high, "low": low, "close": close,
         "volume": volume, "taker_buy_volume": taker},
        index=_index(n),
    )


def sweep_market_ohlcv(
    n_events: int = 60,
    range_bars: int = 26,
    revert_bars: int = 6,
    band: float = 6.0,
    win_prob: float = 0.70,
    seed: int = 3,
) -> pd.DataFrame:
    """Serie con barridos de sell-side liquidity + reversion (control POSITIVO).

    Cada evento: (1) una consolidacion de ``range_bars`` velas en una banda [lo, hi];
    (2) una vela que barre por debajo de ``lo`` con la mecha pero CIERRA de vuelta arriba
    (el trigger long del detector); (3) con prob ``win_prob`` el precio revierte hasta el
    medio de la banda (target -> ganancia); con prob ``1-win_prob`` sigue cayendo y toca el
    stop (perdida). Ruido leve en todo. La banda deriva suavemente entre eventos.

    Parametrizado para que el long-only sea claramente rentable neto de costos, sin ser
    degenerado (incluye perdidas reales).
    """
    rng = np.random.default_rng(seed)
    opens: list[float] = []
    highs: list[float] = []
    lows: list[float] = []
    closes: list[float] = []

    anchor = 100.0     # nivel medio de largo plazo
    band_lo = anchor   # low de la banda actual (random walk con reversion a la media)
    for _ in range(n_events):
        lo = band_lo
        hi = band_lo + band
        mid = (lo + hi) / 2.0
        noise = band * 0.04

        # (1) Consolidacion dentro de [lo, hi].
        prev_c = mid
        for i in range(range_bars):
            c = mid + (band / 2.5) * np.sin(i / 2.0) + rng.normal(0, noise)
            c = float(np.clip(c, lo + noise, hi - noise))
            o = prev_c
            h = max(o, c) + abs(rng.normal(0, noise))
            l = min(o, c) - abs(rng.normal(0, noise))
            h = min(h, hi + noise)  # no romper la banda por arriba durante la consolidacion
            l = max(l, lo + noise * 0.2)  # ni por abajo (el minimo lo hace el sweep)
            opens.append(o); highs.append(h); lows.append(l); closes.append(c)
            prev_c = c

        # (2) Vela de sweep: mecha por debajo de lo, cuerpo cierra arriba de lo.
        depth = band * rng.uniform(0.12, 0.22)
        o = prev_c
        sweep_low = lo - depth
        c = lo + band * 0.06  # cuerpo cierra de vuelta adentro (discount)
        h = max(o, c) + noise
        opens.append(o); highs.append(h); lows.append(sweep_low); closes.append(c)
        prev_c = c
        entry = c

        # (3) Reversion (win) o continuacion bajista (loss).
        win = rng.random() < win_prob
        if win:
            target = mid + band * 0.05
            path = np.linspace(entry, target + rng.uniform(0, band * 0.15), revert_bars)
        else:
            # cae por debajo del sweep_low para tocar el stop (~ sweep_low - 0.5*ATR)
            path = np.linspace(entry, sweep_low - band * 0.15, revert_bars)
        for c in path:
            o = prev_c
            cc = float(c + rng.normal(0, noise))
            h = max(o, cc) + abs(rng.normal(0, noise))
            l = min(o, cc) - abs(rng.normal(0, noise))
            opens.append(o); highs.append(h); lows.append(l); closes.append(cc)
            prev_c = cc

        # Banda del proximo evento: random walk MEAN-REVERTING alrededor de ``anchor``
        # (evita una tendencia compuesta que haga que buy&hold domine el demo).
        band_lo = band_lo + rng.normal(0, band * 0.2) - 0.15 * (band_lo - anchor)
        band_lo = max(band_lo, 20.0)

    n = len(closes)
    return pd.DataFrame(
        {
            "open": np.array(opens),
            "high": np.array(highs),
            "low": np.array(lows),
            "close": np.array(closes),
            "volume": np.ones(n),
        },
        index=_index(n),
    )
