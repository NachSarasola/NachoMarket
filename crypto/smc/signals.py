"""Deteccion de senales SMC sobre OHLCV — pura, vectorizada y causal.

Implementa las definiciones EXACTAS del sistema del usuario (ebooks BEIG vol 1/2),
no las de una libreria generica:

- Swing/fractal de 5 velas (2 a cada lado), confirmado con ``right`` barras de delay.
- Sweep / turtle soup: la mecha barre un nivel de liquidez pero el CUERPO cierra de
  vuelta adentro (regla del libro "mecha = liquidity grab, cuerpo = ruptura").
- Filtro PDA premium/discount por rango.
- BMS/Donchian como estrategia de control (breakout con cierre de cuerpo).

CAUSALIDAD: cada fila de salida en la barra ``t`` depende solo de barras ``<= t``.
Los niveles de liquidez usan ``shift`` para excluir la barra actual y sus swings no
confirmados. Ver ``tests/test_smc_detection.py`` para las garantias verificadas.

Convenciones:
- El DataFrame de entrada tiene columnas ``open, high, low, close, volume`` y un
  indice temporal ascendente (UTC). No se modifica in-place salvo que se indique.
- ``enter_long`` / ``enter_short`` son la barra en cuyo CIERRE se dispara la entrada.
- ``sl_price`` es el stop inicial (nunca se mueve en contra). ``tp1/tp2`` los targets.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

__all__ = [
    "add_atr",
    "swing_mask",
    "confirmed_swings",
    "fair_value_gap",
    "regime_labels",
    "smc_sweep_signals",
    "donchian_bms_signals",
    "volume_zscore",
    "taker_buy_ratio",
    "ma_timing_signals",
    "flow_momentum_signals",
    "funding_extreme_signals",
]

# Ventanas de la etiqueta de regimen (instrumentacion OBSERVACIONAL: no filtra senales,
# solo etiqueta cada trade para el journal y los slices de validate.py — cero parametros
# de trading nuevos).
REGIME_TREND_BARS = 180   # 30 dias en velas de 4h
REGIME_VOL_BARS = 1095    # ~6 meses en velas de 4h (mediana de referencia de ATR%)


def regime_labels(df: pd.DataFrame, atr_period: int = 14) -> pd.Series:
    """Etiqueta causal de regimen por barra: tendencia x volatilidad -> 4 estados.

    - Tendencia: ``up`` si close > SMA(REGIME_TREND_BARS), ``dn`` si no.
    - Volatilidad: ``hi`` si ATR%%(=ATR/close) > su mediana rolling de REGIME_VOL_BARS,
      ``lo`` si no.

    Devuelve strings en {"up_hi","up_lo","dn_hi","dn_lo"} (vacio si aun no hay ventana).
    Solo usa barras <= t (rolling trailing) -> causal.
    """
    close = df["close"].astype(float)
    atr = add_atr(df, atr_period)
    sma = close.rolling(REGIME_TREND_BARS).mean()
    atr_pct = atr / close
    vol_ref = atr_pct.rolling(REGIME_VOL_BARS, min_periods=REGIME_TREND_BARS).median()

    trend = np.where(close > sma, "up", "dn")
    vol = np.where(atr_pct > vol_ref, "hi", "lo")
    labels = pd.Series(
        [f"{t}_{v}" for t, v in zip(trend, vol)], index=df.index, dtype=object
    )
    # Sin ventana suficiente -> etiqueta vacia (no inventar regimen al arranque).
    labels[sma.isna() | vol_ref.isna()] = ""
    return labels.rename("regime")


def add_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """Average True Range (Wilder) causal.

    TR[t] usa high[t], low[t] y close[t-1], todos conocidos al cierre de ``t``.
    El suavizado de Wilder es un EMA con alpha=1/period. La serie resultante en la
    barra ``t`` no usa informacion futura.
    """
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    prev_close = df["close"].astype(float).shift(1)

    tr = pd.concat(
        [
            high - low,
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)

    # Wilder smoothing: EMA con alpha = 1/period (min_periods=period evita valores
    # inestables al arranque).
    atr = tr.ewm(alpha=1.0 / period, adjust=False, min_periods=period).mean()
    return atr.rename("atr")


def swing_mask(series: pd.Series, left: int, right: int, is_high: bool) -> np.ndarray:
    """Mascara booleana de puntos swing (fractales).

    Un swing high en ``i`` cumple: high[i] > high[i-k] para k=1..left, y
    high[i] >= high[i+k] para k=1..right. El uso de ``>`` a la izquierda y ``>=`` a
    la derecha rompe empates en mesetas de forma determinista (marca el extremo
    izquierdo del plateau) y evita marcar dos veces la misma zona plana.

    NOTA: esta mascara usa barras futuras (``right``); NO es causal por si sola. Se
    usa solo como paso intermedio de ``confirmed_swings``, que aplica el delay.
    """
    vals = series.to_numpy(dtype=float)
    n = vals.shape[0]
    if n == 0:
        return np.zeros(0, dtype=bool)

    mask = np.ones(n, dtype=bool)
    neg_inf = -np.inf if is_high else np.inf

    for k in range(1, left + 1):
        shifted = np.concatenate([np.full(k, neg_inf), vals[:-k]])  # vals[i-k]
        mask &= (vals > shifted) if is_high else (vals < shifted)
    for k in range(1, right + 1):
        shifted = np.concatenate([vals[k:], np.full(k, neg_inf)])  # vals[i+k]
        mask &= (vals >= shifted) if is_high else (vals <= shifted)

    # Bordes sin ventana completa no pueden ser swings validos.
    mask[:left] = False
    if right > 0:
        mask[n - right:] = False
    return mask


def confirmed_swings(
    df: pd.DataFrame, left: int = 2, right: int = 2
) -> tuple[pd.Series, pd.Series]:
    """Niveles de swing high/low CONFIRMADOS y disponibles de forma causal.

    Un swing en la barra ``i`` recien se conoce en ``i + right`` (cuando se vieron las
    ``right`` velas a la derecha). Se desplaza el precio del swing por ``right`` barras
    y se hace forward-fill: en cada barra ``t``, la serie devuelve el precio del ultimo
    swing high/low YA confirmado a esa altura.

    Returns:
        (conf_high, conf_low): dos Series alineadas al indice de ``df``.
    """
    high = df["high"]
    low = df["low"]

    sh = swing_mask(high, left, right, is_high=True)
    sl = swing_mask(low, left, right, is_high=False)

    sh_price = pd.Series(np.where(sh, high.to_numpy(dtype=float), np.nan), index=df.index)
    sl_price = pd.Series(np.where(sl, low.to_numpy(dtype=float), np.nan), index=df.index)

    conf_high = sh_price.shift(right).ffill().rename("conf_swing_high")
    conf_low = sl_price.shift(right).ffill().rename("conf_swing_low")
    return conf_high, conf_low


def volume_zscore(df: pd.DataFrame, window: int = 90) -> pd.Series:
    """Z-score causal del volumen vs su media rolling (clímax de volumen = z alto).

    z[t] = (vol[t] - mean(vol[t-window..t-1])) / std(...): la barra actual se compara
    contra la historia PREVIA (shift(1) → causal estricto, la propia barra no infla su media).
    """
    vol = df["volume"].astype(float)
    hist = vol.shift(1)
    mu = hist.rolling(window).mean()
    sd = hist.rolling(window).std()
    z = (vol - mu) / sd.replace(0.0, np.nan)
    return z.rename("volume_z")


def taker_buy_ratio(df: pd.DataFrame, smooth: int = 1) -> pd.Series:
    """Proporción de volumen agresor comprador (proxy de order flow, causal).

    Requiere columna ``taker_buy_volume`` (klines de Binance, campo gratuito con historia
    completa). ratio[t] = taker_buy[t] / volume[t] ∈ [0,1]; 0.5 = flujo neutro; <0.4 =
    agresión vendedora fuerte (interesante como agotamiento en un sweep de mínimos).
    ``smooth`` >1 aplica media rolling causal. Devuelve NaN si falta la columna.
    """
    if "taker_buy_volume" not in df.columns:
        return pd.Series(np.nan, index=df.index, name="taker_buy_ratio")
    vol = df["volume"].astype(float).replace(0.0, np.nan)
    ratio = df["taker_buy_volume"].astype(float) / vol
    ratio = ratio.clip(0.0, 1.0)
    if smooth > 1:
        ratio = ratio.rolling(smooth).mean()
    return ratio.rename("taker_buy_ratio")


def fair_value_gap(df: pd.DataFrame) -> pd.DataFrame:
    """Detecta Fair Value Gaps (imbalances de 3 velas) de forma causal.

    Definicion aritmetica del vol 1 (BISI/SIBI), evaluada en la 3ra vela ``t`` (usa solo
    t, t-1, t-2 -> causal):
      - FVG alcista (BISI): high[t-2] < low[t]  -> hueco de compra; gap=[high[t-2], low[t]].
      - FVG bajista (SIBI): low[t-2] > high[t]  -> hueco de venta;  gap=[high[t], low[t-2]].

    NOTA DE DISEÑO: esta primitiva NO se cablea al baseline congelado ``smc_sweep_signals``
    (v1). El FVG solo tiene edge documentado como PULLBACK en direccion de tendencia (no
    como filtro del sweep), y acoplarlo agregaria parametros libres sin fundamento -> queda
    disponible y testeada para un futuro setup ``fvg_pullback`` SOLO si el nucleo valida.

    Returns:
        DataFrame con columnas: bull_fvg, bear_fvg (bool), fvg_top, fvg_bottom (float|NaN).
    """
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    h2 = high.shift(2)
    l2 = low.shift(2)

    bull = (h2 < low).fillna(False)
    bear = (l2 > high).fillna(False)

    out = pd.DataFrame(index=df.index)
    out["bull_fvg"] = bull
    out["bear_fvg"] = bear
    top = pd.Series(np.nan, index=df.index)
    bot = pd.Series(np.nan, index=df.index)
    top = top.mask(bull, low)     # techo del hueco alcista = low[t]
    bot = bot.mask(bull, h2)      # piso  del hueco alcista = high[t-2]
    top = top.mask(bear, l2)      # techo del hueco bajista = low[t-2]
    bot = bot.mask(bear, high)    # piso  del hueco bajista = high[t]
    out["fvg_top"] = top
    out["fvg_bottom"] = bot
    return out


def _equal_level_count(
    swing_points: pd.Series, level: pd.Series, tol: pd.Series, window: int
) -> pd.Series:
    """Cuenta swings (puntos, no ffill) dentro de +-tol del ``level`` en la ventana.

    ``swing_points`` es una serie sparse (precio del swing en su barra de confirmacion,
    NaN en el resto). Para cada barra ``t`` cuenta cuantos swings confirmados dentro de
    las ultimas ``window`` barras caen dentro de ``[level-tol, level+tol]``. Sirve para
    exigir "equal highs/lows" (>=2) como refinamiento del nivel barrido.
    """
    pts = swing_points.to_numpy(dtype=float)
    lvl = level.to_numpy(dtype=float)
    tolv = tol.to_numpy(dtype=float)
    n = pts.shape[0]
    out = np.zeros(n, dtype=float)

    # Indices donde hay un swing confirmado (punto no NaN).
    valid_idx = np.flatnonzero(~np.isnan(pts))
    if valid_idx.size == 0:
        return pd.Series(out, index=swing_points.index)

    vp = pts[valid_idx]
    for t in range(n):
        if np.isnan(lvl[t]) or np.isnan(tolv[t]):
            continue
        lo_bound = t - window + 1
        # swings con indice en [lo_bound, t]
        sel = valid_idx[(valid_idx >= lo_bound) & (valid_idx <= t)]
        if sel.size == 0:
            continue
        prices = pts[sel]
        out[t] = np.count_nonzero(np.abs(prices - lvl[t]) <= tolv[t])
    return pd.Series(out, index=swing_points.index)


def smc_sweep_signals(
    df: pd.DataFrame,
    lookback: int = 20,
    swing_left: int = 2,
    swing_right: int = 2,
    atr_period: int = 14,
    sl_buffer_atr: float = 0.5,
    sl_cap_pct: float = 0.04,
    require_equal: bool = False,
    equal_tol_atr: float = 0.5,
    pda_lookback: int = 60,
    use_pda_filter: bool = True,
    time_stop_bars: int = 12,
    single_target: bool = True,
) -> pd.DataFrame:
    """Genera senales de sweep-reversal (turtle soup) sobre velas cerradas.

    Setup SHORT (bearish sweep de buy-side liquidity):
        1. Nivel BSL = maximo de las ``lookback`` barras PREVIAS (Donchian high con
           shift(1) -> excluye la barra actual, causal).
        2. La barra actual barre el nivel con la mecha (high > nivel) pero cierra el
           CUERPO por debajo (close < nivel) -> falso breakout.
        3. (opcional) filtro PDA: solo si el precio esta en zona premium del rango.
        4. (opcional) require_equal: >=2 swing highs confirmados cerca del nivel (EQH).
        5. Stop = high de la barra + sl_buffer_atr*ATR; si el stop resultante implica
           un riesgo > ``sl_cap_pct`` del precio de entrada, se DESCARTA la senal (no se
           acerca el stop a la fuerza — eso seria mover el stop, prohibido por el libro).
        6. Target = medio del rango (TP1). Con ``single_target=True`` (default, y el modo
           de v1 para paridad exacta con la estrategia freqtrade) TP2 = TP1: salida unica,
           sin parciales. Con ``single_target=False`` TP2 = extremo opuesto y el backtester
           puede modelar TP1 parcial + breakeven (solo investigacion).
    Setup LONG: espejo exacto.

    Todas las columnas de salida en la barra ``t`` son causales.

    Returns:
        DataFrame con columnas: enter_long, enter_short, sl_price, tp1_price,
        tp2_price, entry_price, atr, bsl_level, ssl_level, pda_mid, enter_tag.
    """
    out = pd.DataFrame(index=df.index)
    close = df["close"].astype(float)
    high = df["high"].astype(float)
    low = df["low"].astype(float)

    atr = add_atr(df, atr_period)

    # Niveles de liquidez del rango previo (causal via shift(1)).
    bsl = high.shift(1).rolling(lookback).max().rename("bsl_level")  # buy-side liq (arriba)
    ssl = low.shift(1).rolling(lookback).min().rename("ssl_level")   # sell-side liq (abajo)

    # Sweep: mecha perfora, cuerpo cierra de vuelta adentro.
    sweep_up = (high > bsl) & (close < bsl)     # barrido de highs -> SHORT
    sweep_dn = (low < ssl) & (close > ssl)      # barrido de lows  -> LONG

    # Filtro PDA premium/discount (rango previo de pda_lookback barras).
    pda_hh = high.shift(1).rolling(pda_lookback).max()
    pda_ll = low.shift(1).rolling(pda_lookback).min()
    pda_mid = ((pda_hh + pda_ll) / 2.0).rename("pda_mid")
    if use_pda_filter:
        short_ctx = close > pda_mid   # premium -> habilita ventas
        long_ctx = close < pda_mid    # discount -> habilita compras
    else:
        short_ctx = pd.Series(True, index=df.index)
        long_ctx = pd.Series(True, index=df.index)

    # Refinamiento EQH/EQL (equal highs/lows cerca del nivel barrido).
    if require_equal:
        sh_mask = swing_mask(high, swing_left, swing_right, is_high=True)
        sl_mask = swing_mask(low, swing_left, swing_right, is_high=False)
        sh_pts = pd.Series(
            np.where(sh_mask, high.to_numpy(dtype=float), np.nan), index=df.index
        ).shift(swing_right)
        sl_pts = pd.Series(
            np.where(sl_mask, low.to_numpy(dtype=float), np.nan), index=df.index
        ).shift(swing_right)
        tol = (atr * equal_tol_atr)
        eqh_ok = _equal_level_count(sh_pts, bsl, tol, lookback) >= 2
        eql_ok = _equal_level_count(sl_pts, ssl, tol, lookback) >= 2
    else:
        eqh_ok = pd.Series(True, index=df.index)
        eql_ok = pd.Series(True, index=df.index)

    raw_short = sweep_up.fillna(False) & short_ctx.fillna(False) & eqh_ok
    raw_long = sweep_dn.fillna(False) & long_ctx.fillna(False) & eql_ok

    # Stops (nunca se mueven en contra; distancia acotada por sl_cap_pct).
    short_sl = high + sl_buffer_atr * atr
    long_sl = low - sl_buffer_atr * atr

    short_risk = (short_sl - close) / close
    long_risk = (close - long_sl) / close

    # Descartar senales cuyo stop natural excede el cap de riesgo (demasiado ancho).
    short_ok = raw_short & (short_risk <= sl_cap_pct) & (short_risk > 0)
    long_ok = raw_long & (long_risk <= sl_cap_pct) & (long_risk > 0)

    # Evitar tomar ambos lados en la misma barra (senal ambigua -> ninguna).
    both = short_ok & long_ok
    short_ok = short_ok & ~both
    long_ok = long_ok & ~both

    out["enter_long"] = long_ok.fillna(False)
    out["enter_short"] = short_ok.fillna(False)
    out["entry_price"] = close
    out["atr"] = atr
    out["bsl_level"] = bsl
    out["ssl_level"] = ssl
    out["pda_mid"] = pda_mid

    # Stops y targets solo donde hay senal (NaN en el resto).
    sl_price = pd.Series(np.nan, index=df.index)
    tp1 = pd.Series(np.nan, index=df.index)
    tp2 = pd.Series(np.nan, index=df.index)

    range_mid = (bsl + ssl) / 2.0
    sl_price = sl_price.mask(out["enter_short"], short_sl)
    sl_price = sl_price.mask(out["enter_long"], long_sl)
    # Short: apunta hacia abajo (mid del rango, luego SSL). Long: hacia arriba.
    tp1 = tp1.mask(out["enter_short"], range_mid)
    tp1 = tp1.mask(out["enter_long"], range_mid)
    if single_target:
        # Salida unica en el medio del rango (paridad con freqtrade v1).
        tp2 = tp1.copy()
    else:
        tp2 = tp2.mask(out["enter_short"], ssl)
        tp2 = tp2.mask(out["enter_long"], bsl)

    out["sl_price"] = sl_price
    out["tp1_price"] = tp1
    out["tp2_price"] = tp2

    tag = pd.Series("", index=df.index, dtype=object)
    tag = tag.mask(out["enter_short"], "smc_sweep_short")
    tag = tag.mask(out["enter_long"], "smc_sweep_long")
    out["enter_tag"] = tag
    out["time_stop_bars"] = time_stop_bars
    out["regime"] = regime_labels(df, atr_period)  # observacional (journal/slices)
    return out


def ma_timing_signals(
    df: pd.DataFrame,
    window: int = 100,
    atr_period: int = 14,
    sl_atr: float = 3.0,
) -> pd.DataFrame:
    """H1 — MA-timing long/flat (Detzel et al. 2021): tendencia LENTA en barras diarias.

    Evidencia: ratios precio/MA(5-100d) baten buy&hold en Sharpe y drawdown (Financial
    Management 2021); señales lentas > rapidas (Risk Management 2026). Nuestro propio GATE 1
    mostro que el benchmark ma_cross domino todo y que operar long bajo la media destruye.

    Reglas (long/flat, pensado para velas 1D):
      - Entrar long al cierre que CRUZA por encima de SMA(window).
      - Salir (exit_long, fill al open siguiente) al cierre que cruza por debajo.
      - Stop protector inicial: close - sl_atr*ATR (paracaidas, no gestion fina).
      - Sin target ni time-stop: la salida es la señal contraria.
    Parametros libres: window, sl_atr (2). El vol-targeting se aplica como overlay del
    backtester (vol_target_annual), no aqui.
    """
    out = pd.DataFrame(index=df.index)
    close = df["close"].astype(float)
    atr = add_atr(df, atr_period)
    sma = close.rolling(window).mean()

    above = (close > sma).astype(bool)
    # shift(fill_value=False) preserva dtype bool: con .fillna(False) pandas 3 devuelve
    # object y ahi `~` es negacion BITWISE de int (~True == -2, truthy) -> cruces falsos.
    prev_above = above.shift(1, fill_value=False)
    cross_up = above & ~prev_above
    cross_dn = ~above & prev_above & sma.notna()

    out["enter_long"] = cross_up.fillna(False)
    out["enter_short"] = False
    out["exit_long"] = cross_dn.fillna(False)
    out["exit_short"] = False
    out["entry_price"] = close
    out["atr"] = atr

    sl_price = pd.Series(np.nan, index=df.index)
    sl_price = sl_price.mask(out["enter_long"], close - sl_atr * atr)
    out["sl_price"] = sl_price
    out["tp1_price"] = np.nan  # sin target: sale por señal contraria (o stop)
    out["tp2_price"] = np.nan
    out["time_stop_bars"] = np.inf

    tag = pd.Series("", index=df.index, dtype=object)
    tag = tag.mask(out["enter_long"], "ma_timing_long")
    out["enter_tag"] = tag
    out["regime"] = regime_labels(df, atr_period)
    return out


def _hysteresis_state(entry_raw: pd.Series, exit_raw: pd.Series) -> pd.Series:
    """Máquina de estado long/flat con histéresis: entra con entry, sale con exit.

    Compartida por flow y funding (una sola implementación → sin divergencias). O(n).
    """
    n = len(entry_raw)
    state = np.zeros(n, dtype=bool)
    prev = False
    e = entry_raw.to_numpy(dtype=bool)
    x = exit_raw.to_numpy(dtype=bool)
    for i in range(n):
        if not prev and e[i]:
            prev = True
        elif prev and x[i]:
            prev = False
        state[i] = prev
    return pd.Series(state, index=entry_raw.index)


def funding_extreme_signals(
    df: pd.DataFrame,
    enter_pct: float = 0.02,
    exit_pct: float = 0.50,
    lookback: int = 1095,
    atr_period: int = 14,
    sl_atr: float = 3.0,
) -> pd.DataFrame:
    """H3a — funding extremo contrarian, long/flat (spec CONGELADA en HIPOTESIS.md).

    Evidencia: BIS WP1087/Management Science — el funding es el precio del apalancamiento;
    su cola NEGATIVA extrema marca capitulación de longs (posicionamiento lavado) y precede
    rebotes a semanas (estimaciones de practicantes: funding <-5% anualizado → +19% a 30d).

    Reglas (long/flat sobre barras de precio con columna ``funding_rate`` ya adjunta):
      - Entrar long cuando funding <= su cuantil ``enter_pct`` rolling (ventana ``lookback``
        de barras, umbral calculado sobre historia PREVIA → causal).
      - Salir cuando funding >= su cuantil ``exit_pct`` (histéresis).
      - Stop paracaídas close − sl_atr·ATR. Sin target/time-stop.
    Parámetros libres: enter_pct, exit_pct, lookback, sl_atr (4).
    Sin columna ``funding_rate`` → ninguna señal. La columna se adjunta con
    ``validate.py --funding <csv>`` (ffill del último funding conocido → causal-conservador).
    """
    out = pd.DataFrame(index=df.index)
    close = df["close"].astype(float)
    atr = add_atr(df, atr_period)

    if "funding_rate" not in df.columns:
        fr = pd.Series(np.nan, index=df.index)
    else:
        fr = df["funding_rate"].astype(float)

    hist = fr.shift(1)  # umbrales sobre historia previa: causal
    q_lo = hist.rolling(lookback, min_periods=lookback // 2).quantile(enter_pct)
    q_mid = hist.rolling(lookback, min_periods=lookback // 2).quantile(exit_pct)

    entry_raw = (fr <= q_lo).fillna(False)
    exit_raw = (fr >= q_mid).fillna(False)

    state_s = _hysteresis_state(entry_raw, exit_raw)
    prev_state = state_s.shift(1, fill_value=False)

    out["enter_long"] = (state_s & ~prev_state)
    out["enter_short"] = False
    out["exit_long"] = (~state_s & prev_state)
    out["exit_short"] = False
    out["entry_price"] = close
    out["atr"] = atr

    sl_price = pd.Series(np.nan, index=df.index)
    sl_price = sl_price.mask(out["enter_long"], close - sl_atr * atr)
    out["sl_price"] = sl_price
    out["tp1_price"] = np.nan
    out["tp2_price"] = np.nan
    out["time_stop_bars"] = np.inf

    tag = pd.Series("", index=df.index, dtype=object)
    tag = tag.mask(out["enter_long"], "funding_extreme_long")
    out["enter_tag"] = tag
    out["regime"] = regime_labels(df, atr_period)
    return out


def flow_momentum_signals(
    df: pd.DataFrame,
    flow_window: int = 6,
    quantile_lookback: int = 360,
    enter_q: float = 0.80,
    exit_q: float = 0.50,
    atr_period: int = 14,
    sl_atr: float = 3.0,
) -> pd.DataFrame:
    """H2 — momentum de order flow (taker imbalance), long/flat.

    Evidencia: el order flow agregado predice retornos de BTC/ETH a 1d-1sem (+0.2%/dia por
    +1σ; Journal of Financial Markets 2026). Dato gratis: ``taker_buy_volume`` de las klines
    (fetch_data --with-flow).

    Reglas (long/flat):
      - flow = media rolling de ``taker_buy_ratio`` sobre ``flow_window`` barras.
      - Entrar long cuando flow supera su cuantil ``enter_q`` rolling (ventana
        ``quantile_lookback``, calculada sobre historia PREVIA -> causal).
      - Salir cuando flow cae bajo su cuantil ``exit_q`` (histeresis para no churnear).
      - Stop protector: close - sl_atr*ATR. Sin target/time-stop.
    Parametros libres: flow_window, enter_q, exit_q, quantile_lookback, sl_atr (5, al tope
    del presupuesto). Sin columna ``taker_buy_volume`` -> ninguna señal (frame vacio).
    """
    out = pd.DataFrame(index=df.index)
    close = df["close"].astype(float)
    atr = add_atr(df, atr_period)

    ratio = taker_buy_ratio(df, smooth=flow_window)
    hist = ratio.shift(1)  # cuantiles sobre historia previa: causal
    q_hi = hist.rolling(quantile_lookback).quantile(enter_q)
    q_lo = hist.rolling(quantile_lookback).quantile(exit_q)

    entry_raw = (ratio > q_hi).fillna(False)
    exit_raw = (ratio < q_lo).fillna(False)

    state_s = _hysteresis_state(entry_raw, exit_raw)
    prev_state = state_s.shift(1, fill_value=False)  # bool dtype preservado (ver ma_timing)

    out["enter_long"] = (state_s & ~prev_state)
    out["enter_short"] = False
    out["exit_long"] = (~state_s & prev_state)
    out["exit_short"] = False
    out["entry_price"] = close
    out["atr"] = atr

    sl_price = pd.Series(np.nan, index=df.index)
    sl_price = sl_price.mask(out["enter_long"], close - sl_atr * atr)
    out["sl_price"] = sl_price
    out["tp1_price"] = np.nan
    out["tp2_price"] = np.nan
    out["time_stop_bars"] = np.inf

    tag = pd.Series("", index=df.index, dtype=object)
    tag = tag.mask(out["enter_long"], "flow_momo_long")
    out["enter_tag"] = tag
    out["regime"] = regime_labels(df, atr_period)
    return out


def donchian_bms_signals(
    df: pd.DataFrame,
    lookback: int = 20,
    atr_period: int = 14,
    sl_buffer_atr: float = 1.5,
    sl_cap_pct: float = 0.06,
    tp_r_multiple: float = 2.0,
    time_stop_bars: int = 18,
) -> pd.DataFrame:
    """Estrategia de CONTROL: BMS/breakout Donchian con cierre de cuerpo.

    Es el "break in market structure" del libro operado como continuacion, y equivale
    a un breakout de canal Donchian (trend following con evidencia academica). Sirve de
    benchmark: si ``smc_sweep_signals`` no lo bate out-of-sample neto de costos, se
    descarta el sweep y se conserva este.

    LONG: close cierra por encima del maximo de las ``lookback`` barras previas.
    SHORT: close cierra por debajo del minimo de las ``lookback`` barras previas.
    Stop ATR-based; target por multiplo R.
    """
    out = pd.DataFrame(index=df.index)
    close = df["close"].astype(float)
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    atr = add_atr(df, atr_period)

    donchian_hi = high.shift(1).rolling(lookback).max()
    donchian_lo = low.shift(1).rolling(lookback).min()

    long_break = (close > donchian_hi).fillna(False)
    short_break = (close < donchian_lo).fillna(False)

    long_sl = close - sl_buffer_atr * atr
    short_sl = close + sl_buffer_atr * atr

    long_risk = (close - long_sl) / close
    short_risk = (short_sl - close) / close

    long_ok = long_break & (long_risk <= sl_cap_pct) & (long_risk > 0)
    short_ok = short_break & (short_risk <= sl_cap_pct) & (short_risk > 0)
    both = long_ok & short_ok
    long_ok = long_ok & ~both
    short_ok = short_ok & ~both

    out["enter_long"] = long_ok.fillna(False)
    out["enter_short"] = short_ok.fillna(False)
    out["entry_price"] = close
    out["atr"] = atr

    sl_price = pd.Series(np.nan, index=df.index)
    sl_price = sl_price.mask(out["enter_long"], long_sl)
    sl_price = sl_price.mask(out["enter_short"], short_sl)
    out["sl_price"] = sl_price

    tp = pd.Series(np.nan, index=df.index)
    tp = tp.mask(out["enter_long"], close + tp_r_multiple * (close - long_sl))
    tp = tp.mask(out["enter_short"], close - tp_r_multiple * (short_sl - close))
    out["tp1_price"] = tp
    out["tp2_price"] = tp

    tag = pd.Series("", index=df.index, dtype=object)
    tag = tag.mask(out["enter_long"], "donchian_bms_long")
    tag = tag.mask(out["enter_short"], "donchian_bms_short")
    out["enter_tag"] = tag
    out["time_stop_bars"] = time_stop_bars
    out["regime"] = regime_labels(df, atr_period)  # observacional (journal/slices)
    return out
