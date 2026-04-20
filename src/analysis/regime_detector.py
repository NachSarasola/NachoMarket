"""Deteccion de regimen de mercado usando Hurst Exponent (TODO 1.3).

3 regimenes:
- MEAN_REVERTING: Hurst < 0.45 — spread normal, market making optimo
- TRENDING: Hurst > 0.55 — aumentar spread 2x, reducir size 50%
- VOLATILE: std rolling > threshold — pausar MM, solo arbitraje

El MarketMakerStrategy consulta el detector antes de emitir signals.
"""

import logging
import math
from collections import deque
from enum import Enum
from typing import NamedTuple

logger = logging.getLogger("nachomarket.regime")

# Thresholds del exponente de Hurst
_HURST_MEAN_REVERT_MAX = 0.45   # < 0.45 → mean reverting
_HURST_TRENDING_MIN = 0.55      # > 0.55 → trending
# Entre 0.45 y 0.55 → aleatorio (random walk)

_MIN_PRICES_FOR_REGIME = 20     # Minimo de precios para calcular regimen
_DEFAULT_WINDOW = 50            # Ventana de precios para el calculo
_VOLATILE_STD_THRESHOLD = 0.08  # std rolling > 8% → VOLATILE


class Regime(str, Enum):
    """Regimen de mercado detectado."""
    MEAN_REVERTING = "MEAN_REVERTING"  # Ideal para market making
    TRENDING = "TRENDING"              # Subir spreads, bajar size
    VOLATILE = "VOLATILE"              # Pausar MM
    UNKNOWN = "UNKNOWN"                # Insuficientes datos


class RegimeState(NamedTuple):
    """Estado completo del regimen para un mercado."""
    regime: Regime
    hurst: float          # Exponente de Hurst estimado
    rolling_std: float    # Desviacion estandar rolling
    n_prices: int         # Cantidad de precios usados
    spread_multiplier: float   # Factor para ajustar spread en MM
    size_multiplier: float     # Factor para ajustar size en MM
    should_pause_mm: bool      # True si hay que pausar market making


class MarketRegimeDetector:
    """Detecta el regimen de un mercado y sugiere ajustes de parametros.

    Mantiene un buffer deslizante de midprices por token_id y calcula
    el regimen en cada actualizacion.

    Uso:
        detector = MarketRegimeDetector()
        detector.update("token_abc", 0.52)
        state = detector.get_state("token_abc")
        if state.should_pause_mm:
            skip market making
    """

    def __init__(
        self,
        window: int = _DEFAULT_WINDOW,
        volatile_std_threshold: float = _VOLATILE_STD_THRESHOLD,
        hurst_mean_revert_max: float = _HURST_MEAN_REVERT_MAX,
        hurst_trending_min: float = _HURST_TRENDING_MIN,
    ) -> None:
        self._window = window
        self._volatile_threshold = volatile_std_threshold
        self._hurst_mr_max = hurst_mean_revert_max
        self._hurst_tr_min = hurst_trending_min
        # token_id → deque de midprices
        self._price_buffers: dict[str, deque[float]] = {}

    # ------------------------------------------------------------------
    # API publica
    # ------------------------------------------------------------------

    def update(self, token_id: str, midprice: float) -> None:
        """Registra un nuevo midprice para el token."""
        if token_id not in self._price_buffers:
            self._price_buffers[token_id] = deque(maxlen=self._window)
        if midprice > 0:
            self._price_buffers[token_id].append(midprice)

    def get_state(self, token_id: str) -> RegimeState:
        """Calcula y retorna el estado de regimen para un token.

        Si no hay suficientes precios, retorna UNKNOWN con multiplicadores 1.0.
        """
        prices = list(self._price_buffers.get(token_id, []))

        if len(prices) < _MIN_PRICES_FOR_REGIME:
            return RegimeState(
                regime=Regime.UNKNOWN,
                hurst=0.5,
                rolling_std=0.0,
                n_prices=len(prices),
                spread_multiplier=1.0,
                size_multiplier=1.0,
                should_pause_mm=False,
            )

        # 1. Rolling std (normalizada al precio medio)
        mean_p = sum(prices) / len(prices)
        std_p = _std(prices)
        rolling_std = std_p / mean_p if mean_p > 0 else 0.0

        # 2. Hurst exponent via R/S analysis
        hurst = _compute_hurst(prices)

        # 3. Clasificar regimen
        if rolling_std > self._volatile_threshold:
            regime = Regime.VOLATILE
        elif hurst < self._hurst_mr_max:
            regime = Regime.MEAN_REVERTING
        elif hurst > self._hurst_tr_min:
            regime = Regime.TRENDING
        else:
            regime = Regime.MEAN_REVERTING  # Random walk → tratar como MM normal

        # 4. Ajustes de parametros segun regimen
        if regime == Regime.VOLATILE:
            spread_mult = 3.0   # Spread 3x para reducir exposicion
            size_mult = 0.25    # Size 25%
            pause_mm = True
        elif regime == Regime.TRENDING:
            spread_mult = 2.0   # Spread 2x
            size_mult = 0.5     # Size 50%
            pause_mm = False
        else:  # MEAN_REVERTING o UNKNOWN
            spread_mult = 1.0
            size_mult = 1.0
            pause_mm = False

        return RegimeState(
            regime=regime,
            hurst=round(hurst, 4),
            rolling_std=round(rolling_std, 4),
            n_prices=len(prices),
            spread_multiplier=spread_mult,
            size_multiplier=size_mult,
            should_pause_mm=pause_mm,
        )

    def get_all_states(self) -> dict[str, RegimeState]:
        """Retorna estados de regimen para todos los tokens con datos."""
        return {tid: self.get_state(tid) for tid in self._price_buffers}

    def clear(self, token_id: str) -> None:
        """Limpia el buffer de precios de un token."""
        self._price_buffers.pop(token_id, None)


# ------------------------------------------------------------------
# Algoritmos de calculo
# ------------------------------------------------------------------

def _std(values: list[float]) -> float:
    """Desviacion estandar poblacional."""
    n = len(values)
    if n < 2:
        return 0.0
    mu = sum(values) / n
    return math.sqrt(sum((x - mu) ** 2 for x in values) / n)


def _compute_hurst(prices: list[float]) -> float:
    """Estima el exponente de Hurst via Rescaled Range (R/S) analysis.

    H < 0.5  → mean-reverting (antipersistente)
    H = 0.5  → random walk
    H > 0.5  → tendencia (persistente)

    Implementacion simplificada con multiples sub-ventanas.
    Retorna 0.5 si hay insuficientes datos o el calculo falla.
    """
    n = len(prices)
    if n < 8:
        return 0.5

    # Convertir a retornos logaritmicos
    try:
        log_returns = [
            math.log(prices[i] / prices[i - 1])
            for i in range(1, n)
            if prices[i] > 0 and prices[i - 1] > 0
        ]
    except (ValueError, ZeroDivisionError):
        return 0.5

    if len(log_returns) < 8:
        return 0.5

    # R/S para distintos tamaños de ventana
    rs_values = []
    ns_values = []

    # Sub-ventanas de distintos tamaños (potencias de 2)
    min_window = 4
    max_window = len(log_returns) // 2

    window = min_window
    while window <= max_window:
        rs_list = []
        # Calcular R/S en segmentos no solapantes de este tamaño
        for start in range(0, len(log_returns) - window + 1, window):
            segment = log_returns[start: start + window]
            rs = _rs_ratio(segment)
            if rs is not None and rs > 0:
                rs_list.append(rs)

        if rs_list:
            avg_rs = sum(rs_list) / len(rs_list)
            rs_values.append(math.log(avg_rs))
            ns_values.append(math.log(window))

        window = max(window + 2, int(window * 1.5))

    if len(rs_values) < 2:
        return 0.5

    # Regresion lineal log(R/S) ~ H * log(n)
    hurst = _linear_slope(ns_values, rs_values)
    # Clamp a rango razonable
    return max(0.01, min(0.99, hurst))


def _rs_ratio(segment: list[float]) -> float | None:
    """Calcula el ratio R/S para un segmento de retornos."""
    n = len(segment)
    if n < 2:
        return None

    mean = sum(segment) / n
    # Desviaciones acumuladas
    cum_dev = []
    cumsum = 0.0
    for r in segment:
        cumsum += r - mean
        cum_dev.append(cumsum)

    R = max(cum_dev) - min(cum_dev)
    S = _std(segment)

    if S == 0.0:
        return None

    return R / S


def _linear_slope(xs: list[float], ys: list[float]) -> float:
    """Pendiente de regresion lineal simple (OLS)."""
    n = len(xs)
    if n < 2:
        return 0.5
    mx = sum(xs) / n
    my = sum(ys) / n
    num = sum((xs[i] - mx) * (ys[i] - my) for i in range(n))
    den = sum((xs[i] - mx) ** 2 for i in range(n))
    if den == 0:
        return 0.5
    return num / den
