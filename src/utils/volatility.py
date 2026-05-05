"""Volatilidad multi-ventana para mercados de Polymarket.

Calcula volatilidad anualizada usando log-returns en 8 timeframes:
  1h, 3h, 6h, 12h, 24h, 7d, 14d, 30d.

Formula:
  r_i = ln(P_i / P_{i-1})
  sigma_raw = std(r_i)
  sigma_annual = sigma_raw * sqrt(8760 / timeframe_hours)

Donde 8760 = 365 * 24 (horas en un anio).
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from typing import Any

_HOURS_PER_YEAR = 8760.0

_DEFAULT_TIMEFRAMES = [1, 3, 6, 12, 24, 168, 336, 720]


def _log_returns(prices: Sequence[float]) -> list[float]:
    """Calcula log-returns entre precios consecutivos."""
    if len(prices) < 2:
        return []
    returns: list[float] = []
    for i in range(1, len(prices)):
        if prices[i - 1] <= 0 or prices[i] <= 0:
            continue
        returns.append(math.log(prices[i] / prices[i - 1]))
    return returns


def _std(values: Sequence[float]) -> float:
    """Desviacion estandar poblacional."""
    n = len(values)
    if n < 2:
        return 0.0
    mean = sum(values) / n
    variance = sum((v - mean) ** 2 for v in values) / n
    return math.sqrt(variance)


def _annualize(sigma_raw: float, timeframe_hours: float) -> float:
    """Anualiza la volatilidad."""
    if timeframe_hours <= 0:
        return 0.0
    return sigma_raw * math.sqrt(_HOURS_PER_YEAR / timeframe_hours)


class VolatilityCalculator:
    """Calculador de volatilidad multi-ventana.

    No depende del SDK: recibe precios como listas de floats.
    El caller (main.py o markets.py) es responsable de obtener
    los precios via GET /prices-history del CLOB.
    """

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        cfg = (config or {}).get("volatility", {})
        self._timeframes: list[int] = cfg.get("timeframes", list(_DEFAULT_TIMEFRAMES))
        self._max_3h = cfg.get("max_volatility_3h", 0.80)

    def calculate(
        self, prices: Sequence[float], timeframe_hours: float
    ) -> float:
        """Calcula volatilidad anualizada para un timeframe.

        Args:
            prices: Serie de precios (orden cronologico).
            timeframe_hours: Duracion total de la serie en horas.

        Returns:
            Volatilidad anualizada (0.0 - inf). 0.0 si datos insuficientes.
        """
        returns = _log_returns(prices)
        if not returns:
            return 0.0
        sigma_raw = _std(returns)
        return _annualize(sigma_raw, timeframe_hours)

    def calculate_all(self, prices: Sequence[float]) -> dict[str, float]:
        """Calcula volatilidad para todos los timeframes configurados.

        Usa los ultimos N precios de la serie segun la granularidad esperada.
        Asume que prices es la serie completa de la ventana mas larga.

        Returns:
            Dict {f"{h}h": annualized_vol} o {f"{h}d": annualized_vol}.
        """
        if len(prices) < 2:
            return {}

        result: dict[str, float] = {}
        for tf_hours in self._timeframes:
            label = f"{tf_hours}h" if tf_hours < 720 else f"{tf_hours // 24}d"
            # Tomar la fraccion correspondiente de precios
            # Asumiendo precios cada ~1h aprox
            n_points = max(2, min(len(prices), tf_hours))
            subset = prices[-n_points:]
            result[label] = self.calculate(subset, tf_hours)
        return result

    def is_high_volatility(
        self, prices: Sequence[float], threshold: float | None = None,
        timeframe_hours: float = 3.0
    ) -> bool:
        """True si la volatilidad supera el umbral en el timeframe dado."""
        limit = threshold if threshold is not None else self._max_3h
        vol = self.calculate(prices, timeframe_hours)
        return vol > limit

    @property
    def timeframes(self) -> list[int]:
        return list(self._timeframes)

    @property
    def max_3h_volatility(self) -> float:
        return self._max_3h
