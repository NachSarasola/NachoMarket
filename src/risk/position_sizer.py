"""Position sizing con Quarter-Kelly para Polymarket.

Funciones standalone + clase PositionSizer que lee configuracion.

Uso tipico:
    kf = kelly_fraction(estimated_prob=0.65, market_price=0.55)  # → 0.062
    size = calculate_size(capital=400, kelly_f=kf)               # → $20
    ok = can_trade(current_exposure=80, capital=400)             # → True
"""

import logging
from typing import Any

logger = logging.getLogger("nachomarket.position_sizer")


def kelly_fraction(
    estimated_prob: float,
    market_price: float,
    kelly_multiplier: float = 0.25,
) -> float:
    """Calcula el kelly fraccional (Quarter-Kelly) para prediction markets.

    Formula: f = kelly_multiplier * (p - q) / (1 - q)
    donde:
      p = estimated_prob  — tu estimacion de que ocurra el outcome
      q = market_price    — precio implicito del mercado (= prob implicita)

    Solo hay edge si p > q. Clampea resultado en [0.0, 0.10].
    Nunca apuesta mas del 10% del capital independientemente del edge.

    Args:
        estimated_prob: Probabilidad estimada del outcome (0 < p < 1).
        market_price: Precio actual del token en el CLOB (0 < q < 1).
        kelly_multiplier: Fraccion del Kelly completo a usar (default 0.25).

    Returns:
        Fraccion del capital a arriesgar, en [0.0, 0.10].
    """
    p, q = estimated_prob, market_price

    if not (0 < p < 1) or not (0 < q < 1):
        return 0.0
    if p <= q:
        return 0.0  # Sin edge

    raw = kelly_multiplier * (p - q) / (1.0 - q)
    return max(0.0, min(raw, 0.10))


def calculate_size(
    capital: float,
    kelly_f: float,
    min_size: float = 5.0,
    max_size: float | None = None,
) -> float:
    """Calcula el tamano de posicion en USDC.

    size = capital * kelly_f
    Clampea entre min_size y max(max_size, capital * 0.05).
    Retorna 0.0 si size resultante < min_size (no vale abrir posicion).

    Args:
        capital: Capital total disponible en USDC.
        kelly_f: Fraccion de Kelly (de kelly_fraction()).
        min_size: Tamano minimo para abrir posicion (default $5).
        max_size: Tope maximo opcional. Si None, usa capital * 5%.

    Returns:
        Tamano en USDC, o 0.0 si es demasiado pequeno.
    """
    if kelly_f <= 0 or capital <= 0:
        return 0.0

    raw = capital * kelly_f
    if raw < min_size:
        return 0.0

    # Techo: siempre al menos capital * 5% (regla INQUEBRANTABLE del bot)
    # Si max_size > capital * 5%, permitir hasta max_size
    upper = capital * 0.05
    if max_size is not None:
        upper = max(max_size, upper)

    return round(min(raw, upper), 2)


def can_trade(
    current_exposure: float,
    capital: float,
    max_risk_pct: float = 0.05,
    new_size: float = 0.0,
) -> bool:
    """Verifica si se puede abrir una nueva posicion sin superar el limite de riesgo.

    Retorna True si exposure_actual + new_size < capital * max_risk_pct.

    Args:
        current_exposure: Exposicion actual total en USDC (suma de posiciones abiertas).
        capital: Capital total disponible en USDC.
        max_risk_pct: Maximo porcentaje de capital en riesgo (default 5%).
        new_size: Tamano de la nueva posicion propuesta en USDC.

    Returns:
        True si hay room para la nueva posicion.
    """
    limit = capital * max_risk_pct
    return (current_exposure + new_size) < limit


# ---------------------------------------------------------------------------
# Clase wrapper para uso con configuracion YAML
# ---------------------------------------------------------------------------

class PositionSizer:
    """Lee config de risk.yaml y expone los calculos de sizing.

    Ejemplo de uso en main.py:
        sizer = PositionSizer(risk_config)
        size = sizer.size_for_signal(capital=400, estimated_prob=0.65, market_price=0.55)
    """

    def __init__(self, config: dict[str, Any]) -> None:
        ps = config.get("position_sizing", {})
        self._method = ps.get("method", "fractional_kelly")
        self._kelly_multiplier = ps.get("kelly_fraction", 0.25)
        self._max_position = ps.get("max_position_usdc", 20.0)
        self._min_position = ps.get("min_position_usdc", 5.0)

    def size_for_signal(
        self,
        capital: float,
        estimated_prob: float,
        market_price: float,
    ) -> float:
        """Tamano de posicion para una senal de trading.

        Combina kelly_fraction + calculate_size con los parametros del config.

        Returns:
            Tamano en USDC, o 0.0 si no hay edge suficiente.
        """
        if self._method == "fixed":
            size = min(self._max_position, capital * 0.05)
            return size if size >= self._min_position else 0.0

        kf = kelly_fraction(estimated_prob, market_price, self._kelly_multiplier)
        size = calculate_size(capital, kf, self._min_position, self._max_position)

        logger.info(
            f"Position size: ${size:.2f} "
            f"(p={estimated_prob:.3f}, q={market_price:.3f}, kf={kf:.4f})"
        )
        return size

    def can_trade(
        self,
        current_exposure: float,
        capital: float,
        new_size: float = 0.0,
    ) -> bool:
        """Wrapper de can_trade() con max_risk_pct = 5%."""
        return can_trade(current_exposure, capital, 0.05, new_size)
