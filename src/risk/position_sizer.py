import logging
from typing import Any

logger = logging.getLogger("nachomarket.position_sizer")


class PositionSizer:
    """Position sizing usando Kelly fraccional."""

    def __init__(self, config: dict[str, Any]) -> None:
        ps_config = config.get("position_sizing", {})
        self._method = ps_config.get("method", "fractional_kelly")
        self._kelly_fraction = ps_config.get("kelly_fraction", 0.25)
        self._max_position = ps_config.get("max_position_usdc", 20.0)
        self._min_position = ps_config.get("min_position_usdc", 1.0)

    def calculate_size(
        self,
        capital: float,
        win_probability: float,
        odds: float,
        max_risk_pct: float = 5.0,
    ) -> float:
        """Calcula el tamano optimo de posicion.

        Args:
            capital: Capital total disponible en USDC
            win_probability: Probabilidad estimada de ganar (0-1)
            odds: Ratio de pago (ej: 2.0 para pago doble)
            max_risk_pct: Maximo porcentaje de capital a arriesgar

        Returns:
            Tamano de posicion en USDC
        """
        if self._method == "fixed":
            return min(self._max_position, capital * max_risk_pct / 100)

        # Kelly fraccional
        kelly_pct = self._full_kelly(win_probability, odds)
        fractional_kelly = kelly_pct * self._kelly_fraction

        # Limitar al maximo permitido
        max_by_risk = capital * max_risk_pct / 100
        position = capital * fractional_kelly / 100

        position = min(position, max_by_risk, self._max_position)
        position = max(position, 0)  # Nunca negativo

        if position < self._min_position:
            logger.debug(f"Position {position:.2f} below minimum, skipping")
            return 0.0

        logger.info(
            f"Position size: ${position:.2f} "
            f"(Kelly={kelly_pct:.1f}%, Fractional={fractional_kelly:.1f}%)"
        )
        return round(position, 2)

    def _full_kelly(self, win_prob: float, odds: float) -> float:
        """Calcula el porcentaje Kelly completo.

        Kelly% = (bp - q) / b
        b = odds decimales - 1
        p = probabilidad de ganar
        q = probabilidad de perder
        """
        if odds <= 1 or win_prob <= 0 or win_prob >= 1:
            return 0.0

        b = odds - 1
        p = win_prob
        q = 1 - p

        kelly = (b * p - q) / b
        return max(kelly * 100, 0)  # Retorna porcentaje, nunca negativo
