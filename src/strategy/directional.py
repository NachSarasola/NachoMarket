import logging
from typing import Any

from src.strategy.base import BaseStrategy

logger = logging.getLogger("nachomarket.directional")


class DirectionalStrategy(BaseStrategy):
    """Trading direccional basado en analisis de probabilidades.

    Usa senales simples para detectar mispricing:
    - Movimiento reciente de precio vs promedio
    - Volumen inusual
    - Proximidad a resolucion
    """

    def __init__(self, client, circuit_breaker, config: dict[str, Any]) -> None:
        super().__init__("directional", client, circuit_breaker, config)
        self._min_edge = config.get("directional_min_edge_pct", 5.0)
        self._order_size = config.get("directional_order_size_usdc", 5.0)

    def should_enter(self, market: dict[str, Any]) -> bool:
        """Entra si detecta un mispricing significativo."""
        signal = self._compute_signal(market)
        if abs(signal) < self._min_edge:
            return False
        logger.info(f"Directional signal: {signal:.2f}% for {market.get('question', 'unknown')}")
        return True

    def evaluate(self, market: dict[str, Any]) -> list[dict[str, Any]]:
        """Genera una orden direccional."""
        signal = self._compute_signal(market)
        token_id = market.get("token_id", "")
        current_price = market.get("mid_price", 0.5)

        if signal > 0:
            # Senial alcista: comprar
            return [{
                "token_id": token_id,
                "side": "BUY",
                "price": round(current_price * 0.99, 4),  # Ligeramente debajo del mid
                "size": self._order_size,
                "post_only": True,
            }]
        elif signal < 0:
            # Senial bajista: vender
            return [{
                "token_id": token_id,
                "side": "SELL",
                "price": round(current_price * 1.01, 4),  # Ligeramente encima del mid
                "size": self._order_size,
                "post_only": True,
            }]

        return []

    def _compute_signal(self, market: dict[str, Any]) -> float:
        """Calcula senial direccional simple.

        Returns:
            Positivo = alcista, negativo = bajista, magnitud = confianza
        """
        current_price = market.get("mid_price", 0.5)
        avg_price = market.get("avg_price_24h", current_price)
        volume_ratio = market.get("volume_ratio", 1.0)  # vol actual / vol promedio

        if avg_price == 0:
            return 0.0

        # Reversion a la media: si precio se movio mucho, esperar reversion
        price_deviation = ((current_price - avg_price) / avg_price) * 100

        # Ajustar por volumen (mas volumen = mas confianza)
        confidence_multiplier = min(volume_ratio, 2.0)

        # Senial negativa si precio subio mucho (esperar que baje)
        return -price_deviation * confidence_multiplier
