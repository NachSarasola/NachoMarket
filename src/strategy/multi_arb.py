import logging
from typing import Any

from src.strategy.base import BaseStrategy

logger = logging.getLogger("nachomarket.multi_arb")


class MultiArbStrategy(BaseStrategy):
    """Arbitraje en mercados multi-outcome.

    En un mercado con N outcomes, la suma de probabilidades debe ser 1.
    Si la suma es > 1, hay oportunidad de vender todas.
    Si la suma es < 1, hay oportunidad de comprar todas.
    """

    def __init__(self, client, circuit_breaker, config: dict[str, Any]) -> None:
        super().__init__("multi_arb", client, circuit_breaker, config)
        self._min_edge_pct = config.get("min_arb_edge_pct", 2.0)
        self._order_size = config.get("arb_order_size_usdc", 5.0)

    def should_enter(self, market: dict[str, Any]) -> bool:
        """Entra si hay una oportunidad de arbitraje."""
        tokens = market.get("tokens", [])
        if len(tokens) < 2:
            return False

        edge = self._calculate_edge(tokens)
        if abs(edge) < self._min_edge_pct:
            return False

        logger.info(f"Arb opportunity found: edge={edge:.2f}%")
        return True

    def evaluate(self, market: dict[str, Any]) -> list[dict[str, Any]]:
        """Genera ordenes para capturar arbitraje."""
        tokens = market.get("tokens", [])
        edge = self._calculate_edge(tokens)
        orders: list[dict[str, Any]] = []

        if edge > self._min_edge_pct:
            # Precios suman > 1: vender todos los outcomes
            for token in tokens:
                orders.append({
                    "token_id": token["token_id"],
                    "side": "SELL",
                    "price": float(token.get("price", 0)),
                    "size": self._order_size,
                    "post_only": True,
                })
        elif edge < -self._min_edge_pct:
            # Precios suman < 1: comprar todos los outcomes
            for token in tokens:
                orders.append({
                    "token_id": token["token_id"],
                    "side": "BUY",
                    "price": float(token.get("price", 0)),
                    "size": self._order_size,
                    "post_only": True,
                })

        logger.info(f"Multi-arb: {len(orders)} orders, edge={edge:.2f}%")
        return orders

    def _calculate_edge(self, tokens: list[dict[str, Any]]) -> float:
        """Calcula el edge de arbitraje.

        Positivo = suma > 1 (oportunidad de venta)
        Negativo = suma < 1 (oportunidad de compra)
        """
        total_price = sum(float(t.get("price", 0)) for t in tokens)
        return (total_price - 1.0) * 100
