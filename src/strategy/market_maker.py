import logging
from typing import Any

from src.strategy.base import BaseStrategy

logger = logging.getLogger("nachomarket.market_maker")


class MarketMakerStrategy(BaseStrategy):
    """Market making con rewards. Siempre usa Post Only."""

    def __init__(self, client, circuit_breaker, config: dict[str, Any]) -> None:
        super().__init__("market_maker", client, circuit_breaker, config)
        mm_config = config.get("market_making", {})
        self._spread_bps = mm_config.get("default_spread_bps", 200)
        self._min_spread_bps = mm_config.get("min_spread_bps", 100)
        self._order_size = mm_config.get("order_size_usdc", 5.0)
        self._num_levels = mm_config.get("num_levels", 3)
        self._level_spacing_bps = mm_config.get("level_spacing_bps", 50)

    def should_enter(self, market: dict[str, Any]) -> bool:
        """Entra si el spread es suficiente para ser rentable."""
        spread = market.get("spread_bps", 0)
        if spread < self._min_spread_bps:
            logger.debug(f"Spread {spread} bps too tight, skipping")
            return False
        return True

    def evaluate(self, market: dict[str, Any]) -> list[dict[str, Any]]:
        """Genera ordenes bid/ask en multiples niveles."""
        token_id = market.get("token_id", "")
        mid_price = market.get("mid_price", 0.5)

        if mid_price <= 0 or mid_price >= 1:
            return []

        orders: list[dict[str, Any]] = []
        half_spread = self._spread_bps / 20000  # bps to decimal, divided by 2

        for level in range(self._num_levels):
            offset = half_spread + (level * self._level_spacing_bps / 10000)

            bid_price = round(mid_price - offset, 4)
            ask_price = round(mid_price + offset, 4)

            # Asegurar precios validos (0, 1)
            if bid_price > 0:
                orders.append({
                    "token_id": token_id,
                    "side": "BUY",
                    "price": bid_price,
                    "size": self._order_size,
                    "post_only": True,  # SIEMPRE Post Only
                })

            if ask_price < 1:
                orders.append({
                    "token_id": token_id,
                    "side": "SELL",
                    "price": ask_price,
                    "size": self._order_size,
                    "post_only": True,
                })

        logger.info(f"Market maker: {len(orders)} orders for {token_id[:8]}...")
        return orders
