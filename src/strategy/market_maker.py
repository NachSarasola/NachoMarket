import logging
from typing import Any

from src.strategy.base import BaseStrategy

logger = logging.getLogger("nachomarket.market_maker")


class MarketMakerStrategy(BaseStrategy):
    """Market making con rewards. Siempre usa Post Only."""

    def __init__(self, client, circuit_breaker, config: dict[str, Any]) -> None:
        super().__init__("market_maker", client, circuit_breaker, config)
        # Soporta tanto settings.yaml (market_maker.*) como risk.yaml (market_making.*)
        mm = config.get("market_maker", config.get("market_making", {}))
        self._spread_offset = mm.get("spread_offset", 0.02)
        self._min_spread = mm.get("min_spread", 0.01)
        self._order_size = mm.get("order_size", mm.get("order_size_usdc", 5.0))
        self._refresh_seconds = mm.get("refresh_seconds", 45)
        self._max_inventory = mm.get("max_inventory_per_market", 50.0)
        self._num_levels = mm.get("num_levels", 3)
        self._level_spacing = mm.get("level_spacing", self._spread_offset / self._num_levels)

    def should_enter(self, market: dict[str, Any]) -> bool:
        """Entra si el spread del mercado es suficiente para ser rentable."""
        spread = market.get("spread", market.get("spread_bps", 0) / 100)
        if spread < self._min_spread:
            logger.debug(f"Spread {spread:.3f} too tight (min {self._min_spread}), skipping")
            return False
        return True

    def evaluate(self, market: dict[str, Any]) -> list[dict[str, Any]]:
        """Genera ordenes bid/ask en multiples niveles."""
        token_id = market.get("token_id", "")
        mid_price = market.get("mid_price", 0.5)

        if mid_price <= 0 or mid_price >= 1:
            return []

        orders: list[dict[str, Any]] = []

        for level in range(self._num_levels):
            offset = self._spread_offset + (level * self._level_spacing)

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
