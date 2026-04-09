import logging
from typing import Any

from src.strategy.base import BaseStrategy, Signal, Trade

logger = logging.getLogger("nachomarket.strategy.market_maker")


class MarketMakerStrategy(BaseStrategy):
    """Market making con rewards. Siempre usa Post Only para evitar taker fees."""

    def __init__(self, client, config: dict[str, Any], **kwargs) -> None:
        super().__init__("market_maker", client, config, **kwargs)
        # Soporta tanto settings.yaml (market_maker.*) como risk.yaml (market_making.*)
        mm = config.get("market_maker", config.get("market_making", {}))
        self._spread_offset = mm.get("spread_offset", 0.02)
        self._min_spread = mm.get("min_spread", 0.01)
        self._order_size = mm.get("order_size", mm.get("order_size_usdc", 5.0))
        self._refresh_seconds = mm.get("refresh_seconds", 45)
        self._max_inventory = mm.get("max_inventory_per_market", 50.0)
        self._num_levels = mm.get("num_levels", 3)
        self._level_spacing = mm.get("level_spacing", self._spread_offset / self._num_levels)

    def should_act(self, market_data: dict[str, Any]) -> bool:
        """Entra si el spread del mercado es suficiente para ser rentable."""
        spread = market_data.get("spread", market_data.get("spread_bps", 0) / 100)
        if spread < self._min_spread:
            self._logger.debug(f"Spread {spread:.3f} too tight (min {self._min_spread}), skipping")
            return False
        return True

    def evaluate(self, market_data: dict[str, Any]) -> list[Signal]:
        """Genera senales bid/ask en multiples niveles alrededor del mid."""
        market_id = market_data.get("condition_id", market_data.get("market_id", ""))
        token_id = market_data.get("token_id", "")
        mid_price = market_data.get("mid_price", 0.5)

        if mid_price <= 0 or mid_price >= 1:
            return []

        # Si hay tokens, usar el primero
        tokens = market_data.get("tokens", [])
        if tokens and not token_id:
            token_id = tokens[0].get("token_id", "")

        signals: list[Signal] = []

        for level in range(self._num_levels):
            offset = self._spread_offset + (level * self._level_spacing)

            bid_price = round(mid_price - offset, 4)
            ask_price = round(mid_price + offset, 4)

            # Confidence decrece con el nivel (ordenes mas alejadas = menos seguras)
            confidence = max(0.3, 1.0 - level * 0.2)

            if bid_price > 0:
                signals.append(self._make_signal(
                    market_id=market_id,
                    token_id=token_id,
                    side="BUY",
                    price=bid_price,
                    size=self._order_size,
                    confidence=confidence,
                ))

            if ask_price < 1:
                signals.append(self._make_signal(
                    market_id=market_id,
                    token_id=token_id,
                    side="SELL",
                    price=ask_price,
                    size=self._order_size,
                    confidence=confidence,
                ))

        self._logger.info(f"Market maker: {len(signals)} senales para {token_id[:8]}...")
        return signals

    def execute(self, signals: list[Signal]) -> list[Trade]:
        """Ejecuta senales como ordenes Post Only (maker)."""
        trades: list[Trade] = []

        for signal in signals:
            try:
                result = self._client.place_limit_order(
                    token_id=signal.token_id,
                    side=signal.side,
                    price=signal.price,
                    size=signal.size,
                    post_only=True,  # SIEMPRE Post Only para market making
                )

                trade = self._make_trade(
                    signal=signal,
                    order_id=result.get("order_id", "unknown"),
                    status=result.get("status", "submitted"),
                    fee_paid=0.0,  # Post Only = maker = sin taker fee
                )
                self.log_trade(trade)
                trades.append(trade)

            except Exception:
                self._logger.exception(
                    f"Error colocando orden MM: {signal.side} {signal.size} @ {signal.price}"
                )
                # Loguear el error como trade fallido
                trade = self._make_trade(
                    signal=signal,
                    order_id="",
                    status="error",
                )
                self.log_trade(trade)
                trades.append(trade)

        return trades
