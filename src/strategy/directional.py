import logging
from typing import Any

from src.strategy.base import BaseStrategy, Signal, Trade

logger = logging.getLogger("nachomarket.strategy.directional")


class DirectionalStrategy(BaseStrategy):
    """Trading direccional basado en reversion a la media.

    Usa senales simples para detectar mispricing:
    - Movimiento reciente de precio vs promedio 24h
    - Volumen inusual como multiplicador de confianza
    """

    def __init__(self, client, config: dict[str, Any], **kwargs) -> None:
        super().__init__("directional", client, config, **kwargs)
        self._min_edge = config.get("directional_min_edge_pct", 5.0)
        self._order_size = config.get("directional_order_size_usdc", 5.0)

    def should_act(self, market_data: dict[str, Any]) -> bool:
        """Entra si detecta un mispricing significativo."""
        signal_strength = self._compute_signal_strength(market_data)
        return abs(signal_strength) >= self._min_edge

    def evaluate(self, market_data: dict[str, Any]) -> list[Signal]:
        """Genera una senal direccional basada en reversion a la media."""
        signal_strength = self._compute_signal_strength(market_data)

        if abs(signal_strength) < self._min_edge:
            return []

        market_id = market_data.get("condition_id", market_data.get("market_id", ""))
        token_id = market_data.get("token_id", "")
        current_price = market_data.get("mid_price", 0.5)

        # Si hay tokens, usar el primero
        tokens = market_data.get("tokens", [])
        if tokens and not token_id:
            token_id = tokens[0].get("token_id", "")

        confidence = min(abs(signal_strength) / 20.0, 1.0)

        if signal_strength > 0:
            # Senial alcista: comprar ligeramente debajo del mid
            return [self._make_signal(
                market_id=market_id,
                token_id=token_id,
                side="BUY",
                price=round(current_price * 0.99, 4),
                size=self._order_size,
                confidence=confidence,
            )]
        else:
            # Senial bajista: vender ligeramente encima del mid
            return [self._make_signal(
                market_id=market_id,
                token_id=token_id,
                side="SELL",
                price=round(current_price * 1.01, 4),
                size=self._order_size,
                confidence=confidence,
            )]

    def execute(self, signals: list[Signal]) -> list[Trade]:
        """Ejecuta senales direccionales como ordenes Post Only."""
        trades: list[Trade] = []

        for signal in signals:
            try:
                result = self._client.place_limit_order(
                    token_id=signal.token_id,
                    side=signal.side,
                    price=signal.price,
                    size=signal.size,
                    post_only=True,
                )

                trade = self._make_trade(
                    signal=signal,
                    order_id=result.get("order_id", "unknown"),
                    status=result.get("status", "submitted"),
                )
                self.log_trade(trade)
                trades.append(trade)

            except Exception:
                self._logger.exception(
                    f"Error colocando orden directional: {signal.side} @ {signal.price}"
                )
                trade = self._make_trade(signal=signal, order_id="", status="error")
                self.log_trade(trade)
                trades.append(trade)

        return trades

    def _compute_signal_strength(self, market_data: dict[str, Any]) -> float:
        """Calcula fuerza de la senial direccional.

        Returns:
            Positivo = alcista (comprar), negativo = bajista (vender).
            Magnitud = confianza.
        """
        current_price = market_data.get("mid_price", 0.5)
        avg_price = market_data.get("avg_price_24h", current_price)
        volume_ratio = market_data.get("volume_ratio", 1.0)

        if avg_price == 0:
            return 0.0

        # Reversion a la media: si precio subio mucho, esperar reversion
        price_deviation = ((current_price - avg_price) / avg_price) * 100
        confidence_multiplier = min(volume_ratio, 2.0)

        # Senial negativa si precio subio mucho (esperar que baje)
        return -price_deviation * confidence_multiplier
