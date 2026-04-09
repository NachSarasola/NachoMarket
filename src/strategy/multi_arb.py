import logging
from typing import Any

from src.strategy.base import BaseStrategy, Signal, Trade

logger = logging.getLogger("nachomarket.strategy.multi_arb")


class MultiArbStrategy(BaseStrategy):
    """Arbitraje en mercados multi-outcome.

    En un mercado con N outcomes, la suma de probabilidades debe ser 1.
    Si la suma es > 1, hay oportunidad de vender todas.
    Si la suma es < 1, hay oportunidad de comprar todas.
    """

    def __init__(self, client, config: dict[str, Any], **kwargs) -> None:
        super().__init__("multi_arb", client, config, **kwargs)
        arb = config.get("multi_arb", {})
        min_edge_raw = arb.get("min_edge", config.get("min_arb_edge_pct", 0.02))
        # Normalizar: si viene como decimal (0.03) convertir a porcentaje (3.0)
        self._min_edge_pct = min_edge_raw * 100 if min_edge_raw < 1 else min_edge_raw
        self._max_position = arb.get("max_position", config.get("arb_order_size_usdc", 5.0))
        self._order_size = self._max_position / 2

    def should_act(self, market_data: dict[str, Any]) -> bool:
        """Entra solo si hay multiples outcomes."""
        tokens = market_data.get("tokens", [])
        return len(tokens) >= 2

    def evaluate(self, market_data: dict[str, Any]) -> list[Signal]:
        """Genera senales para capturar arbitraje multi-outcome."""
        tokens = market_data.get("tokens", [])
        market_id = market_data.get("condition_id", market_data.get("market_id", ""))
        edge = self._calculate_edge(tokens)

        if abs(edge) < self._min_edge_pct:
            return []

        signals: list[Signal] = []
        confidence = min(abs(edge) / 10.0, 1.0)  # Mas edge = mas confianza

        if edge > self._min_edge_pct:
            # Precios suman > 1: vender todos los outcomes
            for token in tokens:
                signals.append(self._make_signal(
                    market_id=market_id,
                    token_id=token.get("token_id", ""),
                    side="SELL",
                    price=float(token.get("price", 0)),
                    size=self._order_size,
                    confidence=confidence,
                ))
        elif edge < -self._min_edge_pct:
            # Precios suman < 1: comprar todos los outcomes
            for token in tokens:
                signals.append(self._make_signal(
                    market_id=market_id,
                    token_id=token.get("token_id", ""),
                    side="BUY",
                    price=float(token.get("price", 0)),
                    size=self._order_size,
                    confidence=confidence,
                ))

        self._logger.info(f"Multi-arb: {len(signals)} senales, edge={edge:.2f}%")
        return signals

    def execute(self, signals: list[Signal]) -> list[Trade]:
        """Ejecuta senales de arb como ordenes Post Only."""
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
                    f"Error colocando orden arb: {signal.side} {signal.size} @ {signal.price}"
                )
                trade = self._make_trade(signal=signal, order_id="", status="error")
                self.log_trade(trade)
                trades.append(trade)

        return trades

    def _calculate_edge(self, tokens: list[dict[str, Any]]) -> float:
        """Calcula el edge de arbitraje.

        Positivo = suma > 1 (oportunidad de venta)
        Negativo = suma < 1 (oportunidad de compra)
        """
        total_price = sum(float(t.get("price", 0)) for t in tokens)
        return (total_price - 1.0) * 100
