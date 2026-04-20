"""Stat-Arb YES/NO Cointegrado (TODO 4.1).

Para cada mercado binario: si mid_YES + mid_NO ≠ $1.00 por mas del threshold,
compra el mas barato y vende el mas caro (FOK para atomicidad).

La suma YES + NO debe converger a $1.00 al resolver el mercado.
Edge = (1.0 - mid_YES - mid_NO) - fees.
"""

import logging
from typing import Any

from src.strategy.base import BaseStrategy, Signal, Trade

logger = logging.getLogger("nachomarket.strategy.stat_arb")


class StatArbStrategy(BaseStrategy):
    """Arbitraje YES/NO basado en cointegracion con la resolucion del mercado.

    Funciona comprando YES y NO cuando su suma < (1.0 - edge_threshold),
    bloqueando el profit hasta resolucion.

    Requiere que el mercado tenga exactamente 2 tokens (YES + NO).
    """

    def __init__(self, client: Any, config: dict[str, Any], **kwargs) -> None:
        super().__init__("stat_arb", client, config, **kwargs)
        arb_cfg = config.get("stat_arb", {})
        self._edge_threshold = arb_cfg.get("edge_threshold", 0.02)   # 2c min
        self._max_position = arb_cfg.get("max_position", 30.0)       # $30 max
        self._max_markets = arb_cfg.get("max_markets_simultaneous", 5)
        self._fee_bps = arb_cfg.get("fee_bps", 100)                  # 1% taker
        self._active_arbs: dict[str, dict[str, Any]] = {}             # condition_id → state

    # ------------------------------------------------------------------
    # BaseStrategy interface
    # ------------------------------------------------------------------

    def should_act(self, market_data: dict[str, Any]) -> bool:
        """Solo actua en mercados binarios con 2 tokens."""
        tokens = market_data.get("tokens", [])
        if len(tokens) != 2:
            return False
        if len(self._active_arbs) >= self._max_markets:
            return False
        return True

    def evaluate(self, market_data: dict[str, Any]) -> list[Signal]:
        """Detecta oportunidades de arb YES+NO < $1.00."""
        tokens = market_data.get("tokens", [])
        if len(tokens) < 2:
            return []

        condition_id = market_data.get("condition_id", "")
        if condition_id in self._active_arbs:
            return []  # Ya en una posicion

        # Obtener precios mid para YES y NO
        yes_token = tokens[0]
        no_token = tokens[1]

        yes_mid = self._get_mid(yes_token, market_data)
        no_mid = self._get_mid(no_token, market_data)

        if yes_mid <= 0 or no_mid <= 0:
            return []

        total = yes_mid + no_mid
        edge_raw = 1.0 - total
        fees = 2 * self._fee_bps / 10_000  # 2 trades (buy YES + buy NO)
        net_edge = edge_raw - fees

        if net_edge < self._edge_threshold:
            return []

        # Señal: comprar ambos lados
        size = min(self._max_position / 2, self._max_position / total)
        signals = []

        for token_info, side_label in [(yes_token, "YES"), (no_token, "NO")]:
            token_id = token_info.get("token_id", "")
            if not token_id:
                continue

            mid = yes_mid if side_label == "YES" else no_mid
            signals.append(Signal(
                strategy_name=self.name,
                market_id=condition_id,
                token_id=token_id,
                side="BUY",
                price=min(mid + 0.01, 0.99),  # Taker price con pequeño premium
                size=size,
                reason=f"stat_arb: YES+NO={total:.3f}, edge={net_edge:.3f}",
                metadata={
                    "arb_side": side_label,
                    "total_sum": total,
                    "net_edge": net_edge,
                    "post_only": False,   # Necesitamos fill inmediato
                },
            ))

        return signals

    def execute(self, signals: list[Signal], market_data: dict[str, Any]) -> list[Trade]:
        """Ejecuta ambas piernas del arb como FOK para atomicidad."""
        if len(signals) < 2:
            return []

        condition_id = signals[0].market_id
        trades = []

        for signal in signals:
            try:
                if self._paper_mode:
                    trade = Trade(
                        strategy_name=self.name,
                        market_id=signal.market_id,
                        token_id=signal.token_id,
                        side=signal.side,
                        price=signal.price,
                        size=signal.size,
                        status="paper",
                        reason=signal.reason,
                    )
                else:
                    result = self._client.place_fok_order(
                        token_id=signal.token_id,
                        side=signal.side,
                        price=signal.price,
                        size=signal.size,
                    )
                    status = "filled" if result.get("status") == "matched" else "rejected"
                    trade = Trade(
                        strategy_name=self.name,
                        market_id=signal.market_id,
                        token_id=signal.token_id,
                        side=signal.side,
                        price=signal.price,
                        size=signal.size,
                        status=status,
                        order_id=result.get("orderID", ""),
                        reason=signal.reason,
                    )

                trades.append(trade)

                if trade.status in ("filled", "paper"):
                    self._logger.info(
                        "StatArb %s pierna %s: %s @ %.4f",
                        signal.metadata.get("arb_side", "?"),
                        signal.side, signal.token_id[:8], signal.price,
                    )

            except Exception:
                self._logger.exception("Error en pierna de stat_arb")

        # Registrar posicion activa si ambas piernas ejecutadas
        filled = [t for t in trades if t.status in ("filled", "paper")]
        if len(filled) == 2:
            self._active_arbs[condition_id] = {
                "yes_token": signals[0].token_id,
                "no_token": signals[1].token_id,
                "size": signals[0].size,
            }

        return trades

    def clear_arb(self, condition_id: str) -> None:
        """Limpia una posicion de arb (llamar tras resolucion del mercado)."""
        self._active_arbs.pop(condition_id, None)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _get_mid(self, token_info: dict[str, Any], market_data: dict[str, Any]) -> float:
        """Obtiene el midprice de un token desde market_data o cliente."""
        token_id = token_info.get("token_id", "")
        if not token_id:
            return 0.0

        # Primero intentar desde market_data (ya enriquecido con WS)
        if "mid_price" in token_info:
            return float(token_info["mid_price"])

        # Fallback: precio del cliente
        try:
            return self._client.get_midpoint(token_id)
        except Exception:
            return 0.0
