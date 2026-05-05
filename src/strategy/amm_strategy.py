"""AMM Strategy — liquidity provision via concentrated liquidity model.

Wrapper que integra AMMEngine con el pipeline BaseStrategy.
Genera escalera de ordenes que replican una bonding curve Uniswap v3.
"""

from __future__ import annotations

import logging
import math
from typing import TYPE_CHECKING, Any

from src.strategy.amm_engine import AMMConfig, AMMEngine, AMMOrder
from src.strategy.base import BaseStrategy, Signal, Trade

if TYPE_CHECKING:
    from src.polymarket.client import PolymarketClient
    from src.risk.circuit_breaker import CircuitBreaker
    from src.risk.inventory import InventoryManager

logger = logging.getLogger("nachomarket.strategy.amm")


class AMMStrategy(BaseStrategy):
    """Estrategia AMM: bonding curve sobre CLOB."""

    def __init__(
        self,
        client: PolymarketClient,
        config: dict[str, Any],
        circuit_breaker: CircuitBreaker | None = None,
        inventory: InventoryManager | None = None,
    ) -> None:
        super().__init__("amm", client, config)
        acfg = config.get("amm", {})
        self._amm_config = AMMConfig.from_dict(acfg)
        self._engine = AMMEngine(self._amm_config)
        self._circuit_breaker = circuit_breaker
        self._inventory = inventory
        self._sync_interval = float(acfg.get("sync_interval_sec", 30))
        self._last_sync: dict[str, float] = {}

    def should_act(self, market_data: dict[str, Any]) -> bool:
        if self._circuit_breaker is not None and self._circuit_breaker.is_triggered():
            return False

        mid = float(market_data.get("mid_price", 0.0) or 0.0)
        if mid <= self._amm_config.p_min or mid >= self._amm_config.p_max:
            return False

        tokens = market_data.get("tokens", [])
        if len(tokens) < 2:
            return False

        return True

    def evaluate(self, market_data: dict[str, Any]) -> list[Signal]:
        cid = market_data.get("condition_id", "")
        mid = float(market_data.get("mid_price", 0.0) or 0.0)
        tokens = market_data.get("tokens", [])
        if len(tokens) < 2 or mid <= 0:
            return []

        self._engine.set_price(mid)

        tid_a = tokens[0].get("token_id", "a")
        tid_b = tokens[1].get("token_id", "b")
        balance_a = market_data.get("token_inventory", {}).get(tid_a, 0.0)
        balance_b = market_data.get("token_inventory", {}).get(tid_b, 0.0)
        available_cash = float(market_data.get("available_cash", 0.0) or 0.0)
        total_collateral = min(available_cash, self._amm_config.max_collateral)

        orders = self._engine.get_orders(
            balance_token_a=balance_a,
            balance_token_b=balance_b,
            total_collateral=total_collateral,
            token_a_id=tid_a,
            token_b_id=tid_b,
        )

        signals: list[Signal] = []
        estimated_prob = mid

        for order in orders:
            if order.size <= 0:
                continue
            sig = Signal(
                market_id=cid,
                token_id=order.token,
                side=order.side,
                price=order.price,
                size=order.size,
                confidence=0.60,
                strategy_name=self.name,
                metadata={"estimated_prob": estimated_prob},
            )
            signals.append(sig)

        return signals

    def execute(self, signals: list[Signal]) -> list[Trade]:
        trades: list[Trade] = []
        for sig in signals:
            try:
                result = self._client.place_limit_order(
                    token_id=sig.token_id,
                    side=sig.side,
                    price=sig.price,
                    size=sig.size,
                    post_only=True,
                )
                trade = self._make_trade(
                    sig, result.get("order_id", ""), result.get("status", "error")
                )
                trades.append(trade)
                self.log_trade(trade)
            except Exception:
                logger.exception("AMM: error colocando orden %s", sig.token_id[:8])
        return trades
