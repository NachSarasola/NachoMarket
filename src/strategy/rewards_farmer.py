"""LP Rewards Farming Optimizer (TODO 4.2).

Maximiza rewards_earned_per_day / capital_locked para mercados con
programas de rewards activos.

Calcula el tamaño optimo de ordenes para maximizar la participacion
en rewards sin exceder el presupuesto de capital.
"""

import logging
import math
from typing import Any

from src.strategy.base import BaseStrategy, Signal, Trade

logger = logging.getLogger("nachomarket.strategy.rewards_farmer")


class RewardsFarmerStrategy(BaseStrategy):
    """Estrategia de farming de rewards de liquidity provision.

    Prioriza mercados con rewards activos y calcula el tamaño optimo
    para maximizar rewards / capital usando una heuristica de grid search.
    """

    def __init__(self, client: Any, config: dict[str, Any], **kwargs) -> None:
        super().__init__("rewards_farmer", client, config, **kwargs)
        rf_cfg = config.get("rewards_farmer", {})
        self._max_capital_per_market = rf_cfg.get("max_capital_per_market", 50.0)
        self._min_rewards_rate = rf_cfg.get("min_rewards_rate", 0.001)  # 0.1% diario
        self._max_markets = rf_cfg.get("max_markets_simultaneous", 3)
        self._spread_offset = rf_cfg.get("spread_offset", 0.015)  # Tight para rewards

        self._active_farms: dict[str, float] = {}  # {condition_id: capital_deployed}

    # ------------------------------------------------------------------
    # BaseStrategy interface
    # ------------------------------------------------------------------

    def should_act(self, market_data: dict[str, Any]) -> bool:
        """Actua si el mercado tiene rewards activos y tasa suficiente."""
        rewards_rate = market_data.get("rewards_rate", 0.0)
        if rewards_rate < self._min_rewards_rate:
            return False
        if len(self._active_farms) >= self._max_markets:
            condition_id = market_data.get("condition_id", "")
            if condition_id not in self._active_farms:
                return False
        return True

    def evaluate(self, market_data: dict[str, Any]) -> list[Signal]:
        """Genera ordenes optimizadas para farming de rewards."""
        condition_id = market_data.get("condition_id", "")
        rewards_rate = market_data.get("rewards_rate", 0.0)
        mid_price = market_data.get("mid_price", 0.5)

        if not condition_id or mid_price <= 0:
            return []

        tokens = market_data.get("tokens", [])
        if not tokens:
            return []

        token_id = tokens[0].get("token_id", "")
        if not token_id:
            return []

        # Calcular tamaño optimo via heuristica
        optimal_size = self._optimize_size(
            rewards_rate=rewards_rate,
            capital_budget=self._max_capital_per_market,
        )

        signals = []

        # Bid en mid - spread
        bid_price = round(max(0.01, mid_price - self._spread_offset), 4)
        signals.append(Signal(
            strategy_name=self.name,
            market_id=condition_id,
            token_id=token_id,
            side="BUY",
            price=bid_price,
            size=optimal_size,
            reason=f"rewards_farming: rate={rewards_rate:.4f}/day",
        ))

        # Ask en mid + spread
        ask_price = round(min(0.99, mid_price + self._spread_offset), 4)
        signals.append(Signal(
            strategy_name=self.name,
            market_id=condition_id,
            token_id=token_id,
            side="SELL",
            price=ask_price,
            size=optimal_size,
            reason=f"rewards_farming: rate={rewards_rate:.4f}/day",
        ))

        return signals

    def execute(self, signals: list[Signal], market_data: dict[str, Any]) -> list[Trade]:
        """Ejecuta ordenes de rewards farming (Post Only)."""
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
                    result = self._client.place_limit_order(
                        token_id=signal.token_id,
                        side=signal.side,
                        price=signal.price,
                        size=signal.size,
                        post_only=True,
                    )
                    trade = Trade(
                        strategy_name=self.name,
                        market_id=signal.market_id,
                        token_id=signal.token_id,
                        side=signal.side,
                        price=signal.price,
                        size=signal.size,
                        status=result.get("status", "unknown"),
                        order_id=result.get("orderID", ""),
                        reason=signal.reason,
                    )
                trades.append(trade)
                if signal.market_id:
                    self._active_farms[signal.market_id] = (
                        self._active_farms.get(signal.market_id, 0.0) + signal.size
                    )
            except Exception:
                self._logger.exception("Error en rewards_farmer execute")
        return trades

    # ------------------------------------------------------------------
    # Optimizacion
    # ------------------------------------------------------------------

    def _optimize_size(
        self,
        rewards_rate: float,
        capital_budget: float,
        grid_points: int = 10,
    ) -> float:
        """Grid search simple para tamaño optimo.

        Maximiza: rewards_per_day = size * rewards_rate
        Sujeto a: size <= capital_budget

        Con restriccion de participation_share (asume participation decrece
        con size mayor relativo al mercado total).
        """
        best_size = capital_budget * 0.5
        best_roi = 0.0

        for i in range(1, grid_points + 1):
            size = capital_budget * (i / grid_points)
            # Rewards diarios = size * rate
            daily_rewards = size * rewards_rate
            # ROI = rewards / capital
            roi = daily_rewards / max(size, 1.0)
            if roi > best_roi:
                best_roi = roi
                best_size = size

        return round(best_size, 2)
