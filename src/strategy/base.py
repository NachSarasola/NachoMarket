import logging
from abc import ABC, abstractmethod
from typing import Any

from src.polymarket.client import PolymarketClient
from src.risk.circuit_breaker import CircuitBreaker

logger = logging.getLogger("nachomarket.strategy")


class BaseStrategy(ABC):
    """Clase abstracta base para todas las estrategias de trading."""

    def __init__(
        self,
        name: str,
        client: PolymarketClient,
        circuit_breaker: CircuitBreaker,
        config: dict[str, Any],
    ) -> None:
        self.name = name
        self._client = client
        self._circuit_breaker = circuit_breaker
        self._config = config
        self._active = True
        logger.info(f"Strategy '{name}' initialized")

    @abstractmethod
    def evaluate(self, market: dict[str, Any]) -> list[dict[str, Any]]:
        """Evalua un mercado y retorna lista de ordenes a colocar.

        Returns:
            Lista de dicts con keys: token_id, side, price, size
        """
        ...

    @abstractmethod
    def should_enter(self, market: dict[str, Any]) -> bool:
        """Determina si se debe entrar en un mercado."""
        ...

    def execute(self, market: dict[str, Any]) -> list[dict[str, Any]]:
        """Ejecuta la estrategia en un mercado con checks de seguridad."""
        if not self._active:
            logger.info(f"Strategy '{self.name}' is paused")
            return []

        if self._circuit_breaker.is_triggered():
            logger.warning(f"Circuit breaker active, skipping {self.name}")
            return []

        if not self.should_enter(market):
            return []

        orders = self.evaluate(market)
        results = []

        for order in orders:
            try:
                result = self._client.place_order(
                    token_id=order["token_id"],
                    side=order["side"],
                    price=order["price"],
                    size=order["size"],
                    post_only=order.get("post_only", True),
                )
                if result:
                    results.append(result)
            except Exception:
                logger.exception(f"Error placing order in {self.name}")
                self._circuit_breaker.record_error()

        return results

    def pause(self) -> None:
        """Pausa la estrategia."""
        self._active = False
        logger.info(f"Strategy '{self.name}' paused")

    def resume(self) -> None:
        """Reanuda la estrategia."""
        self._active = True
        logger.info(f"Strategy '{self.name}' resumed")
