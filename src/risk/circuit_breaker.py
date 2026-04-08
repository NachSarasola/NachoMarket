import logging
import time
from typing import Any

logger = logging.getLogger("nachomarket.circuit_breaker")


class CircuitBreaker:
    """Stops automaticos para proteger capital."""

    def __init__(self, config: dict[str, Any]) -> None:
        cb_config = config.get("circuit_breakers", {})
        self._max_daily_loss = cb_config.get("max_daily_loss_usdc", 20.0)
        self._max_consecutive_losses = cb_config.get("max_consecutive_losses", 5)
        self._max_single_trade_loss = cb_config.get("max_single_trade_loss_usdc", 10.0)
        self._cooldown_min = cb_config.get("cooldown_after_break_min", 60)
        self._max_open_orders = cb_config.get("max_open_orders", 20)

        self._daily_pnl: float = 0.0
        self._consecutive_losses: int = 0
        self._triggered: bool = False
        self._trigger_time: float | None = None
        self._open_orders: int = 0

    def record_trade(self, pnl: float) -> None:
        """Registra el PnL de un trade y evalua circuit breakers."""
        self._daily_pnl += pnl

        if pnl < 0:
            self._consecutive_losses += 1
            if abs(pnl) > self._max_single_trade_loss:
                logger.warning(f"Single trade loss ${abs(pnl):.2f} exceeds limit!")
                self._trigger("single_trade_loss")
        else:
            self._consecutive_losses = 0

        if self._daily_pnl < -self._max_daily_loss:
            logger.critical(f"Daily loss ${abs(self._daily_pnl):.2f} exceeds limit!")
            self._trigger("daily_loss")

        if self._consecutive_losses >= self._max_consecutive_losses:
            logger.warning(f"{self._consecutive_losses} consecutive losses!")
            self._trigger("consecutive_losses")

    def record_error(self) -> None:
        """Registra un error de ejecucion."""
        self._consecutive_losses += 1
        if self._consecutive_losses >= self._max_consecutive_losses:
            self._trigger("consecutive_errors")

    def is_triggered(self) -> bool:
        """Verifica si el circuit breaker esta activo."""
        if not self._triggered:
            return False

        # Verificar si el cooldown ya paso
        if self._trigger_time:
            elapsed_min = (time.time() - self._trigger_time) / 60
            if elapsed_min >= self._cooldown_min:
                self.reset()
                return False

        return True

    def can_place_order(self) -> bool:
        """Verifica si se puede colocar una nueva orden."""
        if self.is_triggered():
            return False
        return self._open_orders < self._max_open_orders

    def order_placed(self) -> None:
        """Incrementa contador de ordenes abiertas."""
        self._open_orders += 1

    def order_closed(self) -> None:
        """Decrementa contador de ordenes abiertas."""
        self._open_orders = max(0, self._open_orders - 1)

    def reset(self) -> None:
        """Resetea el circuit breaker."""
        self._triggered = False
        self._trigger_time = None
        self._consecutive_losses = 0
        logger.info("Circuit breaker reset")

    def reset_daily(self) -> None:
        """Resetea contadores diarios."""
        self._daily_pnl = 0.0
        self._consecutive_losses = 0
        logger.info("Daily counters reset")

    def get_status(self) -> dict[str, Any]:
        """Retorna el estado del circuit breaker."""
        return {
            "triggered": self._triggered,
            "daily_pnl": self._daily_pnl,
            "consecutive_losses": self._consecutive_losses,
            "open_orders": self._open_orders,
        }

    def _trigger(self, reason: str) -> None:
        """Activa el circuit breaker."""
        self._triggered = True
        self._trigger_time = time.time()
        logger.critical(f"CIRCUIT BREAKER TRIGGERED: {reason}")
