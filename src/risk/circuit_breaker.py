"""Circuit breakers para proteger capital.

Stops automaticos:
  1. Drawdown diario > max_daily_drawdown ($20) → pausar TODO
  2. Errores consecutivos > 5 → pausar y alertar por Telegram
  3. Un mercado pierde > 10% en 1 hora → cancelar ordenes de ese mercado
  4. Reset diario a medianoche UTC (activado desde main.py via schedule)
"""

import logging
import time
from collections import deque
from collections.abc import Callable
from typing import Any

logger = logging.getLogger("nachomarket.circuit_breaker")

_ONE_HOUR = 3600.0  # segundos


class CircuitBreaker:
    """Proteccion de capital con multiples capas de circuit breaking.

    Args:
        config: Seccion 'circuit_breakers' del risk.yaml.
        alert_callback: Funcion opcional para alertas Telegram.
                        Firma: callback(reason: str, message: str) → None.
    """

    def __init__(
        self,
        config: dict[str, Any],
        alert_callback: Callable[[str, str], None] | None = None,
    ) -> None:
        cb = config.get("circuit_breakers", {})

        # Thresholds
        self._max_daily_loss = cb.get("max_daily_loss_usdc", 20.0)
        self._max_consecutive_losses = cb.get("max_consecutive_losses", 5)
        self._max_consecutive_errors = cb.get("max_consecutive_errors", 5)
        self._max_single_trade_loss = cb.get("max_single_trade_loss_usdc", 10.0)
        self._cooldown_min = cb.get("cooldown_after_break_min", 60)
        self._max_open_orders = cb.get("max_open_orders", 20)
        # 10% del maximo por mercado — configurable directamente tambien
        self._max_market_loss_1h = cb.get("max_market_loss_1h_usdc", 5.0)

        # Callback para alertas Telegram
        self._alert_callback = alert_callback

        # Estado intradiario
        self._daily_pnl: float = 0.0
        self._consecutive_losses: int = 0
        self._consecutive_errors: int = 0
        self._open_orders: int = 0
        self._triggered: bool = False
        self._trigger_reason: str = ""
        self._trigger_time: float | None = None

        # PnL por mercado en ventana deslizante de 1 hora
        # {market_id: deque([(timestamp, pnl), ...])}
        self._market_pnl: dict[str, deque[tuple[float, float]]] = {}

    # ------------------------------------------------------------------
    # Registro de eventos
    # ------------------------------------------------------------------

    def record_trade(self, pnl: float) -> None:
        """Registra el PnL de un trade y evalua todos los circuit breakers.

        Args:
            pnl: PnL del trade en USDC (negativo = perdida).
        """
        self._daily_pnl += pnl

        if pnl < 0:
            self._consecutive_losses += 1
            self._consecutive_errors = 0  # Trade ejecutado = no es error de sistema

            if abs(pnl) > self._max_single_trade_loss:
                msg = f"Single trade loss ${abs(pnl):.2f} exceeds ${self._max_single_trade_loss}"
                logger.warning(msg)
                self._alert("single_trade_loss", msg)
        else:
            self._consecutive_losses = 0

        # --- Check 1: Drawdown diario ---
        if self._daily_pnl < -self._max_daily_loss:
            msg = (
                f"Daily drawdown ${abs(self._daily_pnl):.2f} > "
                f"limit ${self._max_daily_loss:.2f} — STOPPING ALL TRADING"
            )
            logger.critical(msg)
            self._alert("daily_drawdown", msg)
            self._trigger("daily_drawdown")

        # --- Check 2: Perdidas consecutivas ---
        if self._consecutive_losses >= self._max_consecutive_losses:
            msg = f"{self._consecutive_losses} consecutive losses — pausing"
            logger.warning(msg)
            self._alert("consecutive_losses", msg)
            self._trigger("consecutive_losses")

    def record_error(self) -> None:
        """Registra un error de sistema (timeout, API error, etc.).

        Si se acumulan > max_consecutive_errors seguidos: pausa y alerta.
        """
        self._consecutive_errors += 1

        if self._consecutive_errors >= self._max_consecutive_errors:
            msg = (
                f"{self._consecutive_errors} consecutive errors — "
                "pausing and alerting via Telegram"
            )
            logger.critical(msg)
            self._alert("consecutive_errors", msg)
            self._trigger("consecutive_errors")

    def record_market_pnl(self, market_id: str, pnl: float) -> None:
        """Registra PnL de un trade especifico de mercado.

        Si el mercado pierde > max_market_loss_1h en la ultima hora:
        lo marca para cancelacion de ordenes.

        Args:
            market_id: condition_id del mercado.
            pnl: PnL del trade en USDC.
        """
        if market_id not in self._market_pnl:
            self._market_pnl[market_id] = deque()

        now = time.time()
        self._market_pnl[market_id].append((now, pnl))

        # Limpiar registros mas viejos de 1 hora
        self._evict_old_market_pnl(market_id, now)

        # Evaluar perdida acumulada en la ultima hora
        hourly_pnl = sum(p for _, p in self._market_pnl[market_id])
        if hourly_pnl < -self._max_market_loss_1h:
            msg = (
                f"Market {market_id[:12]}... lost ${abs(hourly_pnl):.2f} in last hour "
                f"(limit ${self._max_market_loss_1h:.2f}) — cancelling orders"
            )
            logger.warning(msg)
            self._alert("market_hourly_loss", msg)

    def get_markets_to_cancel(self) -> list[str]:
        """Retorna market_ids que excedieron la perdida horaria.

        El main loop debe cancelar ordenes en estos mercados.
        """
        now = time.time()
        to_cancel = []
        for market_id, records in self._market_pnl.items():
            self._evict_old_market_pnl(market_id, now)
            hourly_pnl = sum(p for _, p in records)
            if hourly_pnl < -self._max_market_loss_1h:
                to_cancel.append(market_id)
        return to_cancel

    # ------------------------------------------------------------------
    # Estado y consultas
    # ------------------------------------------------------------------

    def is_triggered(self) -> bool:
        """Verifica si el circuit breaker esta activo.

        Si el cooldown ya paso, resetea automaticamente.
        """
        if not self._triggered:
            return False

        if self._trigger_time is not None:
            elapsed_min = (time.time() - self._trigger_time) / 60
            if elapsed_min >= self._cooldown_min:
                logger.info(
                    f"Circuit breaker cooldown expired after {elapsed_min:.0f}m — resetting"
                )
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
        """Resetea el circuit breaker (cooldown expirado o manual)."""
        self._triggered = False
        self._trigger_reason = ""
        self._trigger_time = None
        self._consecutive_losses = 0
        self._consecutive_errors = 0
        logger.info("Circuit breaker reset")

    def reset_daily(self) -> None:
        """Resetea contadores diarios. Llamar a medianoche UTC via schedule.

        NO resetea el circuit breaker si esta triggered — requiere intervencion manual.
        """
        self._daily_pnl = 0.0
        self._consecutive_losses = 0
        self._consecutive_errors = 0
        self._market_pnl.clear()
        logger.info("Daily counters reset (midnight UTC)")

    def get_status(self) -> dict[str, Any]:
        """Retorna el estado completo del circuit breaker."""
        return {
            "triggered": self._triggered,
            "trigger_reason": self._trigger_reason,
            "daily_pnl": self._daily_pnl,
            "consecutive_losses": self._consecutive_losses,
            "consecutive_errors": self._consecutive_errors,
            "open_orders": self._open_orders,
            "markets_over_limit": self.get_markets_to_cancel(),
        }

    # ------------------------------------------------------------------
    # Privados
    # ------------------------------------------------------------------

    def _trigger(self, reason: str) -> None:
        """Activa el circuit breaker si no estaba ya activo."""
        if self._triggered:
            return  # Ya activo, no sobreescribir razon original
        self._triggered = True
        self._trigger_reason = reason
        self._trigger_time = time.time()
        logger.critical(f"CIRCUIT BREAKER TRIGGERED: {reason}")

    def _alert(self, reason: str, message: str) -> None:
        """Llama al alert_callback si esta configurado (Telegram)."""
        if self._alert_callback is None:
            return
        try:
            self._alert_callback(reason, message)
        except Exception:
            logger.exception("Error sending alert")

    def _evict_old_market_pnl(self, market_id: str, now: float) -> None:
        """Elimina registros de PnL mas viejos de 1 hora para un mercado."""
        records = self._market_pnl.get(market_id)
        if records is None:
            return
        cutoff = now - _ONE_HOUR
        while records and records[0][0] < cutoff:
            records.popleft()
