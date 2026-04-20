"""Event-Driven Momentum Strategy (TODO 4.3).

Reacciona a eventos economicos y noticias:
- Antes de eventos de alto impacto: aumentar spreads en mercados relacionados
- Despues del evento: entrar con momentum signal (2 min post-evento)
- Mean-reversion 5 min despues

Integra con FREDClient para el calendario.
"""

import logging
import time
from typing import Any

from src.strategy.base import BaseStrategy, Signal, Trade

logger = logging.getLogger("nachomarket.strategy.event_driven")

_SPREAD_MULTIPLIER_PRE_EVENT = 2.5     # Spreads 2.5x antes del evento
_MOMENTUM_ENTRY_DELAY_SEC = 120        # 2 min post-evento para entrar
_MEAN_REVERSION_DELAY_SEC = 300        # 5 min para mean-reversion
_PRE_EVENT_WINDOW_SEC = 7200           # 2h antes del evento: modo defensivo


class EventDrivenStrategy(BaseStrategy):
    """Estrategia de momentum impulsada por eventos economicos y noticias.

    En modo defensivo (pre-evento): solo aumenta spreads pasivamente.
    En modo momentum (post-evento): entra en direccion del movimiento.
    """

    def __init__(self, client: Any, config: dict[str, Any], **kwargs) -> None:
        super().__init__("event_driven", client, config, **kwargs)
        ed_cfg = config.get("event_driven", {})
        self._max_position = ed_cfg.get("max_position", 20.0)
        self._momentum_delay = ed_cfg.get("momentum_delay_sec", _MOMENTUM_ENTRY_DELAY_SEC)
        self._pre_event_window = ed_cfg.get("pre_event_window_sec", _PRE_EVENT_WINDOW_SEC)

        # Estado de eventos activos
        # {event_name: {"event_time": float, "direction": str, "entered": bool}}
        self._active_events: dict[str, dict[str, Any]] = {}

        # FREDClient (opcional — funciona sin el)
        self._fred = None
        try:
            from src.external.fred import FREDClient
            self._fred = FREDClient()
        except ImportError:
            pass

    # ------------------------------------------------------------------
    # BaseStrategy interface
    # ------------------------------------------------------------------

    def should_act(self, market_data: dict[str, Any]) -> bool:
        """Actua si hay eventos activos o proximos."""
        return bool(self._active_events) or self._is_high_impact_window()

    def evaluate(self, market_data: dict[str, Any]) -> list[Signal]:
        """Genera signals basados en eventos activos."""
        signals = []
        now = time.time()

        for event_name, event in list(self._active_events.items()):
            event_time = event["event_time"]
            elapsed = now - event_time

            # Post-evento: entrar con momentum despues del delay
            if (elapsed >= self._momentum_delay
                    and not event.get("entered")
                    and elapsed < self._mean_reversion_delay()):

                signal = self._build_momentum_signal(
                    event_name, event, market_data
                )
                if signal:
                    signals.append(signal)
                    event["entered"] = True

            # Limpiar eventos muy antiguos
            elif elapsed > 600:  # 10 minutos
                del self._active_events[event_name]

        return signals

    def execute(self, signals: list[Signal], market_data: dict[str, Any]) -> list[Trade]:
        """Ejecuta signals de momentum."""
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
                        post_only=False,
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
            except Exception:
                self._logger.exception("Error ejecutando event_driven signal")
        return trades

    # ------------------------------------------------------------------
    # Control de eventos
    # ------------------------------------------------------------------

    def register_event(
        self,
        event_name: str,
        event_time: float,
        direction: str = "up",
        magnitude: str = "medium",
    ) -> None:
        """Registra un evento para tracking de momentum.

        Args:
            event_name: Nombre del evento (ej. "CPI_2026_04").
            event_time: Timestamp unix del evento.
            direction: "up" o "down" (prediccion de direccion).
            magnitude: "small" | "medium" | "large".
        """
        self._active_events[event_name] = {
            "event_time": event_time,
            "direction": direction,
            "magnitude": magnitude,
            "entered": False,
        }
        logger.info("Evento registrado: %s en %.0fs", event_name, event_time - time.time())

    def get_spread_multiplier(self) -> float:
        """Retorna el multiplicador de spread recomendado.

        2.5x si hay evento de alto impacto proximas 2h, 1.0 en caso contrario.
        """
        if self._is_high_impact_window():
            return _SPREAD_MULTIPLIER_PRE_EVENT
        return 1.0

    # ------------------------------------------------------------------
    # Internos
    # ------------------------------------------------------------------

    def _is_high_impact_window(self) -> bool:
        """Verifica con FREDClient si hay evento de alto impacto proximo."""
        if self._fred is None:
            return False
        try:
            return self._fred.is_high_impact_window(hours_before=2.0)
        except Exception:
            return False

    def _mean_reversion_delay(self) -> float:
        return _MEAN_REVERSION_DELAY_SEC

    def _build_momentum_signal(
        self,
        event_name: str,
        event: dict[str, Any],
        market_data: dict[str, Any],
    ) -> Signal | None:
        """Construye una senal de momentum basada en el evento."""
        tokens = market_data.get("tokens", [])
        if not tokens:
            return None

        condition_id = market_data.get("condition_id", "")
        token_id = tokens[0].get("token_id", "")
        if not token_id:
            return None

        mid = market_data.get("mid_price", 0.5)
        direction = event.get("direction", "up")
        magnitude = event.get("magnitude", "medium")

        size_mult = {"small": 0.5, "medium": 1.0, "large": 1.5}.get(magnitude, 1.0)
        size = min(self._max_position * size_mult, self._max_position)

        if direction == "up":
            side = "BUY"
            price = min(mid + 0.02, 0.95)
        else:
            side = "SELL"
            price = max(mid - 0.02, 0.05)

        return Signal(
            strategy_name=self.name,
            market_id=condition_id,
            token_id=token_id,
            side=side,
            price=price,
            size=size,
            reason=f"event_momentum: {event_name} direction={direction}",
        )
