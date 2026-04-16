"""
Reposicionamiento post-fill: despues de un fill, coloca orden limite en entry+1c.

Logica:
- BUY fill a precio X → coloca SELL limite a X + reposition_offset
- SELL fill a precio X → coloca BUY limite a X - reposition_offset
- Si la orden de reposicionamiento no se ejecuta en timeout_min, la cancela

Esta estrategia captura spread adicional en cada round-trip:
- Con order_size=$15 y offset=$0.01, cada reposicion exitosa gana $0.15
- Sobre 12 mercados con varios fills al dia, el impacto es significativo
"""

import logging
import time
from dataclasses import dataclass, field
from typing import Any

from src.strategy.base import Signal, Trade

logger = logging.getLogger("nachomarket.strategy.repositioner")


@dataclass
class PendingReposition:
    """Una orden de reposicionamiento pendiente."""
    original_order_id: str
    original_side: str       # "BUY" o "SELL"
    fill_price: float
    fill_size: float
    token_id: str
    market_id: str
    reposition_order_id: str = ""   # Seteado despues de colocar la orden
    placed_at: float = field(default_factory=time.time)
    expiry_minutes: float = 90.0


class FillRepositioner:
    """Gestiona reposicionamientos post-fill.

    Uso:
        repositioner = FillRepositioner(config)

        # Cuando se detecta un fill:
        signal = repositioner.on_fill(trade)
        # Si hay signal, ejecutarla

        # En cada ciclo:
        expired_ids = repositioner.check_expirations()
        # Cancelar las ordenes expiradas

        # Cuando se confirma fill de una reposicion:
        profit = repositioner.on_reposition_filled(order_id, fill_price)
    """

    def __init__(self, config: dict[str, Any]) -> None:
        mm = config.get("market_maker", {})
        self._enabled = mm.get("repositioning_enabled", True)
        self._offset = mm.get("reposition_offset", 0.01)
        self._timeout_min = mm.get("reposition_timeout_min", 90.0)
        self._min_price = 0.02   # No colocar ordenes por debajo de 2c
        self._max_price = 0.98   # No colocar ordenes por encima de 98c

        # Pending por original_order_id
        self._pending: dict[str, PendingReposition] = {}
        # Pending por reposition_order_id (para lookup rapido en on_reposition_filled)
        self._by_reposition_id: dict[str, str] = {}

    # ------------------------------------------------------------------
    # API principal
    # ------------------------------------------------------------------

    def on_fill(self, trade: Trade) -> Signal | None:
        """Genera una Signal de reposicionamiento cuando se detecta un fill.

        Args:
            trade: El trade que se filloe (side=BUY o SELL).

        Returns:
            Signal con la orden de reposicionamiento, o None si no aplica.
        """
        if not self._enabled:
            return None

        if trade.status in ("error", "rejected"):
            return None

        if trade.side == "BUY":
            reposition_side = "SELL"
            reposition_price = round(trade.price + self._offset, 4)
        elif trade.side == "SELL":
            reposition_side = "BUY"
            reposition_price = round(trade.price - self._offset, 4)
        else:
            return None

        # Validar precio dentro de rango valido
        if not (self._min_price <= reposition_price <= self._max_price):
            logger.debug(
                f"Reposition price {reposition_price:.4f} out of range, skipping"
            )
            return None

        # Registrar reposicion pendiente
        pending = PendingReposition(
            original_order_id=trade.order_id,
            original_side=trade.side,
            fill_price=trade.price,
            fill_size=trade.size,
            token_id=trade.token_id,
            market_id=trade.market_id,
            placed_at=time.time(),
            expiry_minutes=self._timeout_min,
        )
        self._pending[trade.order_id] = pending

        logger.info(
            f"Reposition queued: {reposition_side} {trade.size} @ {reposition_price:.4f} "
            f"(fill was {trade.side} @ {trade.price:.4f}, +${self._offset})"
        )

        return Signal(
            market_id=trade.market_id,
            token_id=trade.token_id,
            side=reposition_side,
            price=reposition_price,
            size=trade.size,
            confidence=0.8,
            strategy_name="repositioner",
            metadata={"original_order_id": trade.order_id, "is_reposition": True},
        )

    def register_reposition_order(
        self, original_order_id: str, reposition_order_id: str
    ) -> None:
        """Registra el order_id de la orden de reposicionamiento colocada."""
        if original_order_id in self._pending:
            self._pending[original_order_id].reposition_order_id = reposition_order_id
            self._by_reposition_id[reposition_order_id] = original_order_id

    def on_reposition_filled(
        self, reposition_order_id: str, fill_price: float | None = None
    ) -> float | None:
        """Procesa el fill de una orden de reposicionamiento.

        Returns:
            PnL del round-trip (positivo = ganancia), o None si no encontrado.
        """
        orig_id = self._by_reposition_id.pop(reposition_order_id, None)
        if orig_id is None:
            return None

        pending = self._pending.pop(orig_id, None)
        if pending is None:
            return None

        actual_fill = fill_price if fill_price is not None else (
            pending.fill_price + self._offset
            if pending.original_side == "BUY"
            else pending.fill_price - self._offset
        )

        if pending.original_side == "BUY":
            round_trip_pnl = (actual_fill - pending.fill_price) * pending.fill_size
        else:
            round_trip_pnl = (pending.fill_price - actual_fill) * pending.fill_size

        logger.info(
            f"Reposition filled: round-trip PnL = ${round_trip_pnl:.4f} "
            f"(bought @ {pending.fill_price:.4f}, sold @ {actual_fill:.4f}, "
            f"size={pending.fill_size})"
        )
        return round_trip_pnl

    def check_expirations(self) -> list[str]:
        """Retorna IDs de ordenes de reposicionamiento que expiraron.

        Deben ser canceladas por el caller.
        """
        now = time.time()
        expired_reposition_ids: list[str] = []

        expired_orig_ids = [
            orig_id
            for orig_id, p in self._pending.items()
            if now - p.placed_at > p.expiry_minutes * 60
        ]

        for orig_id in expired_orig_ids:
            pending = self._pending.pop(orig_id)
            if pending.reposition_order_id:
                self._by_reposition_id.pop(pending.reposition_order_id, None)
                expired_reposition_ids.append(pending.reposition_order_id)
                logger.info(
                    f"Reposition expired after {self._timeout_min}min: "
                    f"{pending.reposition_order_id[:12]}..."
                )

        return expired_reposition_ids

    @property
    def pending_count(self) -> int:
        """Numero de reposiciones pendientes."""
        return len(self._pending)
