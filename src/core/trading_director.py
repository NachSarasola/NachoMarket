"""TradingLogicDirector: coalesce eventos WebSocket en ciclos de decision.

Problema: si el WS emite 3 price_changes del mismo mercado en 500ms,
no queremos ejecutar el pipeline de trading 3 veces.

Solucion: cola de eventos por mercado con intervalo minimo entre ciclos.
Los eventos que llegan durante la ventana de cooldown se acumulan
y se procesan en el siguiente batch.
"""

from __future__ import annotations

import threading
import time
from collections import deque
from typing import Any


class TradingLogicDirector:
    """Orquestrador que coalesce eventos en ciclos de decision."""

    MIN_INTERVAL_SEC = 0.75

    def __init__(self, min_interval_sec: float = MIN_INTERVAL_SEC) -> None:
        self._min_interval = min_interval_sec
        self._market_queues: dict[str, deque[dict[str, Any]]] = {}
        self._last_cycle: dict[str, float] = {}
        self._lock = threading.Lock()

    def on_market_event(self, market_id: str, event: dict[str, Any]) -> bool:
        """Registra un evento y retorna True si se debe procesar ahora.

        Si el mercado esta en cooldown, el evento se encola y
        se procesara en el siguiente batch.
        """
        with self._lock:
            now = time.time()
            last = self._last_cycle.get(market_id, 0.0)

            if now - last >= self._min_interval:
                self._last_cycle[market_id] = now
                pooled = self._drain_queue(market_id)
                pooled.append(event)
                self._market_queues[market_id] = deque(pooled)
                return True

            if market_id not in self._market_queues:
                self._market_queues[market_id] = deque()
            self._market_queues[market_id].append(event)
            return False

    def get_pending_events(self, market_id: str) -> list[dict[str, Any]]:
        """Retorna eventos pendientes y limpia la cola."""
        with self._lock:
            events = self._drain_queue(market_id)
            self._market_queues.pop(market_id, None)
            return events

    def get_pending_markets(self) -> list[str]:
        """Retorna mercados con eventos pendientes listos para procesar."""
        with self._lock:
            now = time.time()
            ready: list[str] = []
            for mid, last in list(self._last_cycle.items()):
                if now - last >= self._min_interval:
                    if mid in self._market_queues:
                        ready.append(mid)
            return ready

    def _drain_queue(self, market_id: str) -> list[dict[str, Any]]:
        q = self._market_queues.get(market_id)
        if q is None:
            return []
        items = list(q)
        q.clear()
        return items

    def cleanup_stale(self, max_age_sec: float = 300.0) -> int:
        """Elimina colas de eventos de mercados inactivos."""
        removed = 0
        with self._lock:
            now = time.time()
            stale_mids = [
                mid for mid, last in self._last_cycle.items()
                if now - last > max_age_sec and mid not in self._market_queues
            ]
            for mid in stale_mids:
                self._last_cycle.pop(mid, None)
                self._market_queues.pop(mid, None)
                removed += 1
        return removed
