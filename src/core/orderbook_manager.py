"""OrderBookManager: estado local del orderbook con refresh en background.

Mantiene copia thread-safe de las ordenes abiertas del bot,
evitando consultas redundantes a la API en cada ciclo de trading.

Trackea:
  - _orders: order_id → order dict
  - _placing: ordenes en proceso de colocacion
  - _cancelling: ordenes en proceso de cancelacion
"""

from __future__ import annotations

import threading
import time
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from src.polymarket.client import PolymarketClient


class OrderBookManager:
    """Estado local del orderbook con refresco en background thread."""

    def __init__(
        self,
        client: PolymarketClient | None = None,
        refresh_interval: float = 5.0,
        condition_id: str = "",
    ) -> None:
        self._client = client
        self._refresh_interval = refresh_interval
        self._condition_id = condition_id
        self._lock = threading.Lock()
        self._orders: dict[str, dict[str, Any]] = {}
        self._placing: set[str] = set()
        self._cancelling: set[str] = set()
        self._cancelled: set[str] = set()
        self._placed: set[str] = set()
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._last_refresh = 0.0

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._refresh_loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5.0)
            self._thread = None

    # --- Query ---

    def get_order(self, order_id: str) -> dict[str, Any] | None:
        with self._lock:
            return self._orders.get(order_id)

    def get_all_orders(self) -> list[dict[str, Any]]:
        with self._lock:
            return list(self._orders.values())

    def get_open_orders_count(self) -> int:
        with self._lock:
            return len(self._orders)

    # --- Marking ---

    def mark_placing(self, order_id: str) -> None:
        with self._lock:
            self._placing.add(order_id)

    def mark_placed(self, order_id: str, order: dict[str, Any]) -> None:
        with self._lock:
            self._placing.discard(order_id)
            self._placed.add(order_id)
            self._orders[order_id] = order

    def mark_cancelling(self, order_id: str) -> None:
        with self._lock:
            self._cancelling.add(order_id)

    def mark_cancelled(self, order_id: str) -> None:
        with self._lock:
            self._cancelling.discard(order_id)
            self._cancelled.add(order_id)
            self._orders.pop(order_id, None)

    def is_placing(self, order_id: str) -> bool:
        with self._lock:
            return order_id in self._placing

    def is_cancelling(self, order_id: str) -> bool:
        with self._lock:
            return order_id in self._cancelling

    def cleanup_stale(self, max_age_sec: float = 300.0) -> int:
        """Elimina ordenes marcadas como cancelled hace mas de max_age_sec."""
        removed = 0
        with self._lock:
            now = time.time()
            stale = {oid for oid in self._cancelled}
            self._cancelled -= stale
            removed = len(stale)
        return removed

    # --- Background refresh ---

    def _refresh_loop(self) -> None:
        while not self._stop.is_set():
            try:
                self._refresh()
            except Exception:
                pass
            self._stop.wait(self._refresh_interval)

    def _refresh(self) -> None:
        if self._client is None:
            return
        try:
            orders = self._client.get_positions()
        except Exception:
            return

        current_ids: set[str] = set()
        with self._lock:
            for order in orders:
                oid = order.get("id", order.get("order_id", ""))
                if not oid:
                    continue
                current_ids.add(oid)
                if oid not in self._orders:
                    self._orders[oid] = order

            known = set(self._orders.keys())
            removed = known - current_ids - self._placing - self._cancelling
            for oid in removed:
                self._orders.pop(oid, None)

        self._last_refresh = time.time()

    @property
    def last_refresh(self) -> float:
        return self._last_refresh
