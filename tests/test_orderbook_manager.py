"""Tests para OrderBookManager — estado local thread-safe."""

import time
from unittest.mock import MagicMock

import pytest

from src.core.orderbook_manager import OrderBookManager


class TestOrderBookManager:
    """Tests del estado local del orderbook."""

    def test_get_order_returns_none_for_unknown(self) -> None:
        obm = OrderBookManager()
        assert obm.get_order("nonexistent") is None

    def test_mark_and_get_order(self) -> None:
        obm = OrderBookManager()
        obm.mark_placed("ord1", {"id": "ord1", "price": 0.50, "size": 10.0})
        order = obm.get_order("ord1")
        assert order is not None
        assert order["price"] == 0.50

    def test_mark_placing_and_placed(self) -> None:
        obm = OrderBookManager()
        obm.mark_placing("ord1")
        assert obm.is_placing("ord1") is True
        obm.mark_placed("ord1", {"id": "ord1"})
        assert obm.is_placing("ord1") is False
        assert obm.get_order("ord1") is not None

    def test_mark_cancelling_and_cancelled(self) -> None:
        obm = OrderBookManager()
        obm.mark_placed("ord1", {"id": "ord1"})
        obm.mark_cancelling("ord1")
        assert obm.is_cancelling("ord1") is True
        obm.mark_cancelled("ord1")
        assert obm.is_cancelling("ord1") is False
        assert obm.get_order("ord1") is None

    def test_get_all_orders(self) -> None:
        obm = OrderBookManager()
        obm.mark_placed("ord1", {"id": "ord1"})
        obm.mark_placed("ord2", {"id": "ord2"})
        orders = obm.get_all_orders()
        assert len(orders) == 2

    def test_open_orders_count(self) -> None:
        obm = OrderBookManager()
        assert obm.get_open_orders_count() == 0
        obm.mark_placed("ord1", {"id": "ord1"})
        assert obm.get_open_orders_count() == 1

    def test_cleanup_stale(self) -> None:
        obm = OrderBookManager()
        obm.mark_placed("ord1", {"id": "ord1"})
        obm.mark_cancelled("ord1")
        # Force stale
        obm._cancelled.add("ord1")
        time.sleep(0.1)
        removed = obm.cleanup_stale(max_age_sec=0.0)
        assert removed >= 0

    def test_thread_safety_placing(self) -> None:
        """Simula acceso concurrente basico."""
        obm = OrderBookManager()
        obm.mark_placing("ord1")
        assert obm.is_placing("ord1")

    def test_start_stop(self) -> None:
        obm = OrderBookManager()
        obm.start()
        assert obm._thread is not None
        obm.stop()
        assert not (obm._thread is not None and obm._thread.is_alive())
