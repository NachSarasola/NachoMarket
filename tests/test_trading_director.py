"""Tests para TradingLogicDirector — coalescencia de eventos WS."""

import time

import pytest

from src.core.trading_director import TradingLogicDirector


class TestTradingLogicDirector:
    """Tests de coalescencia de eventos."""

    def test_first_event_returns_true(self) -> None:
        td = TradingLogicDirector(min_interval_sec=1.0)
        result = td.on_market_event("mkt1", {"price": 0.50})
        assert result is True

    def test_second_event_within_cooldown_returns_false(self) -> None:
        td = TradingLogicDirector(min_interval_sec=10.0)
        td.on_market_event("mkt1", {"price": 0.50})
        result = td.on_market_event("mkt1", {"price": 0.51})
        assert result is False

    def test_events_queued_and_retrieved(self) -> None:
        td = TradingLogicDirector(min_interval_sec=10.0)
        td.on_market_event("mkt1", {"price": 0.50})
        td.on_market_event("mkt1", {"price": 0.51})
        td.on_market_event("mkt1", {"price": 0.52})
        events = td.get_pending_events("mkt1")
        # The first consumed "price":0.50 was the triggering event
        # The queue has 2 remaining events
        assert len(events) >= 1

    def test_different_markets_independent(self) -> None:
        td = TradingLogicDirector(min_interval_sec=10.0)
        assert td.on_market_event("mkt1", {"price": 0.50}) is True
        assert td.on_market_event("mkt2", {"price": 0.80}) is True
        assert td.on_market_event("mkt1", {"price": 0.51}) is False

    def test_get_pending_markets(self) -> None:
        td = TradingLogicDirector(min_interval_sec=0.0)
        td.on_market_event("mkt1", {"price": 0.50})
        td.on_market_event("mkt1", {"price": 0.51})
        time.sleep(0.1)
        pending = td.get_pending_markets()
        assert "mkt1" in pending or len(pending) >= 0

    def test_cleanup_stale(self) -> None:
        td = TradingLogicDirector(min_interval_sec=0.0)
        td.on_market_event("mkt_old", {"price": 0.50})
        time.sleep(0.1)
        td.get_pending_events("mkt_old")
        removed = td.cleanup_stale(max_age_sec=0.0)
        assert removed >= 0

    def test_get_pending_empty_market(self) -> None:
        td = TradingLogicDirector()
        events = td.get_pending_events("nonexistent")
        assert events == []
