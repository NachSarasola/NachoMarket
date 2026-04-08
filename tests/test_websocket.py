"""Tests para el modulo OrderbookFeed (sin conexion real a Polymarket)."""

import asyncio
import json
import threading
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.polymarket.websocket import (
    OrderbookFeed,
    OrderbookState,
    _apply_level_update,
    _compute_depth,
    _compute_midpoint,
    _parse_levels,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_feed(*token_ids: str) -> OrderbookFeed:
    feed = OrderbookFeed()
    for tid in token_ids:
        feed.subscribe(tid, callback=MagicMock(), condition_id=f"cond_{tid[:4]}")
    return feed


def make_book_event(token_id: str, bids: list, asks: list, sequence: int = 1) -> dict:
    return {
        "event_type": "book",
        "asset_id": token_id,
        "sequence": sequence,
        "bids": [{"price": str(p), "size": str(s)} for p, s in bids],
        "asks": [{"price": str(p), "size": str(s)} for p, s in asks],
    }


def make_price_event(token_id: str, side: str, price: float, size: float) -> dict:
    return {
        "event_type": "price_change",
        "asset_id": token_id,
        "side": side,
        "price": str(price),
        "size": str(size),
    }


# ---------------------------------------------------------------------------
# Tests: helpers funcionales
# ---------------------------------------------------------------------------

class TestParseLevels:
    def test_parses_price_size_dicts(self) -> None:
        raw = [{"price": "0.5", "size": "100"}, {"price": "0.4", "size": "50"}]
        levels = _parse_levels(raw, reverse=True)
        assert levels == [(0.5, 100.0), (0.4, 50.0)]

    def test_sorted_desc_for_bids(self) -> None:
        raw = [{"price": "0.3", "size": "10"}, {"price": "0.6", "size": "20"}]
        levels = _parse_levels(raw, reverse=True)
        assert levels[0][0] > levels[1][0]

    def test_sorted_asc_for_asks(self) -> None:
        raw = [{"price": "0.7", "size": "10"}, {"price": "0.6", "size": "20"}]
        levels = _parse_levels(raw, reverse=False)
        assert levels[0][0] < levels[1][0]

    def test_skips_zero_size(self) -> None:
        raw = [{"price": "0.5", "size": "0"}, {"price": "0.4", "size": "10"}]
        levels = _parse_levels(raw, reverse=True)
        assert len(levels) == 1
        assert levels[0] == (0.4, 10.0)

    def test_skips_invalid_entries(self) -> None:
        raw = [{"price": "bad", "size": "10"}, {"price": "0.5", "size": "100"}]
        levels = _parse_levels(raw, reverse=True)
        assert len(levels) == 1

    def test_empty_list(self) -> None:
        assert _parse_levels([], reverse=True) == []


class TestComputeMidpoint:
    def test_normal_case(self) -> None:
        bids = [(0.48, 100.0), (0.45, 50.0)]
        asks = [(0.52, 100.0), (0.55, 50.0)]
        assert _compute_midpoint(bids, asks) == pytest.approx(0.50)

    def test_empty_bids(self) -> None:
        assert _compute_midpoint([], [(0.5, 10.0)]) == 0.0

    def test_empty_asks(self) -> None:
        assert _compute_midpoint([(0.5, 10.0)], []) == 0.0

    def test_crossed_book_returns_zero(self) -> None:
        # best_bid >= best_ask = datos invalidos
        bids = [(0.6, 100.0)]
        asks = [(0.5, 100.0)]
        assert _compute_midpoint(bids, asks) == 0.0


class TestComputeDepth:
    def test_sums_top_n_levels(self) -> None:
        bids = [(0.5, 100.0), (0.49, 80.0), (0.48, 60.0)]
        asks = [(0.51, 90.0), (0.52, 70.0), (0.53, 50.0)]
        # Top 5 (todos): 100+80+60+90+70+50 = 450
        depth = _compute_depth(bids, asks, levels=5)
        assert depth == pytest.approx(450.0)

    def test_limits_to_n_levels(self) -> None:
        bids = [(0.5, 100.0), (0.49, 80.0), (0.48, 60.0)]
        asks = [(0.51, 90.0), (0.52, 70.0), (0.53, 50.0)]
        depth = _compute_depth(bids, asks, levels=2)
        assert depth == pytest.approx(100.0 + 80.0 + 90.0 + 70.0)

    def test_empty_books(self) -> None:
        assert _compute_depth([], []) == 0.0


class TestApplyLevelUpdate:
    def test_adds_new_level(self) -> None:
        levels = [(0.5, 100.0), (0.4, 50.0)]
        updated = _apply_level_update(levels, 0.45, 75.0, reverse=True)
        prices = [p for p, _ in updated]
        assert 0.45 in prices
        assert prices == sorted(prices, reverse=True)

    def test_updates_existing_level(self) -> None:
        levels = [(0.5, 100.0), (0.4, 50.0)]
        updated = _apply_level_update(levels, 0.5, 200.0, reverse=True)
        assert dict(updated)[0.5] == 200.0
        assert len(updated) == 2

    def test_removes_level_when_size_zero(self) -> None:
        levels = [(0.5, 100.0), (0.4, 50.0)]
        updated = _apply_level_update(levels, 0.5, 0.0, reverse=True)
        prices = [p for p, _ in updated]
        assert 0.5 not in prices
        assert len(updated) == 1


# ---------------------------------------------------------------------------
# Tests: OrderbookFeed (sin IO real)
# ---------------------------------------------------------------------------

class TestOrderbookFeedInit:
    def test_initial_state(self) -> None:
        feed = OrderbookFeed()
        assert feed._subscriptions == {}
        assert feed._orderbooks == {}
        assert feed._running is False
        assert feed._connected is False

    def test_from_config_empty_watch(self, tmp_path) -> None:
        config = tmp_path / "markets.yaml"
        config.write_text("watch_tokens: []\n")
        feed = OrderbookFeed.from_config(str(config))
        assert feed._subscriptions == {}

    def test_from_config_with_tokens(self, tmp_path) -> None:
        config = tmp_path / "markets.yaml"
        config.write_text(
            "watch_tokens:\n"
            "  - token_id: abc123\n"
            "    condition_id: cond456\n"
        )
        feed = OrderbookFeed.from_config(str(config))
        assert "abc123" in feed._subscriptions


class TestSubscribe:
    def test_subscribe_creates_entry(self) -> None:
        feed = OrderbookFeed()
        cb = MagicMock()
        feed.subscribe("tok1", cb, condition_id="cond1")
        assert "tok1" in feed._subscriptions
        assert feed._orderbooks["tok1"].token_id == "tok1"

    def test_subscribe_multiple_callbacks(self) -> None:
        feed = OrderbookFeed()
        cb1, cb2 = MagicMock(), MagicMock()
        feed.subscribe("tok1", cb1, condition_id="cond1")
        feed.subscribe("tok1", cb2, condition_id="cond1")
        assert len(feed._subscriptions["tok1"]) == 2

    def test_unsubscribe_removes_entry(self) -> None:
        feed = make_feed("tok1", "tok2")
        feed.unsubscribe("tok1")
        assert "tok1" not in feed._subscriptions
        assert "tok1" not in feed._orderbooks
        assert "tok2" in feed._subscriptions  # Otro no afectado


class TestGetOrderbook:
    def test_get_returns_none_before_data(self) -> None:
        feed = OrderbookFeed()
        assert feed.get_orderbook("unknown") is None

    def test_get_midpoint_returns_none_before_data(self) -> None:
        feed = make_feed("tok1")
        assert feed.get_midpoint("tok1") is None

    def test_get_orderbook_thread_safe(self) -> None:
        """Verifica que get_orderbook no falla bajo acceso concurrente."""
        feed = make_feed("tok1")
        errors = []

        def reader():
            for _ in range(100):
                try:
                    feed.get_orderbook("tok1")
                    feed.get_midpoint("tok1")
                except Exception as e:
                    errors.append(e)

        threads = [threading.Thread(target=reader) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == []


class TestMessageProcessing:
    """Tests del procesamiento de mensajes sin conexion real."""

    @pytest.mark.asyncio
    async def test_book_snapshot_updates_orderbook(self) -> None:
        feed = make_feed("tok1")
        event = make_book_event(
            "tok1",
            bids=[(0.48, 100.0), (0.45, 50.0)],
            asks=[(0.52, 80.0), (0.55, 30.0)],
        )
        await feed._handle_book_snapshot("tok1", event)

        ob = feed.get_orderbook("tok1")
        assert ob is not None
        assert ob.midpoint == pytest.approx(0.50)
        assert len(ob.bids) == 2
        assert len(ob.asks) == 2
        assert ob.depth > 0

    @pytest.mark.asyncio
    async def test_book_snapshot_fires_init_callback(self) -> None:
        cb = MagicMock()
        feed = OrderbookFeed()
        feed.subscribe("tok1", cb, condition_id="cond1")
        event = make_book_event("tok1", bids=[(0.48, 100.0)], asks=[(0.52, 80.0)])
        await feed._handle_book_snapshot("tok1", event)
        cb.assert_called_once()
        _, _, change_type = cb.call_args[0]
        assert change_type == "book_init"

    @pytest.mark.asyncio
    async def test_price_change_updates_bid_level(self) -> None:
        feed = make_feed("tok1")
        # Seed con un snapshot
        await feed._handle_book_snapshot(
            "tok1",
            make_book_event("tok1", bids=[(0.48, 100.0)], asks=[(0.52, 80.0)]),
        )
        # Actualizar nivel bid
        event = make_price_event("tok1", "BUY", 0.48, 150.0)
        await feed._handle_price_change("tok1", event)

        ob = feed.get_orderbook("tok1")
        bid_dict = dict(ob.bids)
        assert bid_dict[0.48] == pytest.approx(150.0)

    @pytest.mark.asyncio
    async def test_price_change_removes_level_on_zero_size(self) -> None:
        feed = make_feed("tok1")
        await feed._handle_book_snapshot(
            "tok1",
            make_book_event("tok1", bids=[(0.48, 100.0), (0.45, 50.0)], asks=[(0.52, 80.0)]),
        )
        # Size 0 = eliminar nivel
        event = make_price_event("tok1", "BUY", 0.48, 0.0)
        await feed._handle_price_change("tok1", event)

        ob = feed.get_orderbook("tok1")
        prices = [p for p, _ in ob.bids]
        assert 0.48 not in prices
        assert 0.45 in prices

    @pytest.mark.asyncio
    async def test_list_message_dispatches_all_events(self) -> None:
        feed = make_feed("tok1", "tok2")
        events = [
            make_book_event("tok1", bids=[(0.48, 100.0)], asks=[(0.52, 80.0)]),
            make_book_event("tok2", bids=[(0.30, 200.0)], asks=[(0.70, 150.0)]),
        ]
        await feed._process_message(json.dumps(events))

        assert feed.get_orderbook("tok1") is not None
        assert feed.get_orderbook("tok2") is not None

    @pytest.mark.asyncio
    async def test_unknown_token_ignored(self) -> None:
        feed = make_feed("tok1")
        # Mensaje de token no suscrito
        event = make_book_event("unknown_tok", bids=[(0.5, 100.0)], asks=[(0.6, 80.0)])
        await feed._process_message(json.dumps(event))
        assert feed.get_orderbook("unknown_tok") is None

    @pytest.mark.asyncio
    async def test_invalid_json_does_not_crash(self) -> None:
        feed = make_feed("tok1")
        await feed._process_message("not valid json{{{{")  # No debe lanzar


class TestSignificantChanges:
    @pytest.mark.asyncio
    async def test_midpoint_callback_fired_on_large_change(self) -> None:
        cb = MagicMock()
        feed = OrderbookFeed()
        feed.subscribe("tok1", cb, condition_id="cond1")

        # Estado inicial: mid = 0.50
        await feed._handle_book_snapshot(
            "tok1", make_book_event("tok1", bids=[(0.48, 100.0)], asks=[(0.52, 80.0)])
        )
        cb.reset_mock()

        # Nuevo estado: mid = 0.58 → cambio 16% > umbral 2%
        await feed._handle_book_snapshot(
            "tok1", make_book_event("tok1", bids=[(0.56, 100.0)], asks=[(0.60, 80.0)])
        )
        cb.assert_called()
        _, _, change_type = cb.call_args[0]
        assert change_type == "midpoint"

    @pytest.mark.asyncio
    async def test_midpoint_callback_not_fired_on_small_change(self) -> None:
        cb = MagicMock()
        feed = OrderbookFeed()
        feed.subscribe("tok1", cb, condition_id="cond1")

        await feed._handle_book_snapshot(
            "tok1", make_book_event("tok1", bids=[(0.48, 100.0)], asks=[(0.52, 80.0)])
        )
        cb.reset_mock()

        # Nuevo estado: mid = 0.505 → cambio 1% < umbral 2%
        await feed._handle_book_snapshot(
            "tok1", make_book_event("tok1", bids=[(0.49, 100.0)], asks=[(0.52, 80.0)])
        )
        cb.assert_not_called()

    @pytest.mark.asyncio
    async def test_depth_callback_fired_on_large_change(self) -> None:
        cb = MagicMock()
        feed = OrderbookFeed()
        feed.subscribe("tok1", cb, condition_id="cond1")

        # Estado inicial: depth = 180 (100+80)
        await feed._handle_book_snapshot(
            "tok1", make_book_event("tok1", bids=[(0.48, 100.0)], asks=[(0.52, 80.0)])
        )
        cb.reset_mock()

        # Nuevo estado: depth = 20 (10+10) → cambio >10%
        await feed._handle_book_snapshot(
            "tok1", make_book_event("tok1", bids=[(0.48, 10.0)], asks=[(0.52, 10.0)])
        )
        cb.assert_called()
        _, _, change_type = cb.call_args[0]
        assert change_type == "depth"

    @pytest.mark.asyncio
    async def test_async_callback_is_awaited(self) -> None:
        """Verifica que callbacks async son awaited correctamente."""
        called_with = []

        async def async_cb(token_id, ob, change_type):
            called_with.append((token_id, change_type))

        feed = OrderbookFeed()
        feed.subscribe("tok1", async_cb, condition_id="cond1")
        await feed._handle_book_snapshot(
            "tok1", make_book_event("tok1", bids=[(0.48, 100.0)], asks=[(0.52, 80.0)])
        )
        assert ("tok1", "book_init") in called_with


class TestStopStart:
    @pytest.mark.asyncio
    async def test_stop_sets_running_false(self) -> None:
        feed = OrderbookFeed()
        feed._running = True
        await feed.stop()
        assert feed._running is False
