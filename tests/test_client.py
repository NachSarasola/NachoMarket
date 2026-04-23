import json
from pathlib import Path

import pytest

from src.polymarket.client import PolymarketClient


@pytest.fixture
def client(tmp_path: Path) -> PolymarketClient:
    c = PolymarketClient(paper_mode=True)
    c._trades_file = tmp_path / "trades.jsonl"
    return c


class TestInit:
    def test_paper_mode_no_live_client(self, client: PolymarketClient) -> None:
        assert client.paper_mode is True
        assert client._client is None

    def test_signature_type_stored(self, client: PolymarketClient) -> None:
        assert client._signature_type == 1


class TestConnection:
    def test_test_connection_paper(self, client: PolymarketClient) -> None:
        assert client.test_connection() is True


class TestMarkets:
    def test_get_markets_paper_returns_empty(self, client: PolymarketClient) -> None:
        assert client.get_markets() == []

    def test_get_markets_paginates_on_cursor(self, client: PolymarketClient) -> None:
        # Paper mode siempre retorna lista vacia independientemente del cursor
        assert client.get_markets(next_cursor="abc") == []


class TestOrderbook:
    def test_get_orderbook_paper(self, client: PolymarketClient) -> None:
        book = client.get_orderbook("test_token")
        assert "bids" in book
        assert "asks" in book
        assert book["token_id"] == "test_token"

    def test_get_midpoint_paper(self, client: PolymarketClient) -> None:
        mid = client.get_midpoint("test_token")
        assert mid == 0.5

    def test_get_tick_size_paper(self, client: PolymarketClient) -> None:
        tick = client.get_tick_size("test_token")
        assert tick == "0.01"

    def test_get_fee_rate_paper_zero(self, client: PolymarketClient) -> None:
        fee = client.get_fee_rate("test_token")
        assert fee == 0


class TestBalance:
    def test_get_balance_paper(self, client: PolymarketClient) -> None:
        balance = client.get_balance()
        assert balance == 300.0  # Default paper_capital

    def test_get_balance_paper_custom_capital(self, tmp_path: Path) -> None:
        c = PolymarketClient(paper_mode=True, paper_capital=175.0)
        c._trades_file = tmp_path / "trades.jsonl"
        assert c.get_balance() == 175.0

    def test_get_positions_paper_empty(self, client: PolymarketClient) -> None:
        positions = client.get_positions()
        assert positions == []


class TestLimitOrder:
    def test_place_limit_order_paper_buy(self, client: PolymarketClient) -> None:
        result = client.place_limit_order("token_abc123", "BUY", 0.45, 5.0)

        assert result["status"] == "filled_paper"
        assert result["side"] == "BUY"
        assert result["price"] == 0.45
        assert result["size"] == 5.0
        assert result["post_only"] is True
        assert "order_id" in result
        assert result["order_id"].startswith("paper_")

    def test_place_limit_order_paper_sell(self, client: PolymarketClient) -> None:
        result = client.place_limit_order("token_abc123", "SELL", 0.55, 10.0, post_only=False)
        assert result["side"] == "SELL"
        assert result["post_only"] is False

    def test_place_limit_order_logs_to_jsonl(self, client: PolymarketClient) -> None:
        client.place_limit_order("token_xyz", "BUY", 0.3, 20.0)
        client.place_limit_order("token_xyz", "SELL", 0.7, 20.0)

        lines = client._trades_file.read_text().strip().split("\n")
        assert len(lines) == 2

        first = json.loads(lines[0])
        assert first["token_id"] == "token_xyz"
        assert first["type"] == "limit"
        assert first["paper_mode"] is True
        assert "timestamp" in first

    def test_place_limit_order_generates_unique_ids(self, client: PolymarketClient) -> None:
        r1 = client.place_limit_order("tok", "BUY", 0.4, 5.0)
        r2 = client.place_limit_order("tok", "BUY", 0.4, 5.0)
        # Los IDs deben ser distintos (timestamp en ms)
        assert r1["order_id"] != r2["order_id"]


class TestCancel:
    def test_cancel_order_paper(self, client: PolymarketClient) -> None:
        assert client.cancel_order("order_123") is True

    def test_cancel_all_orders_paper(self, client: PolymarketClient) -> None:
        assert client.cancel_all_orders() is True


class TestMergePositions:
    def test_merge_positions_paper(self, client: PolymarketClient) -> None:
        result = client.merge_positions("token_abc", 15.0)
        assert result["status"] == "merged_paper"
        assert result["token_id"] == "token_abc"
        assert result["size"] == 15.0
