import json
from pathlib import Path
from unittest.mock import patch

import pytest

from src.polymarket.client import PolymarketClient


class TestPolymarketClient:
    """Tests para el cliente de Polymarket."""

    def setup_method(self) -> None:
        self.client = PolymarketClient(paper_mode=True)

    def test_paper_mode_init(self) -> None:
        assert self.client.paper_mode is True
        assert self.client._client is None

    def test_get_markets_paper(self) -> None:
        markets = self.client.get_markets()
        assert markets == []

    def test_get_orderbook_paper(self) -> None:
        book = self.client.get_orderbook("test_token")
        assert book == {"bids": [], "asks": []}

    def test_get_price_paper(self) -> None:
        price = self.client.get_price("test_token")
        assert price == 0.5

    def test_get_balance_paper(self) -> None:
        balance = self.client.get_balance()
        assert balance == 400.0

    def test_place_order_paper(self, tmp_path: Path) -> None:
        self.client._trades_file = tmp_path / "trades.jsonl"
        result = self.client.place_order(
            token_id="test_token_123",
            side="BUY",
            price=0.45,
            size=5.0,
        )
        assert result is not None
        assert result["status"] == "filled_paper"
        assert result["side"] == "BUY"
        assert result["price"] == 0.45

        # Verificar que se logueo
        trades = self.client._trades_file.read_text().strip().split("\n")
        assert len(trades) == 1
        trade = json.loads(trades[0])
        assert trade["token_id"] == "test_token_123"

    def test_cancel_order_paper(self) -> None:
        result = self.client.cancel_order("test_order_id")
        assert result is True

    def test_cancel_all_orders_paper(self) -> None:
        result = self.client.cancel_all_orders()
        assert result is True

    def test_get_fee_rate_paper(self) -> None:
        fee = self.client.get_fee_rate_bps()
        assert fee == 0
