"""Tests para FillRepositioner — post-fill repositioning logic."""

import time
from unittest.mock import MagicMock, patch

import pytest

from src.strategy.repositioner import FillRepositioner, PendingReposition
from src.strategy.base import Signal, Trade
from datetime import datetime, timezone


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def make_config(**overrides) -> dict:
    base = {
        "market_maker": {
            "repositioning_enabled": True,
            "reposition_offset": 0.01,
            "reposition_timeout_min": 90.0,
        }
    }
    if overrides:
        base["market_maker"].update(overrides)
    return base


def make_trade(
    side: str = "BUY",
    price: float = 0.50,
    size: float = 15.0,
    token_id: str = "tok1",
    market_id: str = "mkt1",
    order_id: str = "ord_001",
    status: str = "filled_paper",
) -> Trade:
    return Trade(
        timestamp=datetime.now(timezone.utc).isoformat(),
        market_id=market_id,
        token_id=token_id,
        side=side,
        price=price,
        size=size,
        order_id=order_id,
        status=status,
        strategy_name="market_maker",
        fee_paid=0.0,
    )


# ------------------------------------------------------------------
# Tests: on_fill
# ------------------------------------------------------------------

class TestOnFill:
    def setup_method(self) -> None:
        self.repo = FillRepositioner(make_config())

    def test_buy_fill_generates_sell_signal(self) -> None:
        trade = make_trade(side="BUY", price=0.50)
        signal = self.repo.on_fill(trade)
        assert signal is not None
        assert signal.side == "SELL"
        assert abs(signal.price - 0.51) < 0.0001

    def test_sell_fill_generates_buy_signal(self) -> None:
        trade = make_trade(side="SELL", price=0.60)
        signal = self.repo.on_fill(trade)
        assert signal is not None
        assert signal.side == "BUY"
        assert abs(signal.price - 0.59) < 0.0001

    def test_size_matches_original_fill(self) -> None:
        trade = make_trade(side="BUY", price=0.50, size=20.0)
        signal = self.repo.on_fill(trade)
        assert signal.size == 20.0

    def test_token_and_market_preserved(self) -> None:
        trade = make_trade(side="BUY", token_id="tok_abc", market_id="mkt_xyz")
        signal = self.repo.on_fill(trade)
        assert signal.token_id == "tok_abc"
        assert signal.market_id == "mkt_xyz"

    def test_disabled_returns_none(self) -> None:
        repo = FillRepositioner(make_config(repositioning_enabled=False))
        trade = make_trade(side="BUY", price=0.50)
        assert repo.on_fill(trade) is None

    def test_error_status_returns_none(self) -> None:
        trade = make_trade(side="BUY", price=0.50, status="error")
        assert self.repo.on_fill(trade) is None

    def test_rejected_status_returns_none(self) -> None:
        trade = make_trade(side="BUY", price=0.50, status="rejected")
        assert self.repo.on_fill(trade) is None

    def test_price_near_top_clipped(self) -> None:
        """SELL a 0.99 = fuera de rango (max 0.98), no generar signal."""
        trade = make_trade(side="BUY", price=0.985)
        signal = self.repo.on_fill(trade)
        assert signal is None  # 0.985 + 0.01 = 0.995 > 0.98

    def test_price_near_bottom_clipped(self) -> None:
        """BUY a 0.025 = fuera de rango (min 0.02), no generar signal."""
        trade = make_trade(side="SELL", price=0.025)
        signal = self.repo.on_fill(trade)
        assert signal is None  # 0.025 - 0.01 = 0.015 < 0.02

    def test_pending_registered(self) -> None:
        trade = make_trade(side="BUY", price=0.50, order_id="ord_abc")
        self.repo.on_fill(trade)
        assert self.repo.pending_count == 1

    def test_strategy_name_is_repositioner(self) -> None:
        trade = make_trade(side="BUY", price=0.50)
        signal = self.repo.on_fill(trade)
        assert signal.strategy_name == "repositioner"

    def test_metadata_contains_original_id(self) -> None:
        trade = make_trade(side="BUY", order_id="orig_001")
        signal = self.repo.on_fill(trade)
        assert signal.metadata.get("original_order_id") == "orig_001"
        assert signal.metadata.get("is_reposition") is True


# ------------------------------------------------------------------
# Tests: register & on_reposition_filled
# ------------------------------------------------------------------

class TestRepositionFilled:
    def setup_method(self) -> None:
        self.repo = FillRepositioner(make_config())

    def test_round_trip_profit_buy_side(self) -> None:
        """Compro a 0.50, reposicione venta a 0.51 → ganancia $0.15 en $15."""
        trade = make_trade(side="BUY", price=0.50, size=15.0, order_id="orig_1")
        self.repo.on_fill(trade)
        self.repo.register_reposition_order("orig_1", "repo_1")

        pnl = self.repo.on_reposition_filled("repo_1", fill_price=0.51)
        assert pnl is not None
        assert abs(pnl - 0.15) < 0.001  # (0.51 - 0.50) * 15 = 0.15

    def test_round_trip_profit_sell_side(self) -> None:
        """Vendi a 0.60, reposicione compra a 0.59 → ganancia $0.15 en $15."""
        trade = make_trade(side="SELL", price=0.60, size=15.0, order_id="orig_2")
        self.repo.on_fill(trade)
        self.repo.register_reposition_order("orig_2", "repo_2")

        pnl = self.repo.on_reposition_filled("repo_2", fill_price=0.59)
        assert pnl is not None
        assert abs(pnl - 0.15) < 0.001  # (0.60 - 0.59) * 15 = 0.15

    def test_unknown_reposition_id_returns_none(self) -> None:
        pnl = self.repo.on_reposition_filled("nonexistent_id")
        assert pnl is None

    def test_pending_cleared_after_fill(self) -> None:
        trade = make_trade(side="BUY", price=0.50, order_id="orig_3")
        self.repo.on_fill(trade)
        assert self.repo.pending_count == 1
        self.repo.register_reposition_order("orig_3", "repo_3")
        self.repo.on_reposition_filled("repo_3")
        assert self.repo.pending_count == 0


# ------------------------------------------------------------------
# Tests: check_expirations
# ------------------------------------------------------------------

class TestExpirations:
    def test_fresh_pending_not_expired(self) -> None:
        repo = FillRepositioner(make_config(reposition_timeout_min=60.0))
        trade = make_trade(side="BUY", order_id="ord_fresh")
        repo.on_fill(trade)
        expired = repo.check_expirations()
        assert expired == []

    def test_old_pending_expires(self) -> None:
        repo = FillRepositioner(make_config(reposition_timeout_min=0.001))  # 0.06s
        trade = make_trade(side="BUY", order_id="ord_old")
        repo.on_fill(trade)
        repo.register_reposition_order("ord_old", "repo_old")

        time.sleep(0.1)  # Wait for expiry
        expired = repo.check_expirations()
        assert "repo_old" in expired

    def test_expired_cleared_from_pending(self) -> None:
        repo = FillRepositioner(make_config(reposition_timeout_min=0.001))
        trade = make_trade(side="BUY", order_id="ord_clr")
        repo.on_fill(trade)
        repo.register_reposition_order("ord_clr", "repo_clr")
        time.sleep(0.1)
        repo.check_expirations()
        assert repo.pending_count == 0

    def test_no_reposition_id_not_in_expired_list(self) -> None:
        """Si la reposicion no tiene order_id registrado, no aparece en expirados."""
        repo = FillRepositioner(make_config(reposition_timeout_min=0.001))
        trade = make_trade(side="BUY", order_id="ord_noid")
        repo.on_fill(trade)
        # No llamamos register_reposition_order
        time.sleep(0.1)
        expired = repo.check_expirations()
        assert expired == []
        assert repo.pending_count == 0  # Pero sí se limpia


# ------------------------------------------------------------------
# Tests: custom offset
# ------------------------------------------------------------------

class TestCustomOffset:
    def test_custom_offset_applied(self) -> None:
        repo = FillRepositioner(make_config(reposition_offset=0.02))
        trade = make_trade(side="BUY", price=0.50)
        signal = repo.on_fill(trade)
        assert abs(signal.price - 0.52) < 0.0001  # 0.50 + 0.02


# ------------------------------------------------------------------
# Tests: MarketMakerStrategy integration
# ------------------------------------------------------------------

class TestMarketMakerIntegration:
    def setup_method(self) -> None:
        from src.strategy.market_maker import MarketMakerStrategy
        self.client = MagicMock()
        self.client.paper_mode = True
        self.client.place_limit_order.return_value = {
            "order_id": "repo_order_001",
            "status": "submitted",
        }
        self.client.cancel_order.return_value = True
        config = {
            "market_maker": {
                "spread_offset": 0.02,
                "order_size": 15,
                "refresh_seconds": 0,
                "max_inventory_per_market": 50,
                "repositioning_enabled": True,
                "reposition_offset": 0.01,
                "reposition_timeout_min": 90,
            }
        }
        self.mm = MarketMakerStrategy(self.client, config)

    def test_process_fill_places_reposition_order(self) -> None:
        fill = make_trade(side="BUY", price=0.50, size=15.0, order_id="fill_001")
        repos = self.mm.process_fill(fill)
        assert len(repos) == 1
        assert repos[0].side == "SELL"
        assert abs(repos[0].price - 0.51) < 0.0001

    def test_process_fill_calls_place_limit_order(self) -> None:
        fill = make_trade(side="BUY", price=0.50, order_id="fill_002")
        self.mm.process_fill(fill)
        self.client.place_limit_order.assert_called_once()
        call_kwargs = self.client.place_limit_order.call_args
        assert call_kwargs.kwargs.get("side") == "SELL"
        assert call_kwargs.kwargs.get("post_only") is True

    def test_expired_repositions_cancelled_in_run(self) -> None:
        """run() cancela reposiciones expiradas antes de operar."""
        # Registrar una reposicion expirada
        self.mm._repositioner._pending["orig"] = PendingReposition(
            original_order_id="orig",
            original_side="BUY",
            fill_price=0.50,
            fill_size=15.0,
            token_id="tok1",
            market_id="mkt1",
            reposition_order_id="expired_repo",
            placed_at=time.time() - 9999,  # Way in the past
            expiry_minutes=0.001,
        )
        self.mm._repositioner._by_reposition_id["expired_repo"] = "orig"

        market_data = {
            "condition_id": "mkt1",
            "token_id": "tok1",
            "mid_price": 0.50,
            "spread": 0.05,
            "tokens": [{"token_id": "tok1", "price": 0.50}],
        }
        self.mm.run(market_data)
        self.client.cancel_order.assert_called_with("expired_repo")
