"""Tests para MarketProfiler — tracking de rentabilidad por mercado."""

import json
import time
import tempfile
from pathlib import Path
from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from src.risk.market_profitability import MarketProfiler, MarketStats, Trade
from src.strategy.base import Trade as BaseTrade  # compatible, misma interfaz


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def make_config(**overrides) -> dict:
    base = {"min_orders_to_evaluate": 3}
    base.update(overrides)
    return base


def make_trade(
    side: str = "BUY",
    price: float = 0.50,
    size: float = 15.0,
    market_id: str = "mkt1",
    fee_paid: float = 0.0,
    status: str = "submitted",
) -> Trade:
    """Usa Trade de market_profitability (mismos campos que base.Trade)."""
    return Trade(
        timestamp=datetime.now(timezone.utc).isoformat(),
        market_id=market_id,
        token_id="tok1",
        side=side,
        price=price,
        size=size,
        order_id="ord_test",
        status=status,
        strategy_name="market_maker",
        fee_paid=fee_paid,
    )


# ------------------------------------------------------------------
# Tests: MarketStats
# ------------------------------------------------------------------

class TestMarketStats:
    def test_roi_zero_when_no_capital(self) -> None:
        """Sin capital deployado, ROI = 0.0."""
        stats = MarketStats(market_id="m1")
        assert stats.roi == 0.0

    def test_roi_calculated_dynamically(self) -> None:
        """ROI se computa de total_pnl / capital_deployed dinámicamente."""
        stats = MarketStats(market_id="m1")
        stats.total_pnl = 1.0
        stats.capital_deployed = 100.0
        assert abs(stats.roi - 0.01) < 0.0001

    def test_roi_override_when_set(self) -> None:
        """Si _roi_override está seteado, se usa en vez del cálculo."""
        stats = MarketStats(market_id="m1", _roi_override=0.05)
        stats.total_pnl = 1.0
        stats.capital_deployed = 100.0  # daría 0.01 dinámico
        assert abs(stats.roi - 0.05) < 0.0001  # override pisa dinámico

    def test_update_invalidates_roi_override_on_sell(self) -> None:
        """Un sell que cambia PnL invalida el _roi_override."""
        stats = MarketStats(market_id="m1", _roi_override=0.05)
        stats.capital_deployed = 100.0
        stats.total_pnl = 5.0
        trade = make_trade(side="SELL", price=0.60, size=10.0)
        stats.update(trade)
        # _roi_override se reseteó → ROI = total_pnl / capital_deployed
        assert abs(stats.roi - stats.total_pnl / stats.capital_deployed) < 0.0001

    def test_avg_spread_defaults_zero(self) -> None:
        """avg_spread_captured empieza en 0.0 (no se auto-calcula)."""
        stats = MarketStats(market_id="m1")
        assert stats.avg_spread_captured == 0.0

    def test_avg_spread_stored_directly(self) -> None:
        """avg_spread_captured es un campo almacenable, no computado."""
        stats = MarketStats(market_id="m1", avg_spread_captured=0.03)
        assert stats.avg_spread_captured == 0.03

    def test_share_below_since_defaults_none(self) -> None:
        stats = MarketStats(market_id="m1")
        assert stats.share_below_since is None

    def test_last_update_defaults_now(self) -> None:
        before = time.time()
        stats = MarketStats(market_id="m1")
        after = time.time()
        assert before <= stats.last_update <= after


# ------------------------------------------------------------------
# Tests: MarketProfiler update
# ------------------------------------------------------------------

class TestMarketProfilerUpdate:
    def setup_method(self) -> None:
        with patch.object(MarketProfiler, "_save"), \
             patch.object(MarketProfiler, "_load", return_value={}):
            self.profiler = MarketProfiler(make_config())

    def test_creates_stats_on_first_trade(self) -> None:
        t = make_trade(side="BUY", market_id="mkt1")
        self.profiler.update("mkt1", t)
        assert "mkt1" in self.profiler.get_all_stats()

    def test_buy_trade_increases_capital_deployed(self) -> None:
        """capital_deployed = price * size. Para price=0.50, size=15 → 7.5."""
        t = make_trade(side="BUY", price=0.50, size=15.0, market_id="mkt1")
        self.profiler.update("mkt1", t)
        stats = self.profiler.get_all_stats()["mkt1"]
        assert abs(stats.capital_deployed - 7.5) < 0.01

    def test_sell_after_buy_computes_pnl(self) -> None:
        self.profiler.update("mkt1", make_trade(side="BUY", price=0.50, size=15.0))
        self.profiler.update("mkt1", make_trade(side="SELL", price=0.52, size=15.0))
        stats = self.profiler.get_all_stats()["mkt1"]
        # PnL = (0.52 - 0.50) * 15 = 0.30
        assert abs(stats.total_pnl - 0.30) < 0.01

    def test_error_trade_skipped_completely(self) -> None:
        """Trades con status=error no actualizan nada (order_count=0 ni capital)."""
        self.profiler.update("mkt1", make_trade(side="BUY", price=0.50, status="error"))
        stats = self.profiler.get_all_stats()["mkt1"]
        assert stats.capital_deployed == 0.0
        assert stats.order_count == 0

    def test_order_count_increments_on_valid_trades(self) -> None:
        for _ in range(5):
            self.profiler.update("mkt1", make_trade(side="BUY"))
        assert self.profiler.get_all_stats()["mkt1"].order_count == 5

    def test_fill_count_increments_on_profitable_sell(self) -> None:
        """fill_count solo incrementa si pnl > 0 (sell_price > avg_buy)."""
        self.profiler.update("mkt1", make_trade(side="BUY", price=0.50))
        self.profiler.update("mkt1", make_trade(side="SELL", price=0.51))
        assert self.profiler.get_all_stats()["mkt1"].fill_count == 1

    def test_fill_count_not_incremented_on_losing_sell(self) -> None:
        """Sell a pérdida no incrementa fill_count."""
        self.profiler.update("mkt1", make_trade(side="BUY", price=0.60))
        self.profiler.update("mkt1", make_trade(side="SELL", price=0.40))
        assert self.profiler.get_all_stats()["mkt1"].fill_count == 0

    def test_multiple_markets_independent(self) -> None:
        self.profiler.update("mkt1", make_trade(side="BUY", price=0.50, size=10.0))
        self.profiler.update("mkt2", make_trade(side="BUY", price=0.60, size=20.0))
        s1 = self.profiler.get_all_stats()["mkt1"]
        s2 = self.profiler.get_all_stats()["mkt2"]
        assert abs(s1.capital_deployed - 5.0) < 0.01    # 0.50 * 10
        assert abs(s2.capital_deployed - 12.0) < 0.01    # 0.60 * 20

    def test_last_update_updated_on_trade(self) -> None:
        before = time.time()
        self.profiler.update("mkt1", make_trade(side="BUY"))
        after = time.time()
        stats = self.profiler.get_all_stats()["mkt1"]
        assert before <= stats.last_update <= after + 1


# ------------------------------------------------------------------
# Tests: get_unprofitable_markets
# ------------------------------------------------------------------

class TestUnprofitableMarkets:
    def setup_method(self) -> None:
        with patch.object(MarketProfiler, "_save"), \
             patch.object(MarketProfiler, "_load", return_value={}):
            self.profiler = MarketProfiler(make_config(min_orders_to_evaluate=2))

    def test_profitable_market_not_flagged(self) -> None:
        for _ in range(2):
            self.profiler.update("mkt1", make_trade(side="BUY", price=0.48, size=15.0))
        for _ in range(2):
            self.profiler.update("mkt1", make_trade(side="SELL", price=0.52, size=15.0))
        result = self.profiler.get_unprofitable_markets(min_roi=-0.05)
        assert "mkt1" not in result

    def test_unprofitable_market_flagged(self) -> None:
        for _ in range(2):
            self.profiler.update("mkt1", make_trade(side="BUY", price=0.60, size=15.0))
        for _ in range(2):
            self.profiler.update("mkt1", make_trade(side="SELL", price=0.40, size=15.0))
        result = self.profiler.get_unprofitable_markets(min_roi=-0.05)
        assert "mkt1" in result

    def test_insufficient_orders_skipped(self) -> None:
        self.profiler.update("mkt1", make_trade(side="BUY", price=0.60))
        result = self.profiler.get_unprofitable_markets(min_roi=-0.05)
        assert "mkt1" not in result


# ------------------------------------------------------------------
# Tests: get_report
# ------------------------------------------------------------------

class TestGetReport:
    def setup_method(self) -> None:
        with patch.object(MarketProfiler, "_save"), \
             patch.object(MarketProfiler, "_load", return_value={}):
            self.profiler = MarketProfiler(make_config(min_orders_to_evaluate=1))

    def test_report_sorted_by_roi_desc(self) -> None:
        self.profiler.update("mkt1", make_trade(side="BUY", price=0.48, size=15.0))
        self.profiler.update("mkt1", make_trade(side="SELL", price=0.55, size=15.0))
        self.profiler.update("mkt2", make_trade(side="BUY", price=0.60, size=15.0))
        self.profiler.update("mkt2", make_trade(side="SELL", price=0.50, size=15.0))

        report = self.profiler.get_report()
        assert len(report) == 2
        assert report[0]["market_id"] == "mkt1"  # más rentable primero

    def test_report_contains_required_fields(self) -> None:
        self.profiler.update("mkt1", make_trade(side="BUY", price=0.50))
        report = self.profiler.get_report()
        assert len(report) == 1
        entry = report[0]
        for key in ("roi", "total_pnl", "fill_count", "order_count", "capital_deployed"):
            assert key in entry, f"Falta {key} en report"

    def test_report_respects_top_n(self) -> None:
        for i in range(5):
            self.profiler.update(f"mkt{i}", make_trade(side="BUY", market_id=f"mkt{i}"))
        report = self.profiler.get_report(top_n=3)
        assert len(report) <= 3


# ------------------------------------------------------------------
# Tests: get_market_roi
# ------------------------------------------------------------------

class TestGetMarketRoi:
    def setup_method(self) -> None:
        with patch.object(MarketProfiler, "_save"), \
             patch.object(MarketProfiler, "_load", return_value={}):
            self.profiler = MarketProfiler(make_config(min_orders_to_evaluate=2))

    def test_unknown_market_returns_none(self) -> None:
        assert self.profiler.get_market_roi("nonexistent") is None

    def test_insufficient_orders_returns_none(self) -> None:
        self.profiler.update("mkt1", make_trade(side="BUY"))
        assert self.profiler.get_market_roi("mkt1") is None

    def test_returns_roi_with_enough_data(self) -> None:
        self.profiler.update("mkt1", make_trade(side="BUY", price=0.50, size=15.0))
        self.profiler.update("mkt1", make_trade(side="SELL", price=0.52, size=15.0))
        roi = self.profiler.get_market_roi("mkt1")
        assert roi is not None
        assert roi > 0
