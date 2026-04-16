"""Tests para MarketProfiler — tracking de rentabilidad por mercado."""

import json
import tempfile
from pathlib import Path
from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from src.risk.market_profitability import MarketProfiler, MarketStats, PROFITABILITY_FILE
from src.strategy.base import Trade


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
        stats = MarketStats(market_id="m1")
        assert stats.roi == 0.0

    def test_roi_calculation(self) -> None:
        stats = MarketStats(market_id="m1", total_pnl=1.0, capital_deployed=100.0)
        assert abs(stats.roi - 0.01) < 0.0001

    def test_avg_spread_no_prices(self) -> None:
        stats = MarketStats(market_id="m1")
        assert stats.avg_spread_captured == 0.0

    def test_avg_spread_calculation(self) -> None:
        stats = MarketStats(
            market_id="m1",
            buy_prices=[0.49, 0.50],
            sell_prices=[0.51, 0.52],
        )
        # avg_buy = 0.495, avg_sell = 0.515, spread = 0.02
        assert abs(stats.avg_spread_captured - 0.02) < 0.001


# ------------------------------------------------------------------
# Tests: MarketProfiler update
# ------------------------------------------------------------------

class TestMarketProfilerUpdate:
    def setup_method(self) -> None:
        # Patch file save to avoid disk I/O in tests
        with patch.object(MarketProfiler, "_save"), \
             patch.object(MarketProfiler, "_load"):
            self.profiler = MarketProfiler(make_config())

    def test_creates_stats_on_first_trade(self) -> None:
        t = make_trade(side="BUY", market_id="mkt1")
        self.profiler.update("mkt1", t)
        assert "mkt1" in self.profiler.get_all_stats()

    def test_buy_trade_increases_capital_deployed(self) -> None:
        t = make_trade(side="BUY", size=15.0, market_id="mkt1")
        self.profiler.update("mkt1", t)
        stats = self.profiler.get_all_stats()["mkt1"]
        assert stats.capital_deployed == 15.0

    def test_sell_after_buy_computes_pnl(self) -> None:
        self.profiler.update("mkt1", make_trade(side="BUY", price=0.50, size=15.0))
        self.profiler.update("mkt1", make_trade(side="SELL", price=0.52, size=15.0))
        stats = self.profiler.get_all_stats()["mkt1"]
        # PnL = (0.52 - 0.50) * 15 = 0.30
        assert abs(stats.total_pnl - 0.30) < 0.01

    def test_error_trade_does_not_update_pnl(self) -> None:
        self.profiler.update("mkt1", make_trade(side="BUY", price=0.50, status="error"))
        stats = self.profiler.get_all_stats()["mkt1"]
        assert stats.capital_deployed == 0.0
        assert stats.order_count == 1  # Counted but no capital

    def test_order_count_increments(self) -> None:
        for _ in range(5):
            self.profiler.update("mkt1", make_trade(side="BUY"))
        assert self.profiler.get_all_stats()["mkt1"].order_count == 5

    def test_fill_count_increments_on_sell(self) -> None:
        self.profiler.update("mkt1", make_trade(side="BUY", price=0.50))
        self.profiler.update("mkt1", make_trade(side="SELL", price=0.51))
        assert self.profiler.get_all_stats()["mkt1"].fill_count == 1

    def test_multiple_markets_independent(self) -> None:
        self.profiler.update("mkt1", make_trade(side="BUY", size=10.0))
        self.profiler.update("mkt2", make_trade(side="BUY", size=20.0))
        s1 = self.profiler.get_all_stats()["mkt1"]
        s2 = self.profiler.get_all_stats()["mkt2"]
        assert s1.capital_deployed == 10.0
        assert s2.capital_deployed == 20.0

    def test_question_stored(self) -> None:
        t = make_trade(side="BUY", market_id="mkt1")
        self.profiler.update("mkt1", t, question="Will BTC reach 100k?")
        stats = self.profiler.get_all_stats()["mkt1"]
        assert stats.question == "Will BTC reach 100k?"


# ------------------------------------------------------------------
# Tests: get_unprofitable_markets
# ------------------------------------------------------------------

class TestUnprofitableMarkets:
    def setup_method(self) -> None:
        with patch.object(MarketProfiler, "_save"), \
             patch.object(MarketProfiler, "_load"):
            self.profiler = MarketProfiler(make_config(min_orders_to_evaluate=2))

    def test_profitable_market_not_flagged(self) -> None:
        # 3 buys then 3 profitable sells
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
        # Only 1 order, below min_orders_to_evaluate=2
        self.profiler.update("mkt1", make_trade(side="BUY", price=0.60))
        result = self.profiler.get_unprofitable_markets(min_roi=-0.05)
        assert "mkt1" not in result


# ------------------------------------------------------------------
# Tests: get_report
# ------------------------------------------------------------------

class TestGetReport:
    def setup_method(self) -> None:
        with patch.object(MarketProfiler, "_save"), \
             patch.object(MarketProfiler, "_load"):
            self.profiler = MarketProfiler(make_config(min_orders_to_evaluate=1))

    def test_report_sorted_by_roi(self) -> None:
        # mkt1: profitable
        self.profiler.update("mkt1", make_trade(side="BUY", price=0.48, size=15.0))
        self.profiler.update("mkt1", make_trade(side="SELL", price=0.55, size=15.0))
        # mkt2: losing
        self.profiler.update("mkt2", make_trade(side="BUY", price=0.60, size=15.0))
        self.profiler.update("mkt2", make_trade(side="SELL", price=0.50, size=15.0))

        report = self.profiler.get_report()
        assert len(report) == 2
        assert report[0]["market_id"] == "mkt1"  # More profitable first

    def test_report_contains_required_fields(self) -> None:
        self.profiler.update("mkt1", make_trade(side="BUY", price=0.50))
        report = self.profiler.get_report()
        assert len(report) == 1
        entry = report[0]
        assert "roi" in entry
        assert "total_pnl" in entry
        assert "fill_count" in entry
        assert "order_count" in entry

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
             patch.object(MarketProfiler, "_load"):
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
