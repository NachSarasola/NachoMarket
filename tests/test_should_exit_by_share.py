"""Tests para MarketProfiler.should_exit_by_share (tips 18, 20)."""

import time
from unittest.mock import patch

from src.risk.market_profitability import MarketProfiler, MarketStats


def make_profiler() -> MarketProfiler:
    return MarketProfiler({"min_orders_to_evaluate": 3})


class TestShouldExitByShare:
    def test_no_exit_when_share_above_threshold(self):
        """Share del 2% > 0.5% → no exit."""
        p = make_profiler()
        result = p.should_exit_by_share("mkt1", current_share=0.02)
        assert result is False

    def test_no_exit_first_time_below_threshold(self):
        """Primera vez bajo threshold → se guarda timer pero no exit aun."""
        p = make_profiler()
        result = p.should_exit_by_share("mkt1", current_share=0.003)
        assert result is False
        assert p._stats["mkt1"].share_below_since > 0

    def test_exit_after_persistence_hours(self):
        """12+ horas bajo 0.5% → exit True."""
        p = make_profiler()
        past_time = time.time() - 13 * 3600  # 13 horas atras
        p._stats["mkt1"] = MarketStats(
            market_id="mkt1", share_below_since=past_time
        )
        result = p.should_exit_by_share("mkt1", current_share=0.003)
        assert result is True

    def test_no_exit_before_persistence_hours(self):
        """11 horas bajo 0.5% → no exit todavia."""
        p = make_profiler()
        past_time = time.time() - 11 * 3600  # 11 horas atras
        p._stats["mkt1"] = MarketStats(
            market_id="mkt1", share_below_since=past_time
        )
        result = p.should_exit_by_share("mkt1", current_share=0.003)
        assert result is False

    def test_reset_timer_when_share_recovers(self):
        """Share sube de vuelta al 2% → share_below_since se resetea."""
        p = make_profiler()
        past_time = time.time() - 5 * 3600
        p._stats["mkt1"] = MarketStats(
            market_id="mkt1", share_below_since=past_time
        )
        # Share recupera
        result = p.should_exit_by_share("mkt1", current_share=0.02)
        assert result is False
        assert p._stats["mkt1"].share_below_since == 0.0

    def test_custom_threshold_and_persistence(self):
        """Threshold custom 1%, persistence 6h."""
        p = make_profiler()
        past_time = time.time() - 7 * 3600
        p._stats["mkt1"] = MarketStats(
            market_id="mkt1", share_below_since=past_time
        )
        result = p.should_exit_by_share(
            "mkt1", current_share=0.008, threshold=0.01, persistence_hours=6.0
        )
        assert result is True

    def test_new_market_creates_stats_entry(self):
        """Mercado no visto crea entrada en stats."""
        p = make_profiler()
        p.should_exit_by_share("new_market", current_share=0.002)
        assert "new_market" in p._stats

    def test_exact_persistence_boundary(self):
        """Exactamente en el limite de 12h → exit True (>=)."""
        p = make_profiler()
        past_time = time.time() - 12 * 3600
        p._stats["mkt1"] = MarketStats(
            market_id="mkt1", share_below_since=past_time
        )
        result = p.should_exit_by_share("mkt1", current_share=0.003)
        assert result is True
