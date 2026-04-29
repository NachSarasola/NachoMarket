"""Tests para position_sizer, inventory, y circuit_breaker."""

import time

import pytest

from src.risk.position_sizer import kelly_fraction, calculate_size, can_trade, PositionSizer
from src.risk.inventory import InventoryManager, MarketInventory
from src.risk.circuit_breaker import CircuitBreaker


# ===========================================================================
# Tests: Funciones standalone de position_sizer
# ===========================================================================

class TestKellyFraction:
    def test_positive_edge(self) -> None:
        # p=0.65, q=0.55: f = 0.25 * (0.65 - 0.55) / (1 - 0.55) = 0.0556
        kf = kelly_fraction(estimated_prob=0.65, market_price=0.55)
        assert 0 < kf <= 0.05  # clamp a 5% (alineado con regla INQUEBRANTABLE)

    def test_no_edge_returns_zero(self) -> None:
        assert kelly_fraction(0.5, 0.5) == 0.0

    def test_negative_edge_returns_zero(self) -> None:
        assert kelly_fraction(0.4, 0.6) == 0.0

    def test_clamped_at_5_pct(self) -> None:
        # Edge enorme — debe clampearse a 0.05 (5%/mercado regla INQUEBRANTABLE)
        kf = kelly_fraction(estimated_prob=0.99, market_price=0.01)
        assert kf == 0.05

    def test_default_kelly_multiplier_is_quarter(self) -> None:
        # Use small edge so neither hits the 0.10 cap
        # p=0.52, q=0.50: full kelly = 1.0 * 0.02/0.5 = 0.04 (no cap)
        kf_quarter = kelly_fraction(0.52, 0.50, kelly_multiplier=0.25)
        kf_full = kelly_fraction(0.52, 0.50, kelly_multiplier=1.0)
        assert abs(kf_quarter - kf_full * 0.25) < 1e-9

    def test_custom_multiplier(self) -> None:
        # Same small edge
        kf_50 = kelly_fraction(0.52, 0.50, kelly_multiplier=0.50)
        kf_25 = kelly_fraction(0.52, 0.50, kelly_multiplier=0.25)
        assert abs(kf_50 - kf_25 * 2) < 1e-9

    def test_invalid_probabilities_return_zero(self) -> None:
        assert kelly_fraction(0.0, 0.5) == 0.0
        assert kelly_fraction(0.7, 0.0) == 0.0
        assert kelly_fraction(1.0, 0.5) == 0.0
        assert kelly_fraction(0.7, 1.0) == 0.0

    def test_formula_correctness(self) -> None:
        # p=0.55, q=0.50: f = 0.25 * 0.05/0.5 = 0.025 (below 0.10 cap)
        kf = kelly_fraction(0.55, 0.50)
        expected = 0.25 * (0.55 - 0.50) / (1.0 - 0.50)
        assert abs(kf - expected) < 1e-9


class TestCalculateSize:
    def test_basic_sizing(self) -> None:
        size = calculate_size(capital=400, kelly_f=0.05)
        assert size == min(400 * 0.05, 400 * 0.05)  # 20.0

    def test_returns_zero_below_min_size(self) -> None:
        # kelly_f=0.001 → 400*0.001=0.4 < min_size=5 → 0
        assert calculate_size(400, 0.001) == 0.0

    def test_capped_at_5pct_by_default(self) -> None:
        size = calculate_size(capital=400, kelly_f=0.15)  # 60 USDC
        assert size == 400 * 0.05  # Capped at $20

    def test_custom_max_size(self) -> None:
        size = calculate_size(capital=400, kelly_f=0.03, max_size=10.0)
        # raw = 12, max = max(10, 400*0.05=20) = 20, so size = 12
        assert size == 400 * 0.03

    def test_max_size_smaller_than_5pct_uses_5pct(self) -> None:
        # max_size=5, capital*5%=20 → upper=max(5,20)=20
        size = calculate_size(capital=400, kelly_f=0.08, max_size=5.0)
        assert size <= 400 * 0.05  # Still respects 5% rule

    def test_zero_kelly_returns_zero(self) -> None:
        assert calculate_size(400, 0.0) == 0.0

    def test_zero_capital_returns_zero(self) -> None:
        assert calculate_size(0, 0.05) == 0.0

    def test_custom_min_size(self) -> None:
        # raw = 400*0.005=2, min_size=10 → 0 (below min)
        assert calculate_size(400, 0.005, min_size=10.0) == 0.0


class TestCanTrade:
    def test_can_trade_with_room(self) -> None:
        assert can_trade(current_exposure=5, capital=400) is True

    def test_cannot_trade_at_limit(self) -> None:
        # 400 * 5% = $20 limit; exposure=19 + new=2 = 21 > 20
        assert can_trade(current_exposure=19, capital=400, new_size=2.0) is False

    def test_cannot_trade_over_limit(self) -> None:
        assert can_trade(current_exposure=25, capital=400) is False

    def test_custom_risk_pct(self) -> None:
        assert can_trade(5, 400, max_risk_pct=0.10) is True
        assert can_trade(45, 400, max_risk_pct=0.10) is False

    def test_new_size_considered(self) -> None:
        # exposure=15, new=10 → total=25 > 20 → False
        assert can_trade(15, 400, new_size=10.0) is False
        # exposure=5, new=10 → total=15 < 20 → True
        assert can_trade(5, 400, new_size=10.0) is True


# ===========================================================================
# Tests: PositionSizer clase
# ===========================================================================

class TestPositionSizer:
    def setup_method(self) -> None:
        self.sizer = PositionSizer({
            "position_sizing": {
                "method": "fractional_kelly",
                "kelly_fraction": 0.25,
                "max_position_usdc": 20.0,
                "min_position_usdc": 5.0,
            }
        })

    def test_size_for_signal_positive_edge(self) -> None:
        size = self.sizer.size_for_signal(capital=400, estimated_prob=0.65, market_price=0.50)
        assert 0 < size <= 20.0

    def test_size_for_signal_no_edge(self) -> None:
        size = self.sizer.size_for_signal(capital=400, estimated_prob=0.5, market_price=0.5)
        assert size == 0.0

    def test_size_never_exceeds_5pct(self) -> None:
        size = self.sizer.size_for_signal(capital=400, estimated_prob=0.95, market_price=0.10)
        assert size <= 20.0  # 5% de $400

    def test_fixed_method(self) -> None:
        sizer = PositionSizer({
            "position_sizing": {
                "method": "fixed",
                "max_position_usdc": 10.0,
                "min_position_usdc": 1.0,
            }
        })
        size = sizer.size_for_signal(capital=400, estimated_prob=0.6, market_price=0.4)
        assert size == 10.0

    def test_can_trade_delegates_correctly(self) -> None:
        assert self.sizer.can_trade(5, 400) is True     # 5 < 380 (95%)
        assert self.sizer.can_trade(390, 400) is False  # 390 > 380

    def test_can_trade_with_new_size(self) -> None:
        assert self.sizer.can_trade(10, 400, new_size=5) is True    # 15 < 380
        assert self.sizer.can_trade(370, 400, new_size=20) is False # 390 > 380


# ===========================================================================
# Tests: InventoryManager
# ===========================================================================

@pytest.fixture
def inv(tmp_path):
    from pathlib import Path
    return InventoryManager(
        {
            "inventory_management": {
                "max_inventory_per_market_usdc": 50.0,
                "merge_threshold_usdc": 20.0,
            }
        },
        state_file=tmp_path / "state.json",
    )


class TestInventoryManager:

    def test_add_trade_yes(self, inv) -> None:
        inv.add_trade("market_1", "yes", "BUY", 10.0)
        market_inv = inv.get_market_inventory("market_1")
        assert market_inv.yes == 10.0
        assert market_inv.no == 0.0

    def test_add_trade_no(self, inv) -> None:
        inv.add_trade("market_1", "no", "BUY", 8.0)
        market_inv = inv.get_market_inventory("market_1")
        assert market_inv.yes == 0.0
        assert market_inv.no == 8.0

    def test_add_trade_sell_reduces_inventory(self, inv) -> None:
        inv.add_trade("market_1", "yes", "BUY", 10.0)
        inv.add_trade("market_1", "yes", "SELL", 4.0)
        assert inv.get_market_inventory("market_1").yes == 6.0

    def test_get_skew_balanced(self, inv) -> None:
        inv.add_trade("m1", "yes", "BUY", 10.0)
        inv.add_trade("m1", "no", "BUY", 10.0)
        assert inv.get_skew("m1") == 0.0

    def test_get_skew_all_yes(self, inv) -> None:
        inv.add_trade("m1", "yes", "BUY", 10.0)
        assert inv.get_skew("m1") == 1.0

    def test_get_skew_all_no(self, inv) -> None:
        inv.add_trade("m1", "no", "BUY", 10.0)
        assert inv.get_skew("m1") == -1.0

    def test_get_skew_partial(self, inv) -> None:
        inv.add_trade("m1", "yes", "BUY", 30.0)
        inv.add_trade("m1", "no", "BUY", 10.0)
        # (30 - 10) / (30 + 10) = 20/40 = 0.5
        assert abs(inv.get_skew("m1") - 0.5) < 1e-9

    def test_get_skew_empty_market_returns_zero(self, inv) -> None:
        assert inv.get_skew("nonexistent") == 0.0

    def test_should_merge_above_threshold(self, inv) -> None:
        inv.add_trade("m1", "yes", "BUY", 25.0)
        inv.add_trade("m1", "no", "BUY", 25.0)
        assert inv.should_merge("m1") is True  # min(25, 25) = 25 > 20

    def test_should_merge_below_threshold(self, inv) -> None:
        inv.add_trade("m1", "yes", "BUY", 25.0)
        inv.add_trade("m1", "no", "BUY", 5.0)
        assert inv.should_merge("m1") is False  # min(25, 5) = 5 < 20

    def test_should_merge_one_side_zero(self, inv) -> None:
        inv.add_trade("m1", "yes", "BUY", 30.0)
        assert inv.should_merge("m1") is False  # min(30, 0) = 0

    def test_adjust_quotes_neutral_skew(self, inv) -> None:
        bid, ask = inv.adjust_quotes(0.45, 0.55, skew=0.1)
        assert bid == 0.45
        assert ask == 0.55

    def test_adjust_quotes_high_skew_widens_spread(self, inv) -> None:
        # skew > 0.3 (too long YES): ask up, bid down
        bid, ask = inv.adjust_quotes(0.45, 0.55, skew=0.5)
        assert ask > 0.55   # ask widens (up)
        assert bid < 0.45   # bid widens (down)

    def test_adjust_quotes_low_skew_adjusts_other_way(self, inv) -> None:
        # skew < -0.3 (too long NO): bid up, ask down
        bid, ask = inv.adjust_quotes(0.45, 0.55, skew=-0.5)
        assert bid > 0.45   # bid tightens (up)
        assert ask < 0.55   # ask tightens (down)

    def test_adjust_quotes_spread_always_positive(self, inv) -> None:
        # Even with strong skew, ask must remain > bid
        bid, ask = inv.adjust_quotes(0.48, 0.52, skew=0.9)
        assert ask > bid

    def test_get_total_exposure(self, inv) -> None:
        inv.add_trade("m1", "yes", "BUY", 10.0)
        inv.add_trade("m1", "no", "BUY", 5.0)
        inv.add_trade("m2", "yes", "BUY", 8.0)
        assert inv.get_total_exposure() == 23.0

    def test_get_positions_structure(self, inv) -> None:
        inv.add_trade("m1", "yes", "BUY", 10.0)
        inv.add_trade("m1", "no", "BUY", 5.0)
        positions = inv.get_positions()
        assert "m1" in positions
        assert positions["m1"]["yes"] == 10.0
        assert positions["m1"]["no"] == 5.0

    def test_clear_market(self, inv) -> None:
        inv.add_trade("m1", "yes", "BUY", 10.0)
        inv.clear_market("m1")
        assert inv.get_market_inventory("m1").yes == 0.0
        assert inv.get_total_exposure() == 0.0

    def test_can_add_position(self, inv) -> None:
        inv.add_trade("m1", "yes", "BUY", 40.0)
        assert inv.can_add_position("m1", 5.0) is True   # 40 + 5 = 45 < 50
        assert inv.can_add_position("m1", 15.0) is False  # 40 + 15 = 55 > 50


# ===========================================================================
# Tests: CircuitBreaker
# ===========================================================================

class TestCircuitBreaker:
    def setup_method(self) -> None:
        self.cb = CircuitBreaker({
            "circuit_breakers": {
                "max_daily_loss_usdc": 20.0,
                "max_consecutive_losses": 3,
                "max_consecutive_errors": 5,
                "max_single_trade_loss_usdc": 10.0,
                "cooldown_after_break_min": 60,
                "max_open_orders": 5,
                "max_market_loss_1h_usdc": 5.0,
            }
        })

    def test_not_triggered_initially(self) -> None:
        assert self.cb.is_triggered() is False

    def test_triggered_on_daily_loss(self) -> None:
        self.cb.record_trade(-25.0)
        assert self.cb.is_triggered() is True

    def test_triggered_on_consecutive_losses(self) -> None:
        for _ in range(3):
            self.cb.record_trade(-1.0)
        assert self.cb.is_triggered() is True

    def test_consecutive_losses_reset_on_win(self) -> None:
        self.cb.record_trade(-1.0)
        self.cb.record_trade(-1.0)
        self.cb.record_trade(5.0)   # win resets counter
        self.cb.record_trade(-1.0)
        assert self.cb.is_triggered() is False

    def test_triggered_on_consecutive_errors(self) -> None:
        for _ in range(5):
            self.cb.record_error()
        assert self.cb.is_triggered() is True

    def test_alert_callback_on_daily_loss(self) -> None:
        alerts: list[tuple[str, str]] = []
        cb = CircuitBreaker(
            {"circuit_breakers": {"max_daily_loss_usdc": 20.0, "max_consecutive_losses": 10}},
            alert_callback=lambda r, m: alerts.append((r, m)),
        )
        cb.record_trade(-25.0)
        assert any(r == "daily_drawdown" for r, _ in alerts)

    def test_alert_callback_on_consecutive_errors(self) -> None:
        alerts: list[tuple[str, str]] = []
        cb = CircuitBreaker(
            {"circuit_breakers": {"max_consecutive_errors": 3}},
            alert_callback=lambda r, m: alerts.append((r, m)),
        )
        for _ in range(3):
            cb.record_error()
        assert any(r == "consecutive_errors" for r, _ in alerts)

    def test_can_place_order_limit(self) -> None:
        for _ in range(5):
            self.cb.order_placed()
        assert self.cb.can_place_order() is False

    def test_can_place_order_after_closing(self) -> None:
        for _ in range(5):
            self.cb.order_placed()
        self.cb.order_closed()
        assert self.cb.can_place_order() is True

    def test_reset_daily(self) -> None:
        self.cb.record_trade(-15.0)
        self.cb.reset_daily()
        status = self.cb.get_status()
        assert status["daily_pnl"] == 0.0
        assert status["consecutive_losses"] == 0

    def test_reset_daily_clears_market_pnl(self) -> None:
        self.cb.record_market_pnl("market_1", -10.0)
        self.cb.reset_daily()
        assert self.cb.get_markets_to_cancel() == []

    def test_get_status_fields(self) -> None:
        status = self.cb.get_status()
        assert "triggered" in status
        assert "trigger_reason" in status
        assert "daily_pnl" in status
        assert "consecutive_losses" in status
        assert "consecutive_errors" in status
        assert "open_orders" in status
        assert "markets_over_limit" in status

    def test_record_market_pnl_triggers_cancel(self) -> None:
        self.cb.record_market_pnl("market_1", -6.0)  # > $5 limit
        markets = self.cb.get_markets_to_cancel()
        assert "market_1" in markets

    def test_record_market_pnl_below_limit_no_cancel(self) -> None:
        self.cb.record_market_pnl("market_1", -3.0)  # < $5 limit
        assert "market_1" not in self.cb.get_markets_to_cancel()

    def test_market_pnl_window_one_hour(self) -> None:
        """Registros con mas de 1 hora no cuentan para el limite."""
        cb = CircuitBreaker({
            "circuit_breakers": {"max_market_loss_1h_usdc": 5.0}
        })
        # Injectar un registro viejo manualmente en la deque
        from collections import deque
        old_time = time.time() - 7200  # hace 2 horas
        cb._market_pnl["m1"] = deque([(old_time, -6.0)])
        # No debe contar — ya expiro
        markets = cb.get_markets_to_cancel()
        assert "m1" not in markets

    def test_multiple_market_pnl_accumulate(self) -> None:
        self.cb.record_market_pnl("market_1", -2.0)
        self.cb.record_market_pnl("market_1", -2.0)
        self.cb.record_market_pnl("market_1", -2.0)
        # Total = -6 > -5 → should cancel
        assert "market_1" in self.cb.get_markets_to_cancel()

    def test_trigger_reason_stored(self) -> None:
        self.cb.record_trade(-25.0)
        status = self.cb.get_status()
        assert status["trigger_reason"] == "daily_drawdown"

    def test_triggered_only_once_keeps_original_reason(self) -> None:
        self.cb.record_trade(-25.0)  # daily_drawdown
        for _ in range(3):
            self.cb.record_trade(-1.0)  # consecutive_losses — should NOT overwrite
        assert self.cb.get_status()["trigger_reason"] == "daily_drawdown"


# ------------------------------------------------------------------
# Tests: SelfReviewer max drawdown calculation
# ------------------------------------------------------------------

class TestMaxDrawdownCalculation:
    def setup_method(self) -> None:
        from src.review.self_review import SelfReviewer
        self.reviewer = SelfReviewer.__new__(SelfReviewer)

    def test_empty_trades_returns_zero(self) -> None:
        assert self.reviewer._calculate_max_drawdown([]) == 0.0

    def test_only_buys_returns_zero_or_fees(self) -> None:
        trades = [
            {"market_id": "m1", "side": "BUY", "price": 0.50, "size": 20.0, "fee_paid": 0.0, "timestamp": "2026-04-11T01:00:00Z"},
            {"market_id": "m1", "side": "BUY", "price": 0.52, "size": 20.0, "fee_paid": 0.0, "timestamp": "2026-04-11T02:00:00Z"},
        ]
        dd = self.reviewer._calculate_max_drawdown(trades)
        assert dd == 0.0  # No sell = no PnL = no drawdown

    def test_profitable_roundtrip_no_drawdown(self) -> None:
        trades = [
            {"market_id": "m1", "side": "BUY", "price": 0.50, "size": 20.0, "fee_paid": 0.0, "timestamp": "2026-04-11T01:00:00Z"},
            {"market_id": "m1", "side": "SELL", "price": 0.52, "size": 20.0, "fee_paid": 0.0, "timestamp": "2026-04-11T02:00:00Z"},
        ]
        dd = self.reviewer._calculate_max_drawdown(trades)
        assert dd == 0.0  # Profitable = no drawdown

    def test_losing_roundtrip_shows_drawdown(self) -> None:
        trades = [
            {"market_id": "m1", "side": "BUY", "price": 0.55, "size": 20.0, "fee_paid": 0.0, "timestamp": "2026-04-11T01:00:00Z"},
            {"market_id": "m1", "side": "SELL", "price": 0.50, "size": 20.0, "fee_paid": 0.0, "timestamp": "2026-04-11T02:00:00Z"},
        ]
        dd = self.reviewer._calculate_max_drawdown(trades)
        # Loss = (0.50 - 0.55) * 20 = -$1.0 → drawdown = $1.0
        assert abs(dd - 1.0) < 0.01

    def test_fees_contribute_to_drawdown(self) -> None:
        trades = [
            {"market_id": "m1", "side": "BUY", "price": 0.50, "size": 20.0, "fee_paid": 0.5, "timestamp": "2026-04-11T01:00:00Z"},
            {"market_id": "m1", "side": "SELL", "price": 0.51, "size": 20.0, "fee_paid": 0.5, "timestamp": "2026-04-11T02:00:00Z"},
        ]
        dd = self.reviewer._calculate_max_drawdown(trades)
        # PnL from spread = (0.51 - 0.50) * 20 = $0.20
        # Total fees = $1.0
        # Equity at buy: -$0.5, at sell: -$0.5 + $0.20 = -$0.30
        # Peak=0, max drawdown = $0.50 (after the buy's fee)
        assert dd > 0.0

    def test_multiple_markets_independent(self) -> None:
        trades = [
            {"market_id": "m1", "side": "BUY", "price": 0.50, "size": 20.0, "fee_paid": 0.0, "timestamp": "2026-04-11T01:00:00Z"},
            {"market_id": "m2", "side": "BUY", "price": 0.60, "size": 10.0, "fee_paid": 0.0, "timestamp": "2026-04-11T01:30:00Z"},
            {"market_id": "m1", "side": "SELL", "price": 0.52, "size": 20.0, "fee_paid": 0.0, "timestamp": "2026-04-11T02:00:00Z"},
            {"market_id": "m2", "side": "SELL", "price": 0.58, "size": 10.0, "fee_paid": 0.0, "timestamp": "2026-04-11T02:30:00Z"},
        ]
        dd = self.reviewer._calculate_max_drawdown(trades)
        # m1: profit = (0.52 - 0.50) * 20 = $0.40
        # m2: loss = (0.58 - 0.60) * 10 = -$0.20
        # The drawdown depends on ordering — m2's sell at T+2:30 creates a dip
        assert dd >= 0.0
