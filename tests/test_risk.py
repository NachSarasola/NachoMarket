import pytest

from src.risk.position_sizer import PositionSizer
from src.risk.circuit_breaker import CircuitBreaker


class TestPositionSizer:
    def setup_method(self) -> None:
        self.sizer = PositionSizer({
            "position_sizing": {
                "method": "fractional_kelly",
                "kelly_fraction": 0.25,
                "max_position_usdc": 20.0,
                "min_position_usdc": 1.0,
            }
        })

    def test_kelly_positive_edge(self) -> None:
        size = self.sizer.calculate_size(
            capital=400, win_probability=0.6, odds=2.0, max_risk_pct=5.0
        )
        assert 0 < size <= 20.0

    def test_kelly_no_edge(self) -> None:
        size = self.sizer.calculate_size(
            capital=400, win_probability=0.5, odds=2.0, max_risk_pct=5.0
        )
        assert size == 0.0  # No edge = no trade

    def test_kelly_negative_edge(self) -> None:
        size = self.sizer.calculate_size(
            capital=400, win_probability=0.3, odds=2.0, max_risk_pct=5.0
        )
        assert size == 0.0

    def test_never_exceeds_max_risk(self) -> None:
        size = self.sizer.calculate_size(
            capital=400, win_probability=0.9, odds=10.0, max_risk_pct=5.0
        )
        assert size <= 20.0  # 5% de $400

    def test_fixed_method(self) -> None:
        sizer = PositionSizer({
            "position_sizing": {
                "method": "fixed",
                "max_position_usdc": 10.0,
                "min_position_usdc": 1.0,
            }
        })
        size = sizer.calculate_size(capital=400, win_probability=0.6, odds=2.0)
        assert size == 10.0  # min(10, 400 * 5%)


class TestCircuitBreaker:
    def setup_method(self) -> None:
        self.cb = CircuitBreaker({
            "circuit_breakers": {
                "max_daily_loss_usdc": 20.0,
                "max_consecutive_losses": 3,
                "max_single_trade_loss_usdc": 10.0,
                "cooldown_after_break_min": 60,
                "max_open_orders": 5,
            }
        })

    def test_not_triggered_initially(self) -> None:
        assert self.cb.is_triggered() is False

    def test_triggered_on_daily_loss(self) -> None:
        self.cb.record_trade(-25.0)  # Excede $20
        assert self.cb.is_triggered() is True

    def test_triggered_on_consecutive_losses(self) -> None:
        for _ in range(3):
            self.cb.record_trade(-1.0)
        assert self.cb.is_triggered() is True

    def test_consecutive_losses_reset_on_win(self) -> None:
        self.cb.record_trade(-1.0)
        self.cb.record_trade(-1.0)
        self.cb.record_trade(5.0)  # Win resets counter
        self.cb.record_trade(-1.0)
        assert self.cb.is_triggered() is False

    def test_can_place_order_limit(self) -> None:
        for _ in range(5):
            self.cb.order_placed()
        assert self.cb.can_place_order() is False

    def test_reset_daily(self) -> None:
        self.cb.record_trade(-15.0)
        self.cb.reset_daily()
        status = self.cb.get_status()
        assert status["daily_pnl"] == 0.0

    def test_get_status(self) -> None:
        status = self.cb.get_status()
        assert "triggered" in status
        assert "daily_pnl" in status
        assert "consecutive_losses" in status
        assert "open_orders" in status
