"""Tests para CashReserves — gestion de reserva minima de efectivo."""

import pytest

from src.risk.cash_reserves import CashReserves


class TestCashReserves:
    """Tests de reservas de efectivo."""

    def test_normal_reserve_is_ok(self) -> None:
        cr = CashReserves()
        ok, reason = cr.check(total_capital=200.0, cash_available=100.0)
        assert ok is True
        assert "OK" in reason

    def test_warning_below_min_reserve(self) -> None:
        cr = CashReserves()
        # 0.4% < 0.5% min, pero > 0.2% emergency
        ok, reason = cr.check(total_capital=200.0, cash_available=0.80)
        assert ok is True
        assert "WARNING" in reason

    def test_emergency_below_emergency_pct(self) -> None:
        cr = CashReserves()
        # 0.1% < 0.2% emergency
        ok, reason = cr.check(total_capital=200.0, cash_available=0.20)
        assert ok is False
        assert "EMERGENCY" in reason

    def test_zero_capital_is_ok(self) -> None:
        cr = CashReserves()
        ok, _ = cr.check(total_capital=0.0, cash_available=0.0)
        assert ok is True

    def test_available_for_trading_deducts_reserve(self) -> None:
        cr = CashReserves()
        # Capital $200, cash $10. Reserve = $200 * 0.005 = $1.
        # Available = $10 - $1 = $9.
        avail = cr.available_for_trading(total_capital=200.0, cash_available=10.0)
        assert avail == pytest.approx(9.0)

    def test_available_for_trading_floor_zero(self) -> None:
        cr = CashReserves()
        # Cash menor que reserva → 0
        avail = cr.available_for_trading(total_capital=200.0, cash_available=0.50)
        assert avail == 0.0

    def test_is_emergency(self) -> None:
        cr = CashReserves()
        assert cr.is_emergency(200.0, 0.20) is True
        assert cr.is_emergency(200.0, 100.0) is False

    def test_config_overrides_reserves(self) -> None:
        cr = CashReserves({
            "cash_reserves": {
                "min_reserve_pct": 0.02,
                "emergency_pct": 0.01,
            }
        })
        # $200 capital, $3 cash = 1.5% → entre 1% y 2%: warning no emergency
        ok, reason = cr.check(200.0, 3.0)
        assert ok is True
        assert "WARNING" in reason
        # $1 cash = 0.5% → < 1% emergency
        ok, reason = cr.check(200.0, 1.0)
        assert ok is False

    def test_properties_expose_config(self) -> None:
        cr = CashReserves({"cash_reserves": {"min_reserve_pct": 0.01}})
        assert cr.min_reserve_pct == pytest.approx(0.01)
        assert cr.emergency_pct == pytest.approx(0.002)  # default

    def test_exact_boundary_min_reserve(self) -> None:
        cr = CashReserves()
        # Exactly at min reserve
        cash = 200.0 * 0.005  # exact 0.5%
        ok, reason = cr.check(200.0, cash)
        assert ok is True
        assert "OK" in reason or "WARNING" in reason  # >= behaviour

    def test_exact_boundary_emergency(self) -> None:
        cr = CashReserves()
        # Exactly at emergency
        cash = 200.0 * 0.002  # exact 0.2%
        ok, reason = cr.check(200.0, cash)
        assert ok is True  # >= emergency, not strictly less
        assert "WARNING" in reason or "OK" in reason  # >= emergency so not emergency

    def test_no_config_defaults(self) -> None:
        cr = CashReserves(None)
        assert cr.min_reserve_pct == pytest.approx(0.005)
        assert cr.emergency_pct == pytest.approx(0.002)

    def test_small_capital_rounding(self) -> None:
        """Con capital chico la reserva es chica pero sigue funcionando."""
        cr = CashReserves()
        ok, _ = cr.check(total_capital=50.0, cash_available=50.0)
        assert ok is True
