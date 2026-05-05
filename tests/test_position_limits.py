"""Tests para PositionLimitsManager — limites de posiciones."""

import pytest

from src.risk.position_limits import PositionLimitsManager


class TestPositionLimits:
    """Tests de limites de posiciones."""

    def test_can_open_when_under_limits(self) -> None:
        pl = PositionLimitsManager()
        ok, reason = pl.can_open_position(
            current_positions=5, capital=200.0, size_usd=5.0
        )
        assert ok is True

    def test_cannot_open_when_max_positions_reached(self) -> None:
        pl = PositionLimitsManager()
        ok, reason = pl.can_open_position(
            current_positions=15, capital=200.0, size_usd=1.0
        )
        assert ok is False
        assert "15" in reason

    def test_cannot_open_when_size_exceeds_pct(self) -> None:
        pl = PositionLimitsManager()
        # $20 en capital $200 = 10% > 5% max
        ok, reason = pl.can_open_position(
            current_positions=1, capital=200.0, size_usd=20.0
        )
        assert ok is False
        assert "10.0%" in reason

    def test_exactly_at_limit(self) -> None:
        pl = PositionLimitsManager()
        # $10 en $200 = 5% exacta
        ok, reason = pl.can_open_position(
            current_positions=1, capital=200.0, size_usd=10.0
        )
        assert ok is True

    def test_can_open_with_zero_capital(self) -> None:
        pl = PositionLimitsManager()
        ok, _ = pl.can_open_position(
            current_positions=0, capital=0.0, size_usd=5.0
        )
        assert ok is True

    def test_config_overrides(self) -> None:
        pl = PositionLimitsManager({
            "position_limits": {
                "max_positions": 5,
                "max_pct_per_position": 0.10,
            }
        })
        ok, reason = pl.can_open_position(
            current_positions=5, capital=200.0, size_usd=5.0
        )
        assert ok is False  # 5 >= 5 max
        ok, reason = pl.can_open_position(
            current_positions=1, capital=200.0, size_usd=18.0
        )
        assert ok is True  # 9% < 10%

    def test_get_excess_positions(self) -> None:
        pl = PositionLimitsManager()
        excess = pl.get_excess_positions(
            positions={"mkt1": 5.0, "mkt2": 15.0, "mkt3": 2.0},
            capital=200.0,
        )
        assert excess == ["mkt2"]

    def test_get_excess_none_with_zero_capital(self) -> None:
        pl = PositionLimitsManager()
        excess = pl.get_excess_positions(
            positions={"mkt1": 50.0}, capital=0.0
        )
        assert excess == []

    def test_select_worst_to_close(self) -> None:
        pl = PositionLimitsManager()
        result = pl.select_worst_to_close(
            positions={"mkt_a": 5.0, "mkt_b": 3.0, "mkt_c": 8.0},
            pnl_by_market={"mkt_a": -2.0, "mkt_b": 1.0, "mkt_c": -5.0},
        )
        # Peor primero: mkt_c (-5), mkt_a (-2), mkt_b (1)
        assert result == ["mkt_c", "mkt_a", "mkt_b"]

    def test_select_worst_missing_pnl_defaults_zero(self) -> None:
        pl = PositionLimitsManager()
        result = pl.select_worst_to_close(
            positions={"mkt_a": 5.0},
            pnl_by_market={},
        )
        assert result == ["mkt_a"]

    def test_properties(self) -> None:
        pl = PositionLimitsManager()
        assert pl.max_positions == 15
        assert pl.max_pct_per_position == pytest.approx(0.05)
        assert pl.auto_close_enabled is True

    def test_auto_close_disabled(self) -> None:
        pl = PositionLimitsManager({
            "position_limits": {"auto_close_worst": False}
        })
        assert pl.auto_close_enabled is False

    def test_no_config_defaults(self) -> None:
        pl = PositionLimitsManager(None)
        assert pl.max_positions == 15
        ok, _ = pl.can_open_position(14, 200.0, 5.0)
        assert ok is True

    def test_multiple_excess_positions(self) -> None:
        pl = PositionLimitsManager()
        excess = pl.get_excess_positions(
            positions={"mkt1": 15.0, "mkt2": 20.0, "mkt3": 12.0},
            capital=200.0,
        )
        # limit = 200 * 0.05 = 10. mkt1=15, mkt2=20, mkt3=12 → all excess
        assert set(excess) == {"mkt1", "mkt2", "mkt3"}
