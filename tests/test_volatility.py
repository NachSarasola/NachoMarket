"""Tests para VolatilityCalculator — volatilidad multi-ventana."""

import math

import pytest

from src.utils.volatility import (
    VolatilityCalculator,
    _annualize,
    _log_returns,
    _std,
)


class TestLogReturns:
    def test_basic(self) -> None:
        r = _log_returns([1.0, 2.0, 4.0])
        assert len(r) == 2
        assert r[0] == pytest.approx(math.log(2.0))
        assert r[1] == pytest.approx(math.log(2.0))

    def test_no_change(self) -> None:
        r = _log_returns([0.50, 0.50, 0.50])
        assert all(abs(x) < 1e-9 for x in r)

    def test_insufficient_data(self) -> None:
        assert _log_returns([1.0]) == []
        assert _log_returns([]) == []

    def test_zero_price_skipped(self) -> None:
        r = _log_returns([0.0, 0.50, 0.60])
        # 0.0 → 0.50 skipped, 0.50 → 0.60 included
        assert len(r) == 1

    def test_prices(self) -> None:
        r = _log_returns([0.50, 0.55, 0.52, 0.58, 0.53])
        assert len(r) == 4


class TestStd:
    def test_basic(self) -> None:
        assert _std([1.0, 1.0, 1.0]) == 0.0

    def test_positive(self) -> None:
        s = _std([1.0, 2.0, 3.0])
        assert s > 0

    def test_insufficient(self) -> None:
        assert _std([]) == 0.0
        assert _std([5.0]) == 0.0


class TestAnnualize:
    def test_annualize_1h(self) -> None:
        # sigma_raw = 0.01 en 1h → * sqrt(8760) ≈ 0.01 * 93.59 ≈ 0.9359
        result = _annualize(0.01, 1.0)
        expected = 0.01 * math.sqrt(8760.0)
        assert result == pytest.approx(expected)

    def test_zero_timeframe(self) -> None:
        assert _annualize(0.01, 0.0) == 0.0

    def test_24h_annualization(self) -> None:
        # sigma_raw = 0.05 en 24h → * sqrt(365) ≈ 0.05 * 19.10 ≈ 0.955
        result = _annualize(0.05, 24.0)
        expected = 0.05 * math.sqrt(365.0)
        assert result == pytest.approx(expected)


class TestVolatilityCalculator:
    def test_calculate_constant_price(self) -> None:
        vc = VolatilityCalculator()
        vol = vc.calculate([0.50] * 100, timeframe_hours=24.0)
        assert vol == 0.0

    def test_calculate_volatile(self) -> None:
        vc = VolatilityCalculator()
        prices = [0.50 + 0.02 * math.sin(i / 5) for i in range(200)]
        vol = vc.calculate(prices, timeframe_hours=24.0)
        assert vol > 0

    def test_calculate_insufficient_data(self) -> None:
        vc = VolatilityCalculator()
        assert vc.calculate([0.50], 1.0) == 0.0
        assert vc.calculate([], 1.0) == 0.0

    def test_calculate_all(self) -> None:
        vc = VolatilityCalculator()
        prices = [0.50 + 0.01 * math.sin(i / 10) for i in range(800)]
        result = vc.calculate_all(prices)
        for tf in vc.timeframes:
            label = f"{tf}h" if tf < 720 else f"{tf // 24}d"
            assert label in result
            assert result[label] >= 0

    def test_is_high_volatility_false(self) -> None:
        vc = VolatilityCalculator()
        prices = [0.50] * 100
        assert vc.is_high_volatility(prices, threshold=0.50) is False

    def test_is_high_volatility_true(self) -> None:
        vc = VolatilityCalculator()
        # Precios muy volatiles
        prices = [0.50, 0.80, 0.20, 0.90, 0.10, 0.85] * 20
        assert vc.is_high_volatility(prices, threshold=0.01) is True

    def test_custom_threshold(self) -> None:
        vc = VolatilityCalculator()
        prices = [0.50] * 100
        assert vc.is_high_volatility(prices, threshold=0.0) is False

    def test_properties(self) -> None:
        vc = VolatilityCalculator()
        assert len(vc.timeframes) == 8
        assert vc.max_3h_volatility == pytest.approx(0.80)

    def test_config_override(self) -> None:
        vc = VolatilityCalculator({
            "volatility": {
                "timeframes": [1, 24],
                "max_volatility_3h": 0.50,
            }
        })
        assert vc.timeframes == [1, 24]
        assert vc.max_3h_volatility == pytest.approx(0.50)

    def test_no_config_defaults(self) -> None:
        vc = VolatilityCalculator(None)
        assert len(vc.timeframes) == 8

    def test_calculate_all_empty_prices(self) -> None:
        vc = VolatilityCalculator()
        result = vc.calculate_all([])
        assert result == {}

    def test_calculate_all_single_price(self) -> None:
        vc = VolatilityCalculator()
        result = vc.calculate_all([0.50])
        assert result == {}
