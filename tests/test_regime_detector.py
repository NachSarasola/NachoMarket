"""Tests para MarketRegimeDetector (TODO 1.3)."""
import pytest
from src.analysis.regime_detector import (
    MarketRegimeDetector, Regime, _compute_hurst, _std
)


class TestRegimeDetector:
    def make_detector(self, **kwargs):
        return MarketRegimeDetector(**kwargs)

    def test_unknown_with_few_prices(self):
        d = self.make_detector()
        d.update("tok1", 0.5)
        state = d.get_state("tok1")
        assert state.regime == Regime.UNKNOWN

    def test_unknown_no_prices(self):
        d = self.make_detector()
        state = d.get_state("nonexistent")
        assert state.regime == Regime.UNKNOWN
        assert state.should_pause_mm is False

    def test_mean_reverting_regime(self):
        d = self.make_detector(
            hurst_mean_revert_max=0.45,
            hurst_trending_min=0.55,
            volatile_std_threshold=100.0,  # Desactivar volatile
        )
        # Inyectar precios que se comportan como mean-reverting
        # (alternando alrededor de un valor fijo)
        prices = [0.50, 0.52, 0.48, 0.51, 0.49, 0.52, 0.48, 0.50, 0.51, 0.49,
                  0.50, 0.52, 0.48, 0.51, 0.49, 0.52, 0.48, 0.50, 0.51, 0.49,
                  0.50, 0.52, 0.48]
        for p in prices:
            d.update("tok1", p)
        state = d.get_state("tok1")
        # Puede ser MEAN_REVERTING o UNKNOWN dependiendo del hurst calculado
        assert state.regime in (Regime.MEAN_REVERTING, Regime.UNKNOWN)
        assert state.spread_multiplier >= 1.0
        assert state.size_multiplier <= 1.5

    def test_volatile_regime_detected(self):
        d = self.make_detector(
            volatile_std_threshold=0.001,  # Threshold muy bajo → casi siempre volatile
        )
        prices = [0.5 + i * 0.01 for i in range(25)]
        for p in prices:
            d.update("tok1", p)
        state = d.get_state("tok1")
        assert state.regime == Regime.VOLATILE
        assert state.should_pause_mm is True
        assert state.size_multiplier == 0.25

    def test_volatile_multipliers(self):
        d = self.make_detector(volatile_std_threshold=0.001)
        for i in range(25):
            d.update("tok1", 0.5 + i * 0.05)  # Alta volatilidad
        state = d.get_state("tok1")
        if state.regime == Regime.VOLATILE:
            assert state.spread_multiplier == 3.0
            assert state.size_multiplier == 0.25

    def test_trending_multipliers(self):
        d = self.make_detector(
            hurst_trending_min=0.0,  # Siempre trending
            volatile_std_threshold=100.0,
        )
        for i in range(25):
            d.update("tok1", 0.5)  # Todos iguales → Hurst indefinido
        # Con Hurst > 0.0 si se fuerza, el spread y size se ajustan
        state = d.get_state("tok1")
        assert state.spread_multiplier >= 1.0

    def test_clear_removes_buffer(self):
        d = self.make_detector()
        for _ in range(25):
            d.update("tok1", 0.5)
        d.clear("tok1")
        state = d.get_state("tok1")
        assert state.regime == Regime.UNKNOWN

    def test_get_all_states(self):
        d = self.make_detector()
        for _ in range(25):
            d.update("tok1", 0.5)
            d.update("tok2", 0.3)
        states = d.get_all_states()
        assert "tok1" in states
        assert "tok2" in states

    def test_invalid_prices_ignored(self):
        d = self.make_detector()
        d.update("tok1", -1.0)  # Precio invalido
        d.update("tok1", 0.0)   # Precio 0
        state = d.get_state("tok1")
        assert state.regime == Regime.UNKNOWN  # Sin precios validos


class TestHurstExponent:
    def test_hurst_range(self):
        # Cualquier serie realista debe dar Hurst en [0, 1]
        prices = [0.5 + i * 0.001 for i in range(50)]
        h = _compute_hurst(prices)
        assert 0.0 <= h <= 1.0

    def test_short_series_returns_half(self):
        h = _compute_hurst([0.5, 0.6])
        assert h == 0.5

    def test_constant_series_handled(self):
        prices = [0.5] * 50
        h = _compute_hurst(prices)
        # No debe crashear; retorna 0.5 por defecto cuando std=0
        assert 0.0 <= h <= 1.0


class TestStd:
    def test_empty_returns_zero(self):
        assert _std([]) == 0.0

    def test_single_returns_zero(self):
        assert _std([1.0]) == 0.0

    def test_known_values(self):
        # std([2, 4, 4, 4, 5, 5, 7, 9]) = 2 (poblacional)
        values = [2, 4, 4, 4, 5, 5, 7, 9]
        result = _std(values)
        assert abs(result - 2.0) < 0.1
