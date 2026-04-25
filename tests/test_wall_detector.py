"""Tests para src/analysis/wall_detector.py"""

import pytest
from src.analysis.wall_detector import is_large_wall, best_price_near_wall


class TestIsLargeWall:
    def test_detects_wall_10x(self):
        """Wall de 300 shares con min_share=20 → 15x → detectada."""
        book = [(0.47, 300.0), (0.46, 50.0), (0.45, 20.0)]
        found, price = is_large_wall(book, min_share=20.0)
        assert found is True
        assert price == pytest.approx(0.47)

    def test_no_wall_uniform_book(self):
        """Book uniforme de 50 shares con min_share=20 → 2.5x < 10x → no wall."""
        book = [(0.50, 50.0), (0.49, 48.0), (0.48, 52.0)]
        found, price = is_large_wall(book, min_share=20.0)
        assert found is False
        assert price == 0.0

    def test_wall_exactly_10x(self):
        """Wall exactamente en el umbral (10x) → detectada."""
        book = [(0.55, 200.0), (0.54, 10.0)]
        found, price = is_large_wall(book, min_share=20.0, multiplier=10.0)
        assert found is True
        assert price == pytest.approx(0.55)

    def test_wall_below_threshold(self):
        """Wall de 9.9x → no detectada."""
        book = [(0.55, 198.0), (0.54, 10.0)]
        found, price = is_large_wall(book, min_share=20.0, multiplier=10.0)
        assert found is False

    def test_empty_book(self):
        found, price = is_large_wall([], min_share=20.0)
        assert found is False
        assert price == 0.0

    def test_zero_min_share(self):
        """min_share=0 → siempre retorna False (evitar division semantica)."""
        book = [(0.50, 9999.0)]
        found, price = is_large_wall(book, min_share=0.0)
        assert found is False

    def test_custom_multiplier(self):
        """Multiplier 5x: wall de 100 con min_share=20 → 5x → detectada."""
        book = [(0.52, 100.0), (0.51, 30.0)]
        found, price = is_large_wall(book, min_share=20.0, multiplier=5.0)
        assert found is True
        assert price == pytest.approx(0.52)

    def test_returns_first_wall(self):
        """Retorna la primera wall encontrada (mejor precio del book)."""
        book = [(0.48, 500.0), (0.47, 600.0), (0.46, 10.0)]
        found, price = is_large_wall(book, min_share=20.0)
        assert found is True
        assert price == pytest.approx(0.48)


class TestBestPriceNearWall:
    def test_returns_wall_price_when_found(self):
        book = [(0.47, 300.0), (0.46, 10.0)]
        result = best_price_near_wall(book, min_share=20.0, is_bid=True)
        assert result == pytest.approx(0.47)

    def test_returns_zero_when_no_wall(self):
        book = [(0.50, 30.0), (0.49, 25.0)]
        result = best_price_near_wall(book, min_share=20.0, is_bid=True)
        assert result == 0.0

    def test_returns_zero_empty_book(self):
        result = best_price_near_wall([], min_share=20.0, is_bid=False)
        assert result == 0.0
