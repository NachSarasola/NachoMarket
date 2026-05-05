"""Tests para EdgeFilter — thresholds adaptativos por confidence."""

import pytest

from src.risk.edge_filter import EdgeFilter


class TestEdgeFilter:
    """Tests de thresholds adaptativos."""

    def test_high_confidence_passes_with_4pct_edge(self) -> None:
        ef = EdgeFilter()
        passes, edge = ef.has_sufficient_edge(0.55, 0.50, confidence=0.85)
        assert passes is True
        assert edge == pytest.approx(0.05)

    def test_high_confidence_fails_with_3pct_edge(self) -> None:
        ef = EdgeFilter()
        passes, edge = ef.has_sufficient_edge(0.52, 0.50, confidence=0.85)
        assert passes is False
        assert edge == pytest.approx(0.02)

    def test_medium_confidence_passes_with_6pct_edge(self) -> None:
        ef = EdgeFilter()
        passes, _ = ef.has_sufficient_edge(0.56, 0.50, confidence=0.70)
        assert passes is True

    def test_medium_confidence_fails_with_4pct_edge(self) -> None:
        ef = EdgeFilter()
        passes, _ = ef.has_sufficient_edge(0.54, 0.50, confidence=0.70)
        assert passes is False  # 4% < 5% threshold

    def test_low_confidence_passes_with_10pct_edge(self) -> None:
        ef = EdgeFilter()
        passes, _ = ef.has_sufficient_edge(0.60, 0.50, confidence=0.50)
        assert passes is True

    def test_low_confidence_fails_with_5pct_edge(self) -> None:
        ef = EdgeFilter()
        passes, _ = ef.has_sufficient_edge(0.55, 0.50, confidence=0.50)
        assert passes is False  # 5% < 8% threshold

    def test_below_min_confidence_always_fails(self) -> None:
        ef = EdgeFilter()
        passes, _ = ef.has_sufficient_edge(0.90, 0.10, confidence=0.30)
        assert passes is False

    def test_exact_threshold_boundary(self) -> None:
        ef = EdgeFilter()
        # High confidence boundary: exactly 80%
        passes, _ = ef.has_sufficient_edge(0.54, 0.50, confidence=0.80)
        assert passes is True  # 0.04 >= 0.04 high threshold

    def test_edge_zero_always_fails(self) -> None:
        ef = EdgeFilter()
        passes, _ = ef.has_sufficient_edge(0.50, 0.50, confidence=0.90)
        assert passes is False

    def test_invalid_market_price_fails(self) -> None:
        ef = EdgeFilter()
        passes, _ = ef.has_sufficient_edge(0.50, 0.0, confidence=0.90)
        assert passes is False
        passes, _ = ef.has_sufficient_edge(0.50, 1.0, confidence=0.90)
        assert passes is False

    def test_invalid_estimated_prob_fails(self) -> None:
        ef = EdgeFilter()
        passes, _ = ef.has_sufficient_edge(0.0, 0.50, confidence=0.90)
        assert passes is False
        passes, _ = ef.has_sufficient_edge(1.0, 0.50, confidence=0.90)
        assert passes is False

    def test_config_overrides_thresholds(self) -> None:
        ef = EdgeFilter({
            "edge_filter": {
                "high_confidence_edge": 0.02,
                "medium_confidence_edge": 0.03,
                "low_confidence_edge": 0.05,
            }
        })
        passes, _ = ef.has_sufficient_edge(0.52, 0.50, confidence=0.85)
        assert passes is True  # 2% >= 2% (overridden)
        passes, _ = ef.has_sufficient_edge(0.53, 0.50, confidence=0.70)
        assert passes is True  # 3% >= 3% (overridden)

    def test_config_override_min_confidence(self) -> None:
        ef = EdgeFilter({"edge_filter": {"min_confidence": 0.50}})
        passes, _ = ef.has_sufficient_edge(0.60, 0.50, confidence=0.45)
        assert passes is False  # 0.45 < 0.50

    def test_thresholds_property(self) -> None:
        ef = EdgeFilter()
        t = ef.thresholds
        assert t["high_confidence_edge"] == pytest.approx(0.04)
        assert t["medium_confidence_edge"] == pytest.approx(0.05)
        assert t["low_confidence_edge"] == pytest.approx(0.08)

    def test_no_config_is_fine(self) -> None:
        ef = EdgeFilter(None)
        passes, _ = ef.has_sufficient_edge(0.55, 0.50, confidence=0.85)
        assert passes is True

    def test_large_edge_always_passes(self) -> None:
        ef = EdgeFilter()
        passes, edge = ef.has_sufficient_edge(0.90, 0.10, confidence=0.45)
        assert passes is True
        assert edge == pytest.approx(0.80)

    def test_edge_symmetric(self) -> None:
        """Edge es simetrico: misma diferencia en ambas direcciones."""
        ef = EdgeFilter()
        passes1, e1 = ef.has_sufficient_edge(0.40, 0.50, confidence=0.85)
        passes2, e2 = ef.has_sufficient_edge(0.60, 0.50, confidence=0.85)
        assert e1 == pytest.approx(e2)
        assert passes1 == passes2
