"""Tests para CategoryScorer — scoring de categorias con ROI historico."""

import pytest

from src.strategy.category_scorer import CategoryScorer


class TestCategoryScorer:
    """Tests basicos de scoring y bloqueo."""

    def test_seed_scores_present(self) -> None:
        scorer = CategoryScorer()
        assert scorer.get_score("politics") == 65
        assert scorer.get_score("economics") == 15
        assert scorer.get_score("crypto") == 60

    def test_unknown_category_defaults_50(self) -> None:
        scorer = CategoryScorer()
        assert scorer.get_score("nonexistent") == 50.0

    def test_case_insensitive(self) -> None:
        scorer = CategoryScorer()
        assert scorer.get_score("POLITICS") == 65
        assert scorer.get_score("Sports") == 35

    def test_blocked_threshold(self) -> None:
        scorer = CategoryScorer()
        assert scorer.is_blocked("economics") is True   # 15 < 30
        assert scorer.is_blocked("entertainment") is True  # 25 < 30
        assert scorer.is_blocked("politics") is False    # 65 >= 30

    def test_allocation_tiers(self) -> None:
        scorer = CategoryScorer()
        # politics=65 → GOOD → 10%
        assert scorer.get_allocation_pct("politics") == 0.10
        # sports=35 → POOR → 2%
        assert scorer.get_allocation_pct("sports") == 0.02
        # economics=15 → BLOCKED → 0%
        assert scorer.get_allocation_pct("economics") == 0.0
        # unknown=50 → WEAK → 5%
        assert scorer.get_allocation_pct("nonexistent") == 0.05

    def test_config_override_block_threshold(self) -> None:
        scorer = CategoryScorer({"category_scorer": {"block_threshold": 50}})
        assert scorer.is_blocked("sports") is True   # 35 < 50
        assert scorer.is_blocked("politics") is False  # 65 >= 50

    def test_config_override_seed_scores(self) -> None:
        scorer = CategoryScorer({"category_scorer": {"seed_scores": {"sports": 80}}})
        assert scorer.get_score("sports") == 80

    def test_get_all_scores_snapshot(self) -> None:
        scorer = CategoryScorer()
        scores = scorer.get_all_scores()
        assert scores["politics"] == 65
        assert isinstance(scores, dict)

    def test_update_no_recalculation_with_few_trades(self) -> None:
        scorer = CategoryScorer()
        scorer.update_from_trade("sports", 1.0)
        scorer.update_from_trade("sports", -0.5)
        # Solo 2 trades, no recalcula — score sigue siendo seed
        assert scorer.get_score("sports") == 35

    def test_update_recalculates_after_enough_trades(self) -> None:
        scorer = CategoryScorer({"category_scorer": {"min_trades_to_recalculate": 5}})
        # Simular 10 trades con 70% WR y PnL positivo
        for i in range(10):
            pnl = 0.50 if i < 7 else -0.50
            scorer.update_from_trade("sports", pnl)
        # Deberia haber subido de 35 (seed)
        assert scorer.get_score("sports") > 35

    def test_update_blends_with_seed_score(self) -> None:
        """Score nuevo debe ser blend (alpha=0.3) con seed."""
        scorer = CategoryScorer({"category_scorer": {"min_trades_to_recalculate": 5}})
        # 10 trades todos ganadores → WR=100%, PnL=+$5
        for _ in range(10):
            scorer.update_from_trade("crypto", 0.50)
        new_score = scorer.get_score("crypto")
        # Nuevo seria ~60*1.0 + min(5,20) mapped = 60+25=85 → blend 0.3*85+0.7*60=67.5
        assert 60 < new_score < 85

    def test_update_with_all_losses_lowers_score(self) -> None:
        scorer = CategoryScorer({"category_scorer": {"min_trades_to_recalculate": 5}})
        # 10 trades todos perdedores → WR=0%, PnL=-$5
        for _ in range(10):
            scorer.update_from_trade("crypto", -0.50)
        assert scorer.get_score("crypto") < 60

    def test_empty_category_ignored(self) -> None:
        scorer = CategoryScorer()
        scorer.update_from_trade("", 1.0)
        scorer.update_from_trade("  ", 1.0)
        scores = scorer.get_all_scores()
        assert "" not in scores
        assert "  " not in scores

    def test_score_clamped_0_to_100(self) -> None:
        scorer = CategoryScorer({"category_scorer": {"min_trades_to_recalculate": 5}})
        # Muchisimos trades ganadores
        for _ in range(20):
            scorer.update_from_trade("politics", 2.0)
        assert 0 <= scorer.get_score("politics") <= 100

    def test_no_config_is_fine(self) -> None:
        scorer = CategoryScorer(None)
        assert scorer.get_score("politics") == 65
