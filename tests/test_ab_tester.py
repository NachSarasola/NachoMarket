"""Tests para ABTester / ABTestManager (TODO 3.3)."""
import tempfile
import time
from pathlib import Path
from unittest.mock import MagicMock, patch
import pytest

import src.strategy.ab_tester as ab_module
from src.strategy.ab_tester import ABTester, ABTestManager, VariantStats


@pytest.fixture(autouse=True)
def _isolate_state_file(tmp_path, monkeypatch):
    """Aísla el archivo de estado en un directorio temporal por test."""
    monkeypatch.setattr(ab_module, "_AB_STATE_FILE", tmp_path / "ab_test_state.json")


def make_tester(variants=None, evaluation_days=7, **kwargs):
    if variants is None:
        variants = [
            {"name": "v_a", "params": {"spread_offset": 0.01}},
            {"name": "v_b", "params": {"spread_offset": 0.02}},
        ]
    return ABTester(
        strategy_name="market_maker",
        variants=variants,
        evaluation_days=evaluation_days,
        **kwargs,
    )


# ----------------------------
# VariantStats
# ----------------------------

class TestVariantStats:
    def test_winrate_zero_with_no_trades(self):
        v = VariantStats(name="v", params={})
        assert v.winrate == 0.0

    def test_winrate_computed_correctly(self):
        v = VariantStats(name="v", params={}, wins=3, losses=1)
        assert v.winrate == pytest.approx(0.75)

    def test_avg_pnl_zero_without_trades(self):
        v = VariantStats(name="v", params={})
        assert v.avg_pnl_per_trade == 0.0

    def test_avg_pnl_computed(self):
        v = VariantStats(name="v", params={}, pnl_total=10.0, trade_count=4)
        assert v.avg_pnl_per_trade == pytest.approx(2.5)

    def test_to_dict_contains_all_fields(self):
        v = VariantStats(name="v", params={"x": 1})
        d = v.to_dict()
        for key in ["name", "params", "pnl_total", "trade_count", "wins",
                    "losses", "winrate", "avg_pnl_per_trade",
                    "started_at", "is_winner", "is_eliminated"]:
            assert key in d


# ----------------------------
# record_outcome
# ----------------------------

class TestRecordOutcome:
    def test_wins_incremented(self):
        t = make_tester()
        t.record_outcome("v_a", 1.5)
        assert t._variants["v_a"].wins == 1
        assert t._variants["v_a"].pnl_total == pytest.approx(1.5)

    def test_losses_incremented(self):
        t = make_tester()
        t.record_outcome("v_b", -2.0)
        assert t._variants["v_b"].losses == 1

    def test_unknown_variant_ignored(self):
        t = make_tester()
        t.record_outcome("nonexistent", 5.0)  # No debe lanzar

    def test_eliminated_variant_not_updated(self):
        t = make_tester()
        t._variants["v_a"].is_eliminated = True
        t.record_outcome("v_a", 100.0)
        assert t._variants["v_a"].pnl_total == 0.0


# ----------------------------
# evaluate
# ----------------------------

class TestEvaluate:
    def test_no_winner_before_evaluation_period(self):
        t = make_tester(evaluation_days=30)
        for _ in range(5):
            t.record_outcome("v_a", 1.0)
        assert t.evaluate() is None

    def test_winner_chosen_after_period_with_enough_trades(self):
        t = make_tester(evaluation_days=0)  # Expire inmediatamente
        t._started_at = time.time() - 100  # Simular tiempo expirado
        for _ in range(6):
            t.record_outcome("v_a", 2.0)
        for _ in range(6):
            t.record_outcome("v_b", 1.0)
        winner = t.evaluate()
        assert winner == "v_a"

    def test_winner_marked_correctly(self):
        t = make_tester()
        t._started_at = time.time() - 10 * 86400  # 10 días atrás
        for _ in range(6):
            t.record_outcome("v_a", 3.0)
        for _ in range(6):
            t.record_outcome("v_b", 0.5)
        t.evaluate()
        assert t._variants["v_a"].is_winner is True
        assert t._variants["v_b"].is_eliminated is True

    def test_test_deactivated_after_winner(self):
        t = make_tester()
        t._started_at = time.time() - 10 * 86400
        # Need ≥ 10 total trades across all variants
        for _ in range(6):
            t.record_outcome("v_a", 1.0)
        for _ in range(5):
            t.record_outcome("v_b", 0.1)
        t.evaluate()
        assert t._test_active is False

    def test_consolidation_callback_called(self):
        cb = MagicMock()
        t = make_tester(consolidation_callback=cb)
        t._started_at = time.time() - 10 * 86400
        # ≥ 10 trades total
        for _ in range(6):
            t.record_outcome("v_a", 5.0)
        for _ in range(5):
            t.record_outcome("v_b", 0.1)
        t.evaluate()
        cb.assert_called_once()
        args = cb.call_args[0][0]
        assert "spread_offset" in args  # Parámetros del ganador

    def test_no_winner_if_insufficient_trades(self):
        t = make_tester()
        t._started_at = time.time() - 10 * 86400
        # Sólo 3 trades en total (< 10 mínimo)
        t.record_outcome("v_a", 1.0)
        t.record_outcome("v_b", 0.5)
        t.record_outcome("v_a", 1.0)
        winner = t.evaluate()
        assert winner is None

    def test_evaluate_returns_same_winner_after_consolidation(self):
        t = make_tester()
        t._started_at = time.time() - 10 * 86400
        for _ in range(6):
            t.record_outcome("v_a", 1.0)
        winner1 = t.evaluate()
        winner2 = t.evaluate()
        assert winner1 == winner2


# ----------------------------
# get_capital_allocation
# ----------------------------

class TestCapitalAllocation:
    def test_equal_split_during_test(self):
        t = make_tester(capital_fraction_each=0.5)
        alloc = t.get_capital_allocation(100.0)
        assert alloc["v_a"] == pytest.approx(50.0)
        assert alloc["v_b"] == pytest.approx(50.0)

    def test_all_capital_to_winner_after_test(self):
        t = make_tester()
        t._test_active = False
        t._winner = "v_b"
        alloc = t.get_capital_allocation(200.0)
        assert alloc == {"v_b": 200.0}

    def test_empty_when_all_eliminated(self):
        t = make_tester()
        for v in t._variants.values():
            v.is_eliminated = True
        alloc = t.get_capital_allocation(100.0)
        assert alloc == {}

    def test_get_params_for_variant(self):
        t = make_tester()
        params = t.get_params_for_variant("v_a")
        assert params == {"spread_offset": 0.01}

    def test_get_params_unknown_returns_empty(self):
        t = make_tester()
        assert t.get_params_for_variant("nonexistent") == {}


# ----------------------------
# format_telegram
# ----------------------------

class TestFormatTelegram:
    def test_contains_strategy_name(self):
        t = make_tester()
        text = t.format_telegram()
        assert "market_maker" in text

    def test_contains_variant_names(self):
        t = make_tester()
        text = t.format_telegram()
        assert "v_a" in text
        assert "v_b" in text


# ----------------------------
# get_report
# ----------------------------

class TestGetReport:
    def test_report_structure(self):
        t = make_tester()
        r = t.get_report()
        assert "strategy_name" in r
        assert "variants" in r
        assert "test_active" in r
        assert "winner" in r

    def test_report_reflects_winner(self):
        t = make_tester()
        t._started_at = time.time() - 10 * 86400
        # ≥ 10 total trades
        for _ in range(7):
            t.record_outcome("v_b", 3.0)
        for _ in range(4):
            t.record_outcome("v_a", 0.1)
        t.evaluate()
        r = t.get_report()
        assert r["winner"] == "v_b"


# ----------------------------
# ABTestManager
# ----------------------------

class TestABTestManager:
    def test_manager_creates_testers_from_config(self):
        config = {
            "ab_tests": [
                {
                    "strategy": "market_maker",
                    "variants": [
                        {"name": "tight", "params": {"spread_offset": 0.01}},
                        {"name": "wide", "params": {"spread_offset": 0.03}},
                    ],
                    "evaluation_days": 7,
                }
            ]
        }
        mgr = ABTestManager(config)
        assert "market_maker" in mgr._tests

    def test_manager_record_outcome(self):
        config = {
            "ab_tests": [
                {
                    "strategy": "s1",
                    "variants": [{"name": "v1", "params": {}}],
                    "evaluation_days": 7,
                }
            ]
        }
        mgr = ABTestManager(config)
        mgr.record_outcome("s1", "v1", 5.0)
        assert mgr._tests["s1"]._variants["v1"].pnl_total == pytest.approx(5.0)

    def test_manager_get_tester(self):
        config = {"ab_tests": [{"strategy": "mm", "variants": [], "evaluation_days": 7}]}
        mgr = ABTestManager(config)
        assert mgr.get_tester("mm") is not None
        assert mgr.get_tester("unknown") is None

    def test_evaluate_all_returns_winner_names(self):
        config = {
            "ab_tests": [
                {
                    "strategy": "strat",
                    "variants": [
                        {"name": "A", "params": {}},
                        {"name": "B", "params": {}},
                    ],
                    "evaluation_days": 0,
                }
            ]
        }
        mgr = ABTestManager(config)
        t = mgr._tests["strat"]
        t._started_at = time.time() - 100
        # ≥ 10 total trades
        for _ in range(7):
            mgr.record_outcome("strat", "A", 2.0)
        for _ in range(4):
            mgr.record_outcome("strat", "B", 0.1)
        winners = mgr.evaluate_all()
        assert "strat:A" in winners

    def test_no_ab_tests_config(self):
        mgr = ABTestManager({})
        assert mgr.format_all_telegram() == "No hay tests A/B activos."

    def test_empty_ab_tests_list(self):
        mgr = ABTestManager({"ab_tests": []})
        assert len(mgr._tests) == 0
