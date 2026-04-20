"""Tests para StrategyAllocator Thompson Sampling (TODO 3.1)."""
import tempfile
import pytest
from src.strategy.allocator import StrategyAllocator


def make_allocator(strategies=None, **kwargs):
    strategies = strategies or ["market_maker", "multi_arb", "stat_arb"]
    with tempfile.TemporaryDirectory() as tmp:
        state_path = f"{tmp}/allocator_state.json"
        return StrategyAllocator(strategies, state_path=state_path, seed=42, **kwargs)


class TestAllocatorBasic:
    def test_allocations_sum_to_capital(self):
        a = make_allocator()
        allocs = a.get_allocations(total_capital=400.0)
        total = sum(allocs.values())
        assert abs(total - 400.0) < 0.01

    def test_all_strategies_get_allocation(self):
        a = make_allocator()
        allocs = a.get_allocations(400.0)
        assert len(allocs) == 3
        assert all(v >= 0 for v in allocs.values())

    def test_single_strategy(self):
        a = make_allocator(strategies=["market_maker"])
        allocs = a.get_allocations(100.0)
        assert abs(allocs["market_maker"] - 100.0) < 0.01

    def test_empty_strategies(self):
        with tempfile.TemporaryDirectory() as tmp:
            a = StrategyAllocator([], state_path=f"{tmp}/state.json", seed=42)
        allocs = a.get_allocations(400.0)
        assert allocs == {}


class TestThompsonSampling:
    def test_wins_increase_allocation(self):
        with tempfile.TemporaryDirectory() as tmp:
            a = StrategyAllocator(
                ["winner", "loser"],
                state_path=f"{tmp}/state.json",
                explore_start=0.0,  # Sin exploration
                explore_end=0.0,
                seed=42,
            )
            # Dar muchas wins al winner
            for _ in range(50):
                a.record_outcome("winner", pnl=1.0)
                a.record_outcome("loser", pnl=-1.0)

            allocs = a.get_allocations(100.0)
            # Winner debe tener mas capital
            assert allocs["winner"] > allocs["loser"]

    def test_win_probs_reflect_outcomes(self):
        with tempfile.TemporaryDirectory() as tmp:
            a = StrategyAllocator(
                ["good", "bad"],
                state_path=f"{tmp}/state.json",
                seed=42,
            )
            for _ in range(10):
                a.record_outcome("good", pnl=1.0)
            for _ in range(10):
                a.record_outcome("bad", pnl=-1.0)

            probs = a.get_win_probs()
            assert probs["good"] > probs["bad"]


class TestEpsilonDecay:
    def test_epsilon_starts_high(self):
        with tempfile.TemporaryDirectory() as tmp:
            a = StrategyAllocator(
                ["s1"], state_path=f"{tmp}/state.json",
                explore_start=0.3, explore_end=0.05
            )
            # Al inicio, epsilon cerca de 0.3
            epsilon = a._current_epsilon()
            assert epsilon >= 0.25

    def test_epsilon_decreases_over_time(self):
        import time
        with tempfile.TemporaryDirectory() as tmp:
            a = StrategyAllocator(
                ["s1"], state_path=f"{tmp}/state.json",
                explore_start=0.3, explore_end=0.05,
                explore_decay_days=0.001,  # Decay muy rapido
            )
            # Simular tiempo pasado alterando creation_time
            a._creation_time -= 86400 * 0.002  # 2x el decay
            epsilon = a._current_epsilon()
            assert epsilon < 0.3


class TestPersistence:
    def test_state_persisted_and_loaded(self):
        with tempfile.TemporaryDirectory() as tmp:
            state_path = f"{tmp}/state.json"
            a1 = StrategyAllocator(["s1", "s2"], state_path=state_path, seed=42)
            a1.record_outcome("s1", 5.0)
            a1.record_outcome("s2", -2.0)

            # Crear nuevo allocator desde mismo archivo
            a2 = StrategyAllocator(["s1", "s2"], state_path=state_path, seed=42)
            stats = a2.get_stats()
            assert stats["strategies"]["s1"]["wins"] == 1
            assert stats["strategies"]["s2"]["losses"] == 1


class TestGetStats:
    def test_stats_structure(self):
        a = make_allocator()
        stats = a.get_stats()
        assert "epsilon" in stats
        assert "strategies" in stats
        assert "days_since_creation" in stats
        for s in ["market_maker", "multi_arb", "stat_arb"]:
            assert s in stats["strategies"]
            assert "wins" in stats["strategies"][s]
            assert "win_prob" in stats["strategies"][s]
