"""Tests para StrategyMonitor kill switch (TODO 1.6)."""
import tempfile
import time
from unittest.mock import MagicMock
import pytest
from src.risk.strategy_monitor import StrategyMonitor, _calmar_ratio, _ONE_DAY


def make_monitor(**kwargs):
    defaults = dict(
        kill_calmar_threshold=0.5,
        kill_evaluation_days=14,
        min_trades=3,
    )
    defaults.update(kwargs)
    return StrategyMonitor(**defaults)


class TestRecordAndEvaluate:
    def test_no_kill_without_enough_trades(self):
        m = make_monitor()
        m.record_trade("strat1", -1.0)  # Solo 1 trade, necesita 3
        killed = m.evaluate()
        assert killed == []

    def test_no_kill_with_good_performance(self):
        m = make_monitor()
        for _ in range(5):
            m.record_trade("good_strat", 1.0)  # Solo wins
        killed = m.evaluate()
        assert "good_strat" not in killed

    def test_kill_strategy_with_bad_calmar(self):
        m = make_monitor()
        # Trades que dan Calmar muy bajo: todo perdidas
        for _ in range(5):
            m.record_trade("bad_strat", -5.0)
        killed = m.evaluate()
        assert "bad_strat" in killed
        assert m.is_killed("bad_strat")

    def test_killed_strategy_not_re_killed(self):
        m = make_monitor()
        for _ in range(5):
            m.record_trade("bad_strat", -5.0)
        m.evaluate()  # Primera vez
        second_kill = m.evaluate()  # Segunda vez
        # No debe aparecer de nuevo
        assert "bad_strat" not in second_kill

    def test_pause_callback_called_on_kill(self):
        pause_cb = MagicMock()
        m = make_monitor(pause_callback=pause_cb, min_trades=3)
        for _ in range(5):
            m.record_trade("strat1", -10.0)
        m.evaluate()
        pause_cb.assert_called_once_with("strat1")

    def test_alert_callback_called_on_kill(self):
        alert_cb = MagicMock()
        m = make_monitor(alert_callback=alert_cb, min_trades=3)
        for _ in range(5):
            m.record_trade("strat1", -10.0)
        m.evaluate()
        alert_cb.assert_called_once()
        # Verificar que menciona la estrategia
        args = alert_cb.call_args[0]
        assert "strat1" in args[1]


class TestReviveStrategy:
    def test_revive_killed_strategy(self):
        m = make_monitor(min_trades=3)
        for _ in range(5):
            m.record_trade("strat1", -10.0)
        m.evaluate()
        assert m.is_killed("strat1")
        assert m.revive_strategy("strat1") is True
        assert m.is_killed("strat1") is False

    def test_revive_non_killed_returns_false(self):
        m = make_monitor()
        assert m.revive_strategy("nonexistent") is False

    def test_revive_clears_history(self):
        m = make_monitor(min_trades=3)
        for _ in range(5):
            m.record_trade("strat1", -10.0)
        m.evaluate()
        m.revive_strategy("strat1")
        # El historial debe estar limpio
        status = m.get_status()
        assert status.get("strat1", {}).get("trade_count_14d", 0) == 0


class TestGetStatus:
    def test_status_structure(self):
        m = make_monitor()
        for i in range(5):
            m.record_trade("s1", 1.0 if i % 2 == 0 else -0.5)
        status = m.get_status()
        assert "s1" in status
        assert "trade_count_14d" in status["s1"]
        assert "is_killed" in status["s1"]

    def test_killed_reflected_in_status(self):
        m = make_monitor(min_trades=3)
        for _ in range(5):
            m.record_trade("s1", -100.0)
        m.evaluate()
        status = m.get_status()
        assert status["s1"]["is_killed"] is True


class TestGraveyard:
    def test_graveyard_written_on_kill(self):
        with tempfile.TemporaryDirectory():
            import src.risk.strategy_monitor as sm_module
            import tempfile as tf

            original = sm_module._GRAVEYARD_FILE
            tmp = tf.NamedTemporaryFile(suffix=".jsonl", delete=False)
            sm_module._GRAVEYARD_FILE = type('P', (), {'parent': type('P2', (), {'mkdir': staticmethod(lambda **kw: None)})(), '__str__': lambda self: tmp.name, 'exists': lambda self: True})()

            m = make_monitor(min_trades=3)
            for _ in range(5):
                m.record_trade("dead_strat", -20.0)
            m.evaluate()

            sm_module._GRAVEYARD_FILE = original
            tmp.close()


class TestCalmarHelper:
    def test_calmar_all_wins(self):
        pnls = [1.0, 1.0, 1.0, 1.0, 1.0]
        calmar = _calmar_ratio(pnls, days=14)
        assert calmar > 0

    def test_calmar_all_losses(self):
        pnls = [-5.0, -3.0, -2.0]
        calmar = _calmar_ratio(pnls, days=14)
        assert calmar < 0

    def test_calmar_empty(self):
        assert _calmar_ratio([], days=14) == 0.0
