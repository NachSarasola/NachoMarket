"""Tests para rolling drawdown 7/15/30d en CircuitBreaker (TODO 1.4)."""

import time
from unittest.mock import MagicMock

import pytest

from src.risk.circuit_breaker import CircuitBreaker, _ONE_DAY


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_cb(
    threshold_7d: float = 40.0,
    threshold_15d: float = 80.0,
    threshold_30d: float = 120.0,
    scale_down_factor: float = 0.5,
    alert_callback=None,
    scale_down_callback=None,
    pause_strategies_callback=None,
) -> CircuitBreaker:
    config = {
        "circuit_breakers": {"max_daily_loss_usdc": 20.0},
        "rolling_drawdown": {
            "threshold_7d_usdc": threshold_7d,
            "threshold_15d_usdc": threshold_15d,
            "threshold_30d_usdc": threshold_30d,
            "scale_down_factor": scale_down_factor,
        },
    }
    return CircuitBreaker(
        config,
        alert_callback=alert_callback,
        scale_down_callback=scale_down_callback,
        pause_strategies_callback=pause_strategies_callback,
    )


def inject_pnl_days_ago(cb: CircuitBreaker, pnl: float, days_ago: float) -> None:
    """Inyecta un registro de PnL en el rolling buffer con timestamp pasado."""
    ts = time.time() - days_ago * _ONE_DAY
    cb._rolling_pnl.append((ts, pnl))


# ---------------------------------------------------------------------------
# get_rolling_drawdown básico
# ---------------------------------------------------------------------------

class TestGetRollingDrawdown:
    def test_empty_rolling_pnl(self):
        cb = make_cb()
        assert cb.get_rolling_drawdown(7) == 0.0
        assert cb.get_rolling_drawdown(15) == 0.0
        assert cb.get_rolling_drawdown(30) == 0.0

    def test_single_loss_within_window(self):
        cb = make_cb()
        inject_pnl_days_ago(cb, -10.0, days_ago=3)
        assert cb.get_rolling_drawdown(7) == pytest.approx(-10.0)
        assert cb.get_rolling_drawdown(15) == pytest.approx(-10.0)

    def test_loss_outside_window_excluded(self):
        cb = make_cb()
        # 10 días atrás → fuera de ventana 7d
        inject_pnl_days_ago(cb, -50.0, days_ago=10)
        assert cb.get_rolling_drawdown(7) == pytest.approx(0.0)
        assert cb.get_rolling_drawdown(15) == pytest.approx(-50.0)

    def test_mixed_pnl_sums_correctly(self):
        cb = make_cb()
        inject_pnl_days_ago(cb, 5.0, days_ago=2)
        inject_pnl_days_ago(cb, -15.0, days_ago=4)
        inject_pnl_days_ago(cb, -8.0, days_ago=6)
        # 7d window: 5 - 15 - 8 = -18
        assert cb.get_rolling_drawdown(7) == pytest.approx(-18.0)

    def test_record_trade_adds_to_rolling(self):
        cb = make_cb()
        cb.record_trade(-5.0)
        cb.record_trade(-3.0)
        # Ambos deben estar en el rolling buffer (dentro de 7d)
        assert cb.get_rolling_drawdown(7) == pytest.approx(-8.0)

    def test_eviction_of_old_entries(self):
        """Entradas >30d se eliminan del buffer."""
        cb = make_cb()
        inject_pnl_days_ago(cb, -100.0, days_ago=31)
        inject_pnl_days_ago(cb, -5.0, days_ago=1)
        # Forzar eviccion
        cb._evict_old_rolling_pnl(time.time())
        assert cb.get_rolling_drawdown(30) == pytest.approx(-5.0)


# ---------------------------------------------------------------------------
# get_drawdown_report
# ---------------------------------------------------------------------------

class TestDrawdownReport:
    def test_report_returns_all_three_windows(self):
        cb = make_cb()
        report = cb.get_drawdown_report()
        assert "drawdown_7d" in report
        assert "drawdown_15d" in report
        assert "drawdown_30d" in report

    def test_report_values_consistent(self):
        cb = make_cb()
        inject_pnl_days_ago(cb, -20.0, days_ago=5)
        inject_pnl_days_ago(cb, -10.0, days_ago=12)
        report = cb.get_drawdown_report()
        # 7d: solo la de 5 días
        assert report["drawdown_7d"] == pytest.approx(-20.0)
        # 15d: ambas
        assert report["drawdown_15d"] == pytest.approx(-30.0)
        # 30d: ambas
        assert report["drawdown_30d"] == pytest.approx(-30.0)


# ---------------------------------------------------------------------------
# Scale-down 7d
# ---------------------------------------------------------------------------

class TestScaleDown7d:
    def test_scale_down_fires_when_7d_exceeded(self):
        scale_cb = MagicMock()
        alert_cb = MagicMock()
        cb = make_cb(threshold_7d=40.0, scale_down_callback=scale_cb, alert_callback=alert_cb)

        # Inyectar pérdida que supera el threshold 7d
        for _ in range(5):
            inject_pnl_days_ago(cb, -9.0, days_ago=2)
        cb._check_rolling_drawdowns()

        scale_cb.assert_called_once_with(0.5)
        reasons = [call[0][0] for call in alert_cb.call_args_list]
        assert "rolling_7d_drawdown" in reasons

    def test_scale_down_fires_only_once(self):
        scale_cb = MagicMock()
        cb = make_cb(threshold_7d=10.0, scale_down_callback=scale_cb)
        inject_pnl_days_ago(cb, -20.0, days_ago=1)

        cb._check_rolling_drawdowns()
        cb._check_rolling_drawdowns()
        cb._check_rolling_drawdowns()

        scale_cb.assert_called_once()

    def test_scale_down_resets_when_recovered(self):
        scale_cb = MagicMock()
        cb = make_cb(threshold_7d=10.0, scale_down_callback=scale_cb)
        inject_pnl_days_ago(cb, -20.0, days_ago=1)
        cb._check_rolling_drawdowns()
        assert cb._scale_down_active is True

        # Vaciar rolling buffer (simular recuperación)
        cb._rolling_pnl.clear()
        cb._check_rolling_drawdowns()
        assert cb._scale_down_active is False

    def test_scale_down_not_fired_below_threshold(self):
        scale_cb = MagicMock()
        cb = make_cb(threshold_7d=50.0, scale_down_callback=scale_cb)
        inject_pnl_days_ago(cb, -30.0, days_ago=2)  # < 50 threshold
        cb._check_rolling_drawdowns()
        scale_cb.assert_not_called()


# ---------------------------------------------------------------------------
# Pausa estrategias 15d
# ---------------------------------------------------------------------------

class TestPauseStrategies15d:
    def test_pause_fires_when_15d_exceeded(self):
        pause_cb = MagicMock()
        alert_cb = MagicMock()
        cb = make_cb(threshold_15d=80.0, pause_strategies_callback=pause_cb, alert_callback=alert_cb)

        inject_pnl_days_ago(cb, -90.0, days_ago=10)
        cb._check_rolling_drawdowns()

        pause_cb.assert_called_once()
        strategies_paused = pause_cb.call_args[0][0]
        assert "multi_arb" in strategies_paused
        assert "directional" in strategies_paused

    def test_pause_fires_only_once(self):
        pause_cb = MagicMock()
        cb = make_cb(threshold_15d=10.0, pause_strategies_callback=pause_cb)
        inject_pnl_days_ago(cb, -50.0, days_ago=5)

        cb._check_rolling_drawdowns()
        cb._check_rolling_drawdowns()

        pause_cb.assert_called_once()

    def test_arb_pause_resets_when_recovered(self):
        cb = make_cb(threshold_15d=10.0)
        inject_pnl_days_ago(cb, -50.0, days_ago=5)
        cb._check_rolling_drawdowns()
        assert cb._arb_paused is True

        cb._rolling_pnl.clear()
        cb._check_rolling_drawdowns()
        assert cb._arb_paused is False


# ---------------------------------------------------------------------------
# Kill-switch 30d
# ---------------------------------------------------------------------------

class TestKillSwitch30d:
    def test_kill_switch_triggers_circuit_breaker(self):
        alert_cb = MagicMock()
        cb = make_cb(threshold_30d=120.0, alert_callback=alert_cb)

        inject_pnl_days_ago(cb, -130.0, days_ago=20)
        cb._check_rolling_drawdowns()

        assert cb._triggered is True
        assert cb._trigger_reason == "rolling_30d_drawdown"

    def test_kill_switch_sends_alert(self):
        alert_cb = MagicMock()
        cb = make_cb(threshold_30d=10.0, alert_callback=alert_cb)
        inject_pnl_days_ago(cb, -50.0, days_ago=20)
        cb._check_rolling_drawdowns()

        # Debe haber enviado alerta rolling_30d_drawdown
        reasons = [call[0][0] for call in alert_cb.call_args_list]
        assert "rolling_30d_drawdown" in reasons

    def test_kill_switch_not_triggered_below_threshold(self):
        cb = make_cb(threshold_30d=120.0)
        inject_pnl_days_ago(cb, -100.0, days_ago=20)
        cb._check_rolling_drawdowns()
        assert cb._triggered is False


# ---------------------------------------------------------------------------
# Integración con record_trade
# ---------------------------------------------------------------------------

class TestRecordTradeIntegration:
    def test_record_trade_triggers_scale_down(self):
        scale_cb = MagicMock()
        cb = make_cb(threshold_7d=5.0, scale_down_callback=scale_cb)

        # Un solo trade de -10 supera el threshold 7d de $5
        cb.record_trade(-10.0)

        scale_cb.assert_called_once_with(0.5)

    def test_record_trade_triggers_kill_switch(self):
        cb = make_cb(threshold_30d=5.0)
        cb.record_trade(-10.0)
        assert cb._triggered is True

    def test_get_status_includes_rolling_drawdown(self):
        cb = make_cb()
        cb.record_trade(-5.0)
        status = cb.get_status()
        assert "rolling_drawdown" in status
        assert "drawdown_7d" in status["rolling_drawdown"]
        assert "drawdown_15d" in status["rolling_drawdown"]
        assert "drawdown_30d" in status["rolling_drawdown"]
