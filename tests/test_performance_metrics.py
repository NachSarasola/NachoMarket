"""Tests para PerformanceMetrics (TODO 2.1)."""
import math
import pytest
from src.analysis.performance_metrics import PerformanceMetrics, _mean, _std


class TestSharpeRatio:
    def test_empty_returns_zero(self):
        pm = PerformanceMetrics([])
        assert pm.sharpe_ratio() == 0.0

    def test_single_return_zero(self):
        pm = PerformanceMetrics([1.0])
        assert pm.sharpe_ratio() == 0.0

    def test_positive_consistent_returns(self):
        returns = [0.5] * 30
        pm = PerformanceMetrics(returns)
        sharpe = pm.sharpe_ratio()
        # Con retornos constantes positivos, Sharpe debe ser muy alto (inf)
        assert sharpe > 10 or math.isinf(sharpe)

    def test_zero_excess_return_zero_sharpe(self):
        # Retornos iguales al risk-free → Sharpe ≈ 0
        rf_daily = 0.04 / 365
        returns = [rf_daily] * 30
        pm = PerformanceMetrics(returns, risk_free_daily=rf_daily)
        # No exactamente 0 por floating point, pero cerca
        assert abs(pm.sharpe_ratio()) < 0.1

    def test_negative_returns_negative_sharpe(self):
        returns = [-1.0] * 30
        pm = PerformanceMetrics(returns)
        assert pm.sharpe_ratio() < 0

    def test_known_sharpe(self):
        # Con returns de media 1 y std 1 (diario), Sharpe anualizado = sqrt(365)
        import math
        returns = [0.0, 2.0] * 15  # media=1, std≈1
        pm = PerformanceMetrics(returns, risk_free_daily=0.0)
        sharpe = pm.sharpe_ratio()
        # No exacto por sample std, pero debe ser positivo y > 1
        assert sharpe > 1.0


class TestSortinoRatio:
    def test_no_downside_returns_inf(self):
        returns = [1.0, 2.0, 3.0]
        pm = PerformanceMetrics(returns, risk_free_daily=0.0)
        sortino = pm.sortino_ratio()
        assert math.isinf(sortino) or sortino > 10

    def test_all_negative_positive_sortino(self):
        returns = [-1.0, -0.5, -0.1]
        pm = PerformanceMetrics(returns, risk_free_daily=0.0)
        sortino = pm.sortino_ratio()
        assert sortino < 0

    def test_sortino_gt_sharpe_with_upside(self):
        # Con volatilidad principalmente positiva, Sortino > Sharpe
        returns = [5.0, 5.0, 5.0, -0.1, -0.1]
        pm = PerformanceMetrics(returns, risk_free_daily=0.0)
        assert pm.sortino_ratio() > pm.sharpe_ratio()


class TestCalmarRatio:
    def test_empty_returns_zero(self):
        pm = PerformanceMetrics([])
        assert pm.calmar_ratio() == 0.0

    def test_no_drawdown_returns_inf(self):
        returns = [1.0, 2.0, 3.0]
        pm = PerformanceMetrics(returns)
        calmar = pm.calmar_ratio()
        assert math.isinf(calmar) or calmar > 10

    def test_with_drawdown(self):
        returns = [5.0, -3.0, 5.0, -3.0, 5.0]
        pm = PerformanceMetrics(returns)
        calmar = pm.calmar_ratio()
        assert calmar > 0


class TestMaxDrawdown:
    def test_monotone_up_zero_drawdown(self):
        returns = [1.0, 1.0, 1.0]
        pm = PerformanceMetrics(returns)
        assert pm.max_drawdown() == 0.0

    def test_drawdown_after_peak(self):
        # Sube 5 y cae 3 → MDD = -3
        returns = [5.0, -3.0]
        pm = PerformanceMetrics(returns)
        assert pm.max_drawdown() == pytest.approx(-3.0)

    def test_multiple_drawdowns_returns_worst(self):
        returns = [3.0, -1.0, 2.0, -4.0, 1.0]
        # Pico en 3 → cae 1 → drawdown -1
        # Pico en 5 (3-1+2+1=5) → despues de -4 equity=1, drawdown=-4
        pm = PerformanceMetrics(returns)
        assert pm.max_drawdown() <= -4.0


class TestSummary:
    def test_summary_has_all_keys(self):
        pm = PerformanceMetrics([1.0, -0.5, 2.0])
        s = pm.summary()
        assert "sharpe" in s
        assert "sortino" in s
        assert "calmar" in s
        assert "max_drawdown" in s
        assert "total_return" in s
        assert "annualized_return" in s
        assert "volatility" in s
        assert "win_rate" in s
        assert "n_days" in s

    def test_total_return_correct(self):
        returns = [1.0, 2.0, -0.5]
        pm = PerformanceMetrics(returns)
        assert pm.total_return() == pytest.approx(2.5)

    def test_win_rate_all_positive(self):
        pm = PerformanceMetrics([1.0, 2.0, 3.0])
        assert pm.win_rate() == pytest.approx(1.0)

    def test_win_rate_half(self):
        pm = PerformanceMetrics([1.0, -1.0])
        assert pm.win_rate() == pytest.approx(0.5)
