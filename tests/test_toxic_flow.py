"""Tests para ToxicFlowDetector (TODO 4.4)."""
import time
import pytest
from src.analysis.toxic_flow import ToxicFlowDetector


def make_detector(**kwargs):
    defaults = dict(
        adverse_threshold=0.01,
        toxic_duration_sec=3600.0,
        ema_alpha=0.5,
        toxic_score_threshold=0.6,
    )
    defaults.update(kwargs)
    return ToxicFlowDetector(**defaults)


class TestRecordAndObserve:
    def test_no_adverse_when_price_stable(self):
        d = make_detector()
        d.record_fill("tok1", "BUY", fill_price=0.50, mid_before=0.50)
        is_adverse = d.observe_post_fill("tok1", mid_after=0.51)
        # BUY + precio sube → no adverse
        assert is_adverse is False

    def test_adverse_buy_price_drops(self):
        d = make_detector()
        d.record_fill("tok1", "BUY", fill_price=0.52, mid_before=0.50)
        # Precio baja 2% → adverse
        is_adverse = d.observe_post_fill("tok1", mid_after=0.49)
        assert is_adverse is True

    def test_adverse_sell_price_rises(self):
        d = make_detector()
        d.record_fill("tok1", "SELL", fill_price=0.50, mid_before=0.50)
        # Precio sube 2% → adverse para SELL
        is_adverse = d.observe_post_fill("tok1", mid_after=0.51)
        assert is_adverse is True

    def test_no_fill_registered_returns_false(self):
        d = make_detector()
        result = d.observe_post_fill("nonexistent", mid_after=0.50)
        assert result is False

    def test_invalid_prices_no_crash(self):
        d = make_detector()
        d.record_fill("tok1", "BUY", fill_price=0.0, mid_before=0.0)
        result = d.observe_post_fill("tok1", mid_after=0.0)
        assert result is False


class TestToxicityScore:
    def test_score_increases_with_adverse_fills(self):
        d = make_detector(ema_alpha=1.0)  # EMA instantanea
        d.record_fill("tok1", "BUY", fill_price=0.52, mid_before=0.50)
        d.observe_post_fill("tok1", mid_after=0.49)  # Adverse
        score = d.get_toxicity_score("tok1")
        assert score == pytest.approx(1.0)

    def test_score_decreases_with_good_fills(self):
        d = make_detector(ema_alpha=0.5)
        # Empezar con score alto
        d._toxicity_scores["tok1"] = 1.0
        d.record_fill("tok1", "BUY", fill_price=0.50, mid_before=0.50)
        d.observe_post_fill("tok1", mid_after=0.52)  # No adverse
        score = d.get_toxicity_score("tok1")
        assert score < 1.0

    def test_new_token_zero_score(self):
        d = make_detector()
        assert d.get_toxicity_score("new_token") == 0.0


class TestQuarantine:
    def test_quarantine_activated_on_high_score(self):
        d = make_detector(ema_alpha=1.0, toxic_score_threshold=0.5)
        # Un fill adverse con alpha=1.0 pone score=1.0 > 0.5
        d.record_fill("tok1", "BUY", fill_price=0.52, mid_before=0.50)
        d.observe_post_fill("tok1", mid_after=0.49)
        assert d.is_toxic("tok1") is True

    def test_quarantine_expires(self):
        d = make_detector(
            ema_alpha=1.0,
            toxic_score_threshold=0.5,
            toxic_duration_sec=0.01,  # 10ms
        )
        d.record_fill("tok1", "BUY", fill_price=0.52, mid_before=0.50)
        d.observe_post_fill("tok1", mid_after=0.49)
        assert d.is_toxic("tok1") is True
        time.sleep(0.05)
        assert d.is_toxic("tok1") is False

    def test_no_quarantine_without_adverse(self):
        d = make_detector()
        d.record_fill("tok1", "BUY", fill_price=0.50, mid_before=0.50)
        d.observe_post_fill("tok1", mid_after=0.52)
        assert d.is_toxic("tok1") is False

    def test_get_quarantined_tokens(self):
        d = make_detector(ema_alpha=1.0, toxic_score_threshold=0.5)
        d.record_fill("tok1", "BUY", fill_price=0.52, mid_before=0.50)
        d.observe_post_fill("tok1", mid_after=0.48)
        quarantined = d.get_quarantined_tokens()
        assert "tok1" in quarantined

    def test_clear_token_removes_all(self):
        d = make_detector(ema_alpha=1.0, toxic_score_threshold=0.5)
        d._toxicity_scores["tok1"] = 1.0
        d._quarantine_until["tok1"] = time.time() + 3600
        d.clear_token("tok1")
        assert d.get_toxicity_score("tok1") == 0.0
        assert d.is_toxic("tok1") is False


class TestSummary:
    def test_summary_structure(self):
        d = make_detector()
        s = d.summary()
        assert "quarantined_count" in s
        assert "tracked_tokens" in s
        assert "avg_toxicity_score" in s

    def test_summary_no_tokens(self):
        d = make_detector()
        s = d.summary()
        assert s["quarantined_count"] == 0
        assert s["tracked_tokens"] == 0
