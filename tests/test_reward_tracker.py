"""Tests para RewardTracker — ¢/min farmeados por mercado."""

import time
import threading
from collections import deque
from unittest.mock import MagicMock

import pytest

from src.strategy.reward_tracker import RewardTracker, _Sample, EMA_ALPHA


def _make_tracker(percentages: dict, daily_rates: dict | None = None) -> RewardTracker:
    client = MagicMock()
    client.get_reward_percentages.return_value = percentages
    if daily_rates is not None:
        client.get_rewards.return_value = {
            cid: {"rewards_daily_rate": rate} for cid, rate in daily_rates.items()
        }
    else:
        client.get_rewards.side_effect = Exception("no rewards")
    return RewardTracker(client)


def _inject_samples(tracker: RewardTracker, cid: str, samples: list[tuple[float, float, float]]) -> None:
    """Inyecta samples directamente en el buffer (ts, share_pct, daily_rate)."""
    with tracker._lock:
        for ts, share_pct, daily_rate in samples:
            tracker._buffers[cid].append(_Sample(ts=ts, share_pct=share_pct, daily_rate=daily_rate))
        tracker._update_ema(cid)


def test_cents_per_min_none_when_insufficient_history():
    tracker = _make_tracker({})
    assert tracker.cents_per_min("0xabc") is None


def test_cents_per_min_none_with_single_sample():
    tracker = _make_tracker({})
    now = time.time()
    _inject_samples(tracker, "0xabc", [(now, 1.0, 200.0)])
    assert tracker.cents_per_min("0xabc") is None


def test_cents_per_min_basic_delta():
    """(0→1.5% en 60s) × daily_rate=$200 → (1.5/100×200×100)/1 = 30¢/min."""
    tracker = _make_tracker({})
    now = time.time()
    _inject_samples(tracker, "0xabc", [
        (now - 60, 1.0, 200.0),
        (now,      2.5, 200.0),
    ])
    rate = tracker.cents_per_min("0xabc")
    assert rate is not None
    # delta_pct=1.5, delta_usd=1.5/100*200=3, rate=3*100/1=300 ¢/min
    assert abs(rate - 300.0) < 1.0


def test_cents_per_min_none_when_samples_too_close():
    tracker = _make_tracker({})
    now = time.time()
    _inject_samples(tracker, "0xabc", [
        (now - 10, 1.0, 200.0),
        (now,      1.5, 200.0),
    ])
    # Span < 30s → None
    assert tracker.cents_per_min("0xabc") is None


def test_handles_daily_reset_negative_delta():
    """Si delta_pct < 0 (reset UTC), el par se descarta; no marca non_earning."""
    tracker = _make_tracker({})
    now = time.time()
    _inject_samples(tracker, "0xabc", [
        (now - 120, 80.0, 200.0),  # antes del reset
        (now - 60,   0.5, 200.0),  # post-reset: delta = -79.5 → descartar
        (now,        1.0, 200.0),  # delta = +0.5 → válido
    ])
    rate = tracker.cents_per_min("0xabc")
    # Solo el par (1→2) es válido: delta=0.5, 0.5/100*200=1, 1*100/1=100¢/min
    assert rate is not None
    assert rate > 0


def test_changing_daily_rate_uses_prev_sample_rate():
    """Si el pool drena, el cálculo usa daily_rate del sample anterior."""
    tracker = _make_tracker({})
    now = time.time()
    _inject_samples(tracker, "0xabc", [
        (now - 60, 1.0, 200.0),  # daily_rate=200
        (now,      2.0, 50.0),   # daily_rate=50 (drenado), pero usamos prev=200
    ])
    rate = tracker.cents_per_min("0xabc")
    assert rate is not None
    # delta_pct=1.0, prev daily_rate=200 → delta_usd=2, rate=200¢/min
    assert abs(rate - 200.0) < 1.0


def test_ema_smoothing():
    """EMA(α=0.4) suaviza picos: secuencia [300, 300, 300, 30] → valor > 30."""
    tracker = _make_tracker({})
    now = time.time()
    # 4 samples = 3 pares; el último tiene delta muy bajo
    _inject_samples(tracker, "0xabc", [
        (now - 180, 0.0, 200.0),
        (now - 120, 1.5, 200.0),  # par 1: 300¢/min
        (now - 60,  3.0, 200.0),  # par 2: 300¢/min
        (now,       3.1, 200.0),  # par 3: ~10¢/min (cola baja)
    ])
    rate = tracker.cents_per_min("0xabc")
    assert rate is not None
    # EMA no puede haber caído hasta 10 en un solo paso con α=0.4
    assert rate > 50.0


def test_last_share_pct():
    tracker = _make_tracker({})
    now = time.time()
    _inject_samples(tracker, "0xabc", [(now - 60, 2.5, 100.0), (now, 3.1, 100.0)])
    assert tracker.last_share_pct("0xabc") == pytest.approx(3.1)


def test_snapshot_structure():
    tracker = _make_tracker({})
    now = time.time()
    _inject_samples(tracker, "0xabc", [(now - 60, 1.0, 200.0), (now, 2.0, 200.0)])
    snap = tracker.snapshot()
    assert "0xabc" in snap
    assert "cents_per_min" in snap["0xabc"]
    assert "last_share_pct" in snap["0xabc"]
    assert snap["0xabc"]["sample_count"] == 2


def test_sample_calls_client():
    """_sample() llama a get_reward_percentages y get_rewards."""
    tracker = _make_tracker({"0xabc": 2.0}, {"0xabc": 150.0})
    tracker._sample()
    tracker._client.get_reward_percentages.assert_called_once()
    tracker._client.get_rewards.assert_called_once()


def test_thread_start_stop():
    tracker = _make_tracker({})
    tracker.start()
    time.sleep(0.05)
    tracker.stop()
    # No debe lanzar excepción


def test_zero_rate_market():
    """Mercado con daily_rate=0 → 0¢/min, no None."""
    tracker = _make_tracker({})
    now = time.time()
    _inject_samples(tracker, "0xabc", [
        (now - 60, 1.0, 0.0),
        (now,      2.0, 0.0),
    ])
    rate = tracker.cents_per_min("0xabc")
    assert rate is not None
    assert rate == pytest.approx(0.0)
