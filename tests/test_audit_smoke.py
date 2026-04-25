"""Smoke test: verifica wiring end-to-end de todos los fixes (audit fase 2).

Cubre:
- CB.check_balance_floor con loss_reserve_usdc
- PositionSizer clamp Quarter Kelly al 5%
- MarketMakerStrategy lee config merged (markets + settings + risk)
- Trade dataclass tiene los 5 campos enriquecidos (tip 17)
- MarketProfiler.should_exit_by_share funciona
- wall_detector integrado
"""

import time
import yaml
import dataclasses

import pytest

from src.risk.circuit_breaker import CircuitBreaker
from src.risk.position_sizer import PositionSizer
from src.risk.market_profitability import MarketProfiler, MarketStats
from src.strategy.base import Trade
from src.analysis.wall_detector import is_large_wall


def _load_configs():
    with open("config/settings.yaml") as f:
        settings = yaml.safe_load(f)
    with open("config/risk.yaml") as f:
        risk = yaml.safe_load(f)
    with open("config/markets.yaml") as f:
        markets_cfg = yaml.safe_load(f)
    merged = {**settings, **markets_cfg, **risk}
    return settings, risk, markets_cfg, merged


def test_cb_loss_reserve_floor():
    _, risk, _, _ = _load_configs()
    cb = CircuitBreaker(risk)
    assert cb._loss_reserve_usdc == 20.0
    triggered = cb.check_balance_floor(15.0)
    assert triggered is True
    assert cb.is_triggered()


def test_cb_above_floor_no_trigger():
    _, risk, _, _ = _load_configs()
    cb = CircuitBreaker(risk)
    triggered = cb.check_balance_floor(150.0)
    assert triggered is False
    assert not cb.is_triggered()


def test_position_sizer_clamps_at_5pct_of_150():
    _, risk, _, _ = _load_configs()
    ps = PositionSizer(risk)
    size = ps.size_for_signal(capital=150, estimated_prob=0.99, market_price=0.01)
    # max_position_usdc=7.5 (5% de $150) y kelly clamp 0.05
    assert size <= 7.5


def test_market_maker_reads_merged_config(monkeypatch):
    """MM debe recibir config merged con keys de markets.yaml."""
    monkeypatch.setenv("PRIVATE_KEY", "0x" + "1" * 64)
    monkeypatch.setenv("PROXY_ADDRESS", "0x" + "1" * 40)

    from src.polymarket.client import PolymarketClient
    from src.strategy.market_maker import MarketMakerStrategy

    _, _, _, merged = _load_configs()
    client = PolymarketClient(paper_mode=True, paper_capital=150.0)
    mm = MarketMakerStrategy(client, merged)

    # markets.yaml keys
    assert mm._min_mid_change == 0.02
    assert 0 in mm._prime_hours
    assert mm._prime_boost == 1.3
    # settings.yaml keys
    assert mm._refresh_seconds == 90
    assert mm._order_size == 5.5
    assert mm._near_resolution_hours == 336
    assert mm._spread_offset == 0.02


def test_trade_has_enriched_fields():
    fields = {f.name for f in dataclasses.fields(Trade)}
    for required in [
        "mid_at_entry",
        "participation_share_at_entry",
        "category",
        "time_to_exit_sec",
        "rewards_earned",
    ]:
        assert required in fields


def test_should_exit_by_share_via_profiler():
    _, risk, _, _ = _load_configs()
    profiler = MarketProfiler(risk)
    profiler._stats["mkt"] = MarketStats(
        market_id="mkt", share_below_since=time.time() - 13 * 3600
    )
    assert profiler.should_exit_by_share("mkt", current_share=0.001) is True


def test_wall_detector_basic():
    ok, px = is_large_wall([(0.45, 300.0), (0.44, 20.0)], min_share=20.0)
    assert ok is True
    assert px == pytest.approx(0.45)


def test_excluded_categories_in_config():
    _, _, markets_cfg, _ = _load_configs()
    excluded = markets_cfg["filters"]["excluded_categories"]
    assert "entertainment" in excluded


def test_excluded_keywords_in_config():
    _, _, markets_cfg, _ = _load_configs()
    keywords = markets_cfg["filters"]["excluded_keywords"]
    for kw in ["release", "launch", "opening weekend", "tomorrow"]:
        assert kw in keywords


def test_diversification_cap_set():
    _, _, markets_cfg, _ = _load_configs()
    assert markets_cfg["diversification"]["max_per_category"] == 2


def test_strategies_enabled_concentration():
    settings, _, _, _ = _load_configs()
    enabled = settings["strategies_enabled"]
    assert "market_maker" in enabled
    assert "rewards_farmer" in enabled
    # Strategies que no compensan con $150
    assert "multi_arb" not in enabled
    assert "stat_arb" not in enabled
    assert "directional" not in enabled


def test_kelly_fraction_quarter():
    settings, risk, _, _ = _load_configs()
    assert settings["kelly_fraction"] == 0.25
    assert risk["position_sizing"]["kelly_fraction"] == 0.25


def test_capital_total_150():
    settings, risk, _, _ = _load_configs()
    assert settings["capital_total"] == 150
    assert settings["max_daily_drawdown"] == 7.5
    assert risk["circuit_breakers"]["max_daily_loss_usdc"] == 7.5


def test_min_volume_and_resolution_window():
    settings, _, markets_cfg, _ = _load_configs()
    assert markets_cfg["min_daily_volume_usd"] == 40000
    assert markets_cfg["small_market_volume_usd"] == 5000
    assert markets_cfg["filters"]["min_time_to_resolution_hours"] == 336
    assert settings["near_resolution_hours"] == 336
