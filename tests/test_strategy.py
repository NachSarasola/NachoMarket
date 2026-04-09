"""Tests para las estrategias de trading con la nueva API Signal/Trade."""

import json
from pathlib import Path

import pytest

from src.polymarket.client import PolymarketClient
from src.strategy.base import BaseStrategy, Signal, Trade
from src.strategy.market_maker import MarketMakerStrategy
from src.strategy.multi_arb import MultiArbStrategy
from src.strategy.directional import DirectionalStrategy


# ------------------------------------------------------------------
# Fixtures
# ------------------------------------------------------------------

@pytest.fixture
def client() -> PolymarketClient:
    return PolymarketClient(paper_mode=True)


@pytest.fixture
def mm_config() -> dict:
    return {
        "market_maker": {
            "spread_offset": 0.02,
            "min_spread": 0.01,
            "order_size": 5.0,
            "num_levels": 3,
            "level_spacing": 0.01,
            "refresh_seconds": 45,
            "max_inventory_per_market": 50,
        },
    }


@pytest.fixture
def arb_config() -> dict:
    return {
        "multi_arb": {
            "min_edge": 0.02,
            "max_position": 10.0,
        },
    }


@pytest.fixture
def dir_config() -> dict:
    return {
        "directional_min_edge_pct": 5.0,
        "directional_order_size_usdc": 5.0,
    }


# ------------------------------------------------------------------
# Tests: Signal y Trade dataclasses
# ------------------------------------------------------------------

class TestSignal:
    def test_create_signal(self) -> None:
        s = Signal(
            market_id="cond_1",
            token_id="tok_1",
            side="BUY",
            price=0.45,
            size=10.0,
            confidence=0.8,
            strategy_name="test",
        )
        assert s.side == "BUY"
        assert s.confidence == 0.8
        assert s.strategy_name == "test"

    def test_signal_fields(self) -> None:
        s = Signal("m", "t", "SELL", 0.5, 5.0, 0.5, "s")
        assert s.market_id == "m"
        assert s.token_id == "t"


class TestTrade:
    def test_create_trade(self) -> None:
        t = Trade(
            timestamp="2026-01-01T00:00:00+00:00",
            market_id="cond_1",
            token_id="tok_1",
            side="BUY",
            price=0.45,
            size=10.0,
            order_id="ord_1",
            status="submitted",
            strategy_name="test",
            fee_paid=0.0,
        )
        assert t.status == "submitted"
        assert t.fee_paid == 0.0

    def test_trade_default_fee(self) -> None:
        t = Trade("ts", "m", "t", "BUY", 0.5, 5.0, "o", "ok", "s")
        assert t.fee_paid == 0.0


# ------------------------------------------------------------------
# Tests: BaseStrategy (via subclase concreta)
# ------------------------------------------------------------------

class TestBaseStrategy:
    def test_make_signal_fills_strategy_name(self, client, mm_config) -> None:
        strategy = MarketMakerStrategy(client, mm_config)
        signal = strategy._make_signal("m1", "t1", "BUY", 0.5, 10.0, 0.9)
        assert signal.strategy_name == "market_maker"

    def test_make_trade_from_signal(self, client, mm_config) -> None:
        strategy = MarketMakerStrategy(client, mm_config)
        signal = strategy._make_signal("m1", "t1", "BUY", 0.5, 10.0, 0.9)
        trade = strategy._make_trade(signal, "ord_123", "filled_paper")
        assert trade.order_id == "ord_123"
        assert trade.market_id == "m1"
        assert trade.strategy_name == "market_maker"
        assert trade.timestamp  # No vacio

    def test_log_trade_writes_jsonl(self, client, mm_config, tmp_path) -> None:
        import src.strategy.base as base_mod
        original = base_mod.TRADES_FILE
        base_mod.TRADES_FILE = tmp_path / "trades.jsonl"
        try:
            strategy = MarketMakerStrategy(client, mm_config)
            signal = strategy._make_signal("m1", "t1", "BUY", 0.5, 10.0, 0.9)
            trade = strategy._make_trade(signal, "ord_1", "submitted")
            strategy.log_trade(trade)

            lines = base_mod.TRADES_FILE.read_text().strip().split("\n")
            assert len(lines) == 1
            record = json.loads(lines[0])
            assert record["market_id"] == "m1"
            assert record["side"] == "BUY"
            assert record["strategy_name"] == "market_maker"
            assert record["fee_paid"] == 0.0
        finally:
            base_mod.TRADES_FILE = original

    def test_pause_and_resume(self, client, mm_config) -> None:
        strategy = MarketMakerStrategy(client, mm_config)
        assert strategy.is_active is True
        strategy.pause()
        assert strategy.is_active is False
        strategy.resume()
        assert strategy.is_active is True

    def test_run_skips_when_paused(self, client, mm_config) -> None:
        strategy = MarketMakerStrategy(client, mm_config)
        strategy.pause()
        trades = strategy.run({"spread": 0.05, "mid_price": 0.5, "token_id": "t1"})
        assert trades == []

    def test_run_skips_when_should_act_false(self, client, mm_config) -> None:
        strategy = MarketMakerStrategy(client, mm_config)
        # Spread demasiado tight → should_act retorna False
        trades = strategy.run({"spread": 0.001, "mid_price": 0.5, "token_id": "t1"})
        assert trades == []

    def test_run_full_pipeline(self, client, mm_config, tmp_path) -> None:
        import src.strategy.base as base_mod
        original = base_mod.TRADES_FILE
        base_mod.TRADES_FILE = tmp_path / "trades.jsonl"
        try:
            strategy = MarketMakerStrategy(client, mm_config)
            trades = strategy.run({
                "spread": 0.05,
                "mid_price": 0.5,
                "token_id": "t1",
                "condition_id": "cond_1",
            })
            assert len(trades) > 0
            assert all(isinstance(t, Trade) for t in trades)
            assert all(t.strategy_name == "market_maker" for t in trades)
            # Verificar que se loguearon
            lines = base_mod.TRADES_FILE.read_text().strip().split("\n")
            assert len(lines) == len(trades)
        finally:
            base_mod.TRADES_FILE = original


# ------------------------------------------------------------------
# Tests: MarketMakerStrategy
# ------------------------------------------------------------------

class TestMarketMaker:
    def test_should_act_sufficient_spread(self, client, mm_config) -> None:
        strategy = MarketMakerStrategy(client, mm_config)
        assert strategy.should_act({"spread": 0.04}) is True

    def test_should_act_tight_spread(self, client, mm_config) -> None:
        strategy = MarketMakerStrategy(client, mm_config)
        assert strategy.should_act({"spread": 0.005}) is False

    def test_evaluate_returns_signals(self, client, mm_config) -> None:
        strategy = MarketMakerStrategy(client, mm_config)
        signals = strategy.evaluate({
            "token_id": "tok_123",
            "mid_price": 0.5,
            "condition_id": "cond_1",
        })
        assert len(signals) > 0
        assert all(isinstance(s, Signal) for s in signals)
        assert all(s.strategy_name == "market_maker" for s in signals)

    def test_evaluate_generates_bids_and_asks(self, client, mm_config) -> None:
        strategy = MarketMakerStrategy(client, mm_config)
        signals = strategy.evaluate({
            "token_id": "tok_1",
            "mid_price": 0.5,
            "condition_id": "c1",
        })
        sides = {s.side for s in signals}
        assert "BUY" in sides
        assert "SELL" in sides

    def test_evaluate_no_signals_invalid_price(self, client, mm_config) -> None:
        strategy = MarketMakerStrategy(client, mm_config)
        assert strategy.evaluate({"token_id": "t", "mid_price": 0.0}) == []
        assert strategy.evaluate({"token_id": "t", "mid_price": 1.0}) == []

    def test_evaluate_confidence_decreases_with_level(self, client, mm_config) -> None:
        strategy = MarketMakerStrategy(client, mm_config)
        signals = strategy.evaluate({
            "token_id": "tok_1",
            "mid_price": 0.5,
            "condition_id": "c1",
        })
        # Los primeros signals (nivel 0) deben tener mayor confidence
        buy_signals = [s for s in signals if s.side == "BUY"]
        if len(buy_signals) >= 2:
            assert buy_signals[0].confidence >= buy_signals[-1].confidence

    def test_execute_produces_trades(self, client, mm_config, tmp_path) -> None:
        import src.strategy.base as base_mod
        original = base_mod.TRADES_FILE
        base_mod.TRADES_FILE = tmp_path / "trades.jsonl"
        try:
            strategy = MarketMakerStrategy(client, mm_config)
            signal = strategy._make_signal("m1", "t1", "BUY", 0.45, 5.0, 0.9)
            trades = strategy.execute([signal])
            assert len(trades) == 1
            assert trades[0].status == "filled_paper"
            assert trades[0].side == "BUY"
        finally:
            base_mod.TRADES_FILE = original


# ------------------------------------------------------------------
# Tests: MultiArbStrategy
# ------------------------------------------------------------------

class TestMultiArb:
    def test_should_act_with_tokens(self, client, arb_config) -> None:
        strategy = MultiArbStrategy(client, arb_config)
        assert strategy.should_act({"tokens": [{"token_id": "a"}, {"token_id": "b"}]}) is True

    def test_should_act_single_token(self, client, arb_config) -> None:
        strategy = MultiArbStrategy(client, arb_config)
        assert strategy.should_act({"tokens": [{"token_id": "a"}]}) is False

    def test_evaluate_sell_when_sum_above_one(self, client, arb_config) -> None:
        strategy = MultiArbStrategy(client, arb_config)
        signals = strategy.evaluate({
            "condition_id": "c1",
            "tokens": [
                {"token_id": "a", "price": 0.55},
                {"token_id": "b", "price": 0.55},
            ],
        })
        assert len(signals) == 2
        assert all(s.side == "SELL" for s in signals)

    def test_evaluate_no_signals_when_no_edge(self, client, arb_config) -> None:
        strategy = MultiArbStrategy(client, arb_config)
        signals = strategy.evaluate({
            "condition_id": "c1",
            "tokens": [
                {"token_id": "a", "price": 0.50},
                {"token_id": "b", "price": 0.50},
            ],
        })
        assert signals == []

    def test_evaluate_buy_when_sum_below_one(self, client, arb_config) -> None:
        strategy = MultiArbStrategy(client, arb_config)
        signals = strategy.evaluate({
            "condition_id": "c1",
            "tokens": [
                {"token_id": "a", "price": 0.40},
                {"token_id": "b", "price": 0.40},
            ],
        })
        assert len(signals) == 2
        assert all(s.side == "BUY" for s in signals)


# ------------------------------------------------------------------
# Tests: DirectionalStrategy
# ------------------------------------------------------------------

class TestDirectional:
    def test_should_act_strong_signal(self, client, dir_config) -> None:
        strategy = DirectionalStrategy(client, dir_config)
        assert strategy.should_act({"mid_price": 0.6, "avg_price_24h": 0.5, "volume_ratio": 1.5}) is True

    def test_should_act_weak_signal(self, client, dir_config) -> None:
        strategy = DirectionalStrategy(client, dir_config)
        assert strategy.should_act({"mid_price": 0.51, "avg_price_24h": 0.50, "volume_ratio": 1.0}) is False

    def test_evaluate_sell_on_price_up(self, client, dir_config) -> None:
        strategy = DirectionalStrategy(client, dir_config)
        signals = strategy.evaluate({
            "token_id": "tok1",
            "condition_id": "c1",
            "mid_price": 0.7,
            "avg_price_24h": 0.5,
            "volume_ratio": 1.0,
        })
        assert len(signals) == 1
        assert signals[0].side == "SELL"  # Reversion a la media

    def test_evaluate_buy_on_price_down(self, client, dir_config) -> None:
        strategy = DirectionalStrategy(client, dir_config)
        signals = strategy.evaluate({
            "token_id": "tok1",
            "condition_id": "c1",
            "mid_price": 0.3,
            "avg_price_24h": 0.5,
            "volume_ratio": 1.0,
        })
        assert len(signals) == 1
        assert signals[0].side == "BUY"

    def test_evaluate_no_signal_when_avg_zero(self, client, dir_config) -> None:
        strategy = DirectionalStrategy(client, dir_config)
        signals = strategy.evaluate({
            "token_id": "tok1",
            "mid_price": 0.5,
            "avg_price_24h": 0,
        })
        assert signals == []
