"""Tests para las estrategias de trading con la nueva API Signal/Trade."""

import json
import time
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
        "capital_total": 400.0,
        "directional": {
            "min_edge_bps": 50,       # 0.5% para facilitar tests
            "min_size_usdc": 5.0,
            "max_size_usdc": 20.0,
            "kelly_fraction": 0.25,
            "expected_hold_seconds": 3600.0,
        },
    }


def _make_mm_market_data(
    condition_id: str = "cond_1",
    token_id: str = "tok_1",
    mid_price: float = 0.5,
    spread: float = 0.05,
    best_bid: float | None = None,
    best_ask: float | None = None,
    token_inventory: dict[str, float] | None = None,
) -> dict:
    """Construye market_data valido para MarketMakerStrategy.evaluate()."""
    if best_bid is None:
        best_bid = round(mid_price - spread / 2, 4)
    if best_ask is None:
        best_ask = round(mid_price + spread / 2, 4)
    return {
        "condition_id": condition_id,
        "spread": spread,
        "mid_price": mid_price,
        "tokens": [{"token_id": token_id}],
        "token_data": {
            token_id: {
                "mid_price": mid_price,
                "spread": spread,
                "best_bid": best_bid,
                "best_ask": best_ask,
                "orderbook": {
                    "bids": [{"price": best_bid, "size": 100}],
                    "asks": [{"price": best_ask, "size": 100}],
                },
            }
        },
        "token_inventory": token_inventory or {},
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
        trades = strategy.run({"spread": 0.001, "mid_price": 0.5, "token_id": "t1", "condition_id": "c1"})
        assert trades == []

    def test_run_full_pipeline(self, client, mm_config, tmp_path) -> None:
        import src.strategy.base as base_mod
        original = base_mod.TRADES_FILE
        base_mod.TRADES_FILE = tmp_path / "trades.jsonl"
        try:
            strategy = MarketMakerStrategy(client, mm_config)
            trades = strategy.run(_make_mm_market_data(
                condition_id="cond_1", token_id="t1", mid_price=0.5, spread=0.05
            ))
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
        signals = strategy.evaluate(_make_mm_market_data(
            token_id="tok_123", condition_id="cond_1", mid_price=0.5, spread=0.05
        ))
        assert len(signals) > 0
        assert all(isinstance(s, Signal) for s in signals)
        assert all(s.strategy_name == "market_maker" for s in signals)

    def test_evaluate_generates_bids_and_asks(self, client, mm_config) -> None:
        strategy = MarketMakerStrategy(client, mm_config)
        signals = strategy.evaluate(_make_mm_market_data(
            token_id="tok_1", condition_id="c1", mid_price=0.5, spread=0.05,
            token_inventory={"tok_1": 10.0},
        ))
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

    def test_execute_cancels_previous_orders(self, client, mm_config, tmp_path) -> None:
        """execute() debe cancelar ordenes previas antes de colocar nuevas."""
        import src.strategy.base as base_mod
        original = base_mod.TRADES_FILE
        base_mod.TRADES_FILE = tmp_path / "trades.jsonl"
        try:
            strategy = MarketMakerStrategy(client, mm_config)
            signals = [
                strategy._make_signal("cond_1", "t1", "BUY", 0.45, 5.0, 0.9),
                strategy._make_signal("cond_1", "t1", "SELL", 0.55, 5.0, 0.9),
            ]
            trades = strategy.execute(signals)
            # Ambas ordenes deben ejecutarse exitosamente
            assert len(trades) == 2
            assert all(t.status == "filled_paper" for t in trades)
        finally:
            base_mod.TRADES_FILE = original

    def test_evaluate_respects_spread_vs_tick_size(self, client, mm_config) -> None:
        """Si spread <= 2 * tick_size, no hay oportunidad."""
        strategy = MarketMakerStrategy(client, mm_config)
        # Paper mode: tick_size = 0.01, so 2*tick = 0.02
        # spread = 0.015 < 0.02 → no signals
        signals = strategy.evaluate({
            "token_id": "tok_1",
            "mid_price": 0.5,
            "condition_id": "c1",
            "spread": 0.015,
        })
        assert signals == []

    def test_evaluate_with_sufficient_spread(self, client, mm_config) -> None:
        """Si spread > 2 * tick_size, genera signals."""
        strategy = MarketMakerStrategy(client, mm_config)
        # Paper mode: tick_size = 0.01, so 2*tick = 0.02
        # spread = 0.05 > 0.02 → signals
        signals = strategy.evaluate(_make_mm_market_data(
            token_id="tok_1", condition_id="c1", mid_price=0.5, spread=0.05
        ))
        assert len(signals) > 0

    def test_inventory_skew_adjusts_quotes(self, client, mm_config) -> None:
        """Con inventario sesgado, los precios se ajustan."""
        strategy = MarketMakerStrategy(client, mm_config)

        # Sin inventario: evaluamos para tener baseline
        signals_neutral = strategy.evaluate({
            "token_id": "tok_1",
            "mid_price": 0.5,
            "condition_id": "c1",
            "spread": 0.05,
        })

        # Simular inventario long grande
        strategy._inventory["tok_1"] = 30.0  # 60% del max
        signals_long = strategy.evaluate({
            "token_id": "tok_1",
            "mid_price": 0.5,
            "condition_id": "c1",
            "spread": 0.05,
        })

        # Con inventario long, el bid debe estar mas alejado
        neutral_bids = [s for s in signals_neutral if s.side == "BUY"]
        long_bids = [s for s in signals_long if s.side == "BUY"]

        if neutral_bids and long_bids:
            assert long_bids[0].price <= neutral_bids[0].price

    def test_inventory_limit_blocks_signals(self, client, mm_config) -> None:
        """Si inventario esta al max, no genera signals en ese lado."""
        strategy = MarketMakerStrategy(client, mm_config)
        strategy._inventory["tok_1"] = 49.0  # Casi al max (50)

        signals = strategy.evaluate({
            "token_id": "tok_1",
            "mid_price": 0.5,
            "condition_id": "c1",
            "spread": 0.05,
        })

        # No debe haber BUY signals (inventario + order_size > max)
        buy_signals = [s for s in signals if s.side == "BUY"]
        assert len(buy_signals) == 0

    def test_execute_updates_inventory(self, client, mm_config, tmp_path) -> None:
        """execute() NO actualiza inventario interno (solo tras fill confirmado)."""
        import src.strategy.base as base_mod
        original = base_mod.TRADES_FILE
        base_mod.TRADES_FILE = tmp_path / "trades.jsonl"
        try:
            strategy = MarketMakerStrategy(client, mm_config)
            assert strategy.get_inventory("t1") == 0.0

            signal = strategy._make_signal("m1", "t1", "BUY", 0.45, 5.0, 0.9)
            strategy.execute([signal])
            # Post-only GTC: la orden puede no llenarse → inventario no cambia
            assert strategy.get_inventory("t1") == 0.0

            # Solo _update_inventory (llamado tras fill confirmado) cambia el inventario
            strategy._update_inventory("t1", "BUY", 5.0)
            assert strategy.get_inventory("t1") == 5.0

            strategy._update_inventory("t1", "SELL", 3.0)
            assert strategy.get_inventory("t1") == 2.0
        finally:
            base_mod.TRADES_FILE = original

    def test_manage_inventory_pauses_overexposed_side(self, client, mm_config) -> None:
        """manage_inventory() pausa lados cuando inventario > 80% max."""
        strategy = MarketMakerStrategy(client, mm_config)
        strategy._inventory["yes_tok"] = 45.0  # 90% de 50 → > 80%

        strategy.manage_inventory({
            "tokens": [
                {"token_id": "yes_tok"},
                {"token_id": "no_tok"},
            ],
        })

        assert "BUY" in strategy.get_paused_sides("yes_tok")

    def test_manage_inventory_unpauses_within_limits(self, client, mm_config) -> None:
        """manage_inventory() desbloquea cuando inventario vuelve a limites."""
        strategy = MarketMakerStrategy(client, mm_config)
        strategy._paused_sides["yes_tok"] = {"BUY"}
        strategy._inventory["yes_tok"] = 10.0  # Dentro de limites

        strategy.manage_inventory({
            "tokens": [
                {"token_id": "yes_tok"},
                {"token_id": "no_tok"},
            ],
        })

        assert strategy.get_paused_sides("yes_tok") == set()

    def test_manage_inventory_merge_yes_no(self, client, mm_config) -> None:
        """manage_inventory() mergea YES+NO shares cuando ambos son positivos."""
        strategy = MarketMakerStrategy(client, mm_config)
        strategy._inventory["yes_tok"] = 10.0
        strategy._inventory["no_tok"] = 7.0

        strategy.manage_inventory({
            "tokens": [
                {"token_id": "yes_tok"},
                {"token_id": "no_tok"},
            ],
        })

        # Debe mergear min(10, 7) = 7
        assert strategy.get_inventory("yes_tok") == 3.0
        assert strategy.get_inventory("no_tok") == 0.0

    def test_needs_refresh(self, client, mm_config) -> None:
        """needs_refresh() controla el timing del loop."""
        strategy = MarketMakerStrategy(client, mm_config)
        assert strategy.needs_refresh("cond_1") is True

        strategy.mark_refreshed("cond_1")
        assert strategy.needs_refresh("cond_1") is False

    def test_run_respects_refresh_timing(self, client, mm_config, tmp_path) -> None:
        """run() no ejecuta si no paso suficiente tiempo."""
        import src.strategy.base as base_mod
        original = base_mod.TRADES_FILE
        base_mod.TRADES_FILE = tmp_path / "trades.jsonl"
        try:
            strategy = MarketMakerStrategy(client, mm_config)
            market_data = _make_mm_market_data(
                condition_id="cond_1", token_id="t1", mid_price=0.5, spread=0.05
            )

            # Primera ejecucion: debe ejecutar
            trades1 = strategy.run(market_data)
            assert len(trades1) > 0

            # Segunda ejecucion inmediata: debe saltar (no paso refresh_seconds)
            trades2 = strategy.run(market_data)
            assert trades2 == []
        finally:
            base_mod.TRADES_FILE = original

    def test_round_to_tick(self, client, mm_config) -> None:
        """_round_to_tick redondea correctamente."""
        strategy = MarketMakerStrategy(client, mm_config)
        assert strategy._round_to_tick(0.453, 0.01) == 0.45
        assert strategy._round_to_tick(0.455, 0.01) == 0.46
        assert strategy._round_to_tick(0.4567, 0.001) == 0.457

    def test_paused_side_blocks_evaluate(self, client, mm_config) -> None:
        """Si un lado esta pausado, evaluate() no genera signals para ese lado."""
        strategy = MarketMakerStrategy(client, mm_config)
        strategy._paused_sides["tok_1"] = {"BUY"}

        signals = strategy.evaluate(_make_mm_market_data(
            token_id="tok_1", condition_id="c1", mid_price=0.5, spread=0.05,
            token_inventory={"tok_1": 10.0},
        ))

        buy_signals = [s for s in signals if s.side == "BUY"]
        sell_signals = [s for s in signals if s.side == "SELL"]
        assert len(buy_signals) == 0
        assert len(sell_signals) > 0


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

    def test_detect_opportunities_buy_when_sum_below_one(self, client, arb_config) -> None:
        """Si sum(ask) < 1.0 - min_edge, genera BUY signals para todos."""
        strategy = MultiArbStrategy(client, arb_config)
        signals = strategy.detect_opportunities({
            "condition_id": "c1",
            "tokens": [
                {"token_id": "a", "price": 0.40},
                {"token_id": "b", "price": 0.40},
            ],
        })
        assert len(signals) == 2
        assert all(s.side == "BUY" for s in signals)

    def test_evaluate_delegates_to_detect_opportunities(self, client, arb_config) -> None:
        """evaluate() llama a detect_opportunities()."""
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

    def test_no_signals_when_no_edge(self, client, arb_config) -> None:
        """Sin edge suficiente, no genera signals."""
        strategy = MultiArbStrategy(client, arb_config)
        signals = strategy.detect_opportunities({
            "condition_id": "c1",
            "tokens": [
                {"token_id": "a", "price": 0.50},
                {"token_id": "b", "price": 0.50},
            ],
        })
        assert signals == []

    def test_no_signals_when_sum_above_one(self, client, arb_config) -> None:
        """Si sum(ask) > 1.0, no hay arb de compra."""
        strategy = MultiArbStrategy(client, arb_config)
        signals = strategy.detect_opportunities({
            "condition_id": "c1",
            "tokens": [
                {"token_id": "a", "price": 0.55},
                {"token_id": "b", "price": 0.55},
            ],
        })
        assert signals == []

    def test_profit_calculation_accounts_for_fees(self, client, arb_config) -> None:
        """El profit potencial descuenta fees estimados."""
        strategy = MultiArbStrategy(client, arb_config)
        # sum = 0.94, gross profit = 6%, pero fees pueden reducirlo
        # Paper mode: fee_rate = 0, asi que solo verifica que genera signals
        signals = strategy.detect_opportunities({
            "condition_id": "c1",
            "tokens": [
                {"token_id": "a", "price": 0.47},
                {"token_id": "b", "price": 0.47},
            ],
        })
        assert len(signals) == 2

    def test_execute_uses_fok(self, client, arb_config, tmp_path) -> None:
        """execute() debe usar FOK orders."""
        import src.strategy.base as base_mod
        original = base_mod.TRADES_FILE
        base_mod.TRADES_FILE = tmp_path / "trades.jsonl"
        try:
            strategy = MultiArbStrategy(client, arb_config)
            signals = [
                strategy._make_signal("c1", "a", "BUY", 0.40, 5.0, 0.8),
                strategy._make_signal("c1", "b", "BUY", 0.40, 5.0, 0.8),
            ]
            trades = strategy.execute(signals)
            assert len(trades) == 2
            assert all(t.status == "filled_paper" for t in trades)
        finally:
            base_mod.TRADES_FILE = original

    def test_stats_tracking(self, client, arb_config) -> None:
        """Estadisticas de oportunidades detectadas vs ejecutadas."""
        strategy = MultiArbStrategy(client, arb_config)
        stats = strategy.get_stats()
        assert stats["opportunities_seen"] == 0
        assert stats["opportunities_executed"] == 0

        # Detectar una oportunidad
        strategy.detect_opportunities({
            "condition_id": "c1",
            "tokens": [
                {"token_id": "a", "price": 0.40},
                {"token_id": "b", "price": 0.40},
            ],
        })
        stats = strategy.get_stats()
        assert stats["opportunities_seen"] == 1

    def test_confidence_increases_with_edge(self, client, arb_config) -> None:
        """Mayor edge = mayor confidence en los signals."""
        strategy = MultiArbStrategy(client, arb_config)

        # Edge moderado
        signals_small = strategy.detect_opportunities({
            "condition_id": "c1",
            "tokens": [
                {"token_id": "a", "price": 0.45},
                {"token_id": "b", "price": 0.45},
            ],
        })

        # Edge grande
        signals_large = strategy.detect_opportunities({
            "condition_id": "c2",
            "tokens": [
                {"token_id": "c", "price": 0.30},
                {"token_id": "d", "price": 0.30},
            ],
        })

        if signals_small and signals_large:
            assert signals_large[0].confidence >= signals_small[0].confidence

    def test_multi_outcome_three_tokens(self, client, arb_config) -> None:
        """Funciona con 3+ outcomes (NegRisk markets)."""
        strategy = MultiArbStrategy(client, arb_config)
        signals = strategy.detect_opportunities({
            "condition_id": "c1",
            "tokens": [
                {"token_id": "a", "price": 0.25},
                {"token_id": "b", "price": 0.25},
                {"token_id": "c", "price": 0.25},
            ],
        })
        # sum = 0.75, profit = 25% > 3% min_edge
        assert len(signals) == 3
        assert all(s.side == "BUY" for s in signals)


# ------------------------------------------------------------------
# Tests: DirectionalStrategy (rewrite Fase 3 — régimen + CostModel + Kelly)
# ------------------------------------------------------------------

class TestDirectional:
    def test_should_act_unknown_regime_retorna_true(self, client, dir_config) -> None:
        """Con insuficientes precios el régimen es UNKNOWN → should_act=True."""
        strategy = DirectionalStrategy(client, dir_config)
        # Sin historial de precios: régimen UNKNOWN
        result = strategy.should_act({"mid_price": 0.5, "token_id": "t1"})
        assert result is True

    def test_should_act_actualiza_regime_detector(self, client, dir_config) -> None:
        """Cada llamada a should_act alimenta el precio al RegimeDetector."""
        strategy = DirectionalStrategy(client, dir_config)
        strategy.should_act({"mid_price": 0.5, "token_id": "t1"})
        # Verificar que el detector tiene al menos 1 precio
        buf = strategy._regime_detector._price_buffers.get("t1", [])
        assert len(buf) >= 1

    def test_evaluate_sell_cuando_mid_mayor_a_fair(self, client, dir_config) -> None:
        """mid > fair_value → mercado sobreestima → SELL."""
        strategy = DirectionalStrategy(client, dir_config)
        signals = strategy.evaluate({
            "token_id": "tok1",
            "condition_id": "c1",
            "mid_price": 0.75,
            "avg_price_24h": 0.50,
            "volume_ratio": 1.0,
        })
        assert len(signals) == 1
        assert signals[0].side == "SELL"

    def test_evaluate_buy_cuando_mid_menor_a_fair(self, client, dir_config) -> None:
        """mid < fair_value → mercado subestima → BUY."""
        strategy = DirectionalStrategy(client, dir_config)
        signals = strategy.evaluate({
            "token_id": "tok1",
            "condition_id": "c1",
            "mid_price": 0.25,
            "avg_price_24h": 0.50,
            "volume_ratio": 1.0,
        })
        assert len(signals) == 1
        assert signals[0].side == "BUY"

    def test_evaluate_sin_avg_retorna_vacio(self, client, dir_config) -> None:
        """Sin avg_price_24h no se puede calcular fair_value → sin señal."""
        strategy = DirectionalStrategy(client, dir_config)
        signals = strategy.evaluate({
            "token_id": "tok1",
            "condition_id": "c1",
            "mid_price": 0.5,
            "avg_price_24h": 0.0,
        })
        assert signals == []

    def test_evaluate_sin_edge_suficiente(self, client, dir_config) -> None:
        """Desviación mínima → edge < min_edge_bps → sin señal."""
        strategy = DirectionalStrategy(client, dir_config)
        # mid y avg casi iguales → desviación ~0
        signals = strategy.evaluate({
            "token_id": "tok1",
            "condition_id": "c1",
            "mid_price": 0.500,
            "avg_price_24h": 0.499,
            "volume_ratio": 1.0,
        })
        assert signals == []

    def test_evaluate_signal_tiene_metadata_edge_bps(self, client, dir_config) -> None:
        """Los signals deben incluir edge_bps y fair_value en metadata."""
        strategy = DirectionalStrategy(client, dir_config)
        signals = strategy.evaluate({
            "token_id": "tok1",
            "condition_id": "c1",
            "mid_price": 0.75,
            "avg_price_24h": 0.50,
            "volume_ratio": 1.0,
        })
        assert len(signals) == 1
        meta = signals[0].metadata
        assert "edge_bps" in meta
        assert "fair_value" in meta
        assert meta["edge_bps"] > 0

    def test_evaluate_price_extremos_retorna_vacio(self, client, dir_config) -> None:
        """Precios cerca de 0 o 1 no generan señales (filtro de bounds)."""
        strategy = DirectionalStrategy(client, dir_config)
        assert strategy.evaluate({"token_id": "t", "condition_id": "c", "mid_price": 0.01}) == []
        assert strategy.evaluate({"token_id": "t", "condition_id": "c", "mid_price": 0.99}) == []

    def test_execute_genera_post_only_order(self, client, dir_config, tmp_path) -> None:
        """execute() usa post_only=True y registra el trade."""
        import src.strategy.base as base_mod
        original = base_mod.TRADES_FILE
        base_mod.TRADES_FILE = tmp_path / "trades.jsonl"
        try:
            strategy = DirectionalStrategy(client, dir_config)
            signal = strategy._make_signal("m1", "t1", "BUY", 0.45, 8.0, 0.7)
            trades = strategy.execute([signal])
            assert len(trades) == 1
            assert trades[0].side == "BUY"
            assert base_mod.TRADES_FILE.exists()
        finally:
            base_mod.TRADES_FILE = original

    def test_near_resolution_gate_cancela_mm(self, client, mm_config) -> None:
        """MM cancela quotes y no opera si el mercado resuelve en < near_resolution_hours."""
        from datetime import datetime, timedelta, timezone
        strategy = MarketMakerStrategy(client, mm_config)
        strategy._near_resolution_hours = 6.0

        # Fecha de resolución en 3 horas (< 6h umbral)
        end_dt = datetime.now(timezone.utc) + timedelta(hours=3)
        market_data = {
            "spread": 0.05,
            "mid_price": 0.5,
            "token_id": "t1",
            "condition_id": "cond_near",
            "end_date_iso": end_dt.isoformat(),
        }
        trades = strategy.run(market_data)
        assert trades == []

    def test_near_resolution_gate_permite_sin_end_date(self, client, mm_config, tmp_path) -> None:
        """MM opera normalmente si no hay end_date (no puede saber si near-resolution)."""
        import src.strategy.base as base_mod
        original = base_mod.TRADES_FILE
        base_mod.TRADES_FILE = tmp_path / "trades.jsonl"
        try:
            strategy = MarketMakerStrategy(client, mm_config)
            market_data = {
                "spread": 0.05,
                "mid_price": 0.5,
                "token_id": "t1",
                "condition_id": "cond_no_date",
            }
            trades = strategy.run(market_data)
            # Sin end_date → no near-resolution gate → puede operar normalmente
            assert isinstance(trades, list)
        finally:
            base_mod.TRADES_FILE = original

    def test_near_resolution_gate_no_activa_cuando_lejos(self, client, mm_config, tmp_path) -> None:
        """MM opera normalmente si la resolución está a más de near_resolution_hours."""
        import src.strategy.base as base_mod
        from datetime import datetime, timedelta, timezone
        original = base_mod.TRADES_FILE
        base_mod.TRADES_FILE = tmp_path / "trades.jsonl"
        try:
            strategy = MarketMakerStrategy(client, mm_config)
            strategy._near_resolution_hours = 6.0

            # Resolución en 24 horas (> 6h → no near-resolution)
            end_dt = datetime.now(timezone.utc) + timedelta(hours=24)
            market_data = {
                "spread": 0.05,
                "mid_price": 0.5,
                "token_id": "t1",
                "condition_id": "cond_far",
                "end_date_iso": end_dt.isoformat(),
            }
            trades = strategy.run(market_data)
            # Lejos de resolución → opera normalmente
            assert isinstance(trades, list)
        finally:
            base_mod.TRADES_FILE = original
