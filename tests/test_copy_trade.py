"""Tests para CopyTradeStrategy (TODO 4.5)."""
from unittest.mock import MagicMock, patch
import pytest

from src.strategy.copy_trade import CopyTradeStrategy, CopyPosition


def make_strategy(whale_trades=None, **extra_cfg):
    """Crea una instancia de CopyTradeStrategy con mocks."""
    client = MagicMock()
    config = {
        "copy_trade": {
            "min_whale_size_usdc": 5000.0,
            "copy_fraction": 0.07,
            "max_copy_size_usdc": 20.0,
            "stop_loss_pct": 0.10,
            "max_positions": 3,
            "lookback_hours": 1.0,
        },
        **extra_cfg,
    }
    tracker = MagicMock()
    tracker.get_recent_whales.return_value = whale_trades or []
    strat = CopyTradeStrategy(client, config, whale_tracker=tracker)
    return strat, client, tracker


def make_market(cid="0xmarket1", mid=0.55):
    return {
        "condition_id": cid,
        "mid_price": mid,
        "tokens": [{"token_id": "tok1"}],
    }


# ----------------------------
# should_act
# ----------------------------

class TestShouldAct:
    def test_no_tracker_returns_false(self):
        client = MagicMock()
        strat = CopyTradeStrategy(client, {}, whale_tracker=None)
        assert strat.should_act(make_market()) is False

    def test_returns_true_when_tracker_present(self):
        strat, _, _ = make_strategy()
        assert strat.should_act(make_market()) is True

    def test_false_when_max_positions_reached(self):
        strat, _, _ = make_strategy()
        # Llenar posiciones hasta el máximo (3)
        for i in range(3):
            strat._open_positions[f"mkt{i}"] = CopyPosition(
                market_id=f"mkt{i}", token_id="tok",
                side="BUY", entry_price=0.5, size=10, whale_size=5000
            )
        assert strat.should_act(make_market()) is False


# ----------------------------
# evaluate
# ----------------------------

class TestEvaluate:
    def test_no_signals_without_whale_trades(self):
        strat, _, tracker = make_strategy(whale_trades=[])
        signals = strat.evaluate(make_market())
        assert signals == []

    def test_signal_generated_for_large_whale_trade(self):
        trades = [{"trade_id": "w1", "size": 10000, "side": "BUY", "price": 0.55}]
        strat, _, _ = make_strategy(whale_trades=trades)
        signals = strat.evaluate(make_market())
        assert len(signals) == 1
        sig = signals[0]
        assert sig.side == "BUY"
        assert sig.size == pytest.approx(20.0)  # capped at max_copy_size=20

    def test_small_whale_trade_ignored(self):
        trades = [{"trade_id": "w2", "size": 100, "side": "BUY", "price": 0.55}]
        strat, _, _ = make_strategy(whale_trades=trades)
        signals = strat.evaluate(make_market())
        assert signals == []

    def test_copy_size_respects_fraction(self):
        # Whale de $7000 con fraction=0.07 y max=$20 → min(490, 20) = 20
        trades = [{"trade_id": "w3", "size": 7000, "side": "SELL", "price": 0.45}]
        strat, _, _ = make_strategy(whale_trades=trades)
        signals = strat.evaluate(make_market())
        assert len(signals) == 1
        assert signals[0].size <= 20.0

    def test_duplicate_whale_id_not_processed_twice(self):
        trades = [{"trade_id": "dup", "size": 8000, "side": "BUY", "price": 0.5}]
        strat, _, _ = make_strategy(whale_trades=trades)
        signals1 = strat.evaluate(make_market())
        signals2 = strat.evaluate(make_market())
        assert len(signals1) == 1
        assert len(signals2) == 0  # Ya procesado

    def test_existing_position_not_duplicated(self):
        trades = [{"trade_id": "w4", "size": 8000, "side": "BUY", "price": 0.5}]
        strat, _, _ = make_strategy(whale_trades=trades)
        market = make_market()
        strat._open_positions[market["condition_id"]] = CopyPosition(
            market_id=market["condition_id"], token_id="tok1",
            side="BUY", entry_price=0.5, size=10, whale_size=5000
        )
        signals = strat.evaluate(market)
        assert signals == []

    def test_no_market_tokens_returns_empty(self):
        trades = [{"trade_id": "w5", "size": 8000, "side": "BUY", "price": 0.5}]
        strat, _, _ = make_strategy(whale_trades=trades)
        mkt = {"condition_id": "0xabc", "mid_price": 0.5, "tokens": []}
        assert strat.evaluate(mkt) == []

    def test_tracker_exception_returns_empty(self):
        strat, _, tracker = make_strategy()
        tracker.get_recent_whales.side_effect = RuntimeError("API down")
        assert strat.evaluate(make_market()) == []


# ----------------------------
# execute
# ----------------------------

class TestExecute:
    def test_execute_creates_position(self):
        strat, client, _ = make_strategy()
        from src.strategy.base import Trade
        from datetime import datetime, timezone
        mock_trade = Trade(
            timestamp=datetime.now(timezone.utc).isoformat(),
            market_id="0xmarket1",
            token_id="tok1",
            side="BUY",
            price=0.55,
            size=15.0,
            order_id="ord1",
            status="MATCHED",
            strategy_name="copy_trade",
        )
        client.place_order.return_value = mock_trade

        from src.strategy.base import Signal
        signals = [Signal(
            market_id="0xmarket1", token_id="tok1",
            side="BUY", price=0.55, size=15.0,
            confidence=0.6,
            strategy_name="copy_trade",
            metadata={"whale_size": 8000},
        )]
        trades = strat.execute(signals)
        assert len(trades) == 1
        assert "0xmarket1" in strat._open_positions

    def test_execute_skips_on_api_error(self):
        strat, client, _ = make_strategy()
        client.place_order.side_effect = RuntimeError("timeout")

        from src.strategy.base import Signal
        signals = [Signal(
            market_id="0xerr", token_id="tok1",
            side="BUY", price=0.5, size=10.0,
            confidence=0.6,
            strategy_name="copy_trade",
            metadata={"whale_size": 6000},
        )]
        trades = strat.execute(signals)
        assert trades == []


# ----------------------------
# stop-loss
# ----------------------------

class TestStopLoss:
    def test_buy_stop_loss_triggers(self):
        strat, client, _ = make_strategy()
        strat._open_positions["0xmkt"] = CopyPosition(
            market_id="0xmkt", token_id="tok",
            side="BUY", entry_price=0.50, size=10.0, whale_size=6000,
            stop_loss_pct=0.10,
        )
        from src.strategy.base import Trade
        from datetime import datetime, timezone
        exit_trade = Trade(
            timestamp=datetime.now(timezone.utc).isoformat(),
            market_id="0xmkt", token_id="tok",
            side="SELL", price=0.44, size=10.0,
            order_id="exit1", status="MATCHED",
            strategy_name="copy_trade",
        )
        client.place_order.return_value = exit_trade

        market_data_map = {"0xmkt": {"mid_price": 0.44}}  # 12% drop
        exits = strat.check_stop_losses(market_data_map)
        assert len(exits) == 1
        assert "0xmkt" not in strat._open_positions

    def test_buy_stop_not_triggered_above_threshold(self):
        strat, client, _ = make_strategy()
        strat._open_positions["0xmkt2"] = CopyPosition(
            market_id="0xmkt2", token_id="tok",
            side="BUY", entry_price=0.50, size=10.0, whale_size=6000,
            stop_loss_pct=0.10,
        )
        market_data_map = {"0xmkt2": {"mid_price": 0.46}}  # 8% drop — within stop
        exits = strat.check_stop_losses(market_data_map)
        assert exits == []
        assert "0xmkt2" in strat._open_positions

    def test_no_mid_price_skips_check(self):
        strat, client, _ = make_strategy()
        strat._open_positions["0xmkt3"] = CopyPosition(
            market_id="0xmkt3", token_id="tok",
            side="BUY", entry_price=0.5, size=10.0, whale_size=6000,
        )
        exits = strat.check_stop_losses({"0xmkt3": {"mid_price": 0.0}})
        assert exits == []


# ----------------------------
# CopyPosition properties
# ----------------------------

class TestCopyPosition:
    def test_stop_price_buy(self):
        pos = CopyPosition(
            market_id="m", token_id="t",
            side="BUY", entry_price=0.5, size=10, whale_size=5000,
            stop_loss_pct=0.10,
        )
        assert pos.stop_price_buy == pytest.approx(0.45)

    def test_stop_price_sell(self):
        pos = CopyPosition(
            market_id="m", token_id="t",
            side="SELL", entry_price=0.5, size=10, whale_size=5000,
            stop_loss_pct=0.10,
        )
        assert pos.stop_price_sell == pytest.approx(0.55)

    def test_get_open_positions_is_copy(self):
        strat, _, _ = make_strategy()
        strat._open_positions["m1"] = CopyPosition(
            market_id="m1", token_id="t", side="BUY",
            entry_price=0.5, size=10, whale_size=5000
        )
        positions = strat.get_open_positions()
        assert "m1" in positions
        # Modificar la copia no afecta el original
        positions.pop("m1")
        assert "m1" in strat._open_positions
