"""Smoke test para RewardsFarmerStrategy multi-outcome."""

import pytest
from src.strategy.rewards_farmer import RewardsFarmerStrategy


class DummyClient:
    def __init__(self):
        self.paper_mode = True

    def get_positions(self):
        return []

    def post_batch_orders(self, signals):
        return [{"status": "filled_paper", "order_id": f"paper_{i}"} for i in range(len(signals))]


@pytest.fixture
def rf():
    config = {
        "rewards_farmer": {
            "max_capital_per_market": 50.0,
            "min_rewards_pool_usd": 0.0,
            "max_markets_simultaneous": 5,
            "spread_pct_of_max": 0.50,
            "two_sided": True,
            "inventory_merge_threshold": 5.0,
            "competition_share_min": 0.05,
            "max_mid_deviation": 0.35,
        },
        "markets": {},
        "risk": {},
    }
    return RewardsFarmerStrategy(DummyClient(), config)


def test_should_act_binary(rf):
    market = {
        "condition_id": "abc123",
        "mid_price": 0.51,
        "rewards_rate": 10.0,
        "rewards_min_size": 20.0,
        "rewards_max_spread": 4.0,
        "tokens": [
            {"token_id": "t1", "outcome": "yes"},
            {"token_id": "t2", "outcome": "no"},
        ],
        "token_data": {
            "t1": {"mid_price": 0.51, "orderbook": {"asks": [{"price": 0.52, "size": 100}]}},
            "t2": {"mid_price": 0.49, "orderbook": {"asks": [{"price": 0.50, "size": 100}]}},
        },
    }
    assert rf.should_act(market) is True


def test_should_act_multi_outcome(rf):
    market = {
        "condition_id": "abc123",
        "mid_price": 0.51,
        "rewards_rate": 10.0,
        "rewards_min_size": 20.0,
        "rewards_max_spread": 4.0,
        "tokens": [
            {"token_id": "t1", "outcome": "<2"},
            {"token_id": "t2", "outcome": "2-3"},
            {"token_id": "t3", "outcome": "4+"},
        ],
        "token_data": {
            "t1": {"mid_price": 0.51, "orderbook": {"asks": [{"price": 0.52, "size": 100}]}},
            "t2": {"mid_price": 0.35, "orderbook": {"asks": [{"price": 0.36, "size": 100}]}},
            "t3": {"mid_price": 0.14, "orderbook": {"asks": [{"price": 0.15, "size": 100}]}},
        },
    }
    assert rf.should_act(market) is True


def test_evaluate_multi_outcome(rf):
    market = {
        "condition_id": "abc123",
        "mid_price": 0.51,
        "rewards_rate": 10.0,
        "rewards_min_size": 20.0,
        "rewards_max_spread": 4.0,
        "tick_size": 0.01,
        "tokens": [
            {"token_id": "t1", "outcome": "<2"},
            {"token_id": "t2", "outcome": "2-3"},
            {"token_id": "t3", "outcome": "4+"},
        ],
        "token_data": {
            "t1": {"mid_price": 0.51, "orderbook": {"asks": [{"price": 0.52, "size": 100}]}},
            "t2": {"mid_price": 0.35, "orderbook": {"asks": [{"price": 0.36, "size": 100}]}},
            "t3": {"mid_price": 0.14, "orderbook": {"asks": [{"price": 0.15, "size": 100}]}},
        },
        "available_cash": 166.0,
    }
    signals = rf.evaluate(market)
    assert len(signals) == 3
    # Verificar que los sizes son proporcionales a los mids
    sizes = {s.token_id: s.size for s in signals}
    assert sizes["t1"] > sizes["t2"] > sizes["t3"]


def test_execute_does_not_cancel_manual_orders(rf):
    """El bot solo debe cancelar ordenes que el mismo coloco."""
    # Simular que hay una orden manual abierta (no en _pending_orders)
    rf._client.get_positions = lambda: [
        {"id": "manual_1", "asset_id": "t1", "side": "BUY", "price": 0.50, "original_size": 100}
    ]
    signals = rf.evaluate({
        "condition_id": "abc123",
        "mid_price": 0.51,
        "rewards_rate": 10.0,
        "rewards_min_size": 20.0,
        "rewards_max_spread": 4.0,
        "tick_size": 0.01,
        "tokens": [
            {"token_id": "t1", "outcome": "<2"},
            {"token_id": "t2", "outcome": "2-3"},
        ],
        "token_data": {
            "t1": {"mid_price": 0.51, "orderbook": {"asks": [{"price": 0.52, "size": 100}]}},
            "t2": {"mid_price": 0.49, "orderbook": {"asks": [{"price": 0.50, "size": 100}]}},
        },
        "available_cash": 166.0,
    })
    # No hay ordenes propias pendientes, asi que deberia colocar nuevas sin cancelar la manual
    trades = rf.execute(signals)
    assert len(trades) == 2
    # La orden manual no deberia haber sido cancelada (no hay crash y trades se generan)
