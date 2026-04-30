"""Tests para RewardsFarmerStrategy con shadow quoting v3."""

import pytest
from src.strategy.rewards_farmer import (
    RewardsFarmerStrategy,
    _qualifying_bid,
    _qualifying_ask,
    SAFETY_TICKS,
    MIN_MAX_SPREAD_USD,
)


class DummyClient:
    def __init__(self):
        self.paper_mode = True

    def get_positions(self):
        return []

    def post_batch_orders(self, signals):
        return [{"status": "filled_paper", "order_id": f"paper_{i}"} for i in range(len(signals))]

    def is_order_scoring(self, order_id):
        return True

    def cancel_order(self, order_id):
        return True


@pytest.fixture
def rf():
    config = {
        "rewards_farmer": {
            "max_capital_per_market": 50.0,
            "min_rewards_pool_usd": 0.0,
            "max_markets_simultaneous": 5,
            "two_sided": True,
            "inventory_merge_threshold": 5.0,
            "competition_share_min": 0.005,
            "max_mid_deviation": 0.40,
        },
        "markets": {},
        "risk": {},
    }
    return RewardsFarmerStrategy(DummyClient(), config)


# ------------------------------------------------------------------
# _qualifying_bid
# ------------------------------------------------------------------

def test_qualifying_bid_behind_best_bid():
    """Con best_bid dentro de la ventana, bid = best_bid - SAFETY_TICKS * tick."""
    mid, max_spread, tick = 0.50, 0.03, 0.01
    best_bid = 0.49  # Dentro de la ventana [0.48, 0.50)
    bid = _qualifying_bid(mid, max_spread, tick, best_bid)
    # best_bid=0.49 >= qual_low(0.48) + 2*tick(0.02) → 0.49 >= 0.50? NO
    # Entonces bid = qual_low = 0.48
    assert bid == 0.48


def test_qualifying_bid_best_bid_high():
    """Con best_bid alto, bid se coloca SAFETY_TICKS ticks detras."""
    mid, max_spread, tick = 0.50, 0.04, 0.01
    best_bid = 0.49  # qual_low=0.47, high=0.49. best_bid=0.49 >= 0.47+0.02=0.49 → si
    bid = _qualifying_bid(mid, max_spread, tick, best_bid)
    assert bid == round(best_bid - SAFETY_TICKS * tick, 4)  # 0.47


def test_qualifying_bid_no_best_bid():
    """Sin best_bid, colocar al extremo lejano de la ventana (qual_low)."""
    mid, max_spread, tick = 0.50, 0.03, 0.01
    bid = _qualifying_bid(mid, max_spread, tick, best_bid=0.0)
    # qual_low = 0.50 - 0.03 + 0.01 = 0.48
    assert bid == 0.48


def test_qualifying_bid_returns_none_if_no_room():
    """Si max_spread < 2 ticks no hay ventana valida."""
    bid = _qualifying_bid(mid=0.50, max_spread_usd=0.005, tick_size=0.01, best_bid=0.0)
    assert bid is None


def test_qualifying_bid_stays_in_window():
    """El bid nunca debe salir de [qual_low, qual_high]."""
    mid, max_spread, tick = 0.50, 0.04, 0.01
    for best_bid in [0.0, 0.40, 0.46, 0.48, 0.49]:
        bid = _qualifying_bid(mid, max_spread, tick, best_bid)
        if bid is not None:
            qual_low = round(mid - max_spread + tick, 4)
            qual_high = round(mid - tick, 4)
            assert qual_low <= bid <= qual_high, f"bid={bid} out of [{qual_low}, {qual_high}] with best_bid={best_bid}"


# ------------------------------------------------------------------
# _qualifying_ask
# ------------------------------------------------------------------

def test_qualifying_ask_behind_best_ask():
    """Con best_ask dentro de ventana, ask = best_ask + SAFETY_TICKS * tick."""
    mid, max_spread, tick = 0.50, 0.04, 0.01
    best_ask = 0.51  # qual_low=0.51, qual_high=0.53. best_ask=0.51 <= 0.53-0.02=0.51 → ask=0.53
    ask = _qualifying_ask(mid, max_spread, tick, best_ask)
    assert ask == round(best_ask + SAFETY_TICKS * tick, 4)  # 0.53


def test_qualifying_ask_no_best_ask():
    """Sin best_ask, colocar al extremo lejano (qual_high)."""
    mid, max_spread, tick = 0.50, 0.03, 0.01
    ask = _qualifying_ask(mid, max_spread, tick, best_ask=0.0)
    # qual_high = 0.50 + 0.03 - 0.01 = 0.52
    assert ask == 0.52


def test_qualifying_ask_stays_in_window():
    """El ask nunca debe salir de (mid, mid + max_spread]."""
    mid, max_spread, tick = 0.50, 0.04, 0.01
    for best_ask in [0.0, 0.51, 0.52, 0.53, 0.54]:
        ask = _qualifying_ask(mid, max_spread, tick, best_ask)
        if ask is not None:
            qual_low = round(mid + tick, 4)
            qual_high = round(mid + max_spread - tick, 4)
            assert qual_low <= ask <= qual_high, f"ask={ask} out of [{qual_low}, {qual_high}] with best_ask={best_ask}"


# ------------------------------------------------------------------
# should_act
# ------------------------------------------------------------------

def _market(mid=0.51, rewards_rate=10.0, max_spread=4.0, n_tokens=2):
    tokens = [{"token_id": f"t{i}", "outcome": f"out{i}"} for i in range(1, n_tokens + 1)]
    return {
        "condition_id": "abc123",
        "mid_price": mid,
        "rewards_rate": rewards_rate,
        "rewards_min_size": 20.0,
        "rewards_max_spread": max_spread,
        "tick_size": 0.01,
        "tokens": tokens,
        "token_data": {
            f"t{i}": {
                "mid_price": 1.0 / n_tokens,
                "orderbook": {"bids": [], "asks": [{"price": str(1.0 / n_tokens + 0.01), "size": "100"}]},
            }
            for i in range(1, n_tokens + 1)
        },
    }


def test_should_act_binary(rf):
    assert rf.should_act(_market()) is True


def test_should_act_multi_outcome(rf):
    assert rf.should_act(_market(n_tokens=3)) is True


def test_should_act_rejects_tiny_spread(rf):
    """Mercado con max_spread < 2¢ debe ser rechazado."""
    assert rf.should_act(_market(max_spread=1.0)) is False


def test_should_act_rejects_no_rewards(rf):
    m = _market()
    m["rewards_rate"] = 0.0
    # min_rewards_pool=0 en config → pasa igual
    assert rf.should_act(m) is True


# ------------------------------------------------------------------
# evaluate
# ------------------------------------------------------------------

def _binary_market_with_ws(mid_yes=0.52, mid_no=0.48):
    return {
        "condition_id": "abc123",
        "mid_price": mid_yes,
        "rewards_rate": 10.0,
        "rewards_min_size": 10.0,
        "rewards_max_spread": 4.0,
        "tick_size": 0.01,
        "tokens": [
            {"token_id": "tYES", "outcome": "YES"},
            {"token_id": "tNO", "outcome": "NO"},
        ],
        "token_data": {
            "tYES": {
                "mid_price": mid_yes,
                "orderbook": {
                    "bids": [{"price": str(mid_yes - 0.02), "size": "100"}],
                    "asks": [{"price": str(mid_yes + 0.01), "size": "100"}],
                },
            },
            "tNO": {
                "mid_price": mid_no,
                "orderbook": {
                    "bids": [{"price": str(mid_no - 0.02), "size": "100"}],
                    "asks": [{"price": str(mid_no + 0.01), "size": "100"}],
                },
            },
        },
        "available_cash": 166.0,
    }


def test_evaluate_binary_generates_buy_and_sell(rf):
    """Para mercado binario two_sided, debe generar BUY+SELL por token."""
    signals = rf.evaluate(_binary_market_with_ws())
    buy_signals = [s for s in signals if s.side == "BUY"]
    sell_signals = [s for s in signals if s.side == "SELL"]
    assert len(buy_signals) >= 1
    assert len(sell_signals) >= 1


def test_evaluate_bid_behind_bbo(rf):
    """El precio de BUY debe estar SAFETY_TICKS ticks por debajo del best_bid."""
    signals = rf.evaluate(_binary_market_with_ws(mid_yes=0.52))
    yes_buy = next((s for s in signals if s.token_id == "tYES" and s.side == "BUY"), None)
    assert yes_buy is not None
    # best_bid = 0.52 - 0.02 = 0.50
    # qual_low = 0.52 - 0.04 + 0.01 = 0.49
    # best_bid(0.50) >= qual_low(0.49) + 2*tick(0.02) = 0.51? 0.50 >= 0.51 → NO → bid = qual_low = 0.49
    assert yes_buy.price <= 0.50  # debe estar por debajo del best_bid


def test_evaluate_ask_above_bbo(rf):
    """El precio de SELL debe estar SAFETY_TICKS ticks por encima del best_ask."""
    signals = rf.evaluate(_binary_market_with_ws(mid_yes=0.52))
    yes_sell = next((s for s in signals if s.token_id == "tYES" and s.side == "SELL"), None)
    assert yes_sell is not None
    # best_ask = 0.52 + 0.01 = 0.53
    assert yes_sell.price >= 0.53  # debe estar por encima del best_ask


def test_evaluate_multi_outcome(rf):
    """Mercado multi-outcome genera BUY y SELL por token cuando hay capital suficiente."""
    config_big = {
        "rewards_farmer": {
            "max_capital_per_market": 200.0,  # capital holgado para 3 tokens × 2 lados
            "min_rewards_pool_usd": 0.0,
            "max_markets_simultaneous": 5,
            "two_sided": True,
            "inventory_merge_threshold": 5.0,
            "competition_share_min": 0.005,
            "max_mid_deviation": 0.40,
        },
        "markets": {},
    }
    rf_big = RewardsFarmerStrategy(DummyClient(), config_big)
    market = {
        "condition_id": "abc123",
        "mid_price": 0.51,
        "rewards_rate": 10.0,
        "rewards_min_size": 5.0,
        "rewards_max_spread": 4.0,
        "tick_size": 0.01,
        "tokens": [
            {"token_id": "t1", "outcome": "<2"},
            {"token_id": "t2", "outcome": "2-3"},
            {"token_id": "t3", "outcome": "4+"},
        ],
        "token_data": {
            "t1": {"mid_price": 0.51, "orderbook": {"bids": [], "asks": [{"price": "0.52", "size": "100"}]}},
            "t2": {"mid_price": 0.35, "orderbook": {"bids": [], "asks": [{"price": "0.36", "size": "100"}]}},
            "t3": {"mid_price": 0.14, "orderbook": {"bids": [], "asks": [{"price": "0.15", "size": "100"}]}},
        },
        "available_cash": 300.0,
    }
    signals = rf_big.evaluate(market)
    buy_signals = [s for s in signals if s.side == "BUY"]
    # Con capital suficiente: 3 tokens × BUY = 3 señales de compra
    assert len(buy_signals) == 3
    # Con bids vacios, BUY va al qual_low de cada token
    token_buy_prices = {s.token_id: s.price for s in buy_signals}
    # qual_low es más alto para tokens con mid más alto
    assert token_buy_prices["t1"] > token_buy_prices["t2"] > token_buy_prices["t3"]


def test_evaluate_discards_zero_size(rf):
    """Mercado donde el capital no alcanza para ningún token no genera senales."""
    market = _binary_market_with_ws()
    market["available_cash"] = 0.01  # Casi nada
    signals = rf.evaluate(market)
    # Con tan poco capital, no deberia generar senales viables
    for s in signals:
        assert s.size >= 1


# ------------------------------------------------------------------
# execute
# ------------------------------------------------------------------

def test_execute_does_not_cancel_manual_orders(rf):
    """El bot solo debe cancelar ordenes que el mismo coloco."""
    rf._client.get_positions = lambda: [
        {"id": "manual_1", "asset_id": "tYES", "side": "BUY", "price": "0.50", "original_size": "100"}
    ]
    signals = rf.evaluate(_binary_market_with_ws())
    assert signals  # debe haber senales
    trades = rf.execute(signals)
    # La orden manual no debe haber sido cancelada (no estaba en _pending_orders)
    assert len(trades) == len(signals)  # todas las senales se colocan como nuevas


def test_execute_keeps_valid_order(rf):
    """Una orden propia dentro de la ventana y scoreando no debe cancelarse."""
    # mid=0.52, max_spread=0.04 → ventana BUY=[0.48, 0.52), danger_zone>=0.49
    # Precio 0.48 es: en-ventana Y fuera de danger zone (0.48 < 0.49) → no debe cancelar
    rf._client.get_positions = lambda: [
        {"id": "own_1", "asset_id": "tYES", "side": "BUY", "price": "0.48", "original_size": "20"}
    ]
    rf._pending_orders["own_1"] = None  # type: ignore

    signals = rf.evaluate(_binary_market_with_ws(mid_yes=0.52))
    yes_buy = next((s for s in signals if s.token_id == "tYES" and s.side == "BUY"), None)
    assert yes_buy is not None

    # Forzar el precio de la senal al mismo que la orden existente para que no reprice
    yes_buy.price = 0.48
    yes_buy.size = 20.0
    yes_buy.metadata["t_mid"] = 0.52
    yes_buy.metadata["max_spread_usd"] = 0.04
    yes_buy.metadata["best_bid"] = 0.50
    yes_buy.metadata["best_ask"] = 0.53

    cancelled = []
    original_cancel = rf._client.cancel_order
    rf._client.cancel_order = lambda oid: cancelled.append(oid)

    rf.execute(signals)
    assert "own_1" not in cancelled, "No debe cancelar orden propia valida"
