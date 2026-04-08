import pytest

from src.polymarket.client import PolymarketClient
from src.risk.circuit_breaker import CircuitBreaker
from src.strategy.market_maker import MarketMakerStrategy
from src.strategy.multi_arb import MultiArbStrategy
from src.strategy.directional import DirectionalStrategy


@pytest.fixture
def client() -> PolymarketClient:
    return PolymarketClient(paper_mode=True)


@pytest.fixture
def circuit_breaker() -> CircuitBreaker:
    return CircuitBreaker({
        "circuit_breakers": {
            "max_daily_loss_usdc": 20.0,
            "max_consecutive_losses": 5,
            "max_single_trade_loss_usdc": 10.0,
            "cooldown_after_break_min": 60,
            "max_open_orders": 20,
        }
    })


@pytest.fixture
def risk_config() -> dict:
    return {
        "market_making": {
            "default_spread_bps": 200,
            "min_spread_bps": 100,
            "order_size_usdc": 5.0,
            "num_levels": 3,
            "level_spacing_bps": 50,
        },
        "min_arb_edge_pct": 2.0,
        "arb_order_size_usdc": 5.0,
        "directional_min_edge_pct": 5.0,
        "directional_order_size_usdc": 5.0,
    }


class TestMarketMaker:
    def test_should_enter_sufficient_spread(self, client, circuit_breaker, risk_config) -> None:
        strategy = MarketMakerStrategy(client, circuit_breaker, risk_config)
        assert strategy.should_enter({"spread_bps": 200}) is True

    def test_should_not_enter_tight_spread(self, client, circuit_breaker, risk_config) -> None:
        strategy = MarketMakerStrategy(client, circuit_breaker, risk_config)
        assert strategy.should_enter({"spread_bps": 50}) is False

    def test_evaluate_generates_orders(self, client, circuit_breaker, risk_config) -> None:
        strategy = MarketMakerStrategy(client, circuit_breaker, risk_config)
        market = {"token_id": "test_123", "mid_price": 0.5, "spread_bps": 200}
        orders = strategy.evaluate(market)
        assert len(orders) > 0
        assert all(o["post_only"] is True for o in orders)

    def test_evaluate_no_orders_invalid_price(self, client, circuit_breaker, risk_config) -> None:
        strategy = MarketMakerStrategy(client, circuit_breaker, risk_config)
        orders = strategy.evaluate({"token_id": "test", "mid_price": 0.0})
        assert orders == []


class TestMultiArb:
    def test_should_enter_with_edge(self, client, circuit_breaker, risk_config) -> None:
        strategy = MultiArbStrategy(client, circuit_breaker, risk_config)
        market = {
            "tokens": [
                {"token_id": "a", "price": 0.55},
                {"token_id": "b", "price": 0.55},
            ]
        }
        assert strategy.should_enter(market) is True  # Sum = 1.10, edge = 10%

    def test_should_not_enter_no_edge(self, client, circuit_breaker, risk_config) -> None:
        strategy = MultiArbStrategy(client, circuit_breaker, risk_config)
        market = {
            "tokens": [
                {"token_id": "a", "price": 0.50},
                {"token_id": "b", "price": 0.50},
            ]
        }
        assert strategy.should_enter(market) is False  # Sum = 1.0, no edge

    def test_should_not_enter_single_outcome(self, client, circuit_breaker, risk_config) -> None:
        strategy = MultiArbStrategy(client, circuit_breaker, risk_config)
        assert strategy.should_enter({"tokens": [{"token_id": "a"}]}) is False


class TestDirectional:
    def test_should_enter_strong_signal(self, client, circuit_breaker, risk_config) -> None:
        strategy = DirectionalStrategy(client, circuit_breaker, risk_config)
        market = {"mid_price": 0.6, "avg_price_24h": 0.5, "volume_ratio": 1.5}
        assert strategy.should_enter(market) is True  # 20% deviation

    def test_should_not_enter_weak_signal(self, client, circuit_breaker, risk_config) -> None:
        strategy = DirectionalStrategy(client, circuit_breaker, risk_config)
        market = {"mid_price": 0.51, "avg_price_24h": 0.50, "volume_ratio": 1.0}
        assert strategy.should_enter(market) is False  # 2% deviation

    def test_evaluate_buy_signal(self, client, circuit_breaker, risk_config) -> None:
        strategy = DirectionalStrategy(client, circuit_breaker, risk_config)
        # Precio subio mucho -> senial de reversion bajista -> SELL
        market = {
            "token_id": "test",
            "mid_price": 0.7,
            "avg_price_24h": 0.5,
            "volume_ratio": 1.0,
        }
        orders = strategy.evaluate(market)
        assert len(orders) == 1
        assert orders[0]["side"] == "SELL"  # Reversion a la media
