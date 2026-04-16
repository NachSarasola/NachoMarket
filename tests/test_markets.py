"""Tests para MarketAnalyzer — sin llamadas reales a APIs externas."""

import math
import time
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch

import pytest

from src.polymarket.markets import MarketAnalyzer, _Cache, _safe_float


# ------------------------------------------------------------------
# Fixtures
# ------------------------------------------------------------------

def make_config(**overrides) -> dict:
    base = {
        "min_daily_volume_usd": 1000,
        "max_markets_simultaneous": 12,
        "bot_order_size": 15.0,
        "filters": {
            "min_liquidity_usd": 5000,
            "max_spread_pct": 5.0,
            "min_time_to_resolution_hours": 168,  # 7 dias
            "excluded_categories": [],
        },
        "competition": {
            "max_book_depth_per_side": 500.0,
            "min_participation_share": 0.05,
        },
        "preferred_categories": ["crypto", "politics"],
    }
    base.update(overrides)
    return base


def make_client() -> MagicMock:
    client = MagicMock()
    client.paper_mode = True
    client.get_orderbook.return_value = {
        "bids": [{"price": "0.45", "size": "100"}],
        "asks": [{"price": "0.55", "size": "100"}],
    }
    client.get_midpoint.return_value = 0.5
    return client


def make_gamma_market(
    condition_id: str = "cond_123",
    question: str = "Will BTC reach 100k?",
    volume: float = 50000.0,
    end_days_from_now: int = 30,
    category: str = "crypto",
    tokens: list | None = None,
    accepting_orders: bool = True,
) -> dict:
    end_date = (datetime.now(timezone.utc) + timedelta(days=end_days_from_now)).isoformat()
    if tokens is None:
        tokens = [
            {"token_id": f"tok_yes_{condition_id}", "outcome": "Yes", "price": 0.55},
            {"token_id": f"tok_no_{condition_id}", "outcome": "No", "price": 0.45},
        ]
    return {
        "conditionId": condition_id,
        "question": question,
        "category": category,
        "volume24hr": volume,
        "liquidity": volume * 0.5,
        "endDate": end_date,
        "acceptingOrders": accepting_orders,
        "tokens": tokens,
    }


# ------------------------------------------------------------------
# Tests: _Cache
# ------------------------------------------------------------------

class TestCache:
    def test_set_and_get(self) -> None:
        cache = _Cache(ttl_sec=60)
        cache.set("key", [1, 2, 3])
        assert cache.get("key") == [1, 2, 3]

    def test_returns_none_for_missing(self) -> None:
        cache = _Cache()
        assert cache.get("nonexistent") is None

    def test_expires_after_ttl(self) -> None:
        cache = _Cache(ttl_sec=0.01)
        cache.set("key", "value")
        time.sleep(0.02)
        assert cache.get("key") is None

    def test_invalidate_single_key(self) -> None:
        cache = _Cache()
        cache.set("a", 1)
        cache.set("b", 2)
        cache.invalidate("a")
        assert cache.get("a") is None
        assert cache.get("b") == 2

    def test_invalidate_all(self) -> None:
        cache = _Cache()
        cache.set("a", 1)
        cache.set("b", 2)
        cache.invalidate()
        assert cache.get("a") is None
        assert cache.get("b") is None


class TestSafeFloat:
    def test_normal_float(self) -> None:
        assert _safe_float(3.14) == 3.14

    def test_string_float(self) -> None:
        assert _safe_float("42.5") == 42.5

    def test_none_returns_zero(self) -> None:
        assert _safe_float(None) == 0.0

    def test_invalid_string_returns_zero(self) -> None:
        assert _safe_float("not_a_number") == 0.0

    def test_int(self) -> None:
        assert _safe_float(100) == 100.0


# ------------------------------------------------------------------
# Tests: discover_markets
# ------------------------------------------------------------------

class TestDiscoverMarkets:
    @patch("src.polymarket.markets.requests.get")
    def test_fetches_from_gamma_api(self, mock_get) -> None:
        mock_get.return_value = MagicMock(
            status_code=200,
            json=MagicMock(return_value=[make_gamma_market()]),
            raise_for_status=MagicMock(),
        )

        analyzer = MarketAnalyzer(make_client(), make_config())
        markets = analyzer.discover_markets()

        assert len(markets) == 1
        assert markets[0]["question"] == "Will BTC reach 100k?"
        assert markets[0]["volume_24h"] == 50000.0
        mock_get.assert_called()

    @patch("src.polymarket.markets.requests.get")
    def test_filters_low_volume(self, mock_get) -> None:
        mock_get.return_value = MagicMock(
            status_code=200,
            json=MagicMock(return_value=[
                make_gamma_market(condition_id="high", volume=50000),
                make_gamma_market(condition_id="low", volume=500),  # Below 1000 min
            ]),
            raise_for_status=MagicMock(),
        )

        analyzer = MarketAnalyzer(make_client(), make_config())
        markets = analyzer.discover_markets()
        assert len(markets) == 1
        assert markets[0]["condition_id"] == "high"

    @patch("src.polymarket.markets.requests.get")
    def test_filters_not_accepting_orders(self, mock_get) -> None:
        mock_get.return_value = MagicMock(
            status_code=200,
            json=MagicMock(return_value=[
                make_gamma_market(accepting_orders=False),
            ]),
            raise_for_status=MagicMock(),
        )

        analyzer = MarketAnalyzer(make_client(), make_config())
        assert analyzer.discover_markets() == []

    @patch("src.polymarket.markets.requests.get")
    def test_filters_close_to_resolution(self, mock_get) -> None:
        mock_get.return_value = MagicMock(
            status_code=200,
            json=MagicMock(return_value=[
                make_gamma_market(end_days_from_now=2),  # Solo 2 dias, minimo es 7
            ]),
            raise_for_status=MagicMock(),
        )

        analyzer = MarketAnalyzer(make_client(), make_config())
        assert analyzer.discover_markets() == []

    @patch("src.polymarket.markets.requests.get")
    def test_filters_excluded_categories(self, mock_get) -> None:
        config = make_config()
        config["filters"]["excluded_categories"] = ["politics"]

        mock_get.return_value = MagicMock(
            status_code=200,
            json=MagicMock(return_value=[
                make_gamma_market(condition_id="c", category="crypto"),
                make_gamma_market(condition_id="p", category="politics"),
            ]),
            raise_for_status=MagicMock(),
        )

        analyzer = MarketAnalyzer(make_client(), config)
        markets = analyzer.discover_markets()
        assert len(markets) == 1
        assert markets[0]["category"] == "crypto"

    @patch("src.polymarket.markets.requests.get")
    def test_uses_cache_on_second_call(self, mock_get) -> None:
        mock_get.return_value = MagicMock(
            status_code=200,
            json=MagicMock(return_value=[make_gamma_market()]),
            raise_for_status=MagicMock(),
        )

        analyzer = MarketAnalyzer(make_client(), make_config())
        analyzer.discover_markets()
        analyzer.discover_markets()

        # Solo una llamada a la API (la segunda usa cache)
        assert mock_get.call_count == 1

    @patch("src.polymarket.markets.requests.get")
    def test_paginates_correctly(self, mock_get) -> None:
        """Verifica paginacion: dos lotes, segundo incompleto."""
        batch_1 = [make_gamma_market(condition_id=f"m{i}") for i in range(100)]
        batch_2 = [make_gamma_market(condition_id=f"m{100 + i}") for i in range(30)]

        call_count = [0]

        def side_effect(*args, **kwargs):
            resp = MagicMock()
            resp.raise_for_status = MagicMock()
            if call_count[0] == 0:
                resp.json.return_value = batch_1
            else:
                resp.json.return_value = batch_2
            call_count[0] += 1
            return resp

        mock_get.side_effect = side_effect

        analyzer = MarketAnalyzer(make_client(), make_config())
        markets = analyzer.discover_markets()
        assert len(markets) == 130
        assert mock_get.call_count == 2


# ------------------------------------------------------------------
# Tests: get_reward_markets
# ------------------------------------------------------------------

class TestRewardMarkets:
    @patch("src.polymarket.markets.requests.get")
    def test_returns_reward_map(self, mock_get) -> None:
        mock_get.return_value = MagicMock(
            status_code=200,
            json=MagicMock(return_value=[
                {"conditionId": "cond_1", "rewardsDailyRate": 150.0, "minSize": 25, "maxSpread": 0.04},
                {"conditionId": "cond_2", "rewardsDailyRate": 80.0, "minSize": 10, "maxSpread": 0.05},
            ]),
            raise_for_status=MagicMock(),
        )

        analyzer = MarketAnalyzer(make_client(), make_config())
        rewards = analyzer.get_reward_markets()

        assert len(rewards) == 2
        assert rewards["cond_1"]["rewards_daily_rate"] == 150.0
        assert rewards["cond_2"]["min_size"] == 10.0

    @patch("src.polymarket.markets.requests.get")
    def test_enriches_markets(self, mock_get) -> None:
        mock_get.return_value = MagicMock(
            status_code=200,
            json=MagicMock(return_value=[
                {"conditionId": "cond_123", "rewardsDailyRate": 200.0, "minSize": 10, "maxSpread": 0.03},
            ]),
            raise_for_status=MagicMock(),
        )

        analyzer = MarketAnalyzer(make_client(), make_config())
        markets = [{"condition_id": "cond_123", "rewards_active": False, "rewards_rate": 0.0}]
        enriched = analyzer.enrich_with_rewards(markets)

        assert enriched[0]["rewards_active"] is True
        assert enriched[0]["rewards_rate"] == 200.0


# ------------------------------------------------------------------
# Tests: score_market
# ------------------------------------------------------------------

class TestScoreMarket:
    def setup_method(self) -> None:
        self.client = make_client()
        self.analyzer = MarketAnalyzer(self.client, make_config())

    def test_score_between_zero_and_one(self) -> None:
        market = {
            "tokens": [{"token_id": "tok1", "price": 0.5}],
            "volume_24h": 50000,
            "rewards_rate": 100,
            "end_date": (datetime.now(timezone.utc) + timedelta(days=30)).isoformat(),
        }
        score = self.analyzer.score_market(market)
        assert 0.0 <= score <= 1.0

    def test_low_competition_scores_higher(self) -> None:
        """Un mercado con menos profundidad de book debe puntuar mejor."""
        base = {
            "tokens": [{"token_id": "t1", "price": 0.5}],
            "rewards_rate": 100,
            "end_date": (datetime.now(timezone.utc) + timedelta(days=30)).isoformat(),
        }
        # Shallow book = low competition (bot gets higher share)
        self.client.get_orderbook.return_value = {
            "bids": [{"price": "0.45", "size": "50"}],
            "asks": [{"price": "0.55", "size": "50"}],
        }
        low_comp = self.analyzer.score_market({**base, "volume_24h": 5000})

        # Deep book = high competition (bot gets tiny share)
        self.client.get_orderbook.return_value = {
            "bids": [{"price": "0.45", "size": "5000"}],
            "asks": [{"price": "0.55", "size": "5000"}],
        }
        # Invalidate cache so new book is used
        self.analyzer._cache.invalidate()
        high_comp = self.analyzer.score_market({**base, "volume_24h": 500000})

        assert low_comp > high_comp

    def test_market_with_rewards_scores_higher(self) -> None:
        base = {
            "tokens": [{"token_id": "t1", "price": 0.5}],
            "volume_24h": 50000,
            "end_date": (datetime.now(timezone.utc) + timedelta(days=30)).isoformat(),
        }
        no_rewards = {**base, "rewards_rate": 0}
        with_rewards = {**base, "rewards_rate": 300}
        assert self.analyzer.score_market(with_rewards) > self.analyzer.score_market(no_rewards)

    def test_extreme_price_penalizes_volatility(self) -> None:
        """Un mercado con precio cercano a 0 o 1 es mas riesgoso para MM."""
        base = {
            "tokens": [{"token_id": "t1"}],
            "volume_24h": 50000,
            "rewards_rate": 0,
            "end_date": (datetime.now(timezone.utc) + timedelta(days=30)).isoformat(),
        }
        mid_price = {**base, "tokens": [{"token_id": "t1", "price": 0.5}]}
        extreme_price = {**base, "tokens": [{"token_id": "t1", "price": 0.95}]}
        assert self.analyzer.score_market(mid_price) > self.analyzer.score_market(extreme_price)

    def test_no_tokens_gets_zero_scores_gracefully(self) -> None:
        market = {"tokens": [], "volume_24h": 50000, "rewards_rate": 0}
        score = self.analyzer.score_market(market)
        assert score >= 0.0  # No debe explotar

    def test_participation_share_stored_in_market(self) -> None:
        market = {
            "tokens": [{"token_id": "t1", "price": 0.5}],
            "volume_24h": 5000,
            "rewards_rate": 0,
            "end_date": (datetime.now(timezone.utc) + timedelta(days=30)).isoformat(),
        }
        self.analyzer.score_market(market)
        assert "_participation_share" in market
        assert 0.0 < market["_participation_share"] <= 1.0


class TestCompetitionEstimation:
    def setup_method(self) -> None:
        self.client = make_client()
        self.analyzer = MarketAnalyzer(self.client, make_config())

    def test_shallow_book_high_participation(self) -> None:
        """Con book poco profundo, el bot tiene alta participacion."""
        self.client.get_orderbook.return_value = {
            "bids": [{"price": "0.50", "size": "20"}],
            "asks": [{"price": "0.52", "size": "20"}],
        }
        market = {"tokens": [{"token_id": "t1"}]}
        share = self.analyzer._estimate_participation_share(market)
        # total_depth ~ $10+$10.4 = ~$20.4, bot_order_size = $15
        # share ~ 15 / (20.4 + 15) = ~0.42
        assert share > 0.30

    def test_deep_book_low_participation(self) -> None:
        """Con book profundo, el bot tiene baja participacion."""
        self.client.get_orderbook.return_value = {
            "bids": [{"price": "0.50", "size": "10000"}],
            "asks": [{"price": "0.52", "size": "10000"}],
        }
        market = {"tokens": [{"token_id": "t1"}]}
        share = self.analyzer._estimate_participation_share(market)
        # total_depth ~ $5000+$5200 = ~$10200, bot = $15
        # share ~ 15 / 10215 = ~0.0015
        assert share < 0.01

    def test_empty_book_returns_default(self) -> None:
        """Sin datos de book, retorna participacion por defecto."""
        self.client.get_orderbook.return_value = {"bids": [], "asks": []}
        market = {"tokens": [{"token_id": "t1"}]}
        share = self.analyzer._estimate_participation_share(market)
        assert share == 0.10

    def test_no_tokens_returns_default(self) -> None:
        market = {"tokens": []}
        share = self.analyzer._estimate_participation_share(market)
        assert share == 0.10

    def test_book_depth_cached(self) -> None:
        """La profundidad del book se cachea."""
        self.client.get_orderbook.return_value = {
            "bids": [{"price": "0.50", "size": "100"}],
            "asks": [{"price": "0.52", "size": "100"}],
        }
        market = {"tokens": [{"token_id": "t1"}]}
        self.analyzer._get_book_depth(market)
        self.analyzer._get_book_depth(market)
        # Solo una llamada gracias al cache
        assert self.client.get_orderbook.call_count == 1


# ------------------------------------------------------------------
# Tests: select_top_markets
# ------------------------------------------------------------------

class TestSelectTopMarkets:
    @patch("src.polymarket.markets.requests.get")
    def test_returns_n_markets(self, mock_get) -> None:
        questions = [
            "Will BTC reach 100k?", "Will ETH flip BTC?", "Will SOL reach 500?",
            "Will DOGE hit 1 dollar?", "Federal Reserve rate decision?",
            "Will gold reach 3000?", "Champions League winner?",
            "Next US president?", "SpaceX Mars landing?", "AI regulation bill?",
        ]
        gamma_markets = [
            make_gamma_market(
                condition_id=f"m{i}",
                volume=50000 + i * 10000,
                question=questions[i],
            )
            for i in range(10)
        ]

        call_count = [0]

        def side_effect(*args, **kwargs):
            resp = MagicMock()
            resp.raise_for_status = MagicMock()
            url = args[0] if args else kwargs.get("url", "")
            if "gamma-api" in url:
                if call_count[0] == 0:
                    resp.json.return_value = gamma_markets
                else:
                    resp.json.return_value = []
                call_count[0] += 1
            elif "rewards" in url:
                resp.json.return_value = []
            else:
                resp.json.return_value = gamma_markets if call_count[0] == 0 else []
                call_count[0] += 1
            return resp

        mock_get.side_effect = side_effect
        analyzer = MarketAnalyzer(make_client(), make_config())

        top_3 = analyzer.select_top_markets(n=3)
        assert len(top_3) == 3
        # Deben tener _score
        assert all("_score" in m for m in top_3)
        # Ordenados descendente
        scores = [m["_score"] for m in top_3]
        assert scores == sorted(scores, reverse=True)

    @patch("src.polymarket.markets.requests.get")
    def test_returns_empty_when_no_markets(self, mock_get) -> None:
        mock_get.return_value = MagicMock(
            status_code=200,
            json=MagicMock(return_value=[]),
            raise_for_status=MagicMock(),
        )
        analyzer = MarketAnalyzer(make_client(), make_config())
        assert analyzer.select_top_markets() == []

    @patch("src.polymarket.markets.requests.get")
    def test_scan_markets_alias(self, mock_get) -> None:
        mock_get.return_value = MagicMock(
            status_code=200,
            json=MagicMock(return_value=[make_gamma_market()]),
            raise_for_status=MagicMock(),
        )
        analyzer = MarketAnalyzer(make_client(), make_config())
        result = analyzer.scan_markets()
        assert isinstance(result, list)


class TestCacheControl:
    def test_invalidate_cache(self) -> None:
        analyzer = MarketAnalyzer(make_client(), make_config())
        analyzer._cache.set("test", "value")
        analyzer.invalidate_cache()
        assert analyzer._cache.get("test") is None
