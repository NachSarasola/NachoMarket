"""Tests para SafeCompounderStrategy — logica pura sin API."""

from unittest.mock import MagicMock

import pytest

from src.risk.edge_filter import EdgeFilter
from src.strategy.base import Signal, Trade
from src.strategy.category_scorer import CategoryScorer
from src.strategy.safe_compounder import SafeCompounderStrategy


def _make_token(token_id: str, price: float) -> dict:
    return {
        "token_id": token_id,
        "price": price,
        "outcome": "No" if price > 0.5 else "Yes",
    }


def _make_market_data(**overrides) -> dict:
    """Crea market_data simulado con YES barato y NO caro."""
    base: dict = {
        "condition_id": "0xabc123def456",
        "question": "Will something happen?",
        "category": "politics",
        "mid_price": 0.08,
        "tokens": [
            _make_token("yes_token_id", 0.08),
            _make_token("no_token_id", 0.92),
        ],
        "orderbook": {
            "yes_token_id": {
                "bids": [(0.07, 100.0)],
                "asks": [(0.09, 100.0)],
            },
            "no_token_id": {
                "bids": [(0.83, 100.0)],
                "asks": [(0.84, 100.0)],
            },
        },
        "available_cash": 100.0,
        "end_date": "2027-01-01T00:00:00Z",
    }
    base.update(overrides)
    return base


def _make_config(**overrides) -> dict:
    cfg: dict = {
        "safe_compounder": {
            "min_yes_price": 0.01,
            "max_yes_price": 0.20,
            "min_no_ask": 0.80,
            "min_edge": 0.03,
            "max_position_pct": 0.10,
            "kelly_fraction": 0.25,
            "min_order_usdc": 1.0,
            "skip_categories": ["sports", "entertainment", "awards"],
            "skip_keywords": ["mention"],
            "top_n_candidates": 200,
            "refresh_interval_sec": 300,
        }
    }
    cfg["safe_compounder"].update(overrides.get("safe_compounder", {}))
    return cfg


class TestSafeCompounder:
    """Tests de logica pura del Safe Compounder."""

    def _make_strategy(
        self, config_overrides: dict | None = None,
        category_scorer: CategoryScorer | None = None,
        edge_filter: EdgeFilter | None = None,
    ) -> SafeCompounderStrategy:
        client = MagicMock()
        config = _make_config()
        if config_overrides:
            deep = config_overrides.get("safe_compounder", {})
            config["safe_compounder"].update(deep)
        return SafeCompounderStrategy(
            client=client,
            config=config,
            category_scorer=category_scorer,
            edge_filter=edge_filter or EdgeFilter(),
        )

    # --- should_act ---

    def test_should_act_politics(self) -> None:
        st = self._make_strategy()
        assert st.should_act(_make_market_data(category="politics")) is True

    def test_should_act_blocked_category(self) -> None:
        cs = CategoryScorer()
        st = self._make_strategy(category_scorer=cs)
        # economics = 15 < 30 → blocked
        assert st.should_act(_make_market_data(category="economics")) is False

    def test_should_act_skip_category(self) -> None:
        st = self._make_strategy()
        assert st.should_act(_make_market_data(category="sports")) is False

    def test_should_act_skip_keyword(self) -> None:
        st = self._make_strategy()
        data = _make_market_data(question="Will they mention X?")
        assert st.should_act(data) is False

    # --- evaluate ---

    def test_evaluate_ideal_scenario(self) -> None:
        st = self._make_strategy()
        signals = st.evaluate(_make_market_data())
        assert len(signals) == 1
        sig = signals[0]
        assert sig.side == "BUY"
        assert sig.token_id == "no_token_id"
        assert sig.confidence > 0.50
        assert sig.metadata["estimated_prob"] > 0
        assert sig.metadata["edge"] > 0.03

    def test_evaluate_yes_too_expensive(self) -> None:
        st = self._make_strategy()
        data = _make_market_data(
            mid_price=0.30,
            tokens=[
                _make_token("yes_tok", 0.30),
                _make_token("no_tok", 0.70),
            ],
            orderbook={
                "yes_tok": {"bids": [(0.29, 100)], "asks": [(0.31, 100)]},
                "no_tok": {"bids": [(0.69, 100)], "asks": [(0.71, 100)]},
            },
        )
        assert st.evaluate(data) == []  # NO ask < 0.80

    def test_evaluate_no_ask_too_low(self) -> None:
        st = self._make_strategy()
        data = _make_market_data(
            orderbook={
                "yes_token_id": {"bids": [(0.20, 100)], "asks": [(0.22, 100)]},
                "no_token_id": {"bids": [(0.78, 100)], "asks": [(0.79, 100)]},
            },
        )
        assert st.evaluate(data) == []  # NO ask < 0.80

    def test_evaluate_single_token_market(self) -> None:
        st = self._make_strategy()
        data = _make_market_data(
            tokens=[_make_token("only_tok", 0.50)],
        )
        assert st.evaluate(data) == []

    def test_evaluate_edge_too_small(self) -> None:
        st = self._make_strategy()
        data = _make_market_data(
            mid_price=0.18,
            tokens=[
                _make_token("yes_tok", 0.18),
                _make_token("no_tok", 0.82),
            ],
            orderbook={
                "yes_tok": {"bids": [(0.17, 100)], "asks": [(0.19, 100)]},
                "no_tok": {"bids": [(0.81, 100)], "asks": [(0.83, 100)]},
            },
        )
        signals = st.evaluate(data)
        # edge = est_prob - 0.83. est_prob ≈ 1-0.18 = 0.82 (ajustado). edge ~ -0.01
        assert signals == []

    def test_evaluate_no_mid_price(self) -> None:
        st = self._make_strategy()
        data = _make_market_data(mid_price=0.0)
        assert st.evaluate(data) == []

    def test_evaluate_no_tokens(self) -> None:
        st = self._make_strategy()
        data = _make_market_data(tokens=[])
        assert st.evaluate(data) == []

    # --- Token detection ---

    def test_find_yes_token_lowest_price(self) -> None:
        tokens = [
            _make_token("a", 0.60),
            _make_token("b", 0.40),
        ]
        yes = SafeCompounderStrategy._find_yes_token(tokens)
        assert yes is not None
        assert yes["token_id"] == "b"

    def test_find_yes_token_single(self) -> None:
        tokens = [_make_token("only", 0.50)]
        yes = SafeCompounderStrategy._find_yes_token(tokens)
        assert yes is not None

    def test_find_yes_token_empty(self) -> None:
        assert SafeCompounderStrategy._find_yes_token([]) is None

    def test_find_no_token(self) -> None:
        tokens = [
            _make_token("yes", 0.30),
            _make_token("no", 0.70),
        ]
        yes = SafeCompounderStrategy._find_yes_token(tokens)
        no = SafeCompounderStrategy._find_no_token(tokens, yes)
        assert no is not None
        assert no["token_id"] == "no"

    # --- Signal correctness ---

    def test_evaluate_signal_has_metadata(self) -> None:
        st = self._make_strategy()
        signals = st.evaluate(_make_market_data())
        assert len(signals) == 1
        meta = signals[0].metadata
        assert "estimated_prob" in meta
        assert "edge" in meta
        assert "yes_price" in meta
        assert "no_ask" in meta

    def test_evaluate_buy_price_below_no_ask(self) -> None:
        st = self._make_strategy()
        signals = st.evaluate(_make_market_data())
        assert len(signals) == 1
        no_ask = signals[0].metadata["no_ask"]
        assert signals[0].price < no_ask

    def test_evaluate_price_not_below_1_cent(self) -> None:
        st = self._make_strategy()
        signals = st.evaluate(_make_market_data())
        assert len(signals) == 1
        assert signals[0].price >= 0.01

    def test_evaluate_zero_size_if_no_cash(self) -> None:
        st = self._make_strategy()
        data = _make_market_data(available_cash=0.0)
        signals = st.evaluate(data)
        assert signals == []  # size <= 0 → sin senal
