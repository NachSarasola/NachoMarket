"""Tests para MarketFilter — banlist, deduplicacion, y news-risk."""

import time
from datetime import datetime, timezone, timedelta

import pytest

from src.polymarket.market_filter import (
    MarketFilter,
    _tokenize,
    _jaccard_similarity,
)


# ------------------------------------------------------------------
# Fixtures
# ------------------------------------------------------------------

def make_config(**overrides) -> dict:
    base = {
        "banned_markets": {
            "condition_ids": [],
            "question_patterns": [],
        },
        "filters": {
            "min_market_age_hours": 48,
        },
    }
    base.update(overrides)
    return base


def make_market(
    condition_id: str = "cond_1",
    question: str = "Will BTC reach 100k?",
    rewards_rate: float = 0.0,
    end_days: int = 30,
    created_hours_ago: float = 100,
    score: float = 0.5,
) -> dict:
    end_date = (datetime.now(timezone.utc) + timedelta(days=end_days)).isoformat()
    created = (datetime.now(timezone.utc) - timedelta(hours=created_hours_ago)).isoformat()
    return {
        "condition_id": condition_id,
        "question": question,
        "rewards_rate": rewards_rate,
        "end_date": end_date,
        "_score": score,
        "_raw": {"createdAt": created},
    }


# ------------------------------------------------------------------
# Tests: _tokenize y _jaccard_similarity
# ------------------------------------------------------------------

class TestTokenize:
    def test_basic_tokenization(self) -> None:
        tokens = _tokenize("Will Bitcoin reach 100k by 2025?")
        assert "bitcoin" in tokens
        assert "reach" in tokens
        assert "100k" in tokens
        assert "2025" in tokens
        # Stop words removed
        assert "will" not in tokens
        assert "by" not in tokens

    def test_empty_string(self) -> None:
        assert _tokenize("") == set()

    def test_case_insensitive(self) -> None:
        assert _tokenize("BTC") == _tokenize("btc")


class TestJaccardSimilarity:
    def test_identical_sets(self) -> None:
        assert _jaccard_similarity({"a", "b"}, {"a", "b"}) == 1.0

    def test_disjoint_sets(self) -> None:
        assert _jaccard_similarity({"a", "b"}, {"c", "d"}) == 0.0

    def test_partial_overlap(self) -> None:
        sim = _jaccard_similarity({"a", "b", "c"}, {"b", "c", "d"})
        assert abs(sim - 0.5) < 0.01  # 2/4 = 0.5

    def test_empty_sets(self) -> None:
        assert _jaccard_similarity(set(), set()) == 1.0

    def test_one_empty(self) -> None:
        assert _jaccard_similarity({"a"}, set()) == 0.0


# ------------------------------------------------------------------
# Tests: Banlist
# ------------------------------------------------------------------

class TestBanlist:
    def test_not_banned_by_default(self) -> None:
        mf = MarketFilter(make_config())
        assert not mf.is_banned(make_market())

    def test_banned_by_condition_id(self) -> None:
        config = make_config(banned_markets={
            "condition_ids": ["cond_1"],
            "question_patterns": [],
        })
        mf = MarketFilter(config)
        assert mf.is_banned(make_market(condition_id="cond_1"))
        assert not mf.is_banned(make_market(condition_id="cond_2"))

    def test_banned_by_pattern(self) -> None:
        config = make_config(banned_markets={
            "condition_ids": [],
            "question_patterns": ["trump tweet"],
        })
        mf = MarketFilter(config)
        assert mf.is_banned(make_market(question="How many Trump tweets today?"))
        assert not mf.is_banned(make_market(question="Will BTC reach 100k?"))

    def test_pattern_case_insensitive(self) -> None:
        config = make_config(banned_markets={
            "condition_ids": [],
            "question_patterns": ["TRUMP"],
        })
        mf = MarketFilter(config)
        assert mf.is_banned(make_market(question="trump wins election"))

    def test_remove_banned(self) -> None:
        config = make_config(banned_markets={
            "condition_ids": ["cond_ban"],
            "question_patterns": [],
        })
        mf = MarketFilter(config)
        markets = [
            make_market(condition_id="cond_ok"),
            make_market(condition_id="cond_ban"),
        ]
        result = mf.remove_banned(markets)
        assert len(result) == 1
        assert result[0]["condition_id"] == "cond_ok"

    def test_temporal_block(self) -> None:
        mf = MarketFilter(make_config())
        mf.block_market_until("cond_1", hours=1.0)
        assert mf.is_banned(make_market(condition_id="cond_1"))
        assert not mf.is_banned(make_market(condition_id="cond_2"))


# ------------------------------------------------------------------
# Tests: Deduplication
# ------------------------------------------------------------------

class TestDeduplication:
    def test_no_duplicates(self) -> None:
        mf = MarketFilter(make_config())
        markets = [
            make_market(condition_id="c1", question="Will BTC reach 100k?"),
            make_market(condition_id="c2", question="Will ETH flip BTC?"),
        ]
        result = mf.deduplicate(markets)
        assert len(result) == 2

    def test_detects_duplicates(self) -> None:
        mf = MarketFilter(make_config())
        markets = [
            make_market(condition_id="c1", question="How many tweets will Trump post today?", score=0.8),
            make_market(condition_id="c2", question="How many tweets will Trump post this week?", score=0.6),
        ]
        result = mf.deduplicate(markets)
        assert len(result) == 1
        # Keeps higher score
        assert result[0]["condition_id"] == "c1"

    def test_keeps_best_from_group(self) -> None:
        mf = MarketFilter(make_config())
        # Very similar questions that should group (>70% Jaccard)
        markets = [
            make_market(condition_id="c1", question="How many tweets Trump posts daily count", score=0.3),
            make_market(condition_id="c2", question="How many tweets Trump posts daily total", score=0.9),
        ]
        result = mf.deduplicate(markets)
        assert len(result) == 1
        assert result[0]["condition_id"] == "c2"

    def test_single_market(self) -> None:
        mf = MarketFilter(make_config())
        markets = [make_market()]
        assert mf.deduplicate(markets) == markets

    def test_empty_list(self) -> None:
        mf = MarketFilter(make_config())
        assert mf.deduplicate([]) == []

    def test_different_topics_not_grouped(self) -> None:
        mf = MarketFilter(make_config())
        markets = [
            make_market(condition_id="c1", question="Bitcoin price above 100000 dollars"),
            make_market(condition_id="c2", question="Ethereum merge successful upgrade"),
            make_market(condition_id="c3", question="Federal Reserve interest rate decision"),
        ]
        result = mf.deduplicate(markets)
        assert len(result) == 3


# ------------------------------------------------------------------
# Tests: News-risk filter
# ------------------------------------------------------------------

class TestNewsRisk:
    def test_old_market_not_news_dependent(self) -> None:
        mf = MarketFilter(make_config())
        market = make_market(created_hours_ago=200, end_days=30)
        assert not mf.is_news_dependent(market)

    def test_new_market_high_rewards_flagged(self) -> None:
        mf = MarketFilter(make_config())
        market = make_market(
            created_hours_ago=12,  # < 48h
            rewards_rate=800,       # > 500
            end_days=30,
        )
        assert mf.is_news_dependent(market)

    def test_new_market_low_rewards_not_flagged(self) -> None:
        mf = MarketFilter(make_config())
        market = make_market(created_hours_ago=12, rewards_rate=100, end_days=30)
        assert not mf.is_news_dependent(market)

    def test_close_resolution_with_time_keyword_flagged(self) -> None:
        mf = MarketFilter(make_config())
        market = make_market(
            question="Will something happen today?",
            end_days=1,  # < 48h
            created_hours_ago=200,
        )
        assert mf.is_news_dependent(market)

    def test_close_resolution_without_keyword_not_flagged(self) -> None:
        mf = MarketFilter(make_config())
        market = make_market(
            question="Will something happen eventually?",
            end_days=1,
            created_hours_ago=200,
        )
        assert not mf.is_news_dependent(market)

    def test_remove_news_dependent(self) -> None:
        mf = MarketFilter(make_config())
        markets = [
            make_market(condition_id="safe", created_hours_ago=200, end_days=30),
            make_market(condition_id="risky", created_hours_ago=12, rewards_rate=800),
        ]
        result = mf.remove_news_dependent(markets)
        assert len(result) == 1
        assert result[0]["condition_id"] == "safe"


# ------------------------------------------------------------------
# Tests: apply_all pipeline
# ------------------------------------------------------------------

class TestApplyAll:
    def test_full_pipeline(self) -> None:
        config = make_config(banned_markets={
            "condition_ids": ["banned_1"],
            "question_patterns": ["forbidden"],
        })
        mf = MarketFilter(config)
        markets = [
            make_market(condition_id="banned_1", question="Good market"),
            make_market(condition_id="ok_1", question="Will BTC reach 100k?"),
            make_market(condition_id="ok_2", question="forbidden market topic"),
            make_market(condition_id="ok_3", question="Will ETH flip BTC?"),
            make_market(condition_id="news", created_hours_ago=10, rewards_rate=1000),
        ]
        result = mf.apply_all(markets)
        ids = [m["condition_id"] for m in result]
        assert "banned_1" not in ids
        assert "ok_2" not in ids  # banned by pattern
        assert "news" not in ids  # news-risk
        assert "ok_1" in ids
        assert "ok_3" in ids
