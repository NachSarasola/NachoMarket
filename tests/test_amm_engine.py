"""Tests para AMMEngine — motor de liquidez concentrada."""

import math

import pytest

from src.strategy.amm_engine import (
    AMMConfig,
    AMMEngine,
    SinglePoolAMM,
    _buy_size_from_liquidity,
    _estimate_buy_sizes,
    _estimate_sell_sizes,
    _liquidity_from_collateral,
    _liquidity_from_tokens,
    _sell_size_from_liquidity,
)


class TestLiquidityFormulas:
    """Tests unitarios de formulas matematicas."""

    def test_liquidity_from_tokens_positive(self) -> None:
        L = _liquidity_from_tokens(x=100.0, P_i=0.50, P_u=0.60)
        assert L > 0

    def test_liquidity_from_tokens_zero_tokens(self) -> None:
        assert _liquidity_from_tokens(0.0, 0.50, 0.60) == 0.0

    def test_liquidity_from_tokens_invalid_range(self) -> None:
        assert _liquidity_from_tokens(100.0, 0.60, 0.50) == 0.0

    def test_liquidity_from_collateral_positive(self) -> None:
        L = _liquidity_from_collateral(y=100.0, P_l=0.40, P_i=0.50)
        assert L > 0

    def test_liquidity_from_collateral_zero(self) -> None:
        assert _liquidity_from_collateral(0.0, 0.40, 0.50) == 0.0

    def test_sell_size_from_liquidity_increasing(self) -> None:
        L = _liquidity_from_tokens(100.0, 0.50, 0.60)
        s1 = _sell_size_from_liquidity(L, 0.50, 0.60, 0.55)
        s2 = _sell_size_from_liquidity(L, 0.50, 0.60, 0.58)
        assert s2 > s1  # More tokens needed at higher price

    def test_sell_size_no_movement(self) -> None:
        L = _liquidity_from_tokens(100.0, 0.50, 0.60)
        s = _sell_size_from_liquidity(L, 0.50, 0.60, 0.50)
        assert s == 0.0

    def test_buy_size_from_liquidity_increasing(self) -> None:
        L = _liquidity_from_collateral(100.0, 0.40, 0.50)
        s1 = _buy_size_from_liquidity(L, 0.40, 0.50, 0.45)
        s2 = _buy_size_from_liquidity(L, 0.40, 0.50, 0.42)
        assert s2 > s1

    def test_buy_size_no_movement(self) -> None:
        L = _liquidity_from_collateral(100.0, 0.40, 0.50)
        s = _buy_size_from_liquidity(L, 0.40, 0.50, 0.50)
        assert s == 0.0


class TestEstimateSizes:
    """Tests de tamanios incrementales."""

    def test_sell_sizes_sum_to_total(self) -> None:
        x = 100.0
        L = _liquidity_from_tokens(x, 0.50, 0.60)
        prices = [0.52, 0.54, 0.56, 0.58]
        sizes = _estimate_sell_sizes(L, 0.50, 0.60, prices)
        total = sum(sizes)
        # Total should be <= x (all tokens used across the range)
        assert total <= x + 1e-6

    def test_buy_sizes_sum_to_total(self) -> None:
        y = 100.0
        L = _liquidity_from_collateral(y, 0.40, 0.50)
        prices = [0.48, 0.46, 0.44, 0.42]
        sizes = _estimate_buy_sizes(L, 0.40, 0.50, prices)
        total = sum(sizes)
        assert total <= y + 1e-6

    def test_empty_prices(self) -> None:
        L = _liquidity_from_tokens(100.0, 0.50, 0.60)
        assert _estimate_sell_sizes(L, 0.50, 0.60, []) == []


class TestSinglePoolAMM:
    """Tests de un pool individual."""

    def test_sell_prices(self) -> None:
        cfg = AMMConfig(spread=0.01, delta=0.01, depth=0.05)
        pool = SinglePoolAMM(cfg, 0.50)
        prices = pool.get_sell_prices()
        assert len(prices) >= 3
        assert prices[0] == pytest.approx(0.51)
        assert all(prices[i] < prices[i + 1] for i in range(len(prices) - 1))

    def test_buy_prices(self) -> None:
        cfg = AMMConfig(spread=0.01, delta=0.01, depth=0.05)
        pool = SinglePoolAMM(cfg, 0.50)
        prices = pool.get_buy_prices()
        assert len(prices) >= 3
        assert prices[0] == pytest.approx(0.49)
        assert all(prices[i] > prices[i + 1] for i in range(len(prices) - 1))

    def test_set_price_updates_ranges(self) -> None:
        cfg = AMMConfig(depth=0.10)
        pool = SinglePoolAMM(cfg, 0.50)
        pool.set_price(0.60)
        assert pool.price == 0.60
        sell = pool.get_sell_prices()
        assert sell[0] > 0.60

    def test_sell_sizes_with_balance(self) -> None:
        cfg = AMMConfig(spread=0.02, delta=0.02, depth=0.10)
        pool = SinglePoolAMM(cfg, 0.50)
        sizes = pool.get_sell_sizes(200.0)
        assert len(sizes) > 0
        assert sizes[0] > 0

    def test_sell_sizes_zero_balance(self) -> None:
        cfg = AMMConfig()
        pool = SinglePoolAMM(cfg, 0.50)
        sizes = pool.get_sell_sizes(0.0)
        assert all(s == 0.0 for s in sizes)

    def test_buy_sizes_with_collateral(self) -> None:
        cfg = AMMConfig(spread=0.02, delta=0.02, depth=0.10)
        pool = SinglePoolAMM(cfg, 0.50)
        sizes = pool.get_buy_sizes(200.0)
        assert len(sizes) > 0
        assert sizes[0] > 0

    def test_buy_sizes_zero_collateral(self) -> None:
        cfg = AMMConfig()
        pool = SinglePoolAMM(cfg, 0.50)
        sizes = pool.get_buy_sizes(0.0)
        assert all(s == 0.0 for s in sizes)

    def test_phi_positive(self) -> None:
        cfg = AMMConfig(spread=0.01, delta=0.01, depth=0.10)
        pool = SinglePoolAMM(cfg, 0.50)
        assert pool.phi() > 0


class TestAMMEngine:
    """Tests del motor AMM completo."""

    def test_allocate_collateral_sum(self) -> None:
        engine = AMMEngine(AMMConfig())
        engine.set_price(0.50)
        ca, cb = engine.allocate_collateral(100.0)
        assert ca + cb == pytest.approx(100.0)

    def test_allocate_zero_collateral(self) -> None:
        engine = AMMEngine(AMMConfig())
        engine.set_price(0.50)
        ca, cb = engine.allocate_collateral(0.0)
        assert ca == 0.0 and cb == 0.0

    def test_get_orders_generates_all_sides(self) -> None:
        engine = AMMEngine(AMMConfig(
            spread=0.02, delta=0.02, depth=0.10, max_collateral=500.0,
            min_size=5.0,
        ))
        engine.set_price(0.50)
        orders = engine.get_orders(
            balance_token_a=500.0,
            balance_token_b=500.0,
            total_collateral=300.0,
            token_a_id="tok_a",
            token_b_id="tok_b",
        )
        sides = {o.side for o in orders}
        tokens = {o.token for o in orders}
        assert "BUY" in sides
        assert "SELL" in sides
        assert "tok_a" in tokens
        assert "tok_b" in tokens

    def test_get_orders_min_size_filter(self) -> None:
        engine = AMMEngine(AMMConfig(
            spread=0.02, delta=0.02, depth=0.10, max_collateral=100.0,
            min_size=999999.0,  # Filter everything
        ))
        engine.set_price(0.50)
        orders = engine.get_orders(
            balance_token_a=200.0,
            balance_token_b=200.0,
            total_collateral=50.0,
        )
        assert orders == []

    def test_set_price_respects_bounds(self) -> None:
        engine = AMMEngine(AMMConfig(p_min=0.10, p_max=0.90))
        engine.set_price(0.05)
        assert engine.pool_a.price == pytest.approx(0.10)
        engine.set_price(0.99)
        assert engine.pool_a.price == pytest.approx(0.90)

    def test_pool_b_price_is_complement(self) -> None:
        engine = AMMEngine(AMMConfig())
        engine.set_price(0.30)
        assert engine.pool_a.price == pytest.approx(0.30)
        assert engine.pool_b.price == pytest.approx(0.70)

    def test_config_from_dict(self) -> None:
        cfg = AMMConfig.from_dict({
            "p_min": 0.10, "p_max": 0.90, "spread": 0.02,
            "delta": 0.02, "depth": 0.08, "max_collateral": 50.0,
            "min_size": 20.0,
        })
        assert cfg.p_min == 0.10
        assert cfg.max_collateral == 50.0
        assert cfg.min_size == 20.0

    def test_no_collateral_no_buy_orders(self) -> None:
        engine = AMMEngine(AMMConfig(
            spread=0.02, delta=0.02, depth=0.10,
        ))
        engine.set_price(0.50)
        orders = engine.get_orders(
            balance_token_a=200.0,
            balance_token_b=200.0,
            total_collateral=0.0,
        )
        buy_orders = [o for o in orders if o.side == "BUY"]
        assert all(o.size == 0.0 for o in buy_orders) or not buy_orders
