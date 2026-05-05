"""AMM Engine — liquidez concentrada estilo Uniswap v3 sobre CLOB.

Emula una bonding curve de AMM colocando ordenes limit en el CLOB.
Cada orden representa un tramo de liquidez en un rango de precios.

Formulas (Uniswap v3 adaptadas a mercados binarios):

  L = x / (1/sqrt(P_i) - 1/sqrt(P_u))     # liquidity virtual (sell)
  L = y / (sqrt(P_i) - sqrt(P_l))         # liquidity virtual (buy)

Donde:
  - L: liquidez virtual constante en el rango [P_l, P_u]
  - P_i: precio actual (midpoint)
  - x: cantidad de tokens depositados (sell side)
  - y: cantidad de collateral depositado (buy side)

Dos pools virtuales:
  - PoolA: TokenA / Collateral @ precio P
  - PoolB: TokenB / Collateral @ precio 1-P

Buy orders:  P - spread, P - spread - delta, ..., P - depth
Sell orders: P + spread, P + spread + delta, ..., P + depth
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any


@dataclass
class AMMConfig:
    """Configuracion del motor AMM."""

    p_min: float = 0.05
    p_max: float = 0.95
    spread: float = 0.01
    delta: float = 0.01
    depth: float = 0.10
    max_collateral: float = 200.0
    min_size: float = 15.0

    @classmethod
    def from_dict(cls, cfg: dict[str, Any]) -> AMMConfig:
        return cls(
            p_min=float(cfg.get("p_min", 0.05)),
            p_max=float(cfg.get("p_max", 0.95)),
            spread=float(cfg.get("spread", 0.01)),
            delta=float(cfg.get("delta", 0.01)),
            depth=float(cfg.get("depth", 0.10)),
            max_collateral=float(cfg.get("max_collateral", 200.0)),
            min_size=float(cfg.get("min_size", 15.0)),
        )


@dataclass
class AMMOrder:
    """Orden generada por el motor AMM."""

    side: str          # "BUY" | "SELL"
    token: str         # "A" | "B"
    price: float       # 0.0 - 1.0
    size: float        # tokens


def _sell_size_from_liquidity(L: float, P_i: float, P_u: float, P_t: float) -> float:
    """Tokens necesarios para mover precio de P_i a P_t en la banda derecha."""
    if P_t <= P_i or P_u <= P_i or L <= 0:
        return 0.0
    P_t = min(P_t, P_u)
    return L * (1.0 / math.sqrt(P_i) - 1.0 / math.sqrt(P_t))


def _liquidity_from_tokens(x: float, P_i: float, P_u: float) -> float:
    """Liquidez virtual L dado x tokens en banda [P_i, P_u]."""
    if x <= 0 or P_i <= 0 or P_u <= P_i:
        return 0.0
    return x / (1.0 / math.sqrt(P_i) - 1.0 / math.sqrt(P_u))


def _liquidity_from_collateral(y: float, P_l: float, P_i: float) -> float:
    """Liquidez virtual L dado y collateral en banda [P_l, P_i]."""
    if y <= 0 or P_l <= 0 or P_i <= P_l:
        return 0.0
    return y / (math.sqrt(P_i) - math.sqrt(P_l))


def _buy_size_from_liquidity(L: float, P_l: float, P_i: float, P_t: float) -> float:
    """Collateral necesario para mover precio de P_i a P_t en la banda izquierda."""
    if P_t >= P_i or P_i <= P_l or L <= 0:
        return 0.0
    P_t = max(P_t, P_l)
    return L * (math.sqrt(P_i) - math.sqrt(P_t))


def _estimate_sell_sizes(
    L: float, P_i: float, P_u: float, prices: list[float]
) -> list[float]:
    """Tamanios incrementales de sell orders en la banda derecha.

    Args:
        L: liquidez virtual.
        P_i: precio inicial (midpoint).
        P_u: precio superior del rango.
        prices: lista ordenada de precios target (ascendente).

    Returns:
        Lista de tamanios, uno por precio. La suma es el total.
    """
    sizes: list[float] = []
    prev = P_i
    for P_t in prices:
        cum = _sell_size_from_liquidity(L, P_i, P_u, P_t)
        prev_cum = _sell_size_from_liquidity(L, P_i, P_u, prev)
        sizes.append(max(0.0, cum - prev_cum))
        prev = P_t
    return sizes


def _estimate_buy_sizes(
    L: float, P_l: float, P_i: float, prices: list[float]
) -> list[float]:
    """Tamanios incrementales de buy orders en la banda izquierda.

    Args:
        L: liquidez virtual.
        P_l: precio inferior del rango.
        P_i: precio inicial (midpoint).
        prices: lista ordenada de precios target (descendente).

    Returns:
        Lista de tamanios en collateral, uno por precio.
    """
    sizes: list[float] = []
    prev = P_i
    for P_t in prices:
        cum = _buy_size_from_liquidity(L, P_l, P_i, P_t)
        prev_cum = _buy_size_from_liquidity(L, P_l, P_i, prev)
        sizes.append(max(0.0, cum - prev_cum))
        prev = P_t
    return sizes


class SinglePoolAMM:
    """Un pool AMM para un token (A o B).

    Mantiene dos bandas de liquidez concentrada:
      - Left [P - delta, P]: compras (collateral → tokens)
      - Right [P, P + delta]: ventas (tokens → collateral)
    """

    def __init__(self, config: AMMConfig, price: float = 0.50) -> None:
        self._cfg = config
        self._price = price
        self._P_l = max(config.p_min, price - config.depth)
        self._P_u = min(config.p_max, price + config.depth)

    @property
    def price(self) -> float:
        return self._price

    def set_price(self, new_price: float) -> None:
        """Actualiza precio y recalcula rangos."""
        self._price = max(self._cfg.p_min, min(self._cfg.p_max, new_price))
        self._P_l = max(self._cfg.p_min, self._price - self._cfg.depth)
        self._P_u = min(self._cfg.p_max, self._price + self._cfg.depth)

    def get_sell_prices(self) -> list[float]:
        """Precios de venta: P + spread, P + spread + delta, ..., P + depth."""
        prices: list[float] = []
        p = self._price + self._cfg.spread
        while p <= self._price + self._cfg.depth and p < self._cfg.p_max:
            prices.append(round(p, 4))
            p += self._cfg.delta
        return prices

    def get_buy_prices(self) -> list[float]:
        """Precios de compra: P - spread, P - spread - delta, ..., P - depth."""
        prices: list[float] = []
        p = self._price - self._cfg.spread
        while p >= self._price - self._cfg.depth and p > self._cfg.p_min:
            prices.append(round(p, 4))
            p -= self._cfg.delta
        return prices

    def get_sell_sizes(self, token_balance: float) -> list[float]:
        """Tamanios de ordenes de venta basados en balance real."""
        if token_balance <= 0:
            return [0.0] * len(self.get_sell_prices())
        L = _liquidity_from_tokens(token_balance, self._price, self._P_u)
        if L <= 0:
            return [0.0] * len(self.get_sell_prices())
        return _estimate_sell_sizes(L, self._price, self._P_u, self.get_sell_prices())

    def get_buy_sizes(self, collateral: float) -> list[float]:
        """Tamanios de ordenes de compra basados en collateral disponible."""
        if collateral <= 0:
            return [0.0] * len(self.get_buy_prices())
        L = _liquidity_from_collateral(collateral, self._P_l, self._price)
        if L <= 0:
            return [0.0] * len(self.get_buy_prices())
        return _estimate_buy_sizes(L, self._P_l, self._price, self.get_buy_prices())

    def phi(self) -> float:
        """Factor de asignacion de collateral para balance entre pools."""
        buy_prices = self.get_buy_prices()
        if not buy_prices or self._price <= 0 or self._P_l <= 0:
            return 0.0
        first_buy = buy_prices[0]
        denom = math.sqrt(self._price) - math.sqrt(self._P_l)
        if denom <= 0:
            return 0.0
        numer = 1.0 / math.sqrt(first_buy) - 1.0 / math.sqrt(self._price)
        if numer <= 0:
            return 0.0
        return numer / denom


class AMMEngine:
    """Motor AMM completo para un mercado binario.

    Dos pools: PoolA (TokenA) y PoolB (TokenB).
    Collateral compartido asignado via formula phi().
    """

    def __init__(self, config: AMMConfig) -> None:
        self._cfg = config
        self._pool_a = SinglePoolAMM(config, 0.50)
        self._pool_b = SinglePoolAMM(config, 0.50)

    def set_price(self, mid_price: float) -> None:
        """Actualiza precio en ambos pools."""
        self._pool_a.set_price(mid_price)
        self._pool_b.set_price(1.0 - mid_price)

    def allocate_collateral(
        self, total_collateral: float
    ) -> tuple[float, float]:
        """Asigna collateral entre PoolA y PoolB proporcionalmente."""
        phi_a = self._pool_a.phi()
        phi_b = self._pool_b.phi()
        if phi_a + phi_b <= 0:
            half = total_collateral / 2.0
            return half, half

        collat_a = (total_collateral * phi_b) / (phi_a + phi_b)
        collat_b = total_collateral - collat_a
        return max(0.0, collat_a), max(0.0, collat_b)

    def get_orders(
        self,
        balance_token_a: float = 0.0,
        balance_token_b: float = 0.0,
        total_collateral: float = 0.0,
        token_a_id: str = "A",
        token_b_id: str = "B",
    ) -> list[AMMOrder]:
        """Genera todas las ordenes esperadas para el estado actual.

        Returns:
            Lista de AMMOrder con side, token, price, size.
        """
        orders: list[AMMOrder] = []

        collat_a, collat_b = self.allocate_collateral(total_collateral)

        # Pool A — Sell (TokenA → Collateral)
        sell_prices_a = self._pool_a.get_sell_prices()
        sell_sizes_a = self._pool_a.get_sell_sizes(balance_token_a)
        for p, s in zip(sell_prices_a, sell_sizes_a):
            if s >= self._cfg.min_size:
                orders.append(AMMOrder("SELL", token_a_id, p, s))

        # Pool A — Buy (Collateral → TokenA)
        buy_prices_a = self._pool_a.get_buy_prices()
        buy_sizes_a = self._pool_a.get_buy_sizes(collat_a)
        for p, s in zip(buy_prices_a, buy_sizes_a):
            if s >= self._cfg.min_size:
                orders.append(AMMOrder("BUY", token_a_id, p, s))

        # Pool B — Sell (TokenB → Collateral)
        sell_prices_b = self._pool_b.get_sell_prices()
        sell_sizes_b = self._pool_b.get_sell_sizes(balance_token_b)
        for p, s in zip(sell_prices_b, sell_sizes_b):
            if s >= self._cfg.min_size:
                orders.append(AMMOrder("SELL", token_b_id, p, s))

        # Pool B — Buy (Collateral → TokenB)
        buy_prices_b = self._pool_b.get_buy_prices()
        buy_sizes_b = self._pool_b.get_buy_sizes(collat_b)
        for p, s in zip(buy_prices_b, buy_sizes_b):
            if s >= self._cfg.min_size:
                orders.append(AMMOrder("BUY", token_b_id, p, s))

        return orders

    @property
    def pool_a(self) -> SinglePoolAMM:
        return self._pool_a

    @property
    def pool_b(self) -> SinglePoolAMM:
        return self._pool_b

    @property
    def config(self) -> AMMConfig:
        return self._cfg
