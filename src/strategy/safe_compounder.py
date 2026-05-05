"""Safe Compounder — estrategia matematica NO-side sin IA.

Apuesta solo al lado NO en mercados donde YES esta barato
y el NO tiene alta probabilidad de resolverse a favor.

Condiciones de entrada:
  - YES last_price en [min_yes, max_yes] (default [0.01, 0.20])
  - NO best_ask >= min_no_ask (default 0.80)
  - Edge = |estimated_true_prob - NO_ask| > min_edge (default 0.03)
  - max P% del capital por posicion (default 10%)
  - Post Only para fees cero
  - Excluye sports, entertainment, awards
  - Excluye titulos con keywords prohibidas

Estrategia de salida:
  - Take-profit: vender NO si bid >= 0.95
  - Stop-loss: vender NO si ask <= 0.70
  - Limpiar tracking si el mercado resuelve

Kelly fraccional (Half-Kelly) con edge real (corregido v2).
Market discovery propio via Gamma API (no depende de rewards).
"""

from __future__ import annotations

import logging
import math
import re
import time
from datetime import date, datetime, timezone
from typing import TYPE_CHECKING, Any

import requests

from src.risk.position_sizer import kelly_fraction as _kelly_fraction
from src.strategy.base import BaseStrategy, Signal, Trade

if TYPE_CHECKING:
    from src.polymarket.client import PolymarketClient
    from src.risk.circuit_breaker import CircuitBreaker
    from src.risk.edge_filter import EdgeFilter
    from src.risk.inventory import InventoryManager
    from src.risk.position_sizer import PositionSizer
    from src.polymarket.market_filter import MarketFilter
    from src.strategy.category_scorer import CategoryScorer

logger = logging.getLogger("nachomarket.strategy.safe_compounder")

GAMMA_API_URL = "https://gamma-api.polymarket.com/events"


class SafeCompounderStrategy(BaseStrategy):
    """Apuesta matematica al NO, sin depender de IA, con market discovery propio."""

    def __init__(
        self,
        client: PolymarketClient,
        config: dict[str, Any],
        circuit_breaker: CircuitBreaker | None = None,
        position_sizer: PositionSizer | None = None,
        market_filter: MarketFilter | None = None,
        inventory: InventoryManager | None = None,
        category_scorer: CategoryScorer | None = None,
        edge_filter: EdgeFilter | None = None,
    ) -> None:
        super().__init__("safe_compounder", client, config)
        scfg = config.get("safe_compounder", {})

        self._circuit_breaker = circuit_breaker
        self._position_sizer = position_sizer
        self._market_filter = market_filter
        self._inventory = inventory
        self._category_scorer = category_scorer
        self._edge_filter = edge_filter

        self._min_yes_price = float(scfg.get("min_yes_price", 0.01))
        self._max_yes_price = float(scfg.get("max_yes_price", 0.20))
        self._min_no_ask = float(scfg.get("min_no_ask", 0.80))
        self._min_edge = float(scfg.get("min_edge", 0.03))
        self._max_position_pct = float(scfg.get("max_position_pct", 0.10))
        self._kelly_fraction = float(scfg.get("kelly_fraction", 0.25))
        self._min_order_usdc = float(scfg.get("min_order_usdc", 1.0))
        self._min_volume = float(scfg.get("min_volume", 5000.0))
        self._min_age_hours = float(scfg.get("min_market_age_hours", 24.0))
        self._skip_categories: set[str] = set(
            scfg.get("skip_categories", ["sports", "entertainment", "awards"])
        )
        self._skip_keywords: list[str] = scfg.get("skip_keywords", ["mention"])
        self._top_n = int(scfg.get("top_n_candidates", 200))
        self._refresh_sec = float(scfg.get("refresh_interval_sec", 300))

        # Exit thresholds
        self._tp_no_bid = float(scfg.get("tp_no_bid", 0.95))
        self._sl_no_ask = float(scfg.get("sl_no_ask", 0.70))

        self._last_eval: dict[str, float] = {}
        self._active_positions: dict[str, dict[str, Any]] = {}
        self._pending_market_ids: set[str] = set()
        self._pending_orders: dict[str, dict[str, Any]] = {}  # order_id -> {market_id, token_id, side, price, size}

        # Market cache
        self._markets_cache: list[dict[str, Any]] = []
        self._markets_cache_ts: float = 0.0
        self._markets_cache_ttl: float = 300.0

        self._stats = {"scans": 0, "signals": 0, "trades": 0, "exits": 0}

        self._reconcile_open_orders()

    def _reconcile_open_orders(self) -> None:
        """Popula _pending_market_ids y _pending_orders desde ordenes abiertas en el exchange."""
        try:
            orders = self._client.get_positions() or []
            count = 0
            for o in orders:
                mid = o.get("condition_id") or o.get("market_id") or ""
                oid = o.get("id") or o.get("order_id") or ""
                if mid:
                    self._pending_market_ids.add(mid)
                    count += 1
                if oid and mid:
                    self._pending_orders[oid] = {
                        "market_id": mid,
                        "token_id": o.get("token_id", ""),
                        "side": o.get("side", ""),
                        "price": float(o.get("price", 0)),
                        "size": float(o.get("original_size", o.get("size", 0))),
                    }
            if count:
                logger.info("SC reconcilió %d órdenes existentes del exchange", count)
        except Exception:
            pass

    # ------------------------------------------------------------------
    # BaseStrategy interface (para retrocompatibilidad con main loop)
    # ------------------------------------------------------------------

    def should_act(self, market_data: dict[str, Any]) -> bool:
        """Gate rapido: filtros de categoria, keywords, precios y edge."""
        if self._circuit_breaker is not None and self._circuit_breaker.is_triggered():
            return False

        if self._market_filter is not None and self._market_filter.is_banned(market_data):
            return False

        category = str(market_data.get("category", "")).lower()
        if self._category_scorer is not None and self._category_scorer.is_blocked(category):
            return False

        if category in self._skip_categories:
            return False

        question = str(market_data.get("question", "")).lower()
        for kw in self._skip_keywords:
            if kw in question:
                return False

        return True

    def evaluate(self, market_data: dict[str, Any]) -> list[Signal]:
        """Calcula edge y genera senal BUY NO si las condiciones se cumplen."""
        return self._evaluate_market(market_data)

    def execute(self, signals: list[Signal]) -> list[Trade]:
        """Coloca ordenes Post Only BUY NO. Convierte USDC a shares.
        Si Post Only es rechazado por cruzar el book, reinyenta sin Post Only."""
        trades: list[Trade] = []
        for sig in signals:
            try:
                shares = math.ceil(sig.size / sig.price) if sig.price > 0 else 0
                shares = max(shares, 5)
                if shares <= 0:
                    logger.warning("SC: signal con 0 shares: %s @ %.4f", sig.token_id[:10], sig.price)
                    continue

                # Try Post Only first
                result = self._client.place_limit_order(
                    token_id=sig.token_id,
                    side=sig.side, price=sig.price, size=shares,
                    post_only=True,
                )
                status = result.get("status", "error")

                # If Post Only rejected (crosses book), retry as taker at best bid
                if status == "rejected" and "cross" in str(result.get("reason", "")).lower():
                    logger.info("SC: Post Only cruzó el book, reinyentando como taker")
                    bba = self._client.get_best_bid_ask(sig.token_id)
                    best_bid = bba[0] if bba else sig.price
                    taker_price = round(best_bid + 0.005, 4) if best_bid > 0 else sig.price
                    result = self._client.place_limit_order(
                        token_id=sig.token_id,
                        side=sig.side, price=taker_price, size=shares,
                        post_only=False,
                    )
                    status = result.get("status", "error")

                trade = self._make_trade(sig, result.get("order_id", ""), status)
                trades.append(trade)
                self.log_trade(trade)

                # Track order for fill detection
                oid = result.get("order_id", "") or result.get("id", "")
                if oid and status == "live":
                    self._pending_orders[oid] = {
                        "market_id": sig.market_id,
                        "token_id": sig.token_id,
                        "side": sig.side,
                        "price": sig.price,
                        "size": sig.size,
                    }

                logger.info(
                    "SafeCompounder: %s %s %s shares=%s USDC=%s @ %s -> %s",
                    sig.market_id[:10], sig.side, sig.token_id[:8],
                    shares, sig.size, sig.price, trade.status,
                )
            except Exception:
                logger.exception("SafeCompounder: error colocando orden %s", sig.token_id[:8])
        return trades

    # ------------------------------------------------------------------
    # Main scan — market discovery + entries + exits
    # ------------------------------------------------------------------

    def run_scan(self, balance: float) -> list[Trade]:
        """Ciclo completo: descubrir mercados, evaluar, ejecutar, verificar exits."""
        if not self._active:
            return []

        if self._circuit_breaker is not None and self._circuit_breaker.is_triggered():
            return []

        all_trades: list[Trade] = []
        self._stats["scans"] += 1

        # 1. Exit check
        exit_trades = self._check_exits()
        all_trades.extend(exit_trades)

        # 2. Discover markets
        markets = self._discover_markets()
        if not markets:
            return all_trades

        # 4. Fetch orderbooks for accurate NO ask prices
        self._enrich_orderbooks(markets)

        # 5. Generate signals
        signals: list[tuple[Signal, float]] = []
        for mkt in markets:
            sigs = self._evaluate_market(mkt)
            for sig in sigs:
                edge = float(sig.metadata.get("edge", 0.0))
                if edge >= self._min_edge:
                    signals.append((sig, edge))

        # 4. Filter and rank
        signals.sort(key=lambda x: x[1], reverse=True)
        top_signals = [s for s, _ in signals[:self._top_n]]

        logger.info(
            "SafeCompounder: %d mercados -> %d con edge>%.0f%% -> %d top",
            len(markets), len(signals), self._min_edge * 100, len(top_signals),
        )

        # 5. Dedup + exposure cap
        to_execute = self._filter_signals_for_execution(top_signals, balance)

        # 6. Execute
        if to_execute:
            logger.info("SafeCompounder: ejecutando %d/%d senales", len(to_execute), len(top_signals))
            executed = self.execute(to_execute)
            all_trades.extend(executed)
            self._stats["trades"] += len(executed)
            self._stats["signals"] += len(top_signals)
            for sig in to_execute:
                self._pending_market_ids.add(sig.market_id)

        return all_trades

    # ------------------------------------------------------------------
    # Market discovery
    # ------------------------------------------------------------------

    def _discover_markets(self) -> list[dict[str, Any]]:
        """Descubre mercados binarios de Gamma API sin filtro de rewards."""
        now = time.time()
        if self._markets_cache and (now - self._markets_cache_ts) < self._markets_cache_ttl:
            return self._markets_cache

        markets: list[dict[str, Any]] = []
        seen: set[str] = set()

        try:
            for tag in ["Politics", "Crypto", "Economics", "Science", "World"]:
                params: dict[str, Any] = {
                    "closed": "false",
                    "limit": 100,
                    "tag": tag,
                    "order": "volume24hr",
                }
                try:
                    response = requests.get(GAMMA_API_URL, params=params, timeout=15.0)
                    response.raise_for_status()
                    events = response.json()

                    for event in events:
                        if not isinstance(event, dict):
                            continue
                        slug = event.get("slug", "")
                        for mkt_data in event.get("markets", []):
                            mkt = self._normalize_gamma_market(mkt_data, slug)
                            if mkt and mkt["condition_id"] not in seen:
                                markets.append(mkt)
                                seen.add(mkt["condition_id"])
                except Exception as e:
                    logger.debug("SC discover tag=%s: %s", tag, e)

        except Exception as e:
            logger.warning("SC market discovery failed: %s", e)

        self._markets_cache = markets
        self._markets_cache_ts = now
        logger.info("SafeCompounder: %d mercados binarios descubiertos", len(markets))
        return markets

    def _normalize_gamma_market(self, mkt_data: dict[str, Any], slug: str) -> dict[str, Any] | None:
        """Convierte un mercado de Gamma API al formato interno."""
        question = mkt_data.get("question", "") or mkt_data.get("groupItemTitle", "")
        if not question:
            return None

        condition_id = str(mkt_data.get("conditionId", "")) or str(mkt_data.get("id", ""))
        if not condition_id:
            return None

        outcome_prices = mkt_data.get("outcomePrices", [])
        if isinstance(outcome_prices, str):
            import json
            try:
                outcome_prices = json.loads(outcome_prices)
            except Exception:
                outcome_prices = []

        if not outcome_prices or len(outcome_prices) < 2:
            return None

        try:
            yes_price = float(outcome_prices[0])
        except (ValueError, IndexError, TypeError):
            return None

        if yes_price < self._min_yes_price or yes_price > self._max_yes_price:
            return None

        if mkt_data.get("closed", False):
            return None

        clob_ids = mkt_data.get("clobTokenIds", [])
        if isinstance(clob_ids, str):
            import json
            try:
                clob_ids = json.loads(clob_ids)
            except Exception:
                clob_ids = []

        if len(clob_ids) < 2:
            return None

        tokens = [
            {"token_id": str(clob_ids[0]), "outcome": "Yes", "price": float(outcome_prices[0])},
            {"token_id": str(clob_ids[1]), "outcome": "No", "price": float(outcome_prices[1])},
        ]

        category = str(mkt_data.get("category", "")).lower()
        if category in self._skip_categories:
            return None

        for kw in self._skip_keywords:
            if kw in question.lower():
                return None

        volume = float(mkt_data.get("volume", 0) or 0)
        if volume < self._min_volume:
            return None

        # Filtrar mercados muy nuevos (precios aun no estabilizados)
        created_str = mkt_data.get("createdAt", "")
        if created_str and self._min_age_hours > 0:
            try:
                if created_str.endswith("Z"):
                    created_str = created_str[:-1] + "+00:00"
                created_dt = datetime.fromisoformat(created_str)
                age_hours = (datetime.now(timezone.utc) - created_dt).total_seconds() / 3600.0
                if age_hours < self._min_age_hours:
                    return None
            except (ValueError, TypeError):
                pass

        end_date = mkt_data.get("endDate", "") or mkt_data.get("endDateIso", "")
        volume = float(mkt_data.get("volume", 0) or 0)

        return {
            "condition_id": condition_id,
            "question": question,
            "tokens": tokens,
            "mid_price": yes_price,
            "category": category,
            "end_date": end_date,
            "volume": volume,
            "slug": slug,
            "rewards_active": False,
        }

    def _track_fills(self) -> list[Trade]:
        """Consulta el estado de cada orden abierta. Si se lleno, registra fill."""
        trades: list[Trade] = []
        filled_oids: list[str] = []
        for oid, info in list(self._pending_orders.items()):
            try:
                status_data = self._client.get_order_status(oid)
                status = status_data.get("status", "")
                if status in ("ORDER_STATUS_MATCHED", "matched", "filled", "filled_paper"):
                    signal = Signal(
                        market_id=info["market_id"],
                        token_id=info["token_id"],
                        side=info["side"],
                        price=info["price"],
                        size=info["size"],
                        confidence=1.0,
                        strategy_name=self.name,
                        metadata={"fill_detected": True},
                    )
                    trade = self._make_trade(signal, oid, status)
                    self.log_trade(trade)
                    trades.append(trade)
                    filled_oids.append(oid)
                    logger.info("SC fill: %s lleno @ %.4f | %s", oid[:14], info["price"], info["market_id"][:12])
                elif status in ("CANCELLED", "cancelled", "expired"):
                    filled_oids.append(oid)
            except Exception:
                pass
        for oid in filled_oids:
            self._pending_orders.pop(oid, None)
        return trades

    # ------------------------------------------------------------------
    # Exit strategy
    # ------------------------------------------------------------------

    def _check_exits(self) -> list[Trade]:
        """Verifica exits (TP/SL/resolution) en posiciones abiertas."""
        trades: list[Trade] = []
        resolved: list[str] = []

        for market_id in list(self._pending_market_ids):
            # Verificar que tenemos inventario real antes de vender
            if self._inventory is not None:
                inv = self._inventory.get_market_inventory(market_id)
                if inv.total() <= 0:
                    continue
            try:
                response = requests.get(
                    f"https://gamma-api.polymarket.com/markets/{market_id}",
                    timeout=10.0,
                )
                response.raise_for_status()
                data = response.json()

                if data.get("closed", False):
                    resolved.append(market_id)
                    logger.info("SC exit: %s resuelto", market_id[:14])
                    continue

                outcome_prices = data.get("outcomePrices", [])
                if isinstance(outcome_prices, str):
                    import json
                    try:
                        outcome_prices = json.loads(outcome_prices)
                    except Exception:
                        outcome_prices = []

                if len(outcome_prices) < 2:
                    continue

                yes_price = float(outcome_prices[0])
                no_price = float(outcome_prices[1])

                # Take-profit: NO bid >= threshold
                if no_price >= self._tp_no_bid:
                    self._sell_no_position(market_id, data, trades, "TP")
                    resolved.append(market_id)

                # Stop-loss: NO ask <= threshold
                elif no_price <= self._sl_no_ask:
                    self._sell_no_position(market_id, data, trades, "SL")
                    resolved.append(market_id)

            except Exception as e:
                logger.debug("SC exit check error %s: %s", market_id[:14], e)

        for mid in resolved:
            self._pending_market_ids.discard(mid)
            self._stats["exits"] += 1

        return trades

    def _sell_no_position(
        self, market_id: str, data: dict[str, Any], trades: list[Trade], reason: str
    ) -> None:
        """Vende posicion NO al mercado."""
        clob_ids = data.get("clobTokenIds", [])
        if isinstance(clob_ids, str):
            import json
            try:
                clob_ids = json.loads(clob_ids)
            except Exception:
                clob_ids = []
        no_tid = str(clob_ids[1]) if len(clob_ids) > 1 else ""

        if not no_tid:
            return

        try:
            signal = Signal(
                market_id=market_id,
                token_id=no_tid,
                side="SELL",
                price=0.01,
                size=1.0,
                confidence=1.0,
                strategy_name=self.name,
                metadata={"exit_reason": reason},
            )
            result = self._client.place_limit_order(
                token_id=no_tid, side="SELL", price=0.01, size=1.0, post_only=False,
            )
            trade = self._make_trade(signal, result.get("order_id", ""), result.get("status", "error"))
            trades.append(trade)
            self.log_trade(trade)
            logger.info("SC exit [%s]: %s vendido NO", reason, market_id[:14])

            if self._inventory:
                try:
                    self._inventory.clear_market(market_id)
                except Exception:
                    pass
        except Exception:
            logger.exception("SC error vendiendo NO en %s", market_id[:14])

    # ------------------------------------------------------------------
    # Signal filtering
    # ------------------------------------------------------------------

    def _filter_signals_for_execution(
        self, signals: list[Signal], balance: float
    ) -> list[Signal]:
        """Filtra senales: dedup, max_trades, exposure."""
        to_execute: list[Signal] = []
        max_positions = 10
        seen_tokens: set[str] = set()
        running_exposure = len(self._pending_market_ids)

        for sig in signals:
            if len(to_execute) >= 3:
                break
            if sig.market_id in self._pending_market_ids:
                continue
            if sig.token_id in seen_tokens:
                continue
            if running_exposure >= max_positions:
                break
            if balance - sig.size < float(self._config.get("loss_reserve_usdc", 20.0)):
                continue
            to_execute.append(sig)
            seen_tokens.add(sig.token_id)
            running_exposure += 1

        return to_execute

    def _enrich_orderbooks(self, markets: list[dict[str, Any]]) -> None:
        """Fetch real orderbooks via batch API para precios precisos."""
        all_token_ids: list[str] = []
        for mkt in markets:
            for t in mkt.get("tokens", []):
                tid = t.get("token_id", "")
                if tid:
                    all_token_ids.append(tid)

        if not all_token_ids:
            return

        orderbooks: dict[str, dict[str, Any]] = {}
        batch_size = 500
        for i in range(0, len(all_token_ids), batch_size):
            batch = all_token_ids[i : i + batch_size]
            try:
                batch_result = self._client.get_orderbooks_batch(batch)
                if batch_result:
                    orderbooks.update(batch_result)
            except Exception as e:
                logger.warning("SC orderbook batch fetch failed: %s", e)

        # Inject into market data
        for mkt in markets:
            ob = {}
            for t in mkt.get("tokens", []):
                tid = t.get("token_id", "")
                if tid and tid in orderbooks:
                    ob[tid] = orderbooks[tid]
            mkt["orderbook"] = ob

        logger.info("SC: enriched %d/%d orderbooks", len(orderbooks), len(all_token_ids))

    # ------------------------------------------------------------------
    # Market evaluation (shared with main loop and run_scan)
    # ------------------------------------------------------------------

    def _evaluate_market(self, market_data: dict[str, Any]) -> list[Signal]:
        """Calcula edge y genera senal BUY NO si las condiciones se cumplen."""
        cid = market_data.get("condition_id", "")
        tokens = market_data.get("tokens", [])
        if len(tokens) < 2:
            return []

        mid_price = float(market_data.get("mid_price", 0.0) or 0.0)
        if mid_price <= 0:
            return []

        yes_token = self._find_yes_token(tokens)
        no_token = self._find_no_token(tokens, yes_token)
        if yes_token is None or no_token is None:
            return []

        yes_price = float(yes_token.get("price", 0.0) or 0.0)
        if yes_price <= 0:
            yes_price = mid_price

        if yes_price < self._min_yes_price or yes_price > self._max_yes_price:
            return []

        no_ask = self._get_no_ask(market_data, no_token)
        if no_ask is None or no_ask < self._min_no_ask:
            return []

        estimated_true_prob = self._estimate_true_prob(yes_price, market_data)

        # Default entry: estimated_prob - half edge (más agresivo que -1%)
        buy_price = estimated_true_prob - self._min_edge * 0.5
        buy_price = max(self._min_no_ask, round(buy_price, 4))
        buy_price = min(buy_price, no_ask - 0.01)  # Post Only safety
        is_spread_entry = False
        spread = 0.0

        # Si el spread es ancho (>5% del mid), comprar al mid + 30% del spread
        no_mid = float(no_token.get("price", 0.0) or 0.0)
        if no_mid > 0 and no_ask > 0:
            spread = no_ask - no_mid
            if spread > 0.05 and no_mid >= self._min_no_ask:
                buy_price = no_mid + spread * 0.2
                buy_price = max(self._min_no_ask, round(buy_price, 4))
                buy_price = min(buy_price, no_ask - 0.01)  # Post Only safety
                is_spread_entry = True
                logger.info(
                    "SC spread: mid=%.4f ask=%.4f +30%% -> entry@%.4f",
                    no_mid, no_ask, buy_price,
                )

        edge = estimated_true_prob - buy_price
        if is_spread_entry:
            edge = max(edge, spread * 0.7)  # el spread capturado es nuestro edge
        min_edge_required = 0.0 if is_spread_entry else self._min_edge

        if edge < min_edge_required:
            return []

        confidence = self._calc_confidence(yes_price, no_ask, edge)
        if confidence < 0.40:
            return []

        if self._edge_filter is not None:
            passes, _ = self._edge_filter.has_sufficient_edge(
                estimated_true_prob, no_ask, confidence
            )
            if not passes:
                return []

        capital = float(market_data.get("available_cash", 50.0))
        size = self._calc_size(estimated_true_prob, buy_price, capital)
        if size <= 0:
            return []

        no_tid = no_token.get("token_id", "")
        if not no_tid:
            return []

        signal = Signal(
            market_id=cid,
            token_id=no_tid,
            side="BUY",
            price=buy_price,
            size=size,
            confidence=confidence,
            strategy_name=self.name,
            metadata={
                "estimated_prob": estimated_true_prob,
                "edge": edge,
                "yes_price": yes_price,
                "no_ask": no_ask,
            },
        )
        return [signal]

    # --- Internal helpers ---

    @staticmethod
    def _find_yes_token(tokens: list[dict[str, Any]]) -> dict[str, Any] | None:
        """Encuentra el token YES: el de precio mas bajo en binarios."""
        if len(tokens) < 2:
            return tokens[0] if tokens else None
        t0 = tokens[0]
        t1 = tokens[1]
        p0 = float(t0.get("price", 0.0) or 0.0)
        p1 = float(t1.get("price", 0.0) or 0.0)
        return t0 if p0 <= p1 else t1

    @staticmethod
    def _find_no_token(
        tokens: list[dict[str, Any]], yes_token: dict[str, Any] | None
    ) -> dict[str, Any] | None:
        if yes_token is None or len(tokens) < 2:
            return None
        for t in tokens:
            if t.get("token_id") != yes_token.get("token_id"):
                return t
        return None

    def _get_no_ask(
        self, market_data: dict[str, Any], no_token: dict[str, Any]
    ) -> float | None:
        """Obtiene el best ask del token NO."""
        no_tid = no_token.get("token_id", "")
        orderbook = market_data.get("orderbook", {})
        if no_tid and no_tid in orderbook:
            ob = orderbook[no_tid]
            asks = ob.get("asks", []) if isinstance(ob, dict) else getattr(ob, "asks", [])
            if asks:
                return float(asks[0][0] if isinstance(asks[0], (list, tuple)) else asks[0].get("price", 0))
        yes_token = self._find_yes_token(market_data.get("tokens", []))
        if yes_token:
            yes_tid = yes_token.get("token_id", "")
            ob = orderbook.get(yes_tid, {}) if isinstance(orderbook, dict) else {}
            bids = ob.get("bids", []) if isinstance(ob, dict) else []
            if bids:
                yes_bid = float(bids[0][0] if isinstance(bids[0], (list, tuple)) else bids[0].get("price", 0))
                return 1.0 - yes_bid
        # Fallback: estimar NO ask desde el mid price (sin orderbook)
        no_price = float(no_token.get("price", 0.0) or 0.0)
        if no_price > 0:
            return no_price
        return None

    def _estimate_true_prob(
        self, yes_price: float, market_data: dict[str, Any]
    ) -> float:
        """Estima probabilidad real de que NO gane.

        La intuicion: mercados lejanos tienen mas incertidumbre →
        YES suele estar sobrevalorado (lottery ticket effect) →
        NO tiene mas valor real del que el mercado le asigna.

        Aplica un factor de incertidumbre que crece con el horizonte temporal
        usando escala logaritmica para mercados de semanas/meses.
        """
        end_date = market_data.get("end_date", "")
        if not end_date:
            return 1.0 - yes_price

        try:
            if end_date.endswith("Z"):
                end_date = end_date[:-1] + "+00:00"
            end_dt = datetime.fromisoformat(end_date)
            now = datetime.now(timezone.utc)
            hours_left = (end_dt - now).total_seconds() / 3600.0
        except (ValueError, TypeError):
            return 1.0 - yes_price

        if hours_left <= 0:
            return 1.0 - yes_price

        days_left = max(0.0, hours_left / 24.0)
        uncertainty = 0.10 * (1.0 - 1.0 / (1.0 + days_left / 7.0))
        uncertainty = min(0.15, uncertainty)
        adjusted_yes = yes_price * (1.0 - uncertainty)
        adjusted_yes = max(0.001, adjusted_yes)
        return 1.0 - adjusted_yes

    def _calc_confidence(self, yes_price: float, no_ask: float, edge: float) -> float:
        """Confianza basada en edge, precio YES y no_ask."""
        base = 0.50
        edge_boost = min(0.30, edge * 3.0)
        yes_boost = min(0.10, (self._max_yes_price - yes_price) * 0.5)
        ask_boost = min(0.10, (no_ask - self._min_no_ask) * 0.5)
        return min(0.95, base + edge_boost + yes_boost + ask_boost)

    def _calc_size(
        self, estimated_true_prob: float, buy_price: float, capital: float
    ) -> float:
        """Calcula tamanio usando Kelly fraccional con edge real (corregido v2)."""
        max_by_pct = capital * self._max_position_pct

        kf = _kelly_fraction(
            estimated_prob=estimated_true_prob,
            market_price=buy_price,
            kelly_multiplier=self._kelly_fraction,
        )
        if kf <= 0:
            kf = 0.02  # fallback: 2% para edges pequeños pero válidos
        kelly_size = capital * kf

        size = min(kelly_size, max_by_pct)
        size = max(size, self._min_order_usdc)  # mínimo absoluto
        size = math.floor(size * 100) / 100.0
        return size

    # ------------------------------------------------------------------
    # Status
    # ------------------------------------------------------------------

    def get_status(self) -> dict[str, Any]:
        return {
            "active": self._active,
            "positions": len(self._pending_market_ids),
            "stats": dict(self._stats),
            "params": {
                "min_yes": self._min_yes_price,
                "max_yes": self._max_yes_price,
                "min_no_ask": self._min_no_ask,
                "min_edge": self._min_edge,
                "max_pct": self._max_position_pct,
                "kelly": self._kelly_fraction,
                "tp": self._tp_no_bid,
                "sl": self._sl_no_ask,
            },
        }
