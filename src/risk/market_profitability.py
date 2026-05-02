"""Market profitability tracking: detecta mercados que no rinden."""

import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger("nachomarket.market_profitability")

PROFITABILITY_FILE = Path("data/profitability.json")
TRADES_FILE = Path("data/trades.jsonl")


class Trade:
    """Representa un trade individual."""
    def __init__(
        self,
        timestamp: str = "",
        market_id: str = "",
        token_id: str = "",
        side: str = "",
        price: float = 0.0,
        size: float = 0.0,
        order_id: str = "",
        status: str = "",
        strategy_name: str = "",
        fee_paid: float = 0.0,
    ) -> None:
        self.timestamp = timestamp
        self.market_id = market_id
        self.token_id = token_id
        self.side = side
        self.price = price
        self.size = size
        self.order_id = order_id
        self.status = status
        self.strategy_name = strategy_name
        self.fee_paid = fee_paid


class MarketStats:
    """Estadísticas de un mercado para profitability."""

    def __init__(self, market_id: str, **kwargs) -> None:
        self.market_id = market_id
        self.total_pnl: float = kwargs.get("total_pnl", 0.0)
        self.capital_deployed: float = kwargs.get("capital_deployed", 0.0)
        self.buy_prices: list[float] = kwargs.get("buy_prices", [])
        self.sell_prices: list[float] = kwargs.get("sell_prices", [])
        self.order_count: int = kwargs.get("order_count", 0)
        self.fill_count: int = kwargs.get("fill_count", 0)
        self.question: str = kwargs.get("question", "")
        self.roi: float = kwargs.get("roi", 0.0)
        self.avg_spread_captured: float = kwargs.get("avg_spread_captured", 0.0)

    def update(self, trade: Trade) -> None:
        """Actualiza stats con un nuevo trade."""
        if trade.status == "error":
            return
        self.order_count += 1
        if trade.side.upper() == "BUY":
            self.buy_prices.append(trade.price)
            self.capital_deployed += trade.price * trade.size
        elif trade.side.upper() == "SELL":
            self.sell_prices.append(trade.price)
            pnl = (trade.price - self._avg_buy()) * trade.size
            if pnl > 0:
                self.fill_count += 1
            self.total_pnl += pnl

    def _avg_buy(self) -> float:
        """Precio promedio de compra."""
        if not self.buy_prices:
            return 0.5
        return sum(self.buy_prices) / len(self.buy_prices)

    def _avg_sell(self) -> float:
        """Precio promedio de venta."""
        if not self.sell_prices:
            return 0.5
        return sum(self.sell_prices) / len(self.sell_prices)

    @property
    def roi(self) -> float:
        """Return on Investment (PnL / capital invertido)."""
        if self.capital_deployed <= 0:
            return 0.0
        return self.total_pnl / self.capital_deployed


class MarketProfiler:
    """Analiza rentabilidad de mercados para blacklisting y reportes."""

    def __init__(self, config: dict[str, Any]) -> None:
        self._config = config
        self._stats: dict[str, MarketStats] = self._load()
        min_orders = config.get("min_orders_to_evaluate", 3)
        self.min_orders_to_evaluate: int = min_orders

    def _load(self) -> dict[str, MarketStats]:
        """Carga stats persistidas."""
        if PROFITABILITY_FILE.exists():
            try:
                raw = json.loads(PROFITABILITY_FILE.read_text("utf-8"))
                stats = {}
                for cid, data in raw.items():
                    s = MarketStats(cid)
                    s.total_pnl = data.get("total_pnl", 0.0)
                    s.capital_deployed = data.get("capital_deployed", 0.0)
                    s.buy_prices = data.get("buy_prices", [])
                    s.sell_prices = data.get("sell_prices", [])
                    s.order_count = data.get("order_count", 0)
                    s.fill_count = data.get("fill_count", 0)
                    s.question = data.get("question", "")
                    s.roi = data.get("roi", 0.0)
                    s.avg_spread_captured = data.get("avg_spread_captured", 0.0)
                    stats[cid] = s
                logger.info("MarketProfiler: %d mercados cargados", len(stats))
                return stats
            except Exception:
                logger.debug("No se pudo cargar profitability.json", exc_info=True)
        return {}

    def _save(self) -> None:
        """Persiste stats a disco."""
        try:
            data = {}
            for cid, s in self._stats.items():
                data[cid] = {
                    "total_pnl": s.total_pnl,
                    "capital_deployed": s.capital_deployed,
                    "buy_prices": s.buy_prices,
                    "sell_prices": s.sell_prices,
                    "order_count": s.order_count,
                    "fill_count": s.fill_count,
                    "question": s.question,
                    "roi": s.roi,
                    "avg_spread_captured": s.avg_spread_captured,
                }
            PROFITABILITY_FILE.parent.mkdir(parents=True, exist_ok=True)
            PROFITABILITY_FILE.write_text(
                json.dumps(data, ensure_ascii=False, indent=2),
                "utf-8",
            )
        except Exception:
            logger.debug("No se pudo guardar profitability.json", exc_info=True)

    def update(self, market_id: str, trade: Trade) -> None:
        """Actualiza stats con un nuevo trade."""
        if market_id not in self._stats:
            self._stats[market_id] = MarketStats(market_id)
        self._stats[market_id].update(trade)
        self._save()

    def get_all_stats(self) -> dict[str, MarketStats]:
        """Retorna todas las stats."""
        return self._stats

    def get_unprofitable_markets(self, min_roi: float = 0.0) -> list[str]:
        """Retorna mercados con ROI < min_roi y suficientes orders."""
        result = []
        for cid, stats in self._stats.items():
            if stats.order_count >= self.min_orders_to_evaluate and stats.roi < min_roi:
                result.append(cid)
        return result

    def get_report(self, top_n: int = 10) -> list[dict[str, Any]]:
        """Genera reporte ordenado por ROI."""
        report = []
        for cid, stats in self._stats.items():
            if stats.order_count < self.min_orders_to_evaluate:
                continue
            report.append({
                "market_id": cid,
                "question": stats.question,
                "roi": stats.roi,
                "total_pnl": stats.total_pnl,
                "capital_deployed": stats.capital_deployed,
                "order_count": stats.order_count,
                "fill_count": stats.fill_count,
                "avg_spread_captured": stats.avg_spread_captured,
            })
        report.sort(key=lambda x: x["roi"], reverse=True)
        return report[:top_n]

    def get_market_roi(self, market_id: str) -> float | None:
        """Retorna ROI de un mercado, o None si no hay datos suficientes."""
        stats = self._stats.get(market_id)
        if not stats or stats.order_count < self.min_orders_to_evaluate:
            return None
        return stats.roi

    def should_exit_by_share(
        self,
        market_id: str,
        current_share: float,
        threshold: float = 0.5,
        persistence_hours: float = 12.0,
    ) -> bool:
        """True si share < threshold por >persistence_hours."""
        if current_share >= threshold:
            # Share recuperado: resetear timer en MarketStats
            if market_id in self._stats:
                self._stats[market_id].share_below_since = None
            return False

        # Verificar si el share lleva tiempo bajo
        if market_id not in self._stats:
            # Nuevo mercado: crear stats y empezar timer
            self._stats[market_id] = MarketStats(market_id)
            self._stats[market_id].share_below_since = time.time()
            return False

        stats = self._stats[market_id]
        if stats.share_below_since is None:
            stats.share_below_since = time.time()
            return False

        hours_below = (time.time() - stats.share_below_since) / 3600
        if hours_below > persistence_hours:
            logger.info(
                "MarketProfiler: %s... share=%.2f%% <%.1f%% por %.1fh → exit",
                market_id[:12], current_share * 100, threshold * 100, hours_below,
            )
            return True
        return False

    def cleanup_old(self, max_age_days: int = 30) -> None:
        """Limpia stats de mercados muy viejos."""
        cutoff = time.time() - max_age_days * 86400
        to_remove = [
            cid for cid, s in self._stats.items()
            if s.last_update < cutoff  # type: ignore[attr-defined]
        ]
        for cid in to_remove:
            del self._stats[cid]
        if to_remove:
            logger.info("MarketProfiler: %d mercados viejos eliminados", len(to_remove))
            self._save()
