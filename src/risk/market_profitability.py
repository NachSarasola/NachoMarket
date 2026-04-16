"""
Tracking de rentabilidad por mercado.

Registra PnL, rebates, rewards y ROI por mercado para:
1. Detectar mercados no rentables y retirarlos de la rotacion
2. Priorizar mercados rentables en futuros scans
3. Reportar en Telegram /pnl con desglose por mercado
4. Persistir en data/profitability.json para sobrevivir reinicios

Flujo:
    profiler = MarketProfiler(config)
    profiler.update(market_id, trade)   # despues de cada trade
    bad = profiler.get_unprofitable_markets(min_roi=-0.05)  # ROI < -5%
    report = profiler.get_report()      # para Telegram
"""

import json
import logging
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger("nachomarket.risk.profitability")

PROFITABILITY_FILE = Path("data/profitability.json")


@dataclass
class MarketStats:
    """Estadisticas de rentabilidad de un mercado."""
    market_id: str
    question: str = ""
    total_pnl: float = 0.0
    total_rebates: float = 0.0
    total_rewards: float = 0.0
    order_count: int = 0
    fill_count: int = 0
    capital_deployed: float = 0.0
    buy_prices: list[float] = field(default_factory=list)
    sell_prices: list[float] = field(default_factory=list)
    last_updated: str = ""

    @property
    def roi(self) -> float:
        """ROI = PnL total / capital desplegado."""
        if self.capital_deployed <= 0:
            return 0.0
        return (self.total_pnl + self.total_rebates + self.total_rewards) / self.capital_deployed

    @property
    def avg_spread_captured(self) -> float:
        """Spread promedio capturado en round-trips."""
        if not self.buy_prices or not self.sell_prices:
            return 0.0
        avg_buy = sum(self.buy_prices) / len(self.buy_prices)
        avg_sell = sum(self.sell_prices) / len(self.sell_prices)
        return avg_sell - avg_buy


class MarketProfiler:
    """Rastrea y evalua la rentabilidad de cada mercado operado."""

    def __init__(self, config: dict[str, Any]) -> None:
        self._min_orders_to_evaluate = config.get("min_orders_to_evaluate", 10)
        self._stats: dict[str, MarketStats] = {}
        self._load()

    # ------------------------------------------------------------------
    # API principal
    # ------------------------------------------------------------------

    def update(self, market_id: str, trade: Any, question: str = "") -> None:
        """Actualiza estadisticas al ejecutar un trade.

        Args:
            market_id: condition_id del mercado.
            trade: objeto Trade o dict con side/price/size/fee_paid.
            question: pregunta del mercado (para display en reports).
        """
        if market_id not in self._stats:
            self._stats[market_id] = MarketStats(
                market_id=market_id,
                question=question or market_id[:20],
            )

        stats = self._stats[market_id]
        if question:
            stats.question = question

        # Extraer campos del trade (funciona con dataclass o dict)
        side = trade.side if hasattr(trade, "side") else trade.get("side", "")
        price = trade.price if hasattr(trade, "price") else trade.get("price", 0.0)
        size = trade.size if hasattr(trade, "size") else trade.get("size", 0.0)
        fee = trade.fee_paid if hasattr(trade, "fee_paid") else trade.get("fee_paid", 0.0)
        rewards = getattr(trade, "rewards", trade.get("rewards", 0.0) if isinstance(trade, dict) else 0.0)
        status = trade.status if hasattr(trade, "status") else trade.get("status", "")

        stats.order_count += 1
        stats.last_updated = datetime.now(timezone.utc).isoformat()

        if status in ("error", "rejected"):
            return

        # Actualizar precios y capital
        if side == "BUY":
            stats.capital_deployed += size
            stats.buy_prices.append(price)
        elif side == "SELL":
            stats.sell_prices.append(price)
            # Calcular PnL incremental si tenemos buys
            if stats.buy_prices:
                avg_buy = sum(stats.buy_prices) / len(stats.buy_prices)
                incremental_pnl = (price - avg_buy) * size
                stats.total_pnl += incremental_pnl
                stats.fill_count += 1

        # Rebates y fees
        stats.total_rebates -= fee  # fee negativo = rebate (post-only maker)
        stats.total_rewards += rewards

        # Mantener listas acotadas (max 100 precios)
        if len(stats.buy_prices) > 100:
            stats.buy_prices = stats.buy_prices[-100:]
        if len(stats.sell_prices) > 100:
            stats.sell_prices = stats.sell_prices[-100:]

        self._save()

    def get_unprofitable_markets(self, min_roi: float = -0.05) -> list[str]:
        """Retorna IDs de mercados con ROI por debajo del threshold.

        Solo evalua mercados con suficientes ordenes para ser estadisticamente
        significativos (min_orders_to_evaluate, default 10).

        Args:
            min_roi: ROI minimo aceptable (default -5%).

        Returns:
            Lista de condition_ids de mercados no rentables.
        """
        unprofitable = []
        for market_id, stats in self._stats.items():
            if stats.order_count < self._min_orders_to_evaluate:
                continue  # No suficientes datos
            if stats.roi < min_roi:
                unprofitable.append(market_id)
                logger.warning(
                    f"Mercado no rentable: {stats.question[:30]}... "
                    f"ROI={stats.roi:.2%} PnL=${stats.total_pnl:.2f}"
                )
        return unprofitable

    def get_report(self, top_n: int = 10) -> list[dict[str, Any]]:
        """Genera un reporte de los N mercados mas rentables.

        Returns:
            Lista de dicts ordenados por ROI descendente.
        """
        evaluated = [
            s for s in self._stats.values()
            if s.order_count >= self._min_orders_to_evaluate
        ]
        evaluated.sort(key=lambda s: s.roi, reverse=True)

        return [
            {
                "market_id": s.market_id,
                "question": s.question[:40],
                "roi": round(s.roi, 4),
                "total_pnl": round(s.total_pnl, 4),
                "total_rebates": round(s.total_rebates, 4),
                "total_rewards": round(s.total_rewards, 4),
                "fill_count": s.fill_count,
                "order_count": s.order_count,
                "avg_spread_captured": round(s.avg_spread_captured, 4),
                "capital_deployed": round(s.capital_deployed, 2),
            }
            for s in evaluated[:top_n]
        ]

    def get_market_roi(self, market_id: str) -> float | None:
        """ROI de un mercado especifico, o None si no hay datos."""
        stats = self._stats.get(market_id)
        if stats is None or stats.order_count < self._min_orders_to_evaluate:
            return None
        return stats.roi

    def get_all_stats(self) -> dict[str, MarketStats]:
        """Retorna todas las estadisticas (para tests y debug)."""
        return dict(self._stats)

    # ------------------------------------------------------------------
    # Persistencia
    # ------------------------------------------------------------------

    def _save(self) -> None:
        """Persiste estadisticas a data/profitability.json."""
        try:
            PROFITABILITY_FILE.parent.mkdir(parents=True, exist_ok=True)
            data = {
                mid: asdict(stats)
                for mid, stats in self._stats.items()
            }
            PROFITABILITY_FILE.write_text(
                json.dumps(data, indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
        except OSError:
            logger.warning("No se pudo guardar profitability.json")

    def _load(self) -> None:
        """Carga estadisticas persistidas de data/profitability.json."""
        if not PROFITABILITY_FILE.exists():
            return
        try:
            data = json.loads(PROFITABILITY_FILE.read_text(encoding="utf-8"))
            for mid, raw in data.items():
                self._stats[mid] = MarketStats(**raw)
            logger.info(f"Profitability: {len(self._stats)} mercados cargados")
        except (json.JSONDecodeError, TypeError, KeyError):
            logger.warning("No se pudo cargar profitability.json, empezando desde cero")
