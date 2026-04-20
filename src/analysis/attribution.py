"""Atribucion de trades por estrategia, categoria de mercado y regimen (TODO 2.2).

Genera tablas de PnL por (strategy x category x regime) para identificar
que combinaciones dan mas ROI.
"""

import json
import logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger("nachomarket.attribution")

TRADES_FILE = Path("data/trades.jsonl")


class TradeAttribution:
    """Atribucion de PnL por multiples dimensiones.

    Lee data/trades.jsonl y segmenta por:
    - strategy_name (market_maker, multi_arb, stat_arb, ...)
    - market_category (politics, sports, crypto, ...)
    - regime_detected (MEAN_REVERTING, TRENDING, VOLATILE, UNKNOWN)

    Uso:
        attr = TradeAttribution()
        report = attr.report()
        top5 = attr.top_n(5)
        bottom5 = attr.bottom_n(5)
    """

    def __init__(self, trades_path: str = "data/trades.jsonl") -> None:
        self._path = Path(trades_path)

    # ------------------------------------------------------------------
    # Carga
    # ------------------------------------------------------------------

    def _load_trades(self, days: int = 30) -> list[dict[str, Any]]:
        """Lee trades.jsonl con filtro de tiempo."""
        if not self._path.exists():
            return []

        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        trades = []

        try:
            with open(self._path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        trade = json.loads(line)
                        ts_str = trade.get("timestamp", "")
                        if ts_str:
                            ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                            if ts >= cutoff:
                                trades.append(trade)
                    except Exception:
                        continue
        except OSError:
            logger.exception("Error leyendo trades para attribution")

        return trades

    # ------------------------------------------------------------------
    # Generacion de reporte
    # ------------------------------------------------------------------

    def report(self, days: int = 30) -> list[dict[str, Any]]:
        """Genera tabla de PnL por (strategy, category, regime).

        Returns:
            Lista de dicts ordenados por total_pnl desc, con claves:
            strategy, category, regime, total_pnl, trade_count,
            win_rate, avg_pnl, total_deployed.
        """
        trades = self._load_trades(days=days)
        if not trades:
            return []

        # Agregar por tupla (strategy, category, regime)
        buckets: dict[tuple[str, str, str], dict[str, Any]] = defaultdict(
            lambda: {"total_pnl": 0.0, "count": 0, "wins": 0, "deployed": 0.0}
        )

        for trade in trades:
            strategy = trade.get("strategy_name", "unknown")
            category = trade.get("market_category", "unknown")
            regime = trade.get("regime_detected", "UNKNOWN")
            pnl = trade.get("pnl", 0.0) or 0.0
            size = trade.get("size", 0.0) or 0.0

            key = (strategy, category, regime)
            buckets[key]["total_pnl"] += pnl
            buckets[key]["count"] += 1
            buckets[key]["deployed"] += size
            if pnl > 0:
                buckets[key]["wins"] += 1

        result = []
        for (strategy, category, regime), data in buckets.items():
            count = data["count"]
            total_pnl = data["total_pnl"]
            win_rate = data["wins"] / count if count > 0 else 0.0
            avg_pnl = total_pnl / count if count > 0 else 0.0
            roi = total_pnl / data["deployed"] if data["deployed"] > 0 else 0.0

            result.append({
                "strategy": strategy,
                "category": category,
                "regime": regime,
                "total_pnl": round(total_pnl, 4),
                "trade_count": count,
                "win_rate": round(win_rate, 3),
                "avg_pnl": round(avg_pnl, 4),
                "total_deployed": round(data["deployed"], 2),
                "roi": round(roi, 4),
            })

        return sorted(result, key=lambda x: x["total_pnl"], reverse=True)

    def top_n(self, n: int = 5, days: int = 30) -> list[dict[str, Any]]:
        """Top N combinaciones por total_pnl."""
        return self.report(days=days)[:n]

    def bottom_n(self, n: int = 5, days: int = 30) -> list[dict[str, Any]]:
        """Bottom N combinaciones por total_pnl (las peores)."""
        return self.report(days=days)[-n:]

    def by_strategy(self, days: int = 30) -> dict[str, float]:
        """PnL total agregado por estrategia."""
        result: dict[str, float] = defaultdict(float)
        for row in self.report(days=days):
            result[row["strategy"]] += row["total_pnl"]
        return dict(result)

    def by_category(self, days: int = 30) -> dict[str, float]:
        """PnL total agregado por categoria de mercado."""
        result: dict[str, float] = defaultdict(float)
        for row in self.report(days=days):
            result[row["category"]] += row["total_pnl"]
        return dict(result)

    def format_telegram(self, n: int = 5) -> str:
        """Formato para mensaje Telegram /attribution."""
        top = self.top_n(n)
        bottom = self.bottom_n(n)

        lines = ["*Attribution Report (30d)*\n"]
        lines.append(f"🟢 *Top {n}:*")
        for r in top:
            lines.append(
                f"  `{r['strategy']}/{r['category']}` "
                f"PnL:`${r['total_pnl']:+.2f}` "
                f"WR:`{r['win_rate']*100:.0f}%` "
                f"N:`{r['trade_count']}`"
            )

        lines.append(f"\n🔴 *Bottom {n}:*")
        for r in bottom:
            lines.append(
                f"  `{r['strategy']}/{r['category']}` "
                f"PnL:`${r['total_pnl']:+.2f}` "
                f"WR:`{r['win_rate']*100:.0f}%` "
                f"N:`{r['trade_count']}`"
            )

        return "\n".join(lines)
