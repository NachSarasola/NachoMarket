"""Scoring de categorias basado en ROI historico real.

Puntua cada categoria de Polymarket de 0 a 100 usando
win rate y PnL acumulado de trades reales.

Tiers de asignacion:
  80-100 → STRONG (20% allocation)
  60-79  → GOOD (10% allocation)
  40-59  → WEAK (5% allocation)
  20-39  → POOR (2% allocation)
  0-19   → BLOCKED (0%)

Score < 30 → bloqueo automatico.

Seed data de rendimiento conocido:
  - politics: 65 (estable, predecible)
  - crypto: 60 (volatil pero con edge)
  - sports: 35 (impredecible en vivo)
  - economics: 15 (mala experiencia historica)
  - entertainment: 25 (noticias impredecibles)
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger("nachomarket.strategy.category_scorer")

_SEED_SCORES: dict[str, float] = {
    "sports": 35,
    "politics": 65,
    "crypto": 60,
    "economics": 15,
    "entertainment": 25,
    "esports": 25,
    "science": 50,
    "technology": 50,
    "weather": 40,
}

_BLOCK_THRESHOLD = 30
_MIN_TRADES_TO_RECALCULATE = 10


class CategoryScorer:
    """Scoring 0-100 por categoria con datos historicos propios."""

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        cfg = (config or {}).get("category_scorer", {})
        self._block_threshold = cfg.get("block_threshold", _BLOCK_THRESHOLD)
        self._scores: dict[str, float] = dict(_SEED_SCORES)
        seed_overrides = cfg.get("seed_scores", {})
        for k, v in seed_overrides.items():
            self._scores[k.lower()] = float(v)
        self._trades: dict[str, list[float]] = {}
        self._min_trades = cfg.get("min_trades_to_recalculate", _MIN_TRADES_TO_RECALCULATE)

    # --- Public API ---

    def get_score(self, category: str) -> float:
        """Score 0-100. Default 50 si categoria desconocida."""
        return self._scores.get(category.lower(), 50.0)

    def is_blocked(self, category: str) -> bool:
        """True si score < block_threshold."""
        return self.get_score(category) < self._block_threshold

    def get_allocation_pct(self, category: str) -> float:
        """Porcentaje de capital sugerido para esta categoria."""
        score = self.get_score(category)
        if score >= 80:
            return 0.20
        if score >= 60:
            return 0.10
        if score >= 40:
            return 0.05
        if score >= 20:
            return 0.02
        return 0.0

    def get_all_scores(self) -> dict[str, float]:
        """Snapshot de todos los scores conocidos."""
        return dict(self._scores)

    def update_from_trade(self, category: str, pnl: float) -> None:
        """Registra PnL de un trade y recalcula score si hay suficientes datos."""
        cat = category.lower().strip()
        if not cat:
            return
        if cat not in self._trades:
            self._trades[cat] = []
        self._trades[cat].append(pnl)
        self._recalculate(cat)

    # --- Internal ---

    def _recalculate(self, category: str) -> None:
        trades = self._trades.get(category, [])
        if len(trades) < self._min_trades:
            return
        wins = sum(1 for p in trades if p > 0)
        wr = wins / len(trades)
        total_pnl = sum(trades)

        wr_score = wr * 60.0
        pnl_clamped = max(min(total_pnl, 20.0), -20.0)
        pnl_score = ((pnl_clamped + 20.0) / 40.0) * 40.0

        new_score = wr_score + pnl_score
        old_score = self._scores.get(category, 0.0)
        alpha = 0.3
        blended = alpha * new_score + (1.0 - alpha) * old_score
        self._scores[category] = min(100.0, max(0.0, blended))

        logger.info(
            "CategoryScorer: %s WR=%.0f%% PnL=$%.2f score %.0f→%.0f (n=%d)",
            category, wr * 100, total_pnl, old_score, self._scores[category], len(trades),
        )
