"""Deteccion de flujo toxico / adverse selection (TODO 4.4).

Por cada fill, mide el cambio de mid_price en los 5s post-fill.
Si el BUY fue seguido por mid cayendo 1%+ → adverse selection detectada.
Marca el mercado como "toxic" por 1 hora.

El MarketMaker consulta antes de quoting.
"""

import logging
import time
from collections import deque
from typing import Any

logger = logging.getLogger("nachomarket.toxic_flow")

_ADVERSE_SELECTION_THRESHOLD = 0.01   # 1% movimiento adverso
_TOXIC_DURATION_SEC = 3600.0          # 1 hora de cuarentena
_EMA_ALPHA = 0.3                       # Suavizado EMA del toxicity score
_TOXIC_SCORE_THRESHOLD = 0.6          # Score > 0.6 → toxic
_OBSERVATION_WINDOW_SEC = 5.0         # 5s post-fill


class ToxicFlowDetector:
    """Detecta mercados con flujo adverso (informed traders) y los pone en cuarentena.

    Uso:
        detector = ToxicFlowDetector()
        # Registrar fill y precio mid justo antes
        detector.record_fill("token_abc", side="BUY", fill_price=0.52, mid_before=0.50)
        # 5s despues, registrar mid actual
        detector.observe_post_fill("token_abc", mid_after=0.48)
        # Verificar antes de quoting
        if detector.is_toxic("token_abc"):
            skip market making
    """

    def __init__(
        self,
        adverse_threshold: float = _ADVERSE_SELECTION_THRESHOLD,
        toxic_duration_sec: float = _TOXIC_DURATION_SEC,
        ema_alpha: float = _EMA_ALPHA,
        toxic_score_threshold: float = _TOXIC_SCORE_THRESHOLD,
    ) -> None:
        self._adverse_threshold = adverse_threshold
        self._toxic_duration = toxic_duration_sec
        self._alpha = ema_alpha
        self._score_threshold = toxic_score_threshold

        # {token_id: {"side": str, "mid_before": float, "fill_time": float}}
        self._pending_observations: dict[str, dict[str, Any]] = {}

        # {token_id: float} — EMA del toxicity score [0, 1]
        self._toxicity_scores: dict[str, float] = {}

        # {token_id: float} — timestamp hasta cuando esta en cuarentena
        self._quarantine_until: dict[str, float] = {}

        # Historial de fills para analisis
        self._fill_history: dict[str, deque] = {}

    # ------------------------------------------------------------------
    # API publica
    # ------------------------------------------------------------------

    def record_fill(
        self,
        token_id: str,
        side: str,
        fill_price: float,
        mid_before: float,
    ) -> None:
        """Registra un fill para observacion post-fill.

        Args:
            token_id: Token del mercado.
            side: 'BUY' o 'SELL'.
            fill_price: Precio al que se ejecuto el fill.
            mid_before: Mid price justo antes del fill.
        """
        self._pending_observations[token_id] = {
            "side": side,
            "fill_price": fill_price,
            "mid_before": mid_before,
            "fill_time": time.time(),
        }

    def observe_post_fill(self, token_id: str, mid_after: float) -> bool:
        """Evalua si el fill fue adverso comparando mid_before vs mid_after.

        Llama a esto ~5 segundos despues de record_fill().

        Returns:
            True si se detecta adverse selection en este fill.
        """
        obs = self._pending_observations.pop(token_id, None)
        if obs is None:
            return False

        elapsed = time.time() - obs["fill_time"]
        if elapsed > _OBSERVATION_WINDOW_SEC * 3:
            return False  # Observacion muy tardia, ignorar

        side = obs["side"]
        mid_before = obs["mid_before"]

        if mid_before <= 0 or mid_after <= 0:
            return False

        # Calculo del movimiento adverso
        if side == "BUY":
            # Adverse: compramos y el precio baja
            adverse_move = (mid_before - mid_after) / mid_before
        else:
            # Adverse: vendimos y el precio sube
            adverse_move = (mid_after - mid_before) / mid_before

        is_adverse = adverse_move > self._adverse_threshold

        # Actualizar EMA del toxicity score
        current_score = self._toxicity_scores.get(token_id, 0.0)
        new_observation = 1.0 if is_adverse else 0.0
        new_score = self._alpha * new_observation + (1 - self._alpha) * current_score
        self._toxicity_scores[token_id] = new_score

        logger.debug(
            "Post-fill %s: adverse_move=%.3f%% score=%.2f→%.2f %s",
            token_id[:8],
            adverse_move * 100,
            current_score, new_score,
            "⚠️ ADVERSE" if is_adverse else "✓",
        )

        # Si el score supera el threshold, poner en cuarentena
        if new_score >= self._score_threshold:
            self._quarantine_until[token_id] = time.time() + self._toxic_duration
            logger.warning(
                "TOXIC FLOW: %s en cuarentena por %.0fmin (score=%.2f)",
                token_id[:8],
                self._toxic_duration / 60,
                new_score,
            )

        return is_adverse

    def is_toxic(self, token_id: str) -> bool:
        """Retorna True si el token esta en cuarentena por flujo adverso."""
        quarantine_end = self._quarantine_until.get(token_id)
        if quarantine_end is None:
            return False

        if time.time() > quarantine_end:
            # Cuarentena expirada
            del self._quarantine_until[token_id]
            return False

        return True

    def get_toxicity_score(self, token_id: str) -> float:
        """Retorna el toxicity score EMA actual [0.0, 1.0]."""
        return self._toxicity_scores.get(token_id, 0.0)

    def get_quarantined_tokens(self) -> list[str]:
        """Retorna lista de tokens actualmente en cuarentena."""
        now = time.time()
        expired = [t for t, end in self._quarantine_until.items() if now > end]
        for t in expired:
            del self._quarantine_until[t]
        return list(self._quarantine_until.keys())

    def clear_token(self, token_id: str) -> None:
        """Limpia toda la informacion de un token (ej. al desuscribir)."""
        self._pending_observations.pop(token_id, None)
        self._toxicity_scores.pop(token_id, None)
        self._quarantine_until.pop(token_id, None)

    def summary(self) -> dict[str, Any]:
        """Retorna resumen del estado del detector."""
        quarantined = self.get_quarantined_tokens()
        return {
            "quarantined_count": len(quarantined),
            "quarantined_tokens": [t[:8] for t in quarantined],
            "tracked_tokens": len(self._toxicity_scores),
            "avg_toxicity_score": (
                sum(self._toxicity_scores.values()) / len(self._toxicity_scores)
                if self._toxicity_scores else 0.0
            ),
        }
