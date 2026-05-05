"""EdgeFilter: filtro de edge minimo variable por nivel de confidence.

El edge es la diferencia absoluta entre la probabilidad estimada
y el precio de mercado: edge = |estimated_prob - market_price|.

Umbrales adaptativos:
  - High confidence (>0.80): edge >= 0.04 (4%)
  - Medium confidence (0.60-0.80): edge >= 0.05 (5%)
  - Low confidence (0.40-0.60): edge >= 0.08 (8%)
  - Below 0.40: descartar siempre

Los umbrales son configurables via YAML.
"""

from __future__ import annotations

from typing import Any

_HIGH_CONFIDENCE = 0.80
_MEDIUM_CONFIDENCE = 0.60
_MIN_CONFIDENCE = 0.40

_HIGH_EDGE = 0.04
_MED_EDGE = 0.05
_LOW_EDGE = 0.08


class EdgeFilter:
    """Filtro de edge con thresholds adaptativos por confidence."""

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        cfg = (config or {}).get("edge_filter", {})
        self._high_threshold = cfg.get("high_confidence_edge", _HIGH_EDGE)
        self._med_threshold = cfg.get("medium_confidence_edge", _MED_EDGE)
        self._low_threshold = cfg.get("low_confidence_edge", _LOW_EDGE)
        self._min_confidence = cfg.get("min_confidence", _MIN_CONFIDENCE)

    def has_sufficient_edge(
        self, estimated_prob: float, market_price: float, confidence: float
    ) -> tuple[bool, float]:
        """Evalua si el edge es suficiente.

        Args:
            estimated_prob: Probabilidad estimada (0.0 - 1.0).
            market_price: Precio de mercado actual (0.0 - 1.0).
            confidence: Nivel de confianza en la estimacion (0.0 - 1.0).

        Returns:
            (passes, edge_amount) donde passes=True si el edge supera
            el umbral correspondiente al nivel de confidence.
        """
        if confidence < self._min_confidence:
            return False, 0.0
        if market_price <= 0.0 or market_price >= 1.0:
            return False, 0.0
        if estimated_prob <= 0.0 or estimated_prob >= 1.0:
            return False, 0.0

        edge = abs(estimated_prob - market_price)

        if confidence >= _HIGH_CONFIDENCE:
            return edge >= self._high_threshold, edge
        if confidence >= _MEDIUM_CONFIDENCE:
            return edge >= self._med_threshold, edge
        return edge >= self._low_threshold, edge

    @property
    def thresholds(self) -> dict[str, float]:
        """Snapshot de thresholds actuales."""
        return {
            "high_confidence_edge": self._high_threshold,
            "medium_confidence_edge": self._med_threshold,
            "low_confidence_edge": self._low_threshold,
            "min_confidence": self._min_confidence,
        }
