"""Prometheus metrics para monitoreo 24/7.

Expone:
  - nachomarket_orders_placed: Counter por side y strategy
  - nachomarket_orders_cancelled: Counter
  - nachomarket_trades_filled: Counter por side y strategy
  - nachomarket_balance_usd: Gauge
  - nachomarket_open_orders: Gauge
  - nachomarket_daily_pnl: Gauge
  - nachomarket_api_latency_seconds: Histogram por endpoint

Usa prometheus_client. Si no esta instalado, las metricas son no-ops.
"""

from __future__ import annotations

import logging

logger = logging.getLogger("nachomarket.metrics")

_METRICS_AVAILABLE = False

try:
    from prometheus_client import Counter, Gauge, Histogram, start_http_server

    _METRICS_AVAILABLE = True
except ImportError:
    # No-ops: el modulo funciona sin prometheus_client instalado.
    class _NoOp:
        def labels(self, **kwargs):  # type: ignore[no-untyped-def]
            return self

        def inc(self, amount: float = 1.0) -> None:  # type: ignore[no-untyped-def]
            pass

        def set(self, value: float) -> None:  # type: ignore[no-untyped-def]
            pass

        def observe(self, value: float) -> None:  # type: ignore[no-untyped-def]
            pass

    class _CounterNoOp(_NoOp):
        pass

    class _GaugeNoOp(_NoOp):
        pass

    class _HistogramNoOp(_NoOp):
        pass

    Counter = _CounterNoOp  # type: ignore[misc,assignment]
    Gauge = _GaugeNoOp  # type: ignore[misc,assignment]
    Histogram = _HistogramNoOp  # type: ignore[misc,assignment]


# --- Metricas ---

# Contadores
orders_placed = Counter(
    "nachomarket_orders_placed",
    "Total orders placed",
    ["side", "strategy"],
)
orders_cancelled = Counter(
    "nachomarket_orders_cancelled",
    "Total orders cancelled",
)
trades_filled = Counter(
    "nachomarket_trades_filled",
    "Total trades filled",
    ["side", "strategy"],
)

# Gauges
keeper_balance = Gauge(
    "nachomarket_balance_usd",
    "Current balance in USD",
)
open_orders_count = Gauge(
    "nachomarket_open_orders",
    "Number of open orders",
)
daily_pnl = Gauge(
    "nachomarket_daily_pnl",
    "Daily PnL in USD",
)

# Histogramas
api_latency = Histogram(
    "nachomarket_api_latency_seconds",
    "CLOB API request latency",
    ["endpoint"],
    buckets=[0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0],
)


def start_metrics_server(port: int = 9008) -> None:
    """Arranca el servidor HTTP de Prometheus metrics.

    Solo funciona si prometheus_client esta instalado.
    """
    if not _METRICS_AVAILABLE:
        logger.warning(
            "prometheus_client no instalado — metrics server deshabilitado"
        )
        return
    start_http_server(port)
    logger.info("Prometheus metrics server en puerto %d", port)


def is_available() -> bool:
    return _METRICS_AVAILABLE
