import logging
import time
from typing import Any

from src.strategy.base import BaseStrategy, Signal, Trade

logger = logging.getLogger("nachomarket.strategy.multi_arb")


class MultiArbStrategy(BaseStrategy):
    """Arbitraje en mercados multi-outcome (NegRisk markets).

    En un mercado con N outcomes, la suma de precios ask debe ser ~1.0.
    Si sum(ask) < 1.00 - min_edge: comprar 1 share de cada outcome.
    Una share ganara = $1.00, las demas = $0. Net profit = $1 - costo_total.

    Oportunidades duran ~2.7 segundos en promedio. El bot detecta
    pero no fuerza — usa FOK para ejecucion atomica.
    """

    def __init__(self, client, config: dict[str, Any], **kwargs) -> None:
        super().__init__("multi_arb", client, config, **kwargs)
        arb = config.get("multi_arb", {})
        min_edge_raw = arb.get("min_edge", config.get("min_arb_edge_pct", 0.03))
        # Normalizar: si viene como decimal < 1 → porcentaje
        self._min_edge_pct = min_edge_raw * 100 if min_edge_raw < 1 else min_edge_raw
        self._max_position = arb.get("max_position", config.get("arb_order_size_usdc", 5.0))
        self._order_size = arb.get("order_size", self._max_position / 2)

        # Tracking de oportunidades detectadas vs ejecutadas
        self._opportunities_seen = 0
        self._opportunities_executed = 0
        self._last_opportunity_time = 0.0

    # ------------------------------------------------------------------
    # BaseStrategy interface
    # ------------------------------------------------------------------

    def should_act(self, market_data: dict[str, Any]) -> bool:
        """Entra solo si hay multiples outcomes."""
        tokens = market_data.get("tokens", [])
        return len(tokens) >= 2

    def evaluate(self, market_data: dict[str, Any]) -> list[Signal]:
        """Wrapper que llama detect_opportunities() para mantener la interfaz."""
        return self.detect_opportunities(market_data)

    def detect_opportunities(self, market_data: dict[str, Any]) -> list[Signal]:
        """Detecta oportunidades de arbitraje en mercados multi-outcome.

        Para cada evento con multiples outcomes:
        1. Obtener precios ask de TODOS los outcomes
        2. Sumar precios: si sum < 1.00 - min_edge → hay arbitraje
        3. Calcular profit potencial: 1.00 - sum - fees_estimados
        4. Si profit > min_edge (3%): generar senal

        Returns:
            Lista de Signal BUY para cada outcome (comprar 1 de cada).
        """
        tokens = market_data.get("tokens", [])
        market_id = market_data.get("condition_id", market_data.get("market_id", ""))

        if len(tokens) < 2:
            return []

        # --- Obtener precios ask de todos los outcomes ---
        ask_prices = self._get_ask_prices(tokens)
        if not ask_prices:
            return []

        total_ask = sum(ask_prices.values())
        n_outcomes = len(ask_prices)

        # --- Estimar fees totales ---
        estimated_fees = self._estimate_fees(ask_prices)

        # --- Calcular profit potencial ---
        # Comprar 1 share de cada outcome: costo = sum(ask_prices)
        # Una share ganara $1.00, las demas $0
        # Net profit = $1.00 - total_ask - fees
        potential_profit = 1.0 - total_ask - estimated_fees
        profit_pct = potential_profit * 100

        self._logger.debug(
            f"Arb scan: {n_outcomes} outcomes, sum_ask={total_ask:.4f}, "
            f"fees={estimated_fees:.4f}, profit={profit_pct:.2f}%"
        )

        # --- Verificar que profit > min_edge ---
        if profit_pct < self._min_edge_pct:
            return []

        # Oportunidad detectada
        self._opportunities_seen += 1
        self._last_opportunity_time = time.time()

        self._logger.info(
            f"ARB OPPORTUNITY #{self._opportunities_seen}: "
            f"sum_ask={total_ask:.4f}, profit={profit_pct:.2f}%, "
            f"market={market_id[:12]}..."
        )

        # Generar BUY signal para cada outcome
        confidence = min(profit_pct / 10.0, 1.0)  # Mas profit = mas confianza
        signals: list[Signal] = []

        for token_id, ask_price in ask_prices.items():
            signals.append(self._make_signal(
                market_id=market_id,
                token_id=token_id,
                side="BUY",
                price=ask_price,
                size=self._order_size,
                confidence=confidence,
            ))

        return signals

    def execute(self, signals: list[Signal]) -> list[Trade]:
        """Ejecuta arbitraje comprando 1 share de cada outcome con FOK.

        Usa Fill-or-Kill (FOK) para ejecucion atomica:
        - No queremos ejecucion parcial (comprar solo algunos outcomes)
        - Si alguna orden falla, el arb no es rentable

        Si alguna orden falla, cancela las que ya se ejecutaron.
        """
        trades: list[Trade] = []
        if not signals:
            return trades

        successful_trades: list[Trade] = []
        failed = False

        for signal in signals:
            try:
                result = self._client.place_fok_order(
                    token_id=signal.token_id,
                    side=signal.side,
                    price=signal.price,
                    size=signal.size,
                )

                status = result.get("status", "submitted")
                trade = self._make_trade(
                    signal=signal,
                    order_id=result.get("order_id", "unknown"),
                    status=status,
                )
                self.log_trade(trade)
                trades.append(trade)

                # FOK: si el status no es filled, la orden fue rechazada
                if status in ("error", "rejected", "cancelled"):
                    failed = True
                    self._logger.warning(
                        f"FOK order rejected: {signal.side} {signal.token_id[:8]}... "
                        f"@ {signal.price} — status={status}"
                    )
                    break
                else:
                    successful_trades.append(trade)

            except Exception:
                self._logger.exception(
                    f"Error placing FOK arb order: {signal.side} @ {signal.price}"
                )
                trade = self._make_trade(signal=signal, order_id="", status="error")
                self.log_trade(trade)
                trades.append(trade)
                failed = True
                break

        # Si alguna orden fallo, cancelar las exitosas (rollback parcial)
        if failed and successful_trades:
            self._logger.warning(
                f"Arb execution failed — cancelling {len(successful_trades)} "
                f"successful orders for rollback"
            )
            for trade in successful_trades:
                try:
                    self._client.cancel_order(trade.order_id)
                except Exception:
                    self._logger.exception(
                        f"Error cancelling rollback order {trade.order_id}"
                    )

        if not failed:
            self._opportunities_executed += 1
            self._logger.info(
                f"ARB EXECUTED: {len(trades)} orders filled "
                f"(total executed: {self._opportunities_executed}/"
                f"{self._opportunities_seen})"
            )

        return trades

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _get_ask_prices(self, tokens: list[dict[str, Any]]) -> dict[str, float]:
        """Obtiene precios ask de todos los tokens.

        Intenta usar el ask price del orderbook si esta disponible,
        sino usa el precio del token directamente.
        """
        ask_prices: dict[str, float] = {}

        for token in tokens:
            token_id = token.get("token_id", "")
            if not token_id:
                continue

            # Intentar obtener ask del orderbook via cliente
            ask = self._get_best_ask(token_id, token)
            if ask is not None and ask > 0:
                ask_prices[token_id] = ask

        return ask_prices

    def _get_best_ask(self, token_id: str, token_data: dict[str, Any]) -> float | None:
        """Obtiene el mejor ask price para un token."""
        # Si el token ya trae precio (de market_data), usarlo como fallback
        price = float(token_data.get("price", 0))

        # Intentar obtener orderbook para ask price real
        try:
            book = self._client.get_orderbook(token_id)
            asks = book.get("asks", [])
            if asks:
                # Mejor ask = precio mas bajo disponible
                best_ask = min(float(a.get("price", 999)) for a in asks)
                if best_ask < 1.0:
                    return best_ask
        except Exception:
            self._logger.debug(f"Could not fetch orderbook for {token_id[:8]}...")

        return price if price > 0 else None

    def _estimate_fees(self, ask_prices: dict[str, float]) -> float:
        """Estima fees totales para comprar todos los outcomes.

        Polymarket cobra taker fee en FOK orders.
        """
        total_fees = 0.0
        for token_id, ask_price in ask_prices.items():
            try:
                fee_bps = self._client.get_fee_rate(token_id)
            except Exception:
                fee_bps = 20  # Default conservador: 0.20%

            # Fee = size * price * fee_rate
            fee = self._order_size * ask_price * (fee_bps / 10000)
            total_fees += fee

        return total_fees

    def get_stats(self) -> dict[str, Any]:
        """Retorna estadisticas de oportunidades detectadas vs ejecutadas."""
        return {
            "opportunities_seen": self._opportunities_seen,
            "opportunities_executed": self._opportunities_executed,
            "hit_rate": (
                self._opportunities_executed / self._opportunities_seen
                if self._opportunities_seen > 0 else 0.0
            ),
            "last_opportunity_time": self._last_opportunity_time,
        }
