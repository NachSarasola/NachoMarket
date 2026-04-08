import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, OrderType

from src.utils.resilience import retry_with_backoff

load_dotenv()
logger = logging.getLogger("nachomarket.client")


class PolymarketClient:
    """Wrapper sobre py-clob-client con retry, logging y seguridad."""

    def __init__(self, paper_mode: bool = True) -> None:
        self.paper_mode = paper_mode
        self._trades_file = Path("data/trades.jsonl")
        self._trades_file.parent.mkdir(parents=True, exist_ok=True)

        if not paper_mode:
            self._client = ClobClient(
                host="https://clob.polymarket.com",
                key=os.environ["POLYMARKET_API_KEY"],
                chain_id=137,
                funder=os.environ.get("POLYMARKET_FUNDER"),
            )
            logger.info("Polymarket client initialized in LIVE mode")
        else:
            self._client = None
            logger.info("Polymarket client initialized in PAPER mode")

    @retry_with_backoff(max_attempts=3)
    def get_markets(self) -> list[dict[str, Any]]:
        """Obtiene lista de mercados activos."""
        if self.paper_mode:
            logger.info("Paper mode: returning empty markets")
            return []
        response = self._client.get_markets()
        return response if isinstance(response, list) else []

    @retry_with_backoff(max_attempts=3)
    def get_orderbook(self, token_id: str) -> dict[str, Any]:
        """Obtiene el orderbook de un mercado."""
        if self.paper_mode:
            return {"bids": [], "asks": []}
        return self._client.get_order_book(token_id)

    @retry_with_backoff(max_attempts=3)
    def get_price(self, token_id: str) -> float:
        """Obtiene el precio mid de un token."""
        if self.paper_mode:
            return 0.5
        book = self.get_orderbook(token_id)
        bids = book.get("bids", [])
        asks = book.get("asks", [])
        if not bids or not asks:
            return 0.0
        best_bid = float(bids[0]["price"])
        best_ask = float(asks[0]["price"])
        return (best_bid + best_ask) / 2

    @retry_with_backoff(max_attempts=3)
    def get_fee_rate_bps(self) -> int:
        """Obtiene el fee rate actual en basis points. SIEMPRE verificar antes de operar."""
        if self.paper_mode:
            return 0
        # El fee rate se obtiene del endpoint de la API
        try:
            return self._client.get_tick_size()  # Placeholder - usar endpoint correcto
        except Exception:
            logger.warning("Could not fetch fee rate, defaulting to 200 bps")
            return 200

    def place_order(
        self,
        token_id: str,
        side: str,
        price: float,
        size: float,
        post_only: bool = True,
    ) -> dict[str, Any] | None:
        """Coloca una orden. Siempre loguea a trades.jsonl."""
        trade_record = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "token_id": token_id,
            "side": side,
            "price": price,
            "size": size,
            "post_only": post_only,
            "paper_mode": self.paper_mode,
            "status": "pending",
        }

        if self.paper_mode:
            trade_record["status"] = "filled_paper"
            trade_record["order_id"] = f"paper_{datetime.now(timezone.utc).timestamp()}"
            self._log_trade(trade_record)
            logger.info(f"Paper trade: {side} {size} @ {price} on {token_id[:8]}...")
            return trade_record

        try:
            order_args = OrderArgs(
                token_id=token_id,
                price=price,
                size=size,
                side=side,
                fee_rate_bps=self.get_fee_rate_bps(),
            )
            if post_only:
                result = self._client.create_and_post_order(order_args)
            else:
                result = self._client.create_and_post_order(order_args)

            trade_record["status"] = "submitted"
            trade_record["order_id"] = result.get("orderID", "unknown")
            self._log_trade(trade_record)
            logger.info(f"Order placed: {side} {size} @ {price} | ID: {trade_record['order_id']}")
            return trade_record
        except Exception:
            trade_record["status"] = "error"
            self._log_trade(trade_record)
            logger.exception(f"Failed to place order: {side} {size} @ {price}")
            raise

    @retry_with_backoff(max_attempts=3)
    def cancel_order(self, order_id: str) -> bool:
        """Cancela una orden."""
        if self.paper_mode:
            logger.info(f"Paper mode: cancelled order {order_id}")
            return True
        self._client.cancel(order_id)
        logger.info(f"Cancelled order: {order_id}")
        return True

    @retry_with_backoff(max_attempts=3)
    def cancel_all_orders(self) -> bool:
        """Cancela todas las ordenes abiertas."""
        if self.paper_mode:
            logger.info("Paper mode: cancelled all orders")
            return True
        self._client.cancel_all()
        logger.info("Cancelled all open orders")
        return True

    @retry_with_backoff(max_attempts=3)
    def get_balance(self) -> float:
        """Obtiene balance USDC disponible."""
        if self.paper_mode:
            return 400.0  # Capital simulado
        # Implementar segun API de Polymarket
        return 0.0

    def _log_trade(self, trade: dict[str, Any]) -> None:
        """Append trade a trades.jsonl."""
        with open(self._trades_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(trade) + "\n")
