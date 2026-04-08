import asyncio
import json
import logging
from typing import Any, Callable

import websockets

from src.utils.resilience import retry_with_backoff

logger = logging.getLogger("nachomarket.websocket")

POLYMARKET_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


class OrderbookFeed:
    """WebSocket feed para orderbook real-time de Polymarket."""

    def __init__(self) -> None:
        self._subscriptions: dict[str, list[Callable]] = {}
        self._ws: websockets.WebSocketClientProtocol | None = None
        self._running: bool = False

    def subscribe(self, token_id: str, callback: Callable[[dict[str, Any]], None]) -> None:
        """Suscribe un callback para actualizaciones de un token."""
        if token_id not in self._subscriptions:
            self._subscriptions[token_id] = []
        self._subscriptions[token_id].append(callback)
        logger.info(f"Subscribed to orderbook updates for {token_id[:8]}...")

    def unsubscribe(self, token_id: str) -> None:
        """Desuscribe de un token."""
        self._subscriptions.pop(token_id, None)
        logger.info(f"Unsubscribed from {token_id[:8]}...")

    async def start(self) -> None:
        """Inicia la conexion WebSocket con auto-reconnect."""
        self._running = True
        while self._running:
            try:
                await self._connect_and_listen()
            except Exception:
                logger.exception("WebSocket connection error, reconnecting in 5s...")
                await asyncio.sleep(5)

    async def stop(self) -> None:
        """Detiene el feed."""
        self._running = False
        if self._ws:
            await self._ws.close()
            logger.info("WebSocket feed stopped")

    async def _connect_and_listen(self) -> None:
        """Conecta al WebSocket y procesa mensajes."""
        async with websockets.connect(POLYMARKET_WS_URL) as ws:
            self._ws = ws
            logger.info("WebSocket connected to Polymarket")

            # Suscribir a todos los tokens registrados
            for token_id in self._subscriptions:
                subscribe_msg = {
                    "type": "subscribe",
                    "channel": "market",
                    "assets_id": token_id,
                }
                await ws.send(json.dumps(subscribe_msg))

            # Escuchar mensajes
            async for message in ws:
                try:
                    data = json.loads(message)
                    asset_id = data.get("asset_id", "")
                    if asset_id in self._subscriptions:
                        for callback in self._subscriptions[asset_id]:
                            callback(data)
                except json.JSONDecodeError:
                    logger.warning(f"Invalid JSON from WebSocket: {message[:100]}")
                except Exception:
                    logger.exception("Error processing WebSocket message")
