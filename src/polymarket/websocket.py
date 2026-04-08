"""
WebSocket feed para orderbook real-time de Polymarket.

Conecta a wss://ws-subscriptions-clob.polymarket.com/ws/market y mantiene
un diccionario en memoria con el estado actual de cada orderbook.

Thread safety: threading.Lock protege _orderbooks para lecturas sincronas
desde los threads de estrategia mientras el loop asyncio escribe.
"""

import asyncio
import json
import logging
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

import yaml
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed, ConnectionClosedError, WebSocketException

logger = logging.getLogger("nachomarket.websocket")

WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

# Umbrales para disparar callbacks
MIDPOINT_CHANGE_THRESHOLD = 0.02   # 2% cambio en midpoint
DEPTH_CHANGE_THRESHOLD = 0.10      # 10% cambio en depth
DEPTH_LEVELS = 5                   # Niveles que se suman para calcular depth

# Backoff exponencial manual (ademas del built-in de websockets 16)
BACKOFF_BASE = 1.0
BACKOFF_MAX = 60.0
BACKOFF_MULTIPLIER = 2.0


@dataclass
class OrderbookState:
    """Estado actual del orderbook de un token."""

    token_id: str
    bids: list[tuple[float, float]] = field(default_factory=list)  # (price, size) desc
    asks: list[tuple[float, float]] = field(default_factory=list)  # (price, size) asc
    midpoint: float = 0.0
    depth: float = 0.0          # Suma de sizes en top-N niveles (ambos lados)
    last_updated: float = 0.0   # unix timestamp
    sequence: int = 0


# Tipo de callback: recibe token_id, orderbook actual y tipo de cambio
ChangeCallback = Callable[[str, OrderbookState, str], None]


class OrderbookFeed:
    """Feed WebSocket para orderbooks de Polymarket.

    Uso tipico:
        feed = OrderbookFeed.from_config("config/markets.yaml")
        feed.subscribe("token_id_1", condition_id="cond_1", callback=my_fn)
        asyncio.run(feed.start())

    El callback recibe (token_id, orderbook_state, change_type) donde
    change_type es 'midpoint' o 'depth'.
    """

    def __init__(self) -> None:
        # token_id → list de (condition_id, callback)
        self._subscriptions: dict[str, list[tuple[str, ChangeCallback]]] = {}
        # token_id → OrderbookState (protegido por _lock)
        self._orderbooks: dict[str, OrderbookState] = {}
        self._lock = threading.Lock()
        self._running = False
        self._connected = False
        # Referencia a la conexion activa para suscripciones dinamicas
        self._ws_ref: Any | None = None
        self._ws_lock = asyncio.Lock()  # Para send() concurrente

    # ------------------------------------------------------------------
    # API publica — thread-safe
    # ------------------------------------------------------------------

    @classmethod
    def from_config(cls, config_path: str = "config/markets.yaml") -> "OrderbookFeed":
        """Crea un feed pre-configurado leyendo token_ids de markets.yaml."""
        feed = cls()
        path = Path(config_path)
        if not path.exists():
            logger.warning(f"Config no encontrada: {config_path}")
            return feed

        with open(path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f) or {}

        watch = config.get("watch_tokens", [])
        for entry in watch:
            token_id = entry.get("token_id", "")
            condition_id = entry.get("condition_id", "")
            if token_id:
                feed._subscriptions[token_id] = []
                logger.info(f"Pre-configurado para escuchar: {token_id[:8]}...")
                # condition_id se almacena internamente para el mensaje de suscripcion
                feed._condition_ids = getattr(feed, "_condition_ids", {})
                feed._condition_ids[token_id] = condition_id

        return feed

    def subscribe(
        self,
        token_id: str,
        callback: ChangeCallback,
        condition_id: str = "",
    ) -> None:
        """Registra un callback para cambios significativos en un token.

        Args:
            token_id: ID del token de Polymarket.
            callback: Funcion llamada con (token_id, OrderbookState, change_type).
            condition_id: Condition ID del mercado (necesario para el mensaje WS).
        """
        with self._lock:
            if token_id not in self._subscriptions:
                self._subscriptions[token_id] = []
            self._subscriptions[token_id].append((condition_id, callback))

            # Inicializar estado vacio si no existe
            if token_id not in self._orderbooks:
                self._orderbooks[token_id] = OrderbookState(token_id=token_id)

        logger.info(f"Suscrito a {token_id[:8]}... (condition={condition_id[:8] if condition_id else 'N/A'}...)")

        # Si ya hay una conexion activa, suscribir en caliente
        if self._connected and self._ws_ref is not None:
            asyncio.create_task(self._send_subscribe(token_id, condition_id))

    def unsubscribe(self, token_id: str) -> None:
        """Elimina todas las suscripciones de un token."""
        with self._lock:
            self._subscriptions.pop(token_id, None)
            self._orderbooks.pop(token_id, None)
        logger.info(f"Desuscrito de {token_id[:8]}...")

    def get_orderbook(self, token_id: str) -> OrderbookState | None:
        """Retorna el estado actual del orderbook. Thread-safe."""
        with self._lock:
            return self._orderbooks.get(token_id)

    def get_midpoint(self, token_id: str) -> float | None:
        """Retorna el midpoint actual. Thread-safe. Retorna None si no hay datos."""
        with self._lock:
            ob = self._orderbooks.get(token_id)
            return ob.midpoint if ob and ob.midpoint > 0 else None

    def get_all_midpoints(self) -> dict[str, float]:
        """Retorna midpoints de todos los tokens suscritos. Thread-safe."""
        with self._lock:
            return {
                tid: ob.midpoint
                for tid, ob in self._orderbooks.items()
                if ob.midpoint > 0
            }

    def is_connected(self) -> bool:
        """Indica si el WebSocket esta conectado."""
        return self._connected

    # ------------------------------------------------------------------
    # Ciclo de vida
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Inicia el feed con reconexion automatica y backoff exponencial.

        Este metodo corre indefinidamente hasta que stop() sea llamado.
        """
        self._running = True
        backoff = BACKOFF_BASE

        logger.info("OrderbookFeed iniciando...")

        while self._running:
            try:
                # websockets 16: async with connect() hace un intento unico.
                # El backoff lo manejamos manualmente para tener control total.
                async with connect(
                    WS_URL,
                    ping_interval=30,
                    ping_timeout=10,
                    open_timeout=15,
                    max_size=2 * 1024 * 1024,  # 2 MB por mensaje
                ) as ws:
                    self._ws_ref = ws
                    self._connected = True
                    backoff = BACKOFF_BASE  # Reset backoff en conexion exitosa
                    logger.info(f"WebSocket conectado a {WS_URL}")

                    # Suscribir a todos los tokens configurados
                    await self._subscribe_all(ws)

                    # Loop de mensajes
                    await self._message_loop(ws)

            except ConnectionClosedOK:
                logger.info("WebSocket cerrado correctamente")
                if not self._running:
                    break

            except (ConnectionClosed, ConnectionClosedError) as e:
                logger.warning(f"WebSocket desconectado: {e} — reconectando en {backoff:.1f}s")

            except WebSocketException as e:
                logger.error(f"Error WebSocket: {e} — reconectando en {backoff:.1f}s")

            except OSError as e:
                logger.error(f"Error de red: {e} — reconectando en {backoff:.1f}s")

            except Exception:
                logger.exception(f"Error inesperado en WS — reconectando en {backoff:.1f}s")

            finally:
                self._connected = False
                self._ws_ref = None

            if not self._running:
                break

            await asyncio.sleep(backoff)
            backoff = min(backoff * BACKOFF_MULTIPLIER, BACKOFF_MAX)

        logger.info("OrderbookFeed detenido")

    async def stop(self) -> None:
        """Senaliza el stop y cierra la conexion activa."""
        self._running = False
        if self._ws_ref is not None:
            try:
                await self._ws_ref.close()
            except Exception:
                pass
        logger.info("OrderbookFeed: stop solicitado")

    # ------------------------------------------------------------------
    # Suscripciones WebSocket
    # ------------------------------------------------------------------

    async def _subscribe_all(self, ws: Any) -> None:
        """Envia mensajes de suscripcion para todos los tokens registrados."""
        with self._lock:
            tokens = list(self._subscriptions.keys())

        for token_id in tokens:
            condition_id = self._get_condition_id(token_id)
            await self._send_subscribe_ws(ws, token_id, condition_id)

    async def _send_subscribe(self, token_id: str, condition_id: str) -> None:
        """Envia suscripcion a la conexion activa (usado para suscripciones en caliente)."""
        if self._ws_ref is not None:
            await self._send_subscribe_ws(self._ws_ref, token_id, condition_id)

    async def _send_subscribe_ws(self, ws: Any, token_id: str, condition_id: str) -> None:
        """Envia el mensaje de suscripcion al WebSocket."""
        msg: dict[str, Any] = {
            "type": "subscribe",
            "assets_ids": [token_id],
        }
        if condition_id:
            msg["markets"] = [condition_id]

        async with self._ws_lock:
            try:
                await ws.send(json.dumps(msg))
                logger.debug(f"Suscripcion enviada: {token_id[:8]}...")
            except Exception:
                logger.exception(f"Error enviando suscripcion para {token_id[:8]}...")

    def _get_condition_id(self, token_id: str) -> str:
        """Busca el condition_id para un token_id."""
        condition_ids = getattr(self, "_condition_ids", {})
        if token_id in condition_ids:
            return condition_ids[token_id]
        # Buscar en callbacks registrados
        callbacks = self._subscriptions.get(token_id, [])
        if callbacks:
            return callbacks[0][0]  # condition_id esta en la primera posicion
        return ""

    # ------------------------------------------------------------------
    # Loop de mensajes
    # ------------------------------------------------------------------

    async def _message_loop(self, ws: Any) -> None:
        """Escucha mensajes del WebSocket y los despacha."""
        async for raw_message in ws:
            if not self._running:
                break
            try:
                await self._process_message(raw_message)
            except json.JSONDecodeError:
                logger.warning(f"JSON invalido recibido: {str(raw_message)[:120]}")
            except Exception:
                logger.exception("Error procesando mensaje WS")

    async def _process_message(self, raw: str | bytes) -> None:
        """Parsea y despacha un mensaje del WebSocket."""
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")

        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            logger.warning(f"JSON invalido recibido: {str(raw)[:120]}")
            return

        # Polymarket puede enviar una lista de eventos o un evento solo
        if isinstance(data, list):
            for event in data:
                await self._dispatch_event(event)
        elif isinstance(data, dict):
            await self._dispatch_event(data)

    async def _dispatch_event(self, event: dict[str, Any]) -> None:
        """Despacha un evento segun su tipo."""
        event_type = event.get("event_type", event.get("type", ""))
        token_id = event.get("asset_id", "")

        if not token_id:
            return

        with self._lock:
            if token_id not in self._subscriptions:
                return  # Token no suscrito

        if event_type == "book":
            await self._handle_book_snapshot(token_id, event)
        elif event_type == "price_change":
            await self._handle_price_change(token_id, event)
        elif event_type == "tick_size_change":
            logger.debug(f"tick_size_change para {token_id[:8]}...")
        elif event_type in ("last_trade_price", "trade"):
            logger.debug(f"Trade event para {token_id[:8]}...")
        else:
            logger.debug(f"Evento desconocido '{event_type}' para {token_id[:8]}...")

    # ------------------------------------------------------------------
    # Procesamiento de eventos
    # ------------------------------------------------------------------

    async def _handle_book_snapshot(self, token_id: str, event: dict[str, Any]) -> None:
        """Procesa un snapshot completo del orderbook."""
        raw_bids = event.get("bids", [])
        raw_asks = event.get("asks", [])

        bids = _parse_levels(raw_bids, reverse=True)   # Desc por precio
        asks = _parse_levels(raw_asks, reverse=False)  # Asc por precio

        midpoint = _compute_midpoint(bids, asks)
        depth = _compute_depth(bids, asks)

        new_ob = OrderbookState(
            token_id=token_id,
            bids=bids,
            asks=asks,
            midpoint=midpoint,
            depth=depth,
            last_updated=time.time(),
            sequence=int(event.get("sequence", 0)),
        )

        old_ob = self._atomic_update(token_id, new_ob)
        await self._check_and_fire_callbacks(token_id, old_ob, new_ob)

        logger.debug(
            f"Book snapshot {token_id[:8]}... "
            f"bids={len(bids)} asks={len(asks)} mid={midpoint:.4f}"
        )

    async def _handle_price_change(self, token_id: str, event: dict[str, Any]) -> None:
        """Procesa un cambio de precio (actualizacion parcial del orderbook)."""
        # price_change puede ser un cambio en bid o ask
        price = float(event.get("price", 0))
        size = float(event.get("size", 0))
        side = event.get("side", "").upper()

        with self._lock:
            ob = self._orderbooks.get(token_id)
            if ob is None:
                return
            # Clonar listas para no mutar el estado compartido
            bids = list(ob.bids)
            asks = list(ob.asks)

        if side == "BUY":
            bids = _apply_level_update(bids, price, size, reverse=True)
        elif side == "SELL":
            asks = _apply_level_update(asks, price, size, reverse=False)

        midpoint = _compute_midpoint(bids, asks)
        depth = _compute_depth(bids, asks)

        with self._lock:
            old_ob = self._orderbooks.get(token_id)

        new_ob = OrderbookState(
            token_id=token_id,
            bids=bids,
            asks=asks,
            midpoint=midpoint,
            depth=depth,
            last_updated=time.time(),
            sequence=(old_ob.sequence + 1) if old_ob else 0,
        )

        self._atomic_update(token_id, new_ob)
        await self._check_and_fire_callbacks(token_id, old_ob, new_ob)

    def _atomic_update(self, token_id: str, new_ob: OrderbookState) -> OrderbookState | None:
        """Actualiza el orderbook de forma atomica. Retorna el estado anterior."""
        with self._lock:
            old_ob = self._orderbooks.get(token_id)
            self._orderbooks[token_id] = new_ob
        return old_ob

    # ------------------------------------------------------------------
    # Deteccion de cambios y callbacks
    # ------------------------------------------------------------------

    async def _check_and_fire_callbacks(
        self,
        token_id: str,
        old_ob: OrderbookState | None,
        new_ob: OrderbookState,
    ) -> None:
        """Verifica si hubo cambio significativo y dispara callbacks."""
        if old_ob is None or old_ob.last_updated == 0:
            # Primer snapshot con datos reales: siempre notificar
            await self._fire_callbacks(token_id, new_ob, "book_init")
            return

        # --- Cambio en midpoint ---
        if old_ob.midpoint > 0 and new_ob.midpoint > 0:
            mid_change = abs(new_ob.midpoint - old_ob.midpoint) / old_ob.midpoint
            if mid_change >= MIDPOINT_CHANGE_THRESHOLD:
                logger.info(
                    f"Cambio significativo midpoint {token_id[:8]}...: "
                    f"{old_ob.midpoint:.4f} → {new_ob.midpoint:.4f} "
                    f"({mid_change * 100:.1f}%)"
                )
                await self._fire_callbacks(token_id, new_ob, "midpoint")

        # --- Cambio en depth ---
        if old_ob.depth > 0 and new_ob.depth > 0:
            depth_change = abs(new_ob.depth - old_ob.depth) / old_ob.depth
            if depth_change >= DEPTH_CHANGE_THRESHOLD:
                logger.info(
                    f"Cambio significativo depth {token_id[:8]}...: "
                    f"{old_ob.depth:.2f} → {new_ob.depth:.2f} "
                    f"({depth_change * 100:.1f}%)"
                )
                await self._fire_callbacks(token_id, new_ob, "depth")

    async def _fire_callbacks(
        self,
        token_id: str,
        ob: OrderbookState,
        change_type: str,
    ) -> None:
        """Ejecuta los callbacks registrados para un token."""
        with self._lock:
            callbacks = list(self._subscriptions.get(token_id, []))

        for _condition_id, callback in callbacks:
            try:
                # Los callbacks pueden ser sync o async
                if asyncio.iscoroutinefunction(callback):
                    await callback(token_id, ob, change_type)
                else:
                    # Ejecutar sync callback en el executor para no bloquear el loop
                    loop = asyncio.get_event_loop()
                    await loop.run_in_executor(None, callback, token_id, ob, change_type)
            except Exception:
                logger.exception(
                    f"Error en callback para {token_id[:8]}... ({change_type})"
                )


# ------------------------------------------------------------------
# Helpers funcionales (sin estado)
# ------------------------------------------------------------------

def _parse_levels(
    raw: list[dict[str, Any]],
    reverse: bool,
) -> list[tuple[float, float]]:
    """Convierte lista de {price, size} a lista de tuplas (float, float) ordenada."""
    levels: list[tuple[float, float]] = []
    for level in raw:
        try:
            price = float(level.get("price", level.get("p", 0)))
            size = float(level.get("size", level.get("s", 0)))
            if price > 0 and size > 0:
                levels.append((price, size))
        except (ValueError, TypeError):
            continue
    return sorted(levels, key=lambda x: x[0], reverse=reverse)


def _apply_level_update(
    levels: list[tuple[float, float]],
    price: float,
    size: float,
    reverse: bool,
) -> list[tuple[float, float]]:
    """Aplica una actualizacion de nivel al orderbook.

    Si size == 0, elimina el nivel. Si existe, lo actualiza. Si no, lo agrega.
    """
    updated = [(p, s) for p, s in levels if p != price]
    if size > 0:
        updated.append((price, size))
    return sorted(updated, key=lambda x: x[0], reverse=reverse)


def _compute_midpoint(
    bids: list[tuple[float, float]],
    asks: list[tuple[float, float]],
) -> float:
    """Calcula el midpoint como promedio del mejor bid y mejor ask."""
    if not bids or not asks:
        return 0.0
    best_bid = bids[0][0]   # Mayor precio de compra
    best_ask = asks[0][0]   # Menor precio de venta
    if best_bid >= best_ask:
        return 0.0  # Orderbook cruzado, datos invalidos
    return (best_bid + best_ask) / 2


def _compute_depth(
    bids: list[tuple[float, float]],
    asks: list[tuple[float, float]],
    levels: int = DEPTH_LEVELS,
) -> float:
    """Calcula la profundidad como suma de sizes en los top-N niveles de cada lado."""
    bid_depth = sum(size for _, size in bids[:levels])
    ask_depth = sum(size for _, size in asks[:levels])
    return bid_depth + ask_depth
