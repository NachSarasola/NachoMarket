import itertools
import json
import logging
import os
import threading
import time
from collections import deque
from datetime import datetime, timezone
from functools import wraps
from pathlib import Path
from typing import Any, Callable

from dotenv import load_dotenv
from py_clob_client_v2 import (
    ClobClient,
    ApiCreds,
    AssetType,
    BalanceAllowanceParams,
    MarketOrderArgs,
    OpenOrderParams,
    OrderArgs,
    OrderMarketCancelParams,
    OrderType,
    OrderPayload,
    PartialCreateOrderOptions,
    PostOrdersV2Args,
)
from py_clob_client_v2.clob_types import BookParams

from src.utils.resilience import retry_with_backoff

load_dotenv()
logger = logging.getLogger("nachomarket.client")

CLOB_HOST = "https://clob.polymarket.com"
CHAIN_ID = 137


class RateLimiter:
    """Sliding window rate limiter para APIs de Polymarket.

    Respeta los rate limits del CLOB (9000 req/10s general, 1500/10s /book).
    Usa 400 req/10s (~4.4% del global) como margen seguro.
    """

    def __init__(self, max_requests: int, window_seconds: float = 10.0) -> None:
        self._max_requests = max_requests
        self._window = window_seconds
        self._timestamps: deque[float] = deque()
        self._lock = threading.Lock()

    def acquire(self) -> None:
        """Bloquea hasta que haya capacidad disponible en la ventana."""
        with self._lock:
            now = time.monotonic()
            while self._timestamps and self._timestamps[0] < now - self._window:
                self._timestamps.popleft()
            if len(self._timestamps) >= self._max_requests:
                sleep_time = self._timestamps[0] + self._window - now + 0.02
                if sleep_time > 0:
                    time.sleep(sleep_time)
                return self.acquire()
            self._timestamps.append(now)


def _log_api_call(fn: Callable) -> Callable:
    """Decorator: loguea cada llamada a la API con timestamp, args y resultado.

    Tambien aplica rate limiting para no exceder limites del CLOB.
    """

    @wraps(fn)
    def wrapper(self: "PolymarketClient", *args: Any, **kwargs: Any) -> Any:
        self._rate_limiter.acquire()
        ts = datetime.now(timezone.utc).isoformat()
        arg_summary = ", ".join(
            [repr(a)[:40] for a in args] + [f"{k}={repr(v)[:40]}" for k, v in kwargs.items()]
        )
        logger.debug(f"[{ts}] {fn.__name__}({arg_summary})")
        try:
            result = fn(self, *args, **kwargs)
            logger.debug(f"[{fn.__name__}] OK → {str(result)[:80]}")
            return result
        except Exception as exc:
            logger.warning(f"[{fn.__name__}] ERROR → {type(exc).__name__}: {exc}")
            raise

    return wrapper


class PolymarketClient:
    """Wrapper sobre py-clob-client-v2 con autenticacion, retry y logging completo.

    Convencion oficial de signature_type (py-clob-client-v2):
        signature_type=0 — EOA (MetaMask directo, sin proxy)
        signature_type=1 — POLY_PROXY (Magic Link / email wallets)
        signature_type=2 — POLY_GNOSIS_SAFE (MetaMask + proxy wallet — mas comun)

    Niveles de la API:
        Level 0 — sin auth (mercados, orderbook, precios)
        Level 1 — solo private key (crear ordenes)
        Level 2 — private key + ApiCreds (colocar, cancelar, balance)
    """

    def __init__(
        self,
        paper_mode: bool = True,
        signature_type: int = 1,
        paper_capital: float = 300.0,
    ) -> None:
        self.paper_mode = paper_mode
        self._signature_type = signature_type
        self._paper_capital = paper_capital
        self._trades_file = Path("data/trades.jsonl")
        self._trades_file.parent.mkdir(parents=True, exist_ok=True)
        self._client: ClobClient | None = None
        self._order_counter = itertools.count(1)
        self._rate_limiter = RateLimiter(max_requests=400, window_seconds=10.0)
        # Cache de tokens inválidos (404) para no reconsultar
        self._invalid_tokens: set[str] = set()
        # Cache de tick_size y neg_risk por token (invalidado en tick_size_change WS)
        self._tick_sizes: dict[str, str] = {}
        self._neg_risk: dict[str, bool] = {}
        # Cache de rewards con fallback: si la API falla, usar ultimo valor exitoso
        self._rewards_cache: dict[str, dict[str, Any]] = {}
        self._rewards_cache_file = Path("data/rewards_cache.json")
        self._load_rewards_cache()
        # Heartbeat thread (evita cancelacion automatica de ordenes por inactividad)
        self._heartbeat_thread: threading.Thread | None = None
        self._heartbeat_running: bool = False
        self._heartbeat_id: str = ""

        if not paper_mode:
            self._client = self._build_client(signature_type)
            funder = os.environ.get("POLYMARKET_PROXY_ADDRESS", "") if signature_type == 2 else None
            logger.info(
                f"PolymarketClient inicializado en modo LIVE "
                f"(signature_type={signature_type}, "
                f"signer={self._client.get_address()}, "
                f"funder={funder or 'N/A (EOA mode)'})"
            )
        else:
            logger.info(
                "PolymarketClient inicializado en modo PAPER "
                f"(capital simulado=${paper_capital:.2f} pUSD)"
            )

    # ------------------------------------------------------------------
    # Heartbeat (mantiene la sesion viva para evitar cancelacion automatica)
    # ------------------------------------------------------------------

    def start_heartbeat(self, interval_sec: float = 5.0) -> None:
        """Inicia un thread daemon que envia heartbeat cada N segundos.

        Polymarket cancela todas las ordenes abiertas si no recibe heartbeat
        dentro de ~10-15 segundos. Es CRITICO para market making.
        """
        if self.paper_mode or self._client is None:
            return
        self._heartbeat_running = True
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop,
            args=(interval_sec,),
            daemon=True,
            name="polymarket-heartbeat",
        )
        self._heartbeat_thread.start()
        logger.info("Heartbeat iniciado (interval=%.0fs)", interval_sec)

    def stop_heartbeat(self) -> None:
        """Solicita la detencion del heartbeat thread."""
        self._heartbeat_running = False
        if self._heartbeat_thread is not None:
            self._heartbeat_thread.join(timeout=2.0)
            logger.info("Heartbeat detenido")

    def _heartbeat_loop(self, interval_sec: float) -> None:
        """Loop interno del heartbeat."""
        while self._heartbeat_running and self._client is not None:
            try:
                self._rate_limiter.acquire()
                result = self._client.post_heartbeat(self._heartbeat_id)
                if isinstance(result, dict):
                    self._heartbeat_id = result.get("heartbeat_id", "")
                logger.debug("Heartbeat OK: %s", self._heartbeat_id[:16])
            except Exception as exc:
                err_msg = str(exc)
                if "Invalid Heartbeat ID" in err_msg:
                    # Resetear a string vacio para que el servidor genere uno nuevo
                    self._heartbeat_id = ""
                    logger.info("Heartbeat ID invalido, reseteado")
                else:
                    logger.exception("Heartbeat fallo")
            time.sleep(interval_sec)

    # ------------------------------------------------------------------
    # Inicializacion
    # ------------------------------------------------------------------

    @staticmethod
    def _build_client(signature_type: int) -> ClobClient:
        """Construye el ClobClient leyendo credenciales de .env."""
        private_key = os.environ.get("POLYMARKET_PRIVATE_KEY", "")
        if not private_key:
            raise EnvironmentError("POLYMARKET_PRIVATE_KEY no configurado en .env")

        api_key = os.environ.get("POLYMARKET_API_KEY", "")
        api_secret = os.environ.get("POLYMARKET_SECRET", "")
        api_passphrase = os.environ.get("POLYMARKET_PASSPHRASE", "")

        creds: ApiCreds | None = None
        if api_key and api_secret and api_passphrase:
            creds = ApiCreds(
                api_key=api_key,
                api_secret=api_secret,
                api_passphrase=api_passphrase,
            )

        funder: str | None = None
        if signature_type == 2:
            funder = os.environ.get("POLYMARKET_PROXY_ADDRESS")
            if not funder:
                raise EnvironmentError(
                    "signature_type=2 requiere POLYMARKET_PROXY_ADDRESS en .env"
                )

        return ClobClient(
            host=CLOB_HOST,
            chain_id=CHAIN_ID,
            key=private_key,
            creds=creds,
            signature_type=signature_type,
            funder=funder,
        )

    # ------------------------------------------------------------------
    # Health
    # ------------------------------------------------------------------

    @_log_api_call
    @retry_with_backoff(max_attempts=3)
    def test_connection(self) -> bool:
        """Verifica que el servidor responde y, si hay credenciales, que son validas.

        Returns:
            True si la conexion es exitosa.

        Raises:
            Exception si las credenciales son invalidas o el servidor no responde.
        """
        if self.paper_mode:
            logger.info("Paper mode: test_connection OK (simulado)")
            return True

        # Level 0: health check del servidor
        ok = self._client.get_ok()
        if not ok:
            raise ConnectionError("Polymarket CLOB no responde")

        # Level 2: verificar credenciales si estan configuradas
        if self._client.creds:
            # get_api_keys lanza excepcion si las credenciales son invalidas
            self._client.get_api_keys()
            logger.info("Credenciales Level 2 verificadas correctamente")
        else:
            logger.info("Sin credenciales Level 2; solo endpoints publicos disponibles")

        server_time = self._client.get_server_time()
        logger.info(f"Server time: {server_time}")
        return True

    # ------------------------------------------------------------------
    # Mercados
    # ------------------------------------------------------------------

    @_log_api_call
    @retry_with_backoff(max_attempts=3)
    def get_markets(self, next_cursor: str = "MA==") -> list[dict[str, Any]]:
        """Obtiene lista de mercados activos. Pagina hasta el final automaticamente."""
        if self.paper_mode:
            return []

        all_markets: list[dict[str, Any]] = []
        cursor = next_cursor

        while True:
            response = self._client.get_markets(next_cursor=cursor)
            if not response:
                break

            # La API retorna un dict con data[] y next_cursor
            if isinstance(response, dict):
                all_markets.extend(response.get("data", []))
                cursor = response.get("next_cursor", "LTE=")
                if cursor in ("LTE=", "", None):
                    break
            elif isinstance(response, list):
                all_markets.extend(response)
                break

        logger.info(f"get_markets: {len(all_markets)} mercados obtenidos")
        return all_markets

    # ------------------------------------------------------------------
    # Orderbook y precios
    # ------------------------------------------------------------------

    @_log_api_call
    @retry_with_backoff(max_attempts=3)
    def get_orderbook(self, token_id: str) -> dict[str, Any]:
        """Obtiene el orderbook de un token. Retorna bids/asks como listas de dicts."""
        if self.paper_mode:
            return {"bids": [], "asks": [], "token_id": token_id}

        if token_id in self._invalid_tokens:
            return {"bids": [], "asks": [], "token_id": token_id}

        try:
            obs = self._client.get_order_book(token_id)
        except Exception as e:
            if "404" in str(e) or "No orderbook exists" in str(e):
                self._invalid_tokens.add(token_id)
                return {"bids": [], "asks": [], "token_id": token_id}
            raise

        # py-clob-client-v2 puede retornar dict o objeto con atributos
        if isinstance(obs, dict):
            raw_bids = obs.get("bids", [])
            raw_asks = obs.get("asks", [])
            return {
                "token_id": token_id,
                "bids": [{"price": str(b.get("price", "")), "size": str(b.get("size", ""))} for b in raw_bids],
                "asks": [{"price": str(a.get("price", "")), "size": str(a.get("size", ""))} for a in raw_asks],
            }

        # OrderBookSummary con atributos bids y asks (lista de OrderSummary)
        return {
            "token_id": token_id,
            "bids": [{"price": str(b.price), "size": str(b.size)} for b in (obs.bids or [])],
            "asks": [{"price": str(a.price), "size": str(a.size)} for a in (obs.asks or [])],
        }

    @_log_api_call
    @retry_with_backoff(max_attempts=2)
    def get_orderbooks_batch(self, token_ids: list[str]) -> dict[str, dict[str, Any]]:
        """Obtiene orderbooks para multiples tokens via /books batch.

        Rate limit: 500 req/10s. Cada batch de hasta 500 tokens cuenta como 1 req.
        """
        if self.paper_mode:
            return {tid: {"bids": [], "asks": []} for tid in token_ids}

        valid_ids = [tid for tid in token_ids if tid not in self._invalid_tokens]
        if not valid_ids:
            return {}

        # Pasar dicts planos (compatible con SDK v1.0.0 en ambas variantes)
        params = [{"token_id": tid} for tid in valid_ids]
        books = self._client.get_order_books(params)

        result: dict[str, dict[str, Any]] = {}
        for book in books:
            tid = book.get("asset_id") if isinstance(book, dict) else getattr(book, "asset_id", None)
            if not tid:
                continue
            if isinstance(book, dict):
                result[tid] = {
                    "bids": [{"price": str(b.get("price", "")), "size": str(b.get("size", ""))} for b in book.get("bids", [])],
                    "asks": [{"price": str(a.get("price", "")), "size": str(a.get("size", ""))} for a in book.get("asks", [])],
                }
            else:
                result[tid] = {
                    "bids": [{"price": str(b.price), "size": str(b.size)} for b in (book.bids or [])],
                    "asks": [{"price": str(a.price), "size": str(a.size)} for a in (book.asks or [])],
                }
        return result

    @_log_api_call
    @retry_with_backoff(max_attempts=3)
    def get_midpoint(self, token_id: str) -> float:
        """Obtiene el precio mid de un token.

        Returns:
            Precio mid como float (0.0 si no hay liquidez).
        """
        if self.paper_mode:
            return 0.5

        if token_id in self._invalid_tokens:
            return 0.0

        try:
            result = self._client.get_midpoint(token_id)
        except Exception as e:
            if "404" in str(e) or "No orderbook exists" in str(e):
                self._invalid_tokens.add(token_id)
                return 0.0
            raise

        # La API retorna {"mid": "0.52"} o similar
        if isinstance(result, dict):
            return float(result.get("mid", 0.0))
        return float(result or 0.0)

    def get_best_bid_ask(self, token_id: str) -> tuple[float, float]:
        """Obtiene best_bid y best_ask usando get_price() que es más confiable.

        get_orderbook() a veces retorna datos stale (0.01/0.99) mientras que
        get_price() sempre retorna precios reales.

        Returns:
            Tuple (best_bid, best_ask). Si no hay datos, retorna (0.0, 1.0).
        """
        if self.paper_mode:
            return 0.01, 0.99

        if token_id in self._invalid_tokens:
            return 0.0, 1.0

        best_bid, best_ask = 0.0, 1.0
        try:
            buy_result = self._client.get_price(token_id, side="BUY")
            sell_result = self._client.get_price(token_id, side="SELL")

            if isinstance(buy_result, dict):
                best_bid = float(buy_result.get("price", 0))
            if isinstance(sell_result, dict):
                best_ask = float(sell_result.get("price", 1))

            if best_bid <= 0 or best_ask >= 1.0:
                best_bid, best_ask = 0.0, 1.0

        except Exception as e:
            if "404" in str(e) or "No orderbook exists" in str(e):
                self._invalid_tokens.add(token_id)

        return best_bid, best_ask

    @_log_api_call
    @retry_with_backoff(max_attempts=3)
    def get_tick_size(self, token_id: str) -> str:
        """Obtiene el tick size de un mercado.

        Returns:
            Tick size como string: '0.1' | '0.01' | '0.001' | '0.0001'
        """
        if self.paper_mode:
            return "0.01"

        return self._client.get_tick_size(token_id)

    def _get_cached_tick_size(self, token_id: str) -> str:
        """Retorna tick_size cacheado, consultando la API solo si no está en cache."""
        if token_id in self._invalid_tokens:
            logger.warning("Token invalido (cache), usando tick_size default: %s...", token_id[:8])
            return "0.01"
        if token_id not in self._tick_sizes:
            try:
                self._tick_sizes[token_id] = self.get_tick_size(token_id)
            except Exception as e:
                if "Invalid token id" in str(e):
                    self._invalid_tokens.add(token_id)
                    logger.warning("Token invalido detectado, cacheado para skip: %s...", token_id[:8])
                    return "0.01"
                raise
        return self._tick_sizes[token_id]

    def _get_cached_neg_risk(self, token_id: str) -> bool:
        """Retorna neg_risk cacheado, consultando la API solo si no está en cache."""
        if token_id not in self._neg_risk:
            try:
                self._neg_risk[token_id] = self._client.get_neg_risk(token_id)
            except Exception:
                logger.debug("No se pudo obtener neg_risk para %s...", token_id[:8])
                self._neg_risk[token_id] = False
        return self._neg_risk[token_id]

    def invalidate_tick_size_cache(self, token_id: str) -> None:
        """Invalida el cache de tick_size y neg_risk para un token.
        Llamar cuando llegue un evento tick_size_change del WebSocket.
        """
        self._tick_sizes.pop(token_id, None)
        self._neg_risk.pop(token_id, None)
        logger.info("Cache invalidado para %s... (tick_size_change)", token_id[:8])

    @_log_api_call
    @retry_with_backoff(max_attempts=3)
    def get_fee_rate(self, token_id: str) -> int:
        """Obtiene el fee rate en basis points para un token.

        SIEMPRE verificar antes de operar para no pagar fees inesperados.

        Returns:
            Fee rate en bps (ej: 200 = 2%).
        """
        if self.paper_mode:
            return 0

        # Sin try/except aquí: el @retry_with_backoff del decorador maneja reintentos.
        # Si falla definitivamente, la excepción sube al caller (place_limit_order),
        # que cancela la orden — más seguro que operar con fee incorrecto.
        return self._client.get_fee_rate_bps(token_id)

    @_log_api_call
    @retry_with_backoff(max_attempts=6, min_wait=2, max_wait=60)
    def get_rewards(self) -> dict[str, dict[str, Any]]:
        """Obtiene mercados con rewards activos via SDK.

        Endpoint canonico: GET /rewards/markets/current.
        6 reintentos con backoff exponential (2s→4s→... cap 60s).
        Si todos fallan, la excepcion sube al caller (get_reward_markets)
        que usa el cache en disco como fallback.

        Returns:
            Dict de condition_id → {rewards_daily_rate, min_size, max_spread}.
        """
        if self.paper_mode or self._client is None:
            return {}

        raw = self._client.get_current_rewards()

        rewards_map: dict[str, dict[str, Any]] = {}
        for entry in raw:
            cid = entry.get("condition_id", "")
            if not cid:
                continue
            configs = entry.get("rewards_config", [])
            if not configs:
                continue
            cfg = configs[0]
            rate = cfg.get("rate_per_day", 0) if isinstance(cfg, dict) else 0
            rewards_map[cid] = {
                "rewards_daily_rate": float(rate),
                "min_size": float(entry.get("rewards_min_size", 0)),
                "max_spread": float(entry.get("rewards_max_spread", 0)),
            }

        self._rewards_cache = rewards_map
        logger.info("get_rewards: %d mercados con rewards activos", len(rewards_map))
        self._save_rewards_cache()
        return rewards_map

    def _load_rewards_cache(self) -> None:
        """Carga cache de rewards desde disco."""
        try:
            if self._rewards_cache_file.exists():
                data = json.loads(self._rewards_cache_file.read_text("utf-8"))
                self._rewards_cache = data
                logger.info("_load_rewards_cache: %d mercados cargados", len(data))
        except Exception:
            logger.debug("No se pudo cargar rewards_cache.json (primera vez?)")

    def _save_rewards_cache(self) -> None:
        """Persiste cache de rewards a disco."""
        try:
            self._rewards_cache_file.parent.mkdir(parents=True, exist_ok=True)
            self._rewards_cache_file.write_text(
                json.dumps(self._rewards_cache, ensure_ascii=False), "utf-8"
            )
        except Exception as e:
            logger.debug("No se pudo guardar rewards_cache.json: %s", e)

    @_log_api_call
    @retry_with_backoff(max_attempts=3)
    def get_reward_percentages(self) -> dict[str, float]:
        """Obtiene porcentaje de rewards por mercado en tiempo real.

        Endpoint: GET /rewards/user/percentages
        Docs: https://docs.polymarket.com/api-reference/rewards/get-reward-percentages-for-user

        Returns:
            Dict condition_id → porcentaje (0-100).
        """
        if self.paper_mode or self._client is None:
            return {}

        raw = self._client.get_reward_percentages()
        if isinstance(raw, dict):
            return {cid: float(pct) for cid, pct in raw.items()}
        return {}

    @_log_api_call
    @retry_with_backoff(max_attempts=3)
    def is_order_scoring(self, order_id: str) -> bool:
        """Verifica si una orden esta scoreando para rewards.

        Endpoint: GET /order-scoring?order_id=...
        Docs: https://docs.polymarket.com/api-reference/trade/get-order-scoring-status

        Returns:
            True si la orden esta activa y califica para rewards.
        """
        if self.paper_mode or self._client is None:
            return False

        result = self._client.is_order_scoring(order_id)
        if isinstance(result, dict):
            return bool(result.get("scoring", False))
        return False

    @_log_api_call
    @retry_with_backoff(max_attempts=3)
    def get_clob_market_info(self, condition_id: str) -> dict[str, Any]:
        """Obtiene parametros CLOB de un mercado: tokens, tick, fees, rewards.

        Endpoint: GET /markets/{condition_id}
        Docs: https://docs.polymarket.com/api-reference/markets/get-clob-market-info

        Returns:
            Dict con tokens, minimum_tick_size, neg_risk, fees_enabled, etc.
        """
        if self.paper_mode or self._client is None:
            return {}

        return self._client.get_market(condition_id)

    # ------------------------------------------------------------------
    # Balance y posiciones
    # ------------------------------------------------------------------

    @_log_api_call
    @retry_with_backoff(max_attempts=3)
    def get_balance(self) -> float:
        """Obtiene el balance pUSD disponible en la cuenta.

        Returns:
            Balance en pUSD como float.
        """
        if self.paper_mode:
            return self._paper_capital

        params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
        result = self._client.get_balance_allowance(params)
        # Retorna {"balance": "123456789", ...} en unidades de 6 decimales (pUSD)
        raw_balance = result.get("balance", "0")
        balance = float(raw_balance) / 1_000_000

        if balance == 0.0 and self._signature_type == 0:
            logger.warning(
                "Balance=0 con signature_type=0 (EOA mode). "
                "Si depositaste via Polymarket.com, tu pUSD está en el PROXY address, "
                "no en la EOA. Cambiá signature_type=2 en config/settings.yaml y "
                "definí POLYMARKET_PROXY_ADDRESS en .env."
            )

        return balance

    @_log_api_call
    @retry_with_backoff(max_attempts=3)
    def get_positions(self) -> list[dict[str, Any]]:
        """Obtiene las ordenes abiertas (posiciones activas) de la cuenta.

        Returns:
            Lista de ordenes abiertas con sus detalles.
        """
        if self.paper_mode:
            return []

        params = OpenOrderParams()
        orders = self._client.get_open_orders(params=params)
        logger.info(f"get_positions: {len(orders)} ordenes abiertas")
        return orders if isinstance(orders, list) else []

    # ------------------------------------------------------------------
    # Trading
    # ------------------------------------------------------------------

    @_log_api_call
    @retry_with_backoff(max_attempts=3)
    def place_limit_order(
        self,
        token_id: str,
        side: str,
        price: float,
        size: float,
        post_only: bool = True,
    ) -> dict[str, Any]:
        """Coloca una orden limite. Siempre loguea a data/trades.jsonl.

        Args:
            token_id: ID del token de Polymarket.
            side: 'BUY' o 'SELL'.
            price: Precio limite (entre 0 y 1).
            size: Cantidad en pUSD.
            post_only: Si True, usa Post Only (maker, sin pagar taker fees).
                       SIEMPRE True para market making.

        Returns:
            Dict con order_id, status y detalles de la orden.

        Raises:
            Exception si la API rechaza la orden.
        """
        if token_id in self._invalid_tokens:
            logger.warning("Token invalido (cache), orden rechazada: %s...", token_id[:8])
            return {"status": "error", "reason": "invalid_token", "token_id": token_id}

        trade_record: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "type": "limit",
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
            ts_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
            trade_record["order_id"] = f"paper_{ts_ms}_{next(self._order_counter)}"
            self._log_trade(trade_record)
            logger.info(f"[PAPER] {side} {size} pUSD @ {price} | token={token_id[:8]}...")
            return trade_record

        try:
            # Cachear tick_size y neg_risk para evitar +2 requests API por orden
            tick_size = self._get_cached_tick_size(token_id)
            neg_risk = self._get_cached_neg_risk(token_id)
            options = PartialCreateOrderOptions(
                tick_size=tick_size,
                neg_risk=neg_risk,
            )

            order_args = OrderArgs(
                token_id=token_id,
                price=price,
                size=size,
                side=side,
            )

            signed_order = self._client.create_order(order_args, options=options)
            result = self._client.post_order(
                signed_order,
                order_type=OrderType.GTC,
                post_only=post_only,
            )

            order_id = result.get("orderID", result.get("id", "unknown"))
            trade_record["status"] = result.get("status", "submitted")
            trade_record["order_id"] = order_id
            self._log_trade(trade_record)
            logger.info(
                "Orden colocada: %s %s pUSD @ %s | order_id=%s | post_only=%s",
                side, size, price, order_id, post_only,
            )
            return trade_record

        except Exception as exc:
            trade_record["status"] = "error"
            self._log_trade(trade_record)
            err_msg = str(exc).lower()

            # Errores de usuario/negocio: no ameritan retry ni circuit breaker
            user_errors = [
                "lower than the minimum",
                "not enough balance",
                "allowance",
                "invalid order",
                "minimum tick size",
                "price must be",
                "crosses book",
                "post-only",
            ]
            is_user_error = any(phrase in err_msg for phrase in user_errors)

            if is_user_error:
                logger.warning(
                    "Orden rechazada por API (%s): %s %s @ %s en %s... — %s",
                    "user_error" if "minimum" in err_msg or "balance" in err_msg else "validation",
                    side, size, price, token_id[:8],
                    str(exc)[:120],
                )
                return {"status": "rejected", "reason": err_msg, "token_id": token_id}

            logger.exception(
                "Error al colocar orden: %s %s @ %s en %s...",
                side, size, price, token_id[:8],
            )
            raise

    @_log_api_call
    @retry_with_backoff(max_attempts=3)
    def post_batch_orders(self, signals: list[Any]) -> list[dict[str, Any]]:
        """Coloca multiples ordenes limite en un solo batch request.

        Polymarket permite hasta 15 ordenes por request via post_orders().
        Esto reduce latencia y evita que el book se mueva entre ordenes.

        Args:
            signals: Lista de Signal con token_id, side, price, size.

        Returns:
            Lista de dicts con order_id y status, uno por signal.
        """
        results: list[dict[str, Any]] = []
        if not signals:
            return results

        if self.paper_mode:
            for sig in signals:
                ts_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
                order_id = f"paper_{ts_ms}_{next(self._order_counter)}"
                results.append({
                    "status": "filled_paper",
                    "order_id": order_id,
                    "token_id": sig.token_id,
                    "side": sig.side,
                    "price": sig.price,
                    "size": sig.size,
                })
                logger.info(
                    "[PAPER] %s %s pUSD @ %s | token=%s...",
                    sig.side, sig.size, sig.price, sig.token_id[:8],
                )
            return results

        try:
            orders_with_type: list[Any] = []
            for sig in signals:
                tick_size = self._get_cached_tick_size(sig.token_id)
                neg_risk = self._get_cached_neg_risk(sig.token_id)
                options = PartialCreateOrderOptions(
                    tick_size=tick_size,
                    neg_risk=neg_risk,
                )
                order_args = OrderArgs(
                    token_id=sig.token_id,
                    price=sig.price,
                    size=sig.size,
                    side=sig.side,
                )
                signed_order = self._client.create_order(order_args, options=options)
                orders_with_type.append(
                    PostOrdersV2Args(order=signed_order, orderType=OrderType.GTC)
                )

            batch_result = self._client.post_orders(orders_with_type, post_only=True)
            # batch_result puede ser una lista de dicts u objeto
            if isinstance(batch_result, list):
                for res in batch_result:
                    order_id = res.get("orderID", res.get("id", "unknown")) if isinstance(res, dict) else getattr(res, "orderID", "unknown")
                    status = res.get("status", "submitted") if isinstance(res, dict) else getattr(res, "status", "submitted")
                    results.append({"status": status, "order_id": order_id})
            else:
                # Fallback: devolver submitted generico si la respuesta no es lista
                for _ in signals:
                    results.append({"status": "submitted", "order_id": "unknown"})
            return results
        except Exception:
            logger.exception("Error en post_batch_orders")
            # Fallback: devolver error para todas
            for _ in signals:
                results.append({"status": "error", "order_id": ""})
            return results

    @_log_api_call
    @retry_with_backoff(max_attempts=2)
    def place_fok_order(
        self,
        token_id: str,
        side: str,
        price: float,
        size: float,
    ) -> dict[str, Any]:
        """Coloca una orden Fill-or-Kill (FOK). Se ejecuta completa o se cancela.

        Usa MarketOrderArgs + create_market_order segun convencion oficial del SDK.

        Args:
            token_id: ID del token de Polymarket.
            side: 'BUY' o 'SELL'.
            price: Precio limite maximo (para BUY) o minimo (para SELL) — slippage protection.
            size: Para BUY: dólares a gastar. Para SELL: shares a vender.

        Returns:
            Dict con order_id, status y detalles de la orden.
        """
        trade_record: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "type": "fok",
            "token_id": token_id,
            "side": side,
            "price": price,
            "size": size,
            "paper_mode": self.paper_mode,
            "status": "pending",
        }

        if self.paper_mode:
            trade_record["status"] = "filled_paper"
            ts_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
            trade_record["order_id"] = f"paper_{ts_ms}_{next(self._order_counter)}"
            self._log_trade(trade_record)
            logger.info("[PAPER] FOK %s %s pUSD @ %s | token=%s...", side, size, price, token_id[:8])
            return trade_record

        try:
            order_args = MarketOrderArgs(
                token_id=token_id,
                price=price,
                amount=size,
                side=side,
            )

            signed_order = self._client.create_market_order(order_args)
            result = self._client.post_order(
                signed_order,
                order_type=OrderType.FOK,
            )

            order_id = result.get("orderID", result.get("id", "unknown"))
            trade_record["status"] = result.get("status", "submitted")
            trade_record["order_id"] = order_id
            self._log_trade(trade_record)
            logger.info(
                "FOK orden colocada: %s %s @ %s | order_id=%s",
                side, size, price, order_id,
            )
            return trade_record

        except Exception:
            trade_record["status"] = "error"
            self._log_trade(trade_record)
            logger.exception(
                "Error al colocar FOK: %s %s @ %s en %s...",
                side, size, price, token_id[:8],
            )
            raise

    @_log_api_call
    @retry_with_backoff(max_attempts=3)
    def get_order_status(self, order_id: str) -> dict[str, Any]:
        """Consulta el estado de una orden por su ID.

        Returns:
            Dict con 'status' (MATCHED|OPEN|CANCELLED), 'size_matched', 'price'.
        """
        if self.paper_mode:
            # En paper mode simular fill aleatorio (~30% de probabilidad)
            import random
            filled = random.random() < 0.30
            return {
                "order_id": order_id,
                "status": "ORDER_STATUS_MATCHED" if filled else "ORDER_STATUS_LIVE",
                "size_matched": 1.0 if filled else 0.0,
                "price": 0.50,
            }

        try:
            result = self._client.get_order(order_id)
            # py-clob-client-v2 retorna un dict o un objeto con atributos
            # Preservar formato oficial ORDER_STATUS_* para comparacion consistente
            if isinstance(result, dict):
                return {
                    "order_id": order_id,
                    "status": str(result.get("status", "ORDER_STATUS_UNKNOWN")),
                    "size_matched": float(result.get("size_matched", 0)),
                    "price": float(result.get("price", 0)),
                }
            return {
                "order_id": order_id,
                "status": str(getattr(result, "status", "ORDER_STATUS_UNKNOWN")),
                "size_matched": float(getattr(result, "size_matched", 0)),
                "price": float(getattr(result, "price", 0)),
            }
        except Exception:
            logger.debug(f"No se pudo obtener estado de orden {order_id[:12]}...")
            return {"order_id": order_id, "status": "UNKNOWN", "size_matched": 0.0, "price": 0.0}

    @_log_api_call
    @retry_with_backoff(max_attempts=3)
    def cancel_order(self, order_id: str) -> bool:
        """Cancela una orden por su ID.

        Returns:
            True si la cancelacion fue exitosa.
        """
        if self.paper_mode:
            logger.info(f"[PAPER] Orden cancelada: {order_id}")
            return True

        self._client.cancel_order(OrderPayload(orderID=order_id))
        logger.info(f"Orden cancelada: {order_id}")
        return True

    @_log_api_call
    @retry_with_backoff(max_attempts=3)
    def cancel_all_orders(self) -> bool:
        """Cancela todas las ordenes abiertas de la cuenta.

        Returns:
            True si la operacion fue exitosa.
        """
        if self.paper_mode:
            logger.info("[PAPER] Todas las ordenes canceladas")
            return True

        self._client.cancel_all()
        logger.info("Todas las ordenes canceladas")
        return True

    @_log_api_call
    @retry_with_backoff(max_attempts=3)
    def cancel_market_orders(self, condition_id: str = "", token_id: str = "") -> bool:
        """Cancela todas las ordenes de un mercado o token especifico.

        Args:
            condition_id: Condition ID del mercado.
            token_id: Asset ID del token especifico (opcional).

        Returns:
            True si la operacion fue exitosa.
        """
        if self.paper_mode:
            logger.info(
                f"[PAPER] Ordenes canceladas para market={condition_id[:8]}... "
                f"token={token_id[:8] if token_id else 'all'}..."
            )
            return True

        payload = OrderMarketCancelParams(
            market=condition_id,
            asset_id=token_id,
        )
        self._client.cancel_market_orders(payload)
        logger.info("Ordenes canceladas: market=%s... token=%s...", condition_id[:8], token_id[:8] if token_id else "all")
        return True

    # ------------------------------------------------------------------
    # Exit de posiciones (NO es merge on-chain real)
    # ------------------------------------------------------------------

    @_log_api_call
    @retry_with_backoff(max_attempts=3)
    def exit_position_market(self, token_id: str, size: float) -> dict[str, Any]:
        """Reduce una posicion vendiendo shares al mercado.

        NOTA: Este metodo NO realiza un merge on-chain real (YES+NO -> pUSD).
        Un merge verdadero requiere transaccion al NegRiskAdapter
        (0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296) via web3, que no esta
        implementado. En su lugar, coloca una orden GTC SELL post_only
        ligeramente por debajo del midpoint para ser filled como maker
        (0% fee) en lugar de taker (1% fee).

        Args:
            token_id: Token a cerrar.
            size: Shares a vender (no dolares).

        Returns:
            Dict con resultado de la operacion.
        """
        if self.paper_mode:
            logger.info("[PAPER] reduce_position: SELL %s shares en %s...", size, token_id[:8])
            return {"status": "reduced_paper", "token_id": token_id, "size": size}

        mid = self.get_midpoint(token_id)
        if mid <= 0:
            raise ValueError("No se pudo obtener midpoint para %s..." % token_id[:8])

        tick_size = float(self._get_cached_tick_size(token_id))
        # Precio 1 tick por debajo del mid para ser maker (0% fee)
        sell_price = round(mid - tick_size, 6)
        sell_price = max(tick_size, min(sell_price, 1 - tick_size))

        options = PartialCreateOrderOptions(
            tick_size=self._get_cached_tick_size(token_id),
            neg_risk=self._get_cached_neg_risk(token_id),
        )
        order_args = OrderArgs(
            token_id=token_id,
            price=sell_price,
            size=size,
            side="SELL",
        )

        signed_order = self._client.create_order(order_args, options=options)
        result = self._client.post_order(signed_order, order_type=OrderType.GTC, post_only=True)

        order_id = result.get("orderID", result.get("id", "unknown"))
        logger.info(
            "reduce_position: SELL %s @ %s | order_id=%s | token=%s...",
            size, sell_price, order_id, token_id[:8],
        )
        return {
            "status": "submitted",
            "order_id": order_id,
            "token_id": token_id,
            "size": size,
            "price": sell_price,
        }

    # ------------------------------------------------------------------
    # Reconciliación on-chain (TODO 1.2)
    # ------------------------------------------------------------------

    def reconcile_state(
        self,
        state_path: str = "data/state.json",
        alert_delta_threshold: float = 1.0,
    ) -> dict[str, Any]:
        """Reconcilia el estado local con el estado real on-chain.

        Compara:
        - Balance pUSD on-chain vs state.json['balance']
        - Número de órdenes abiertas on-chain vs state.json['open_orders']

        Args:
            state_path: Ruta al archivo de estado local.
            alert_delta_threshold: Delta en pUSD que activa desync=True.

        Returns:
            Dict con claves:
                balance_onchain (float)
                balance_local (float | None)
                balance_delta (float)
                open_orders_onchain (int)
                open_orders_local (int | None)
                desync (bool) — True si delta supera threshold
                state_updated (bool) — True si se actualizó el archivo

        En modo paper retorna valores simulados sin tocar on-chain.
        """
        result: dict[str, Any] = {
            "balance_onchain": 0.0,
            "balance_local": None,
            "balance_delta": 0.0,
            "open_orders_onchain": 0,
            "open_orders_local": None,
            "desync": False,
            "state_updated": False,
        }

        if self.paper_mode:
            result["balance_onchain"] = self._paper_capital
            logger.info("reconcile_state: paper mode — simulado")
            return result

        # --- 1. Leer estado on-chain ---
        try:
            balance_onchain = self.get_balance()
            result["balance_onchain"] = balance_onchain
        except Exception:
            logger.exception("reconcile_state: error obteniendo balance on-chain")
            return result

        try:
            orders_onchain = self.get_positions()
            result["open_orders_onchain"] = len(orders_onchain)
        except Exception:
            logger.exception("reconcile_state: error obteniendo órdenes on-chain")
            orders_onchain = []

        # --- 2. Leer estado local ---
        state_file = Path(state_path)
        local_state: dict[str, Any] = {}
        if state_file.exists():
            try:
                with open(state_file, "r", encoding="utf-8") as f:
                    local_state = json.load(f)
            except Exception:
                logger.warning("reconcile_state: no se pudo leer %s", state_path)

        balance_local = local_state.get("balance_usdc")
        orders_local = local_state.get("open_orders_count")
        result["balance_local"] = balance_local
        result["open_orders_local"] = orders_local

        # --- 3. Comparar ---
        if balance_local is not None:
            delta = abs(balance_onchain - float(balance_local))
            result["balance_delta"] = delta
            if delta > alert_delta_threshold:
                result["desync"] = True
                logger.warning(
                    "reconcile_state: DESYNC detectado — on-chain=%.4f local=%.4f delta=%.4f",
                    balance_onchain, balance_local, delta,
                )

        # --- 4. Actualizar state.json con ground truth ---
        try:
            local_state["balance_usdc"] = balance_onchain
            local_state["open_orders_count"] = len(orders_onchain)
            local_state["last_reconcile"] = datetime.now(timezone.utc).isoformat()
            state_file.parent.mkdir(parents=True, exist_ok=True)
            with open(state_file, "w", encoding="utf-8") as f:
                json.dump(local_state, f, indent=2, ensure_ascii=False)
            result["state_updated"] = True
            logger.info(
                "reconcile_state: OK — balance=%.4f pUSD, órdenes=%d",
                balance_onchain, len(orders_onchain),
            )
        except Exception:
            logger.exception("reconcile_state: error guardando state.json")

        return result

    # ------------------------------------------------------------------
    # Internos
    # ------------------------------------------------------------------

    def _log_trade(self, trade: dict[str, Any]) -> None:
        """Append a trades.jsonl. Nunca falla silenciosamente."""
        try:
            with open(self._trades_file, "a", encoding="utf-8") as f:
                f.write(json.dumps(trade, ensure_ascii=False) + "\n")
        except OSError:
            logger.exception("No se pudo escribir en trades.jsonl")
