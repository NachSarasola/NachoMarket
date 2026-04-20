import itertools
import json
import logging
import os
from datetime import datetime, timezone
from functools import wraps
from pathlib import Path
from typing import Any, Callable

from dotenv import load_dotenv
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import (
    ApiCreds,
    AssetType,
    BalanceAllowanceParams,
    MarketOrderArgs,
    OpenOrderParams,
    OrderArgs,
    OrderType,
)

from src.utils.resilience import retry_with_backoff

load_dotenv()
logger = logging.getLogger("nachomarket.client")

CLOB_HOST = "https://clob.polymarket.com"
CHAIN_ID = 137


def _log_api_call(fn: Callable) -> Callable:
    """Decorator: loguea cada llamada a la API con timestamp, args y resultado."""

    @wraps(fn)
    def wrapper(self: "PolymarketClient", *args: Any, **kwargs: Any) -> Any:
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
    """Wrapper sobre py-clob-client con autenticacion, retry y logging completo.

    Modos de autenticacion:
        signature_type=1 — Magic Link / EOA (solo POLYMARKET_PRIVATE_KEY)
        signature_type=2 — Browser wallet / proxy address (PRIVATE_KEY + PROXY_ADDRESS)

    Niveles de la API:
        Level 0 — sin auth (mercados, orderbook, precios)
        Level 1 — solo private key (crear ordenes)
        Level 2 — private key + ApiCreds (colocar, cancelar, balance)
    """

    def __init__(self, paper_mode: bool = True, signature_type: int = 1) -> None:
        self.paper_mode = paper_mode
        self._signature_type = signature_type
        self._trades_file = Path("data/trades.jsonl")
        self._trades_file.parent.mkdir(parents=True, exist_ok=True)
        self._client: ClobClient | None = None
        self._order_counter = itertools.count(1)

        if not paper_mode:
            self._client = self._build_client(signature_type)
            logger.info(
                f"PolymarketClient inicializado en modo LIVE "
                f"(signature_type={signature_type}, "
                f"address={self._client.get_address()})"
            )
        else:
            logger.info("PolymarketClient inicializado en modo PAPER")

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

        obs = self._client.get_order_book(token_id)
        # OrderBookSummary tiene atributos bids y asks (lista de OrderSummary)
        return {
            "token_id": token_id,
            "bids": [{"price": str(b.price), "size": str(b.size)} for b in (obs.bids or [])],
            "asks": [{"price": str(a.price), "size": str(a.size)} for a in (obs.asks or [])],
        }

    @_log_api_call
    @retry_with_backoff(max_attempts=3)
    def get_midpoint(self, token_id: str) -> float:
        """Obtiene el precio mid de un token.

        Returns:
            Precio mid como float (0.0 si no hay liquidez).
        """
        if self.paper_mode:
            return 0.5

        result = self._client.get_midpoint(token_id)
        # La API retorna {"mid": "0.52"} o similar
        if isinstance(result, dict):
            return float(result.get("mid", 0.0))
        return float(result or 0.0)

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

        try:
            return self._client.get_fee_rate_bps(token_id)
        except Exception:
            logger.warning(
                f"No se pudo obtener fee_rate para {token_id[:8]}..., usando 200 bps por defecto"
            )
            return 200

    # ------------------------------------------------------------------
    # Balance y posiciones
    # ------------------------------------------------------------------

    @_log_api_call
    @retry_with_backoff(max_attempts=3)
    def get_balance(self) -> float:
        """Obtiene el balance USDC disponible en la cuenta.

        Returns:
            Balance en USDC como float.
        """
        if self.paper_mode:
            return 400.0

        params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
        result = self._client.get_balance_allowance(params)
        # Retorna {"balance": "123456789", ...} en unidades de 6 decimales (USDC)
        raw_balance = result.get("balance", "0")
        return float(raw_balance) / 1_000_000

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
        orders = self._client.get_orders(params=params)
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
            size: Cantidad en USDC.
            post_only: Si True, usa Post Only (maker, sin pagar taker fees).
                       SIEMPRE True para market making.

        Returns:
            Dict con order_id, status y detalles de la orden.

        Raises:
            Exception si la API rechaza la orden.
        """
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
            logger.info(f"[PAPER] {side} {size} USDC @ {price} | token={token_id[:8]}...")
            return trade_record

        try:
            # Obtener fee rate dinamicamente (regla INQUEBRANTABLE)
            fee_rate_bps = self.get_fee_rate(token_id)

            order_args = OrderArgs(
                token_id=token_id,
                price=price,
                size=size,
                side=side,
                fee_rate_bps=fee_rate_bps,
            )

            # create_order firma la orden; post_order la envia
            signed_order = self._client.create_order(order_args)
            result = self._client.post_order(
                signed_order,
                orderType=OrderType.GTC,
                post_only=post_only,
            )

            order_id = result.get("orderID", result.get("id", "unknown"))
            trade_record["status"] = result.get("status", "submitted")
            trade_record["order_id"] = order_id
            trade_record["fee_rate_bps"] = fee_rate_bps
            self._log_trade(trade_record)
            logger.info(
                f"Orden colocada: {side} {size} USDC @ {price} "
                f"| order_id={order_id} | fee={fee_rate_bps}bps"
            )
            return trade_record

        except Exception:
            trade_record["status"] = "error"
            self._log_trade(trade_record)
            logger.exception(
                f"Error al colocar orden: {side} {size} @ {price} en {token_id[:8]}..."
            )
            raise

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

        Ideal para arbitraje donde no queremos ejecucion parcial.

        Args:
            token_id: ID del token de Polymarket.
            side: 'BUY' o 'SELL'.
            price: Precio limite maximo (para BUY) o minimo (para SELL).
            size: Cantidad en USDC.

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
            logger.info(f"[PAPER] FOK {side} {size} USDC @ {price} | token={token_id[:8]}...")
            return trade_record

        try:
            fee_rate_bps = self.get_fee_rate(token_id)

            order_args = OrderArgs(
                token_id=token_id,
                price=price,
                size=size,
                side=side,
                fee_rate_bps=fee_rate_bps,
            )

            signed_order = self._client.create_order(order_args)
            result = self._client.post_order(
                signed_order,
                orderType=OrderType.FOK,
                post_only=False,
            )

            order_id = result.get("orderID", result.get("id", "unknown"))
            trade_record["status"] = result.get("status", "submitted")
            trade_record["order_id"] = order_id
            trade_record["fee_rate_bps"] = fee_rate_bps
            self._log_trade(trade_record)
            logger.info(
                f"FOK orden colocada: {side} {size} USDC @ {price} "
                f"| order_id={order_id} | fee={fee_rate_bps}bps"
            )
            return trade_record

        except Exception:
            trade_record["status"] = "error"
            self._log_trade(trade_record)
            logger.exception(
                f"Error al colocar FOK: {side} {size} @ {price} en {token_id[:8]}..."
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
                "status": "MATCHED" if filled else "OPEN",
                "size_matched": 1.0 if filled else 0.0,
                "price": 0.50,
            }

        try:
            result = self._client.get_order(order_id)
            # py-clob-client retorna un dict o un objeto con atributos
            if isinstance(result, dict):
                return {
                    "order_id": order_id,
                    "status": result.get("status", "UNKNOWN"),
                    "size_matched": float(result.get("size_matched", 0)),
                    "price": float(result.get("price", 0)),
                }
            return {
                "order_id": order_id,
                "status": getattr(result, "status", "UNKNOWN"),
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

        self._client.cancel(order_id)
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

        self._client.cancel_market_orders(market=condition_id, asset_id=token_id)
        logger.info(f"Ordenes canceladas: market={condition_id[:8]}... token={token_id[:8] if token_id else 'all'}...")
        return True

    # ------------------------------------------------------------------
    # Merge de posiciones
    # ------------------------------------------------------------------

    @_log_api_call
    @retry_with_backoff(max_attempts=3)
    def merge_positions(self, token_id: str, size: float) -> dict[str, Any]:
        """Intenta reducir una posicion vendiendo al mercado.

        Polymarket no tiene un endpoint de 'merge' directo en la API REST.
        Esta implementacion coloca una orden de venta a precio de mercado
        para reducir el inventario.

        Args:
            token_id: Token a cerrar.
            size: Cantidad en USDC a vender.

        Returns:
            Dict con resultado de la operacion.
        """
        if self.paper_mode:
            logger.info(f"[PAPER] merge_positions: SELL {size} USDC en {token_id[:8]}...")
            return {"status": "merged_paper", "token_id": token_id, "size": size}

        # Obtener precio actual para vender al mercado
        mid = self.get_midpoint(token_id)
        if mid <= 0:
            raise ValueError(f"No se pudo obtener midpoint para {token_id[:8]}...")

        tick_size = float(self.get_tick_size(token_id))
        # Redondear al tick mas cercano
        sell_price = round(round(mid / tick_size) * tick_size, 6)
        # Asegurar precio valido
        sell_price = max(tick_size, min(sell_price, 1 - tick_size))

        fee_rate_bps = self.get_fee_rate(token_id)
        order_args = OrderArgs(
            token_id=token_id,
            price=sell_price,
            size=size,
            side="SELL",
            fee_rate_bps=fee_rate_bps,
        )

        signed_order = self._client.create_order(order_args)
        # Para merge usamos GTC sin post_only (prioridad de ejecucion sobre fee)
        result = self._client.post_order(signed_order, orderType=OrderType.GTC, post_only=False)

        order_id = result.get("orderID", result.get("id", "unknown"))
        logger.info(
            f"merge_positions: SELL {size} @ {sell_price} | "
            f"order_id={order_id} | token={token_id[:8]}..."
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
        - Balance USDC on-chain vs state.json['balance']
        - Número de órdenes abiertas on-chain vs state.json['open_orders']

        Args:
            state_path: Ruta al archivo de estado local.
            alert_delta_threshold: Delta en USDC que activa desync=True.

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
            result["balance_onchain"] = 400.0
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
                "reconcile_state: OK — balance=%.4f USDC, órdenes=%d",
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
