"""
NachoMarket — Entry point y loop principal.

Uso:
    python -m src.main              # paper mode (default, seguro)
    python -m src.main --paper      # paper mode explícito
    python -m src.main --live       # LIVE — dinero real, requiere .env
    python -m src.main --review-only  # solo self-review con Claude Haiku y salir
"""

import argparse
import asyncio
import json
import logging
import signal
import sys
import threading
import time
from pathlib import Path
from typing import Any

import schedule
import yaml
from dotenv import load_dotenv

from src.polymarket.client import PolymarketClient
from src.polymarket.markets import MarketAnalyzer
from src.polymarket.websocket import OrderbookFeed, OrderbookState
from src.risk.circuit_breaker import CircuitBreaker
from src.risk.inventory import InventoryManager
from src.risk.position_sizer import PositionSizer
from src.review.self_review import SelfReviewer
from src.strategy.rewards_farmer import RewardsFarmerStrategy
from src.telegram.bot import TelegramBot, send_alert
from src.utils.geo_check import verify_geo_access
from src.utils.logger import setup_logger

load_dotenv()

# Intervalo entre actualizaciones forzadas de mercados (minutos)
_MARKET_UPDATE_INTERVAL_MIN = 5  # Reintentar rewards cada 5 min mientras API inestable
_FULL_SCAN_INTERVAL_HOURS = 4  # Scan profundo cada 4h (PROMPT: detectar nuevos mercados)
# Tiempo que esperamos a que el WS conecte tras arrancar
_WS_STARTUP_WAIT_SEC = 10.0
# Máximo de errores consecutivos antes de auto-pausar
_MAX_CONSECUTIVE_ERRORS = 10


class NachoMarketBot:
    """Orquestador principal del bot de trading.

    Coordina todos los componentes:
    - PolymarketClient    — acceso al CLOB API
    - OrderbookFeed       — WebSocket real-time (thread separado)
    - MarketAnalyzer      — selección inteligente de mercados (cache 15 min)
    - Strategies          — evaluate() → filter_signals() → execute()
    - CircuitBreaker      — protección de capital con alerts a Telegram
    - PositionSizer       — Quarter-Kelly para sizing
    - InventoryManager    — tracking YES/NO + detección de merges
    - SelfReviewer        — Claude Haiku cada 8h
    - TelegramBot         — control y notificaciones (thread separado)
    """

    def __init__(self, paper_mode: bool = True, review_only: bool = False) -> None:
        self._paper_mode = paper_mode
        self._review_only = review_only
        self._state = "running"  # running | paused | stopped

        # --- Cargar configs YAML ---
        self._settings = _load_yaml("config/settings.yaml")
        self._markets_config = _load_yaml("config/markets.yaml")
        self._risk_config = _load_yaml("config/risk.yaml")

        if paper_mode:
            self._settings["mode"] = "paper"

        # --- a. Logger: primero siempre ---
        self._logger = setup_logger(
            "nachomarket",
            log_file=self._settings.get("log_file", "data/bot.log"),
            level=self._settings.get("log_level", "INFO"),
        )
        # Suprimir logs ruidosos del SDK (tormenta de 404s internos)
        sdk_logger = logging.getLogger("py_clob_client_v2")
        sdk_logger.setLevel(logging.CRITICAL)
        for h in sdk_logger.handlers:
            h.setLevel(logging.CRITICAL)

        mode_label = "PAPER" if paper_mode else "LIVE"
        if review_only:
            self._logger.info("NachoMarket iniciando en modo REVIEW-ONLY")
        else:
            self._logger.info("NachoMarket iniciando en modo %s", mode_label)

        # En modo review-only inicializamos solo el reviewer
        if review_only:
            self._reviewer = SelfReviewer(
                model=self._settings.get("review_model", "claude-haiku-4-5-20251001"),
            )
            return

        # -------- Inicialización completa --------

        # --- b. PolymarketClient + test de conexión ---
        self._client = PolymarketClient(
            paper_mode=paper_mode,
            signature_type=self._settings.get("signature_type", 1),
            paper_capital=float(self._settings.get("capital_total", 166.0)),
        )
        self._logger.info("Verificando conexión con Polymarket CLOB...")
        self._client.test_connection()  # lanza excepción si falla
        self._logger.info("Conexión OK")

        # Heartbeat CRITICO: sin esto Polymarket cancela ordenes GTC tras ~15s de inactividad
        self._client.start_heartbeat(interval_sec=5.0)

        # --- c. WebSocket feed (el thread se arranca en run()) ---
        self._feed = OrderbookFeed()
        # Dead man's switch: pausar bot si el feed queda sin mensajes >60s
        self._feed.register_health_callback(self._on_feed_health_event)
        self._feed_was_stale: bool = False  # Para no repetir alertas
        self._deadman_paused: bool = False  # Auto-resume tras dead man's switch

        # --- d. Telegram bot (auto-arranca thread daemon en __init__) ---
        # Se inicializa antes que los demás para poder recibir alertas de arranque.
        self._telegram = TelegramBot(bot_controller=self)

        # --- e. Strategies habilitadas en settings ---
        enabled = self._settings.get("strategies_enabled", ["rewards_farmer"])
        # Merged config: las estrategias necesitan acceso a markets.yaml
        # (time_windows, min_mid_change_to_reposition, bot_order_size, competition,
        # loss_reserve_usdc) y a risk.yaml (cost_model, position_sizing).
        # Orden: settings (base) ← markets ← risk → keys de risk pisan, settings es default.

        # --- f. Risk: circuit breaker + position sizer + inventory (DEBE estar primero) ---
        self._circuit_breaker = CircuitBreaker(
            self._risk_config,
            alert_callback=self._cb_alert_handler,
            scale_down_callback=self._on_scale_down,
            pause_strategies_callback=self._on_pause_strategies,
        )
        self._position_sizer = PositionSizer(self._risk_config)
        self._inventory = InventoryManager(self._risk_config)

        merged_strategy_config = {**self._settings, **self._markets_config, **self._risk_config}
        _strategy_factories = {
            "rewards_farmer": lambda: RewardsFarmerStrategy(self._client, merged_strategy_config, circuit_breaker=self._circuit_breaker),
        }
        self._strategies = [
            _strategy_factories[name]()
            for name in enabled
            if name in _strategy_factories
        ]
        self._logger.info(
            "Estrategias habilitadas: %s", [s.name for s in self._strategies]
        )

        # Balance cacheado: arranca con capital_total del config, se actualiza cada ciclo
        self._cached_balance: float = float(self._settings.get("capital_total", 166.0))

        # --- g. Scheduler (self-review, market updates) ---
        # Merge RF config into markets_config para que enrich_with_rewards lo use
        if "rewards_farmer" in self._settings:
            self._markets_config["rewards_farmer"] = self._settings["rewards_farmer"]
        self._market_analyzer = MarketAnalyzer(self._client, self._markets_config)
        self._active_markets: list[dict[str, Any]] = []

        # --- h. Self-reviewer (Telegram callback resuelto en runtime) ---
        self._reviewer = SelfReviewer(
            model=self._settings.get("review_model", "claude-haiku-4-5-20251001"),
            capital=float(self._settings.get("capital_total", 300.0)),
        )

        # --- i. Position Merger (merge on-chain via NegRiskAdapter) ---
        from src.rewards.merger import PositionMerger
        import os
        self._merger = PositionMerger(
            private_key=os.environ.get("POLYMARKET_PRIVATE_KEY"),
            rpc_url=os.environ.get("POLYGON_RPC_URL"),
            paper_mode=paper_mode,
        )
        if self._merger.is_ready:
            self._logger.info("PositionMerger: merge on-chain habilitado")
        else:
            self._logger.info("PositionMerger: usando fallback (sell at market)")

        # No se cargan modulos de analisis ni estrategias secundarias
        # (eliminados en refactor v3 para simplificar)

        self._loop_interval: int = self._settings.get(
            "main_loop_interval_sec", 10
        )
        self._start_time: float = time.time()

    # ------------------------------------------------------------------
    # Punto de entrada
    # ------------------------------------------------------------------

    def run(self) -> None:
        """Inicia el bot. Bloquea hasta SIGTERM / SIGINT / /kill."""

        # Modo review-only: ejecutar y salir
        if self._review_only:
            self._logger.info("Ejecutando self-review manual...")
            result = self._reviewer.run_review()
            print(json.dumps(result, indent=2, ensure_ascii=False))
            return

        # Registrar signal handlers para shutdown graceful
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)

        # Arrancar WebSocket en thread daemon
        self._start_ws_feed()

        # Scan inicial de mercados + suscripción al WS
        self._logger.info("Scan inicial de mercados...")
        self._update_markets()

        # Esperar brevemente a que el WS establezca conexión
        self._logger.info(
            "Esperando %.1fs para que el WebSocket conecte...", _WS_STARTUP_WAIT_SEC
        )
        time.sleep(_WS_STARTUP_WAIT_SEC)

        # Configurar schedule
        review_hours = self._settings.get("review_interval_hours", 8)
        schedule.every(review_hours).hours.do(self._run_review)
        schedule.every(_MARKET_UPDATE_INTERVAL_MIN).minutes.do(self._update_markets)
        schedule.every(_FULL_SCAN_INTERVAL_HOURS).hours.do(self._full_market_scan)
        schedule.every().day.at("00:00").do(self._daily_reset)
        # Reconciliación on-chain cada 6h (TODO 1.2)
        schedule.every(6).hours.do(self._run_reconciliation)
        # Monitorear reward percentages en tiempo real (API /rewards/user/percentages)
        schedule.every(5).minutes.do(self._monitor_rewards_pct)

        # Notificar arranque exitoso
        strat_names = ", ".join(s.name for s in self._strategies)
        mode_label = "PAPER" if self._paper_mode else "LIVE 🔴"
        send_alert(
            f"▶️ *NachoMarket iniciado* — `{mode_label}`\n"
            f"Estrategias: `{strat_names}`\n"
            f"Mercados: `{len(self._active_markets)}` | "
            f"WS: `{'✓ conectado' if self._feed.is_connected() else 'conectando...'}`"
        )
        self._logger.info(
            "Loop principal iniciado (interval=%ds, mercados=%d)",
            self._loop_interval,
            len(self._active_markets),
        )

        # ---- Loop principal ----
        consecutive_errors = 0
        while self._state != "stopped":
            try:
                schedule.run_pending()

                if self._state == "paused":
                    time.sleep(1)
                    continue

                self._trading_cycle()
                consecutive_errors = 0
                time.sleep(self._loop_interval)

            except KeyboardInterrupt:
                self._logger.info("KeyboardInterrupt — iniciando shutdown")
                self._shutdown()

            except Exception:
                consecutive_errors += 1
                self._logger.exception(
                    "Error en main loop (consecutivo #%d)", consecutive_errors
                )
                self._circuit_breaker.record_error()
                send_alert(
                    f"⚠️ Error en main loop `#{consecutive_errors}`\n"
                    f"Estado: `{self._state}` | Mercados: `{len(self._active_markets)}`"
                )

                # Backoff exponencial con techo de 60 s
                backoff = min(self._loop_interval * consecutive_errors, 60)
                self._logger.info("Reintentando en %ds...", backoff)
                time.sleep(backoff)

                # Tras demasiados errores consecutivos: auto-pausar
                if consecutive_errors >= _MAX_CONSECUTIVE_ERRORS:
                    self._logger.critical(
                        "%d errores consecutivos — pausando bot (intervención manual requerida)",
                        _MAX_CONSECUTIVE_ERRORS,
                    )
                    send_alert(
                        f"🛑 *{_MAX_CONSECUTIVE_ERRORS} errores consecutivos* — bot PAUSADO\n"
                        "Usar `/resume` cuando el problema esté resuelto."
                    )
                    self.pause()
                    consecutive_errors = 0

        self._logger.info("Loop principal terminado")

    # ------------------------------------------------------------------
    # Ciclo de trading
    # ------------------------------------------------------------------

    def _trading_cycle(self) -> None:
        """Un ciclo completo: riesgo → mercados → estrategias → merges."""

        self._cycle_count = getattr(self, '_cycle_count', 0) + 1

        # Actualizar balance cacheado (una vez por ciclo para no saturar la API)
        try:
            self._cached_balance = self._client.get_balance()
        except Exception:
            self._logger.debug(
                "No se pudo actualizar balance; usando valor cacheado $%.2f",
                self._cached_balance,
            )

        # Circuit breaker: si está activo, skip del ciclo.
        if self._circuit_breaker.is_triggered():
            self._logger.warning("Circuit breaker activo — ciclo salteado")
            return

        # Tip 16: piso absoluto de balance
        if self._circuit_breaker.check_balance_floor(self._cached_balance):
            send_alert(
                f"🛑 *Loss reserve breach* (tip 16)\n"
                f"Balance: `${self._cached_balance:.2f}` < piso reservado\n"
                f"Trading detenido — requiere intervencion manual."
            )
            self.pause()
            return

        # Cancelar órdenes en mercados que superaron la pérdida horaria
        for market_id in self._circuit_breaker.get_markets_to_cancel():
            try:
                self._client.cancel_market_orders(condition_id=market_id)
                self._logger.warning(
                    "Órdenes canceladas: mercado %s... (límite horario)", market_id[:12]
                )
            except Exception:
                self._logger.exception(
                    "Error cancelando órdenes para %s...", market_id[:12]
                )

        # Usar mercados cacheados (actualizados cada 15 min por el scheduler)
        markets = self._active_markets
        if not markets:
            self._logger.warning("Sin mercados activos — ciclo salteado")
            return

        # Log periódico cada 10 ciclos (~5min) para confirmar que el loop esta vivo
        if self._cycle_count % 10 == 0:
            rewards_mkts = sum(1 for m in markets if m.get("rewards_active"))
            self._logger.info(
                "alive: ciclo #%d | balance=$%.2f | mercados=%d (con rewards=%d) | WS=%s",
                self._cycle_count, self._cached_balance, len(markets), rewards_mkts,
                "conectado" if self._feed.is_connected() else "DESCONECTADO",
            )

        # Para cada mercado, correr todas las estrategias habilitadas
        for market in markets:
            # Enriquecer con datos real-time del WebSocket si están disponibles
            market_data = self._enrich_with_ws(market)

            # Inyectar inventario real por token para que estrategias tomen decisiones informadas
            # (evita SELL sin shares, ajusta skew con datos reales del InventoryManager)
            condition_id = market.get("condition_id", "")
            tokens = market.get("tokens", [])
            if condition_id and tokens:
                market_inv = self._inventory.get_market_inventory(condition_id)
                token_inventory: dict[str, float] = {}
                for t in tokens:
                    tid = t.get("token_id", "")
                    if not tid:
                        continue
                    token_inventory[tid] = market_inv.positions.get(tid, 0.0)
                market_data["token_inventory"] = token_inventory

            # Inyectar cash disponible para que RF sizee correctamente
            market_data["available_cash"] = self._cached_balance

            for strategy in self._strategies:
                if not strategy.is_active:
                    continue
                try:
                    trades = self._run_strategy(strategy, market_data)
                    for trade in trades:
                        self._handle_trade(trade, market)
                except Exception:
                    self._logger.exception(
                        "Error: estrategia=%s mercado=%s...", strategy.name, condition_id[:12]
                    )
                    self._circuit_breaker.record_error()

        # Verificar fills de RF y gestionar inventario
        self._check_rf_inventory(markets)

        # Verificar y ejecutar merges de inventario YES+NO → USDC
        self._check_merges(markets)

    # ------------------------------------------------------------------
    # Pipeline de estrategia: evaluate → filter → execute
    # ------------------------------------------------------------------

    def _run_strategy(
        self, strategy: Any, market_data: dict[str, Any]
    ) -> list[Any]:
        """Ejecuta el pipeline completo de una estrategia para un mercado.

        Flujo:
        1. [MM] needs_refresh() — respetar el timer de refresh de órdenes
        2. [MM] manage_inventory() — gestionar inventario antes de evaluar
        3. should_act() — filtro rápido de la estrategia
        4. evaluate()  — generar señales
        5. _filter_signals() — aplicar reglas de riesgo
        6. execute()   — colocar órdenes reales
        7. [MM] mark_refreshed() — registrar timestamp del ciclo

        Returns:
            Lista de Trade ejecutados (vacía si no hay señales o pasan el filtro).
        """
        condition_id = market_data.get("condition_id", "")
        current_mid = float(market_data.get("mid_price", 0.0) or 0.0)

        # [MM-específico] Respetar refresh_seconds para no sobre-solicitar la API.
        # Pasar mid para que la regla min_mid_change_to_reposition (tip 21) actúe.
        needs_refresh_fn = getattr(strategy, "needs_refresh", None)
        if needs_refresh_fn is not None and not needs_refresh_fn(condition_id, current_mid):
            return []

        # [MM-específico] Gestionar inventario antes de evaluar señales
        manage_inv_fn = getattr(strategy, "manage_inventory", None)
        if manage_inv_fn is not None:
            manage_inv_fn(market_data)

        # Filtro rápido de la estrategia (spread mínimo, condiciones básicas)
        if not strategy.should_act(market_data):
            return []

        # Evaluar: generar señales de trading
        signals = strategy.evaluate(market_data)
        if not signals:
            return []

        # Filtrar señales por reglas de riesgo
        filtered = self._filter_signals(signals)
        if not filtered:
            return []

        # Ejecutar: colocar órdenes en el CLOB
        trades = strategy.execute(filtered)

        # [MM-específico] Registrar timestamp del refresh y mid actual
        mark_refreshed_fn = getattr(strategy, "mark_refreshed", None)
        if mark_refreshed_fn is not None:
            mark_refreshed_fn(condition_id, current_mid)

        return trades

    def _filter_signals(self, signals: list[Any]) -> list[Any]:
        """Aplica reglas de riesgo a las señales generadas por evaluate().

        Checks en orden:
        1. Circuit breaker activo → descartar todo
        2. can_place_order() → límite de órdenes abiertas
        3. Exposure total del capital (regla 5% por señal)
        4. Exposure por mercado (límite individual)

        La exposure proyectada se acumula a lo largo del loop para evitar
        aprobar múltiples señales que juntas excederían el límite.

        Returns:
            Subconjunto de señales que pasan todas las reglas.
        """
        if self._circuit_breaker.is_triggered():
            return []

        filtered: list[Any] = []
        # Exposure por token: para market making, BUY y SELL en el mismo token
        # se offsetan. La exposición real es max(buy, sell) por token.
        token_exposure: dict[str, tuple[float, float]] = {}  # token_id -> (buy, sell)
        base_exposure = self._inventory.get_total_exposure()

        for sig in signals:
            # Límite de órdenes abiertas
            if not self._circuit_breaker.can_place_order():
                self._logger.info(
                    "Risk filter: límite de órdenes abiertas alcanzado — descartando resto"
                )
                break

            # Calcular exposure proyectado con esta señal
            buys, sells = token_exposure.get(sig.token_id, (0.0, 0.0))
            if sig.side == "BUY":
                buys += sig.size
            else:
                sells += sig.size
            token_exposure[sig.token_id] = (buys, sells)

            projected_exposure = base_exposure + sum(
                max(b, s) for b, s in token_exposure.values()
            )

            # Límite de exposure total (60% del capital)
            if not self._position_sizer.can_trade(
                projected_exposure, self._cached_balance, 0.0
            ):
                self._logger.info(
                    "Risk filter: %s %s $%.2f @ %s descartada — exposure $%.2f > 70%% de $%.2f",
                    sig.strategy_name, sig.side, sig.size, sig.token_id[:8],
                    projected_exposure, self._cached_balance,
                )
                # Revertir el incremento para esta señal
                buys, sells = token_exposure.get(sig.token_id, (0.0, 0.0))
                if sig.side == "BUY":
                    buys -= sig.size
                else:
                    sells -= sig.size
                if buys > 0 or sells > 0:
                    token_exposure[sig.token_id] = (buys, sells)
                else:
                    token_exposure.pop(sig.token_id, None)
                continue

            # Límite de exposure por mercado individual
            if not self._inventory.can_add_position(sig.market_id, sig.size):
                self._logger.info(
                    "Risk filter: %s %s descartada — límite de mercado %s... alcanzado",
                    sig.strategy_name, sig.side, sig.market_id[:12],
                )
                continue

            filtered.append(sig)

        if len(filtered) < len(signals):
            self._logger.info(
                "Risk filter: %d señales → %d aprobadas", len(signals), len(filtered)
            )

        return filtered

    # ------------------------------------------------------------------
    # Post-ejecución de trades
    # ------------------------------------------------------------------

    def _handle_trade(self, trade: Any, market: dict[str, Any]) -> None:
        """Procesa un trade ejecutado: inventory, circuit breaker, PnL, Telegram."""
        tokens = market.get("tokens", [])

        # Solo actualizar inventario para trades realmente ejecutados (fills)
        filled_statuses = {"ORDER_STATUS_MATCHED", "matched", "filled_paper", "paper"}
        if trade.status in filled_statuses:
            self._inventory.add_trade(
                market_id=trade.market_id,
                token_type=trade.token_id,
                side=trade.side,
                size=trade.size,
                token_id=trade.token_id,
            )

            # Spread capture tracking: registrar costo por token para calcular
            # spread cuando se mergeean ambas posiciones
            if not hasattr(self, "_spread_tracker"):
                self._spread_tracker: dict[str, dict[str, float]] = {}
            tracker = self._spread_tracker.setdefault(trade.market_id, {})
            if trade.side == "BUY":
                key = trade.token_id[:16]
                prev_cost = tracker.get(f"cost_{key}", 0.0)
                prev_size = tracker.get(f"size_{key}", 0.0)
                tracker[f"cost_{key}"] = prev_cost + trade.price * trade.size
                tracker[f"size_{key}"] = prev_size + trade.size

            # Holding rewards: marcar si el mercado es elegible (4% APY)
            if not hasattr(self, "_holding_eligible_markets"):
                self._holding_eligible_markets: set[str] = set()
            category = str(market.get("category", "")).lower()
            question = str(market.get("question", "")).lower()
            # Mercados de largo plazo: elecciones 2028, geopolítica
            if any(kw in question for kw in ["2028", "election", "president", "senate"]):
                self._holding_eligible_markets.add(trade.market_id)

        # Notificar circuit breaker
        if trade.status == "error":
            self._circuit_breaker.record_error()
        elif trade.status == "rejected":
            # Errores de usuario (size min, no balance, etc) no son errores de sistema
            self._logger.debug("Trade rechazado por API, no cuenta como error de sistema")
        else:
            # Estimar PnL incremental para el circuit breaker
            pnl = self._estimate_trade_pnl(trade)
            if pnl is not None:
                self._circuit_breaker.record_trade(pnl)
                self._circuit_breaker.record_market_pnl(trade.market_id, pnl)

        # Actualizar contador de ordenes abiertas para circuit breaker
        if trade.status not in ("error", "rejected"):
            if trade.side in ("BUY", "SELL") and trade.order_id:
                self._circuit_breaker.order_placed()
        if trade.status in ("ORDER_STATUS_MATCHED", "matched", "filled_paper", "paper"):
            if trade.order_id:
                self._circuit_breaker.order_closed()

        # Notificación Telegram
        if not self._settings.get("telegram_alert_on_trade", True):
            return

        mode_tag = "[PAPER] " if self._paper_mode else ""
        side_icon = "🟢" if trade.side == "BUY" else "🔴"
        ok_icon = "✅" if trade.status not in ("error", "rejected") else "❌"
        question = market.get("question", "")[:40]

        send_alert(
            f"{ok_icon} {side_icon} {mode_tag}*{trade.side}* "
            f"`{trade.size} USDC` @ `{trade.price:.4f}`\n"
            f"_{question}_\n"
            f"`{trade.strategy_name}` | `{trade.status}`"
        )

    # ------------------------------------------------------------------
    # PnL estimation para circuit breaker
    # ------------------------------------------------------------------

    def _estimate_trade_pnl(self, trade: Any) -> float | None:
        """Estima PnL incremental de un trade para el circuit breaker.

        Para SELL: PnL = (sell_price - avg_buy_price) * size
        Para BUY: registra el costo pero no genera PnL aún (retorna None).
        """
        mid = trade.market_id
        if not hasattr(self, "_trade_tracker"):
            self._trade_tracker: dict[str, dict] = {}

        if mid not in self._trade_tracker:
            self._trade_tracker[mid] = {"buy_prices": [], "buy_sizes": [], "sell_count": 0}

        tracker = self._trade_tracker[mid]

        if trade.side == "BUY":
            tracker["buy_prices"].append(trade.price)
            tracker["buy_sizes"].append(trade.size)
            return None  # BUY no genera PnL aun

        if trade.side == "SELL" and tracker["buy_prices"]:
            total_buy_cost = sum(
                p * s for p, s in zip(tracker["buy_prices"], tracker["buy_sizes"])
            )
            total_buy_size = sum(tracker["buy_sizes"])
            avg_buy = total_buy_cost / total_buy_size if total_buy_size > 0 else 0
            pnl = (trade.price - avg_buy) * trade.size
            return pnl

        return None

    # ------------------------------------------------------------------
    # Fill detection y reposicionamiento
    # ------------------------------------------------------------------

    # ------------------------------------------------------------------
    # Merges de inventario YES+NO → USDC
    # ------------------------------------------------------------------

    def _check_merges(self, markets: list[dict[str, Any]]) -> None:
        """Detecta y ejecuta merges de inventario cuando corresponde.

        Para mercados binarios (2 tokens):
        - Si el merger on-chain está listo: mergePositions() en NegRiskAdapter
          (quema YES+NO, devuelve pUSD completo, sin perder spread)
        - Si no: close_position_with_fok() (vende al mercado con FOK, pierde spread)

        Multi-outcome no tiene merge nativo.
        """
        markets_by_id = {m["condition_id"]: m for m in markets}
        merged_any = False
        use_onchain = self._merger.is_ready

        # --- InventoryManager (MM strategy) ---
        positions = self._inventory.get_positions()
        if positions:
            for market_id, pos in positions.items():
                if not self._inventory.should_merge(market_id):
                    continue
                market = markets_by_id.get(market_id)
                if not market:
                    continue
                tokens = market.get("tokens", [])
                if len(tokens) != 2:
                    continue
                token_ids = list(pos.keys())
                if len(token_ids) != 2:
                    continue
                vals = list(pos.values())
                merge_size = min(abs(vals[0]), abs(vals[1]))
                if merge_size <= 0:
                    continue
                question = market.get("question", "")[:30]
                try:
                    if use_onchain:
                        result = self._merger.merge_positions(market_id, merge_size)
                        if result.get("status") == "success":
                            self._logger.info(
                                "Merged ON-CHAIN %.2f shares (MM) en %s... -> $%.2f pUSD (tx=%s)",
                                merge_size, market_id[:12], merge_size, result.get("tx_hash", "")[:16],
                            )
                        else:
                            self._logger.warning("Merge on-chain fallo, usando fallback sell")
                            self._client.close_position_with_fok(token_ids[0], merge_size)
                    else:
                        self._client.close_position_with_fok(token_ids[0], merge_size)
                    self._inventory.clear_market(market_id)
                    send_alert(
                        f"♻️ Merged `{merge_size:.2f}` shares (MM)\n"
                        f"_{question}..._\n-> `${merge_size:.2f}` pUSD"
                        f"{' (on-chain)' if use_onchain else ' (sell)'}"
                    )
                    merged_any = True
                except Exception:
                    self._logger.exception(
                        "Error al mergear posición MM en %s...", market_id[:12]
                    )

        # --- RF inventory local ---
        from src.strategy.rewards_farmer import RewardsFarmerStrategy
        rf_strategies = [
            s for s in self._strategies
            if isinstance(s, RewardsFarmerStrategy) and s.is_active
        ]
        if rf_strategies:
            rf = rf_strategies[0]
            for market_id in list(rf._fill_inventory.keys()):
                if not rf.should_merge(market_id):
                    continue
                market = markets_by_id.get(market_id)
                if not market:
                    continue
                tokens = market.get("tokens", [])
                if len(tokens) != 2:
                    continue
                inv = rf.get_fill_inventory(market_id)
                vals = list(inv.values())
                if len(vals) != 2:
                    continue
                merge_size = min(abs(vals[0]), abs(vals[1]))
                if merge_size <= 0:
                    continue
                question = market.get("question", "")[:30]
                try:
                    if use_onchain:
                        result = self._merger.merge_positions(market_id, merge_size)
                        if result.get("status") == "success":
                            self._logger.info(
                                "Merged ON-CHAIN %.2f shares (RF) en %s... -> $%.2f pUSD (tx=%s)",
                                merge_size, market_id[:12], merge_size, result.get("tx_hash", "")[:16],
                            )
                        else:
                            self._logger.warning("Merge on-chain RF fallo, usando fallback sell")
                            yes_token_id = tokens[0].get("token_id", "")
                            if yes_token_id:
                                self._client.close_position_with_fok(yes_token_id, merge_size)
                    else:
                        yes_token_id = tokens[0].get("token_id", "")
                        if yes_token_id:
                            self._client.close_position_with_fok(yes_token_id, merge_size)
                    rf.mark_merged(market_id, merge_size)
                    send_alert(
                        f"♻️ Merged `{merge_size:.2f}` shares (RF)\n"
                        f"_{question}..._\n-> `${merge_size:.2f}` pUSD"
                        f"{' (on-chain)' if use_onchain else ' (sell)'}"
                    )
                    merged_any = True
                except Exception:
                    self._logger.exception(
                        "Error al mergear posición RF en %s...", market_id[:12]
                    )

        if merged_any:
            try:
                self._cached_balance = self._client.get_balance()
            except Exception:
                pass

            # Spread capture: calcular PnL del merge vs costo total de ambas posiciones
            if hasattr(self, "_spread_tracker"):
                for market_id in list(self._spread_tracker.keys()):
                    tracker = self._spread_tracker.get(market_id, {})
                    total_cost = sum(v for k, v in tracker.items() if k.startswith("cost_"))
                    total_size = sum(v for k, v in tracker.items() if k.startswith("size_"))
                    if total_size > 0 and total_cost > 0:
                        avg_cost_per_share = total_cost / total_size
                        # Merge devuelve $1 por par YES+NO, así que el spread es 1 - avg_cost
                        spread_captured = (1.0 - avg_cost_per_share) * total_size
                        if spread_captured != 0:
                            self._logger.info(
                                "Spread capture en %s...: $%.4f (avg_cost=%.4f, shares=%.2f)",
                                market_id[:12], spread_captured, avg_cost_per_share, total_size,
                            )
                    # Limpiar tracker del mercado mergeado
                    self._spread_tracker.pop(market_id, None)

    # ------------------------------------------------------------------
    # RF v2: inventario y monitoreo de rewards
    # ------------------------------------------------------------------

    def _check_rf_inventory(self, markets: list[dict[str, Any]]) -> None:
        """Detecta fills en ordenes de RF y gestiona inventario two-sided.

        - Registra fills en el inventario local del RF
        - Detecta imbalance YES/NO y ajusta quotes
        - Mergea YES+NO cuando ambas >= merge_threshold
        """
        from datetime import datetime, timezone

        from src.strategy.base import Trade
        from src.strategy.rewards_farmer import RewardsFarmerStrategy

        rf_strategies = [
            s for s in self._strategies
            if isinstance(s, RewardsFarmerStrategy) and s.is_active
        ]
        if not rf_strategies:
            return

        rf = rf_strategies[0]
        if not rf._pending_orders:
            return

        markets_by_id = {m.get("condition_id", ""): m for m in markets}
        filled_ids: list[str] = []

        for order_id, signal in list(rf._pending_orders.items()):
            try:
                status = self._client.get_order_status(order_id)
            except Exception:
                continue

            status_val = status.get("status", "")
            if status_val == "ORDER_STATUS_MATCHED":
                # signal puede ser None para ordenes reconciliadas al arranque
                fill_px = float(status.get("price", signal.price if signal else 0.0))
                fill_sz = float(status.get("size_matched", status.get("original_size", signal.size if signal else 0.0)))
                market_id_for_fill = signal.market_id if signal else status.get("market_id", "")
                token_id_for_fill = signal.token_id if signal else status.get("asset_id", "")
                side_for_fill = signal.side if signal else str(status.get("side", "")).upper()
                market = markets_by_id.get(market_id_for_fill, {})
                tokens = market.get("tokens", [])
                if token_id_for_fill and side_for_fill:
                    rf.record_fill(
                        token_id=token_id_for_fill,
                        side=side_for_fill,
                        size=fill_sz,
                        market_id=market_id_for_fill,
                        tokens=tokens,
                    )
                if signal:
                    self._handle_trade(
                        Trade(
                            timestamp=datetime.now(timezone.utc).isoformat(),
                            strategy_name=signal.strategy_name,
                            market_id=signal.market_id,
                            token_id=signal.token_id,
                            side=signal.side,
                            price=fill_px,
                            size=fill_sz,
                            status="ORDER_STATUS_MATCHED",
                            order_id=order_id,
                        ),
                        market,
                    )
                self._logger.info(
                    "RF fill detectado: %s %s %.2fsh @ %.4f en %s...",
                    side_for_fill, token_id_for_fill[:8], fill_sz, fill_px,
                    market_id_for_fill[:12],
                )
                filled_ids.append(order_id)
                self._circuit_breaker.order_closed()
            elif status_val in ("ORDER_STATUS_CANCELLED", "CANCELLED"):
                filled_ids.append(order_id)
                self._circuit_breaker.order_closed()
            # Si esta LIVE, mantener en pending_orders para revisar en proximo ciclo

        for oid in filled_ids:
            rf._pending_orders.pop(oid, None)

    def _monitor_rewards_pct(self) -> None:
        """Consulta reward percentages via API y monitorea competencia.

        Llamado cada 5 minutos por el scheduler.
        Segun PROMPT paso 6:
        - Observar orderbook y estimar makers dentro del max_spread
        - Calcular nuestro share estimado
        - Si share < 0.5%: rotar a otro mercado
        - Si share > 5%: considerar aumentar size
        - Trackear top/bottom mercados para el review
        """
        try:
            percentages = self._client.get_reward_percentages()
        except Exception:
            self._logger.debug("No se pudieron consultar reward percentages")
            return

        if not percentages:
            return

        from src.strategy.rewards_farmer import RewardsFarmerStrategy
        rf_strategies = [
            s for s in self._strategies
            if isinstance(s, RewardsFarmerStrategy) and s.is_active
        ]
        if not rf_strategies:
            return

        rf = rf_strategies[0]
        rf.update_reward_pct(percentages)

        # Inicializar tracker de competencia si no existe
        if not hasattr(self, "_competition_history"):
            self._competition_history: dict[str, list[float]] = {}

        active_cids = set(rf._active_farms.keys())
        low_share_markets: list[str] = []
        high_share_markets: list[str] = []

        for cid in active_cids & set(percentages.keys()):
            pct = percentages[cid]

            # Trackear historial de share por mercado
            history = self._competition_history.setdefault(cid, [])
            history.append(pct)
            # Mantener solo las ultimas 12 muestras (1 hora a 5min/sample)
            if len(history) > 12:
                self._competition_history[cid] = history[-12:]

            # PROMPT: share < 0.5% -> demasiada competencia, rotar
            if pct < 0.5:
                self._logger.warning(
                    "RF CRITICO: share %.2f%% en %s... — rotando a otro mercado",
                    pct, cid[:12],
                )
                low_share_markets.append(cid)
            elif pct < 5.0:
                self._logger.warning(
                    "RF: share bajo (%.1f%%) en %s... — evaluar rotacion",
                    pct, cid[:12],
                )
            elif pct > 5.0:
                high_share_markets.append(cid)
                self._logger.info(
                    "RF: share alto (%.1f%%) en %s... — considerar aumentar size",
                    pct, cid[:12],
                )

        # Log resumen de competencia
        high_share = sum(1 for pct in percentages.values() if pct > 10.0)
        total = len(percentages)
        self._logger.info(
            "Competition: %d mercados tracked, %d con share >10%%, "
            "activos=%d, low_share=%d, high_share=%d",
            total, high_share, len(rf._active_farms), len(low_share_markets), len(high_share_markets),
        )

    # ------------------------------------------------------------------
    # WebSocket feed
    # ------------------------------------------------------------------

    def _start_ws_feed(self) -> threading.Thread:
        """Inicia el feed de orderbook real-time en un thread daemon separado."""
        logger_ref = self._logger  # Capturar referencia para el closure

        def _thread_main() -> None:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(self._feed.start())
            except Exception:
                logger_ref.exception("WebSocket feed thread crashed")
            finally:
                loop.close()

        thread = threading.Thread(
            target=_thread_main, daemon=True, name="ws-orderbook"
        )
        thread.start()
        self._logger.info("WebSocket feed thread iniciado")
        return thread

    def _subscribe_to_markets(self, markets: list[dict[str, Any]]) -> None:
        """Registra los tokens de los mercados activos en el WebSocket feed.

        Las suscripciones nuevas se envían al servidor WS en caliente si ya
        hay conexión establecida; si no, se envían al reconectar.
        """
        with self._feed._lock:
            already_subscribed = set(self._feed._subscriptions.keys())

        new_count = 0
        for market in markets:
            condition_id = market.get("condition_id", "")
            for token in market.get("tokens", []):
                token_id = token.get("token_id", "")
                if token_id and token_id not in already_subscribed:
                    self._feed.subscribe(
                        token_id=token_id,
                        callback=self._on_ws_change,
                        condition_id=condition_id,
                    )
                    already_subscribed.add(token_id)
                    new_count += 1

        if new_count:
            self._logger.info(
                "WS: %d nuevos tokens suscritos (total %d)",
                new_count, len(already_subscribed),
            )

    def _enrich_with_ws(self, market: dict[str, Any]) -> dict[str, Any]:
        """Agrega datos real-time del WebSocket al dict de market_data.

        Enriquece con datos de TODOS los tokens del mercado (YES + NO),
        guardandolos en token_data para que las estrategias operen ambos lados.

        Fallback: si el WebSocket aun no tiene datos para un token, usa la
        REST API (get_orderbook) una sola vez para desbloquear el primer ciclo.
        """
        tokens = market.get("tokens", [])
        if not tokens:
            return market

        enriched = dict(market)
        token_data: dict[str, dict[str, Any]] = {}
        missing_tokens: list[str] = []

        for token in tokens:
            token_id = token.get("token_id", "")
            if not token_id:
                continue

            ws_mid = self._feed.get_midpoint(token_id)
            ob = self._feed.get_orderbook(token_id)

            td: dict[str, Any] = {}
            if ob is not None and ob.bids and ob.asks:
                best_bid = ob.bids[0][0]
                best_ask = ob.asks[0][0]
                if best_bid < best_ask:
                    td["mid_price"] = ws_mid if ws_mid is not None else round((best_bid + best_ask) / 2, 4)
                    td["spread"] = round(best_ask - best_bid, 4)
                    td["best_bid"] = best_bid
                    td["best_ask"] = best_ask
                    td["orderbook"] = {
                        "bids": [{"price": p, "size": s} for p, s in ob.bids],
                        "asks": [{"price": p, "size": s} for p, s in ob.asks],
                    }
            else:
                missing_tokens.append(token_id)

            if not td:
                # Fallback a REST API usando get_best_bid_ask() que es más confiable
                # que get_orderbook() (el cual puede retornar datos stale 0.01/0.99)
                try:
                    best_bid, best_ask = self._client.get_best_bid_ask(token_id)
                    if best_bid > 0 and best_ask < 1.0 and best_bid < best_ask:
                        td["mid_price"] = round((best_bid + best_ask) / 2, 4)
                        td["spread"] = round(best_ask - best_bid, 4)
                        td["best_bid"] = best_bid
                        td["best_ask"] = best_ask
                        # Orderbook sin size real — solo precios para calcular mid
                        td["orderbook"] = {
                            "bids": [{"price": str(best_bid), "size": "0"}],
                            "asks": [{"price": str(best_ask), "size": "0"}],
                        }
                        self._logger.info(
                            "REST fallback (sin depth real) para %s... en %s...",
                            token_id[:8], market.get("condition_id", "")[:8]
                        )
                except Exception:
                    pass

            if td:
                token_data[token_id] = td

        if missing_tokens:
            self._logger.info(
                "WS data pending for tokens %s in market %s... (usando REST fallback)",
                [t[:8] for t in missing_tokens], market.get("condition_id", "")[:8]
            )

        # Compatibilidad: si tenemos datos del primer token, inyectarlos tambien a nivel market
        if token_data:
            first_tid = tokens[0].get("token_id", "")
            if first_tid in token_data:
                first = token_data[first_tid]
                if "mid_price" in first:
                    enriched["mid_price"] = first["mid_price"]
                if "spread" in first:
                    enriched["spread"] = first["spread"]
                if "best_bid" in first:
                    enriched["best_bid"] = first["best_bid"]
                if "best_ask" in first:
                    enriched["best_ask"] = first["best_ask"]
                if "orderbook" in first:
                    enriched["orderbook"] = first["orderbook"]

        enriched["token_data"] = token_data
        return enriched

    def _on_ws_change(
        self, token_id: str, ob: OrderbookState, change_type: str
    ) -> None:
        """Callback del WebSocket para cambios significativos en el orderbook."""
        self._logger.debug(
            "WS %s: token=%s... mid=%.4f depth=%.1f",
            change_type, token_id[:8], ob.midpoint, ob.depth,
        )

    def _on_feed_health_event(self, event_type: str, staleness_sec: float) -> None:
        """Dead man's switch callback: reacciona a eventos de salud del WS feed.

        - "stale"    → pausa bot + cancela órdenes + alerta Telegram.
        - "recovered"→ auto-resume si la pausa fue por dead man's switch.
        """
        if event_type == "stale":
            if self._feed_was_stale:
                return
            self._feed_was_stale = True
            self._logger.critical(
                "DEAD MAN'S SWITCH: feed WS sin mensajes hace %.0fs — pausando bot",
                staleness_sec,
            )
            if self._state == "running":
                self._deadman_paused = True
                self.pause()

            try:
                self._client.cancel_all_orders()
                self._logger.warning("Dead man's switch: todas las órdenes canceladas")
            except Exception:
                self._logger.exception("Dead man's switch: error cancelando órdenes")

            send_alert(
                "🚨 *DEAD MAN'S SWITCH ACTIVADO*\n"
                f"Feed WS sin mensajes hace `{staleness_sec:.0f}s`\n"
                "Bot *PAUSADO* y órdenes *CANCELADAS* automáticamente.\n"
                "Se reanudara automaticamente cuando el feed se recupere."
            )

        elif event_type == "recovered":
            self._feed_was_stale = False
            paused_by_deadman = getattr(self, '_deadman_paused', False)
            self._deadman_paused = False

            if paused_by_deadman and self._state == "paused":
                self._logger.info(
                    "Feed WS recuperado (staleness=%.1fs) — reanudando bot automaticamente",
                    staleness_sec,
                )
                self.resume()
                send_alert(
                    "✅ *Feed WS recuperado*\n"
                    f"Último dato: `{staleness_sec:.1f}s` atrás.\n"
                    "Bot *REANUDADO* automaticamente."
                )
            else:
                self._logger.info(
                    "Feed WS recuperado (staleness=%.1fs). Bot sigue PAUSADO. "
                    "Usar /resume para reactivar.",
                    staleness_sec,
                )
                send_alert(
                    "✅ *Feed WS recuperado*\n"
                    f"Último dato: `{staleness_sec:.1f}s` atrás.\n"
                    "Bot sigue *PAUSADO* por seguridad → usar `/resume` para reactivar."
                )

    # ------------------------------------------------------------------
    # Actualización de mercados (schedule + startup)
    # ------------------------------------------------------------------

    def _daily_reset(self) -> None:
        """Reset de contadores diarios. Llamado a 00:00 UTC."""
        self._circuit_breaker.reset_daily()
        for strat in self._strategies:
            if hasattr(strat, "reset_daily_counters"):
                strat.reset_daily_counters()
        self._logger.info("Reset diario: circuit_breaker + contadores de fill rate")

    def _update_markets(self) -> None:
        """Quick refresh: re-evalua mercados usando cache del MarketAnalyzer.

        Llamado cada 5 minutos. NO invalida el cache de Gamma API (15 min TTL).
        Solo re-scorea mercados cacheados y re-suscribe al WS.
        El scan profundo (_full_market_scan) corre cada 4 horas.
        """
        try:
            markets = self._market_analyzer.scan_markets()
            if markets:
                self._active_markets = markets
                self._subscribe_to_markets(markets)
                self._logger.info(
                    "Quick refresh: %d mercados activos | WS: %s",
                    len(markets),
                    "conectado" if self._feed.is_connected() else "desconectado",
                )
        except Exception:
            self._logger.debug("Quick refresh fallo (cache expirado?)")

    def _full_market_scan(self) -> None:
        """Scan profundo: invalida cache y re-descubre mercados de Gamma + CLOB.

        Llamado cada 4 horas para detectar nuevos mercados con rewards.
        """
        try:
            self._market_analyzer.invalidate_cache()
            markets = self._market_analyzer.scan_markets()
            self._active_markets = markets
            self._subscribe_to_markets(markets)
            self._logger.info(
                "Full scan completado: %d mercados activos | WS: %s",
                len(markets),
                "conectado" if self._feed.is_connected() else "desconectado",
            )
        except Exception:
            self._logger.exception("Error en full market scan")

    # ------------------------------------------------------------------
    # Self-review programado
    # ------------------------------------------------------------------

    def _run_review(self) -> None:
        """Ejecuta self-review con Claude Haiku (llamado por el scheduler cada 8h)."""
        self._logger.info("Iniciando self-review programado...")
        try:
            state = self.get_status()
            review = self._reviewer.run_review(state=state)
            self._logger.info("Self-review completado: %s", review.get("status", "ok"))

            # Si Claude recomienda pausar, hacerlo de forma preventiva
            analysis = review.get("analysis")
            if isinstance(analysis, dict) and analysis.get("should_pause"):
                self._logger.warning(
                    "Claude recomienda pausa preventiva (risk_level=%s)",
                    analysis.get("risk_level", "?"),
                )
                self.pause()

        except Exception:
            self._logger.exception("Self-review fallido")

    def _run_reconciliation(self) -> None:
        """Reconcilia estado on-chain vs state.json local. Ejecutado cada 6h."""
        self._logger.info("Iniciando reconciliación on-chain...")
        try:
            result = self._client.reconcile_state()
            if result.get("desync"):
                delta = result.get("balance_delta", 0.0)
                send_alert(
                    f"⚠️ *Desync detectado* en reconciliación\n"
                    f"On-chain: `${result['balance_onchain']:.4f}` USDC\n"
                    f"Local: `${result['balance_local']:.4f}` USDC\n"
                    f"Delta: `${delta:.4f}` | State actualizado con ground truth."
                )
            else:
                self._logger.info(
                    "Reconciliación OK: balance=%.4f USDC, ordenes=%d",
                    result.get("balance_onchain", 0.0),
                    result.get("open_orders_onchain", 0),
                )
        except Exception:
            self._logger.exception("Error en reconciliación on-chain")

    def force_review(self) -> dict[str, Any]:
        """Ejecuta un self-review inmediato. Llamado desde Telegram /review.

        Returns:
            Dict con {'triggered': True} — el review notifica por Telegram
            directamente vía SelfReviewer._notify_telegram().
        """
        self._run_review()
        return {"triggered": True}


    # ------------------------------------------------------------------
    # Circuit breaker → Telegram
    # ------------------------------------------------------------------

    def _cb_alert_handler(self, reason: str, message: str) -> None:
        """Recibe alertas del CircuitBreaker y las envía por Telegram."""
        icons = {
            "daily_drawdown":      "🛑",
            "consecutive_losses":  "📉",
            "consecutive_errors":  "💥",
            "single_trade_loss":   "⚠️",
            "market_hourly_loss":  "⏰",
            "rolling_30d_drawdown": "💀",
            "rolling_15d_drawdown": "📉",
            "rolling_7d_drawdown":  "📊",
        }
        icon = icons.get(reason, "🚨")
        send_alert(f"{icon} *Circuit Breaker* [`{reason}`]\n{message}")

        # Pausar el bot en cualquier trigger que implique stop total
        _pause_triggers = {
            "daily_drawdown",
            "consecutive_losses",
            "consecutive_errors",
            "rolling_30d_drawdown",
        }
        if reason in _pause_triggers:
            self.pause()

    def _on_scale_down(self, factor: float) -> None:
        """Reduce el tamaño de órdenes tras rolling 7d drawdown."""
        self._logger.warning("Scale-down activado: factor=%.2f — reduciendo order_size", factor)
        for strategy in self._strategies:
            if hasattr(strategy, "scale_order_size"):
                strategy.scale_order_size(factor)
        send_alert(
            f"📊 *Scale-down activado* (factor `{factor:.0%}`)\n"
            "Reduciendo tamaño de órdenes por rolling 7d drawdown."
        )

    def _on_pause_strategies(self, strategies: list[str]) -> None:
        """Pausa estrategias específicas por rolling 15d drawdown."""
        self._logger.warning("Pausando estrategias por 15d drawdown: %s", strategies)
        for strategy in self._strategies:
            if strategy.name in strategies:
                strategy.pause()
        send_alert(
            f"📉 *Estrategias pausadas* por rolling 15d drawdown:\n"
            f"`{'`, `'.join(strategies)}`"
        )

    # ------------------------------------------------------------------
    # Control (interfaz para Telegram y señales del OS)
    # ------------------------------------------------------------------

    def get_status(self) -> dict[str, Any]:
        """Retorna estado completo del bot para Telegram /status y self-review.

        Consulta ordenes abiertas reales via CLOB API para mostrar estado preciso.
        """
        cb = self._circuit_breaker.get_status()
        monitor_status = getattr(self, "_strategy_monitor", None)
        if monitor_status is not None:
            monitor_status = monitor_status.get_status()
        else:
            monitor_status = {}
        killed_strategies = [name for name, info in monitor_status.items() if info.get("is_killed")]

        # Consultar ordenes abiertas reales desde el CLOB
        try:
            open_orders = self._client.get_positions()
            open_orders_count = len(open_orders)
            open_exposure = sum(
                float(o.get("price", 0)) * float(o.get("original_size", 0))
                for o in open_orders
                if str(o.get("side", "")).upper() == "BUY"
            )
        except Exception:
            open_orders_count = 0
            open_exposure = 0.0

        # Inventory exposure solo de posiciones reales on-chain (no state.json viejo)
        inventory_exposure = self._inventory.get_total_exposure()
        total_exposure = inventory_exposure + open_exposure

        return {
            "state": self._state,
            "paper_mode": self._paper_mode,
            "balance_usdc": self._cached_balance,
            "total_exposure": total_exposure,
            "inventory_exposure": inventory_exposure,
            "open_exposure": open_exposure,
            "daily_pnl": cb.get("daily_pnl", 0),
            "open_orders": open_orders_count,
            "circuit_breaker": cb["triggered"],
            "trigger_reason": cb["trigger_reason"],
            "consecutive_errors": cb["consecutive_errors"],
            "markets_over_limit": cb["markets_over_limit"],
            "active_markets": len(self._active_markets),
            "ws_connected": self._feed.is_connected(),
            "strategies": [s.name for s in self._strategies if s.is_active],
            "killed_strategies": killed_strategies,
            "rolling_drawdown": cb.get("rolling_drawdown", {}),
            "start_time": self._start_time,
        }

    def get_positions(self) -> dict[str, dict[str, float]]:
        """Retorna posiciones de inventario por mercado."""
        return self._inventory.get_positions()

    def get_positions_detail(self) -> list[dict[str, Any]]:
        """Retorna posiciones enriquecidas para /positions de Telegram (tip 11).

        Para cada mercado activo incluye: mid actual, participation share,
        inventory en USDC y horas desde la ultima orden.
        """
        import time as _time
        result = []
        positions = self._inventory.get_positions()

        for market in self._active_markets:
            cid = market.get("condition_id", "")
            if not cid:
                continue

            inv = positions.get(cid, {})
            yes_inv = inv.get("yes", 0.0)
            no_inv = inv.get("no", 0.0)
            total_inv = abs(yes_inv) + abs(no_inv)

            # Horas desde ultimo refresh del MM
            mm = next(
                (s for s in self._strategies if s.name == "market_maker"), None
            )
            last_ts = mm._last_refresh.get(cid, 0.0) if mm else 0.0
            hours_since = (_time.time() - last_ts) / 3600.0 if last_ts > 0 else None

            result.append({
                "condition_id": cid,
                "question": market.get("question", "")[:45],
                "mid_price": market.get("mid_price", 0.0),
                "participation_share": market.get("_participation_share", 0.0),
                "yes_inventory": yes_inv,
                "no_inventory": no_inv,
                "total_inventory_usdc": total_inv,
                "hours_since_last_order": hours_since,
                "rewards_active": market.get("rewards_active", False),
            })

        return result

    def pause(self) -> None:
        """Pausa el trading instantáneamente. Llamado por Telegram /pause o CB."""
        if self._state == "paused":
            return
        self._state = "paused"
        try:
            self._client.cancel_all_orders()
            self._logger.info("Bot PAUSADO - órdenes abiertas canceladas")
        except Exception:
            self._logger.exception("Bot PAUSADO - error cancelando órdenes abiertas")
        for strategy in self._strategies:
            strategy.pause()
        self._logger.info("Bot PAUSADO")
        send_alert("⏸️ *Bot PAUSADO* — Trading detenido. Usar `/resume` para reactivar.")

    def resume(self) -> None:
        """Reanuda el trading. Llamado por Telegram /resume."""
        self._state = "running"
        self._deadman_paused = False  # Reset: fue manual
        for strategy in self._strategies:
            strategy.resume()
        self._logger.info("Bot REANUDADO")
        send_alert("▶️ *Bot REANUDADO* — Trading activo nuevamente.")

    def kill(self) -> None:
        """Cancela todo y detiene el bot. Llamado por Telegram /kill."""
        self._shutdown()

    # ------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------

    def _handle_shutdown(self, signum: int, frame: Any) -> None:
        """Handler para SIGTERM y SIGINT."""
        self._logger.info("Señal de shutdown recibida (signum=%d)", signum)
        self._shutdown()

    def _shutdown(self) -> None:
        """Shutdown graceful: cancela órdenes, notifica Telegram, detiene el loop."""
        if self._state == "stopped":
            return  # Evitar doble shutdown

        self._logger.info("Iniciando shutdown graceful...")
        self._state = "stopped"

        send_alert(
            "🛑 *NachoMarket deteniéndose*\n"
            "Cancelando todas las órdenes abiertas..."
        )

        # Detener heartbeat antes de cancelar ordenes (evita race condition)
        try:
            self._client.stop_heartbeat()
        except Exception:
            self._logger.debug("Error deteniendo heartbeat (ignorado)")

        # Cancelar todas las órdenes abiertas
        try:
            self._client.cancel_all_orders()
            self._logger.info("Todas las órdenes canceladas")
        except Exception:
            self._logger.exception("Error al cancelar órdenes durante shutdown")

        # Pausar estrategias
        for strategy in self._strategies:
            strategy.pause()

        # El thread del WS es daemon — muere automáticamente con el proceso
        self._logger.info("Shutdown completado")


# ------------------------------------------------------------------
# Helpers de módulo
# ------------------------------------------------------------------

def _load_yaml(path: str) -> dict[str, Any]:
    """Carga un archivo YAML. Retorna {} si no existe o falla el parsing."""
    file_path = Path(path)
    if not file_path.exists():
        return {}
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except Exception as exc:
        print(f"[WARNING] No se pudo cargar {path}: {exc}", file=sys.stderr)
        return {}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="NachoMarket — Bot de Market Making para Polymarket",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Ejemplos:\n"
            "  python -m src.main                   # paper mode (default)\n"
            "  python -m src.main --paper            # paper mode explícito\n"
            "  python -m src.main --live             # LIVE — dinero real\n"
            "  python -m src.main --review-only      # solo self-review y salir\n"
        ),
    )

    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--paper",
        action="store_true",
        default=False,
        help="Paper trading: simula ejecución sin dinero real (modo default)",
    )
    mode_group.add_argument(
        "--live",
        action="store_true",
        default=False,
        help="LIVE: opera con dinero real. REQUIERE credenciales completas en .env",
    )
    parser.add_argument(
        "--review-only",
        action="store_true",
        default=False,
        dest="review_only",
        help="Solo ejecutar self-review con Claude Haiku y salir",
    )
    return parser.parse_args()


def main() -> None:
    """Entry point principal."""
    args = _parse_args()

    # Determinar modo: línea de comandos > archivo config > default paper
    if args.live:
        paper_mode = False
    elif args.paper:
        paper_mode = True
    else:
        # Leer de settings.yaml
        try:
            from pathlib import Path
            import yaml
            settings_path = Path("config/settings.yaml")
            if settings_path.exists():
                with open(settings_path) as f:
                    config = yaml.safe_load(f) or {}
                    mode = config.get("mode", "paper").lower()
                    paper_mode = (mode == "paper")
            else:
                paper_mode = True
        except Exception:
            paper_mode = True  # Default seguro

    if args.live:
        print(
            "\n⚠️  MODO LIVE ACTIVADO — Operando con DINERO REAL.\n"
            "   Asegúrate de que el .env tiene las credenciales correctas.\n"
            "   Presiona Ctrl+C para cancelar en los próximos 5 segundos...\n",
            file=sys.stderr,
        )
        try:
            time.sleep(5)
        except KeyboardInterrupt:
            print("Cancelado.", file=sys.stderr)
            sys.exit(0)

    # Pre-flight: verificar acceso geográfico (Argentina bloqueada)
    if not args.review_only:
        try:
            verify_geo_access()
        except ConnectionError as geo_err:
            print(f"\n🚫 GEO-BLOCK: {geo_err}", file=sys.stderr)
            print(
                "   El bot debe correr desde un VPS fuera de Argentina.\n"
                "   Ver: scripts/deploy.sh para desplegar en un VPS fuera de Argentina.",
                file=sys.stderr,
            )
            sys.exit(1)

    try:
        bot = NachoMarketBot(paper_mode=paper_mode, review_only=args.review_only)
        bot.run()
    except KeyboardInterrupt:
        print("\nInterrumpido por el usuario.", file=sys.stderr)
        sys.exit(0)
    except Exception as exc:
        print(f"\nError fatal al iniciar el bot: {exc}", file=sys.stderr)
        logging.getLogger("nachomarket").exception("Fatal startup error")
        sys.exit(1)


if __name__ == "__main__":
    main()
