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

from src.analysis.correlation import CorrelationTracker
from src.analysis.regime_detector import MarketRegimeDetector
from src.analysis.toxic_flow import ToxicFlowDetector
from src.analysis.var import VaRCalculator
from src.external.fred import FREDClient
from src.external.polyscan import WhaleTracker
from src.polymarket.client import PolymarketClient
from src.polymarket.markets import MarketAnalyzer
from src.polymarket.websocket import OrderbookFeed, OrderbookState
from src.risk.blacklist import MarketBlacklist
from src.risk.circuit_breaker import CircuitBreaker
from src.risk.inventory import InventoryManager
from src.risk.market_profitability import MarketProfiler
from src.risk.position_sizer import PositionSizer
from src.risk.strategy_monitor import StrategyMonitor
from src.review.self_review import SelfReviewer
from src.strategy.allocator import StrategyAllocator
from src.strategy.stages import StageMachine
from src.strategy.copy_trade import CopyTradeStrategy
from src.strategy.directional import DirectionalStrategy
from src.strategy.event_driven import EventDrivenStrategy
from src.strategy.market_maker import MarketMakerStrategy
from src.strategy.multi_arb import MultiArbStrategy
from src.strategy.rewards_farmer import RewardsFarmerStrategy
from src.strategy.stat_arb import StatArbStrategy
from src.telegram.bot import TelegramBot, send_alert
from src.utils.logger import setup_logger

load_dotenv()

# Intervalo entre actualizaciones forzadas de mercados (minutos)
_MARKET_UPDATE_INTERVAL_MIN = 15
# Tiempo que esperamos a que el WS conecte tras arrancar
_WS_STARTUP_WAIT_SEC = 3.0
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
        )
        self._logger.info("Verificando conexión con Polymarket CLOB...")
        self._client.test_connection()  # lanza excepción si falla
        self._logger.info("Conexión OK")

        # --- c. WebSocket feed (el thread se arranca en run()) ---
        self._feed = OrderbookFeed()
        # Dead man's switch: pausar bot si el feed queda sin mensajes >60s
        self._feed.register_health_callback(self._on_feed_health_event)
        self._feed_was_stale: bool = False  # Para no repetir alertas

        # --- d. Telegram bot (auto-arranca thread daemon en __init__) ---
        # Se inicializa antes que los demás para poder recibir alertas de arranque.
        self._telegram = TelegramBot(bot_controller=self)

        # --- e. Strategies habilitadas en settings ---
        enabled = self._settings.get("strategies_enabled", ["market_maker", "multi_arb"])
        _strategy_factories = {
            "market_maker":   lambda: MarketMakerStrategy(self._client, self._settings),
            "multi_arb":      lambda: MultiArbStrategy(self._client, self._settings),
            "directional":    lambda: DirectionalStrategy(self._client, self._settings),
            "stat_arb":       lambda: StatArbStrategy(self._client, self._settings),
            "rewards_farmer": lambda: RewardsFarmerStrategy(self._client, self._settings),
            "event_driven":   lambda: EventDrivenStrategy(self._client, self._settings),
            "copy_trade":     lambda: CopyTradeStrategy(
                self._client, self._settings, whale_tracker=None  # wired post-init
            ),
        }
        self._strategies = [
            _strategy_factories[name]()
            for name in enabled
            if name in _strategy_factories
        ]
        self._logger.info(
            "Estrategias habilitadas: %s", [s.name for s in self._strategies]
        )

        # --- f. Risk: circuit breaker + position sizer + inventory ---
        self._circuit_breaker = CircuitBreaker(
            self._risk_config,
            alert_callback=self._cb_alert_handler,
        )
        self._position_sizer = PositionSizer(self._risk_config)
        self._inventory = InventoryManager(self._risk_config)
        self._profiler = MarketProfiler(self._risk_config)
        # Balance cacheado para no hacer HTTP en cada signal filter
        self._cached_balance: float = 400.0

        # --- f2. Blacklist (WR-based, Fase 4) ---
        self._blacklist = MarketBlacklist.from_config(self._settings)
        for strategy in self._strategies:
            strategy.set_blacklist(self._blacklist)

        # --- f3. Stage machine (Fase 2) ---
        strategy_names = [s.name for s in self._strategies]
        self._stage_machine = StageMachine(
            strategy_names=strategy_names,
            alert_callback=send_alert,
        )

        # --- g. Scheduler (self-review, market updates) ---
        self._market_analyzer = MarketAnalyzer(self._client, self._markets_config)
        self._active_markets: list[dict[str, Any]] = []

        # --- h. Self-reviewer (Telegram callback resuelto en runtime) ---
        self._reviewer = SelfReviewer(
            model=self._settings.get("review_model", "claude-haiku-4-5-20251001"),
            stage_machine=self._stage_machine,
        )

        # --- i. Hedge-fund-grade: analysis + alpha modules ---
        self._regime_detector = MarketRegimeDetector(self._settings)
        self._toxic_flow = ToxicFlowDetector(self._risk_config.get("toxic_flow", {}))
        self._correlation = CorrelationTracker(self._settings)
        self._var_calc = VaRCalculator()

        # Strategy monitor (kill switch) + bandit allocator
        sm_cfg = self._risk_config.get("strategy_monitor", {})
        self._strategy_monitor = StrategyMonitor(
            kill_calmar_threshold=sm_cfg.get("kill_calmar_threshold", 0.5),
            kill_evaluation_days=sm_cfg.get("kill_evaluation_days", 14),
            min_trades=sm_cfg.get("min_trades_for_kill", 10),
            pause_callback=self._on_strategy_killed,
            alert_callback=send_alert,
        )
        self._allocator = StrategyAllocator(
            strategies=[name for name in self._settings.get("strategies_enabled", [])],
            total_capital=self._settings.get("capital_total", 400.0),
        )
        self._allocator.set_stage_machine(self._stage_machine)

        # External data (PolyScan + FRED)
        self._whale_tracker = WhaleTracker()
        self._fred_client = FREDClient()

        self._loop_interval: int = self._settings.get(
            "main_loop_interval_sec", 10
        )

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
        schedule.every().day.at("00:00").do(self._circuit_breaker.reset_daily)
        # Reconciliación on-chain cada 6h (TODO 1.2)
        schedule.every(6).hours.do(self._run_reconciliation)
        # Refresh blacklist WR cada 6h (Fase 4)
        schedule.every(6).hours.do(self._run_blacklist_refresh)
        # Evaluación del strategy monitor cada hora (kill switch)
        schedule.every(1).hours.do(self._run_strategy_monitor)
        # Poll whale tracker cada 60s
        schedule.every(60).seconds.do(self._poll_whale_tracker)
        # Evaluar A/B bandit allocator diariamente
        schedule.every(1).days.do(self._run_allocator_evaluation)

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

        # Actualizar balance cacheado (una vez por ciclo para no saturar la API)
        try:
            self._cached_balance = self._client.get_balance()
        except Exception:
            self._logger.debug(
                "No se pudo actualizar balance; usando valor cacheado $%.2f",
                self._cached_balance,
            )

        # Circuit breaker: si está activo, skip del ciclo
        if self._circuit_breaker.is_triggered():
            self._logger.warning("Circuit breaker activo — ciclo salteado")
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

        # Detectar fills y disparar reposicionamiento antes de operar
        self._check_fills_and_reposition(markets)

        # Para cada mercado, correr todas las estrategias habilitadas
        for market in markets:
            # Enriquecer con datos real-time del WebSocket si están disponibles
            market_data = self._enrich_with_ws(market)

            # Actualizar regime detector y correlation tracker con mid price
            mid = market_data.get("mid_price", 0.0)
            condition_id = market.get("condition_id", "")
            tokens = market.get("tokens", [])
            token_id = tokens[0].get("token_id", "") if tokens else ""
            if token_id and mid > 0:
                self._regime_detector.update(token_id, mid)
                self._correlation.update(token_id, mid)
            # Añadir estado de régimen al market_data
            if token_id:
                regime_state = self._regime_detector.get_state(token_id)
                market_data["regime"] = regime_state.regime.value if regime_state else "unknown"
                market_data["spread_multiplier"] = (
                    regime_state.spread_multiplier if regime_state else 1.0
                )
                market_data["pause_mm"] = (
                    regime_state.pause_mm if regime_state else False
                )
                # Marcar mercados con toxic flow como evitables
                market_data["toxic_flow"] = self._toxic_flow.is_toxic(token_id)

            for strategy in self._strategies:
                if not strategy.is_active:
                    continue
                # Skip MM en régimen VOLATILE
                if market_data.get("pause_mm") and strategy.name == "market_maker":
                    continue
                # Skip mercados con toxic flow (excepto stat_arb que aprovecha ineficiencias)
                if market_data.get("toxic_flow") and strategy.name not in ("stat_arb", "multi_arb"):
                    self._logger.debug(
                        "Toxic flow detectado en %s... — salteando %s",
                        condition_id[:12], strategy.name,
                    )
                    continue
                # Skip estrategias que el monitor mató
                if self._strategy_monitor.is_killed(strategy.name):
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

        # [MM-específico] Respetar refresh_seconds para no sobre-solicitar la API
        needs_refresh_fn = getattr(strategy, "needs_refresh", None)
        if needs_refresh_fn is not None and not needs_refresh_fn(condition_id):
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

        # [MM-específico] Registrar timestamp del refresh
        mark_refreshed_fn = getattr(strategy, "mark_refreshed", None)
        if mark_refreshed_fn is not None:
            mark_refreshed_fn(condition_id)

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
        projected_exposure = self._inventory.get_total_exposure()

        for sig in signals:
            # Límite de órdenes abiertas
            if not self._circuit_breaker.can_place_order():
                self._logger.debug(
                    "Signal descartada: límite de órdenes abiertas alcanzado"
                )
                break  # Si el CB no permite más, descartar el resto también

            # Regla 5%: exposure total no puede superar 5% del capital por operación
            if not self._position_sizer.can_trade(
                projected_exposure, self._cached_balance, sig.size
            ):
                self._logger.debug(
                    "Signal descartada: exposure proyectada $%.2f + $%.2f "
                    "excedería 5%% de $%.2f",
                    projected_exposure, sig.size, self._cached_balance,
                )
                continue

            # Límite de exposure por mercado individual
            if not self._inventory.can_add_position(sig.market_id, sig.size):
                self._logger.debug(
                    "Signal descartada: límite de mercado %s... alcanzado",
                    sig.market_id[:12],
                )
                continue

            filtered.append(sig)
            projected_exposure += sig.size  # Acumular exposición proyectada

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
        yes_token_id = tokens[0].get("token_id", "") if tokens else ""
        token_type = "yes" if trade.token_id == yes_token_id else "no"

        # Actualizar inventario
        self._inventory.add_trade(
            market_id=trade.market_id,
            token_type=token_type,
            side=trade.side,
            size=trade.size,
        )

        # Notificar circuit breaker
        if trade.status == "error":
            self._circuit_breaker.record_error()
        else:
            # Estimar PnL incremental para el circuit breaker
            pnl = self._estimate_trade_pnl(trade)
            if pnl is not None:
                self._circuit_breaker.record_trade(pnl)
                self._circuit_breaker.record_market_pnl(trade.market_id, pnl)

        # Actualizar profiler de rentabilidad por mercado
        question = market.get("question", "")
        self._profiler.update(trade.market_id, trade, question=question)

        # Alimentar strategy monitor + bandit allocator con el PnL del trade
        pnl_for_monitor = self._estimate_trade_pnl(trade)
        if pnl_for_monitor is not None:
            self._strategy_monitor.record_trade(trade.strategy_name, pnl_for_monitor)
            self._allocator.record_outcome(trade.strategy_name, pnl_for_monitor)

        # Alimentar toxic flow detector con el fill
        tokens = market.get("tokens", [])
        token_id = tokens[0].get("token_id", "") if tokens else ""
        if token_id and trade.side in ("BUY", "SELL"):
            self._toxic_flow.record_fill(
                token_id=token_id,
                side=trade.side,
                fill_price=trade.price,
                mid_before=market.get("mid_price", trade.price),
            )

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

    def _check_fills_and_reposition(self, markets: list[dict[str, Any]]) -> None:
        """Detecta fills recientes y dispara reposicionamiento en MM strategy.

        Usa get_order_status() para verificar ordenes pendientes del repositioner.
        Solo verifica ordenes que el repositioner está trackeando para minimizar
        el numero de llamadas a la API.
        """
        from src.strategy.market_maker import MarketMakerStrategy

        mm_strategies = [
            s for s in self._strategies
            if isinstance(s, MarketMakerStrategy) and s.is_active
        ]
        if not mm_strategies:
            return

        mm = mm_strategies[0]

        # Solo procesar si hay repositiones pendientes
        if mm._repositioner.pending_count == 0:
            return

        # Verificar estado de ordenes pending de reposicion
        # (las pending originales que esperan ser filleadas)
        pending_ids = list(mm._repositioner._pending.keys())
        market_map = {
            m.get("condition_id", ""): m for m in markets
        }

        for orig_order_id in pending_ids:
            pending = mm._repositioner._pending.get(orig_order_id)
            if pending is None or pending.reposition_order_id:
                continue  # Ya tiene reposicion en vuelo

            try:
                status = self._client.get_order_status(orig_order_id)
                if status.get("status") == "MATCHED":
                    # Construir un Trade simulado con los datos del fill
                    from src.strategy.base import Trade
                    from datetime import datetime, timezone
                    fill_trade = Trade(
                        timestamp=datetime.now(timezone.utc).isoformat(),
                        market_id=pending.market_id,
                        token_id=pending.token_id,
                        side=pending.original_side,
                        price=pending.fill_price,
                        size=pending.fill_size,
                        order_id=orig_order_id,
                        status="MATCHED",
                        strategy_name="market_maker",
                        fee_paid=0.0,
                    )
                    market = market_map.get(pending.market_id, {})
                    repo_trades = mm.process_fill(fill_trade)
                    for t in repo_trades:
                        self._handle_trade(t, market)
            except Exception:
                self._logger.debug(
                    "No se pudo verificar fill de orden %s...", orig_order_id[:12]
                )

    # ------------------------------------------------------------------
    # Merges de inventario YES+NO → USDC
    # ------------------------------------------------------------------

    def _check_merges(self, markets: list[dict[str, Any]]) -> None:
        """Detecta y ejecuta merges de inventario cuando corresponde.

        Lógica: si min(yes_shares, no_shares) > merge_threshold → merge.
        Ejecuta un SELL al mid price para reducir la posición.
        """
        positions = self._inventory.get_positions()
        if not positions:
            return

        markets_by_id = {m["condition_id"]: m for m in markets}

        for market_id, pos in positions.items():
            if not self._inventory.should_merge(market_id):
                continue

            market = markets_by_id.get(market_id)
            if not market:
                continue  # Mercado ya no está en nuestra lista activa

            tokens = market.get("tokens", [])
            if not tokens:
                continue

            yes_token_id = tokens[0].get("token_id", "")
            merge_size = min(pos.get("yes", 0.0), pos.get("no", 0.0))

            if merge_size <= 0 or not yes_token_id:
                continue

            try:
                self._client.merge_positions(yes_token_id, merge_size)
                self._inventory.clear_market(market_id)

                question = market.get("question", "")[:30]
                self._logger.info(
                    "Merged %.2f shares en %s... ('%s...') → $%.2f USDC recuperados",
                    merge_size, market_id[:12], question, merge_size,
                )
                send_alert(
                    f"♻️ Merged `{merge_size:.2f}` shares\n"
                    f"_{question}..._\n"
                    f"→ `${merge_size:.2f}` USDC recuperados"
                )
            except Exception:
                self._logger.exception(
                    "Error al mergear posición en %s...", market_id[:12]
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

        Si el WS tiene datos válidos para el primer token (YES), los incluye:
        - mid_price: midpoint del orderbook en tiempo real
        - spread: best_ask - best_bid
        - best_bid, best_ask: mejores niveles del libro

        Si no hay datos WS disponibles, retorna el market_data sin cambios.
        """
        tokens = market.get("tokens", [])
        if not tokens:
            return market

        token_id = tokens[0].get("token_id", "")
        if not token_id:
            return market

        enriched = dict(market)

        ws_mid = self._feed.get_midpoint(token_id)
        if ws_mid is not None and 0.0 < ws_mid < 1.0:
            enriched["mid_price"] = ws_mid

        ob = self._feed.get_orderbook(token_id)
        if ob is not None and ob.bids and ob.asks:
            best_bid = ob.bids[0][0]
            best_ask = ob.asks[0][0]
            if best_bid < best_ask:
                enriched["spread"] = round(best_ask - best_bid, 4)
                enriched["best_bid"] = best_bid
                enriched["best_ask"] = best_ask

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

        - "stale"    → pausa bot + cancela órdenes + alerta CRÍTICA Telegram.
        - "recovered"→ alerta de recuperación (NO reanuda sola; requiere /resume).

        El callback es ejecutado desde el asyncio loop del WS thread, pero
        pause() / send_alert() son thread-safe, así que no hay race condition.
        """
        if event_type == "stale":
            if self._feed_was_stale:
                return  # Ya notificado, no spamear
            self._feed_was_stale = True
            self._logger.critical(
                "DEAD MAN'S SWITCH: feed WS sin mensajes hace %.0fs — pausando bot",
                staleness_sec,
            )
            # 1. Pausar el bot (stop trading cycle)
            if self._state == "running":
                self.pause()

            # 2. Cancelar todas las órdenes abiertas
            try:
                self._client.cancel_all_orders()
                self._logger.warning("Dead man's switch: todas las órdenes canceladas")
            except Exception:
                self._logger.exception("Dead man's switch: error cancelando órdenes")

            # 3. Alerta CRÍTICA por Telegram
            send_alert(
                "🚨 *DEAD MAN'S SWITCH ACTIVADO*\n"
                f"Feed WS sin mensajes hace `{staleness_sec:.0f}s`\n"
                "Bot *PAUSADO* y órdenes *CANCELADAS* automáticamente.\n"
                "Verificar conexión → usar `/resume` para reactivar."
            )

        elif event_type == "recovered":
            self._feed_was_stale = False
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

    def _update_markets(self) -> None:
        """Re-escanea mercados e invalida el cache del MarketAnalyzer.

        Llamado al arranque y cada _MARKET_UPDATE_INTERVAL_MIN minutos.
        Actualiza self._active_markets y suscribe nuevos tokens al WS.
        """
        try:
            self._market_analyzer.invalidate_cache()
            markets = self._market_analyzer.scan_markets()
            self._active_markets = markets
            self._subscribe_to_markets(markets)
            self._logger.info(
                "Mercados actualizados: %d activos | WS: %s",
                len(markets),
                "conectado" if self._feed.is_connected() else "desconectado",
            )
        except Exception:
            self._logger.exception("Error actualizando mercados")

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

    def _on_strategy_killed(self, strategy_name: str) -> None:
        """Callback del StrategyMonitor: pausa la estrategia con Calmar bajo."""
        self._logger.warning("StrategyMonitor: matando estrategia '%s'", strategy_name)
        for strategy in self._strategies:
            if strategy.name == strategy_name:
                strategy.pause()
                break

    def _run_strategy_monitor(self) -> None:
        """Evalúa el strategy monitor (kill switch). Ejecutado cada hora."""
        try:
            killed = self._strategy_monitor.evaluate()
            if killed:
                self._logger.warning("Strategy monitor mató: %s", killed)
        except Exception:
            self._logger.exception("Error en strategy monitor evaluation")

    def _poll_whale_tracker(self) -> None:
        """Actualiza whale tracker. Ejecutado cada 60s."""
        try:
            self._whale_tracker.poll()
        except Exception:
            self._logger.debug("Error en whale tracker poll (no crítico)")

    def _run_allocator_evaluation(self) -> None:
        """Actualiza allocations del bandit allocator. Ejecutado diariamente."""
        try:
            allocs = self._allocator.get_allocations(self._cached_balance)
            self._logger.info("Bandit allocations: %s", allocs)
        except Exception:
            self._logger.exception("Error en allocator evaluation")

    def _run_blacklist_refresh(self) -> None:
        """Refresca la blacklist por win-rate. Ejecutado cada 6h."""
        try:
            newly = self._blacklist.refresh()
            if newly:
                names = ", ".join(f"`{m[:12]}...`" for m in newly)
                self._logger.warning("Blacklist actualizada: %d mercados nuevos — %s", len(newly), newly)
                send_alert(f"🚫 *Blacklist WR* — {len(newly)} mercado(s) bloqueado(s):\n{names}")
        except Exception:
            self._logger.exception("Error en blacklist refresh")

    # ------------------------------------------------------------------
    # Circuit breaker → Telegram
    # ------------------------------------------------------------------

    def _cb_alert_handler(self, reason: str, message: str) -> None:
        """Recibe alertas del CircuitBreaker y las envía por Telegram."""
        icons = {
            "daily_drawdown":     "🛑",
            "consecutive_losses": "📉",
            "consecutive_errors": "💥",
            "single_trade_loss":  "⚠️",
            "market_hourly_loss": "⏰",
        }
        icon = icons.get(reason, "🚨")
        send_alert(f"{icon} *Circuit Breaker* [`{reason}`]\n{message}")

        # Si el breaker se disparó por drawdown diario, pausar el bot
        if reason == "daily_drawdown":
            self.pause()

    # ------------------------------------------------------------------
    # Control (interfaz para Telegram y señales del OS)
    # ------------------------------------------------------------------

    def get_status(self) -> dict[str, Any]:
        """Retorna estado completo del bot para Telegram /status y self-review."""
        cb = self._circuit_breaker.get_status()
        monitor_status = self._strategy_monitor.get_status()
        killed_strategies = [name for name, info in monitor_status.items() if info.get("is_killed")]
        return {
            "state": self._state,
            "paper_mode": self._paper_mode,
            "balance_usdc": self._cached_balance,
            "daily_pnl": cb["daily_pnl"],
            "open_orders": cb["open_orders"],
            "circuit_breaker": cb["triggered"],
            "trigger_reason": cb["trigger_reason"],
            "consecutive_errors": cb["consecutive_errors"],
            "total_exposure": self._inventory.get_total_exposure(),
            "markets_over_limit": cb["markets_over_limit"],
            "active_markets": len(self._active_markets),
            "ws_connected": self._feed.is_connected(),
            "strategies": [s.name for s in self._strategies if s.is_active],
            "killed_strategies": killed_strategies,
            "rolling_drawdown": cb.get("rolling_drawdown", {}),
        }

    def get_positions(self) -> dict[str, dict[str, float]]:
        """Retorna posiciones de inventario por mercado."""
        return self._inventory.get_positions()

    def pause(self) -> None:
        """Pausa el trading instantáneamente. Llamado por Telegram /pause o CB."""
        if self._state == "paused":
            return
        self._state = "paused"
        for strategy in self._strategies:
            strategy.pause()
        self._logger.info("Bot PAUSADO")

    def resume(self) -> None:
        """Reanuda el trading. Llamado por Telegram /resume."""
        self._state = "running"
        for strategy in self._strategies:
            strategy.resume()
        self._logger.info("Bot REANUDADO")

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
