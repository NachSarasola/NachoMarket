"""Bot de Telegram para monitoreo y control del bot de trading.

Corre en un thread daemon separado dentro del proceso principal.
Expone send_alert() como funcion de modulo — llamable desde cualquier modulo
sin importar ciclos ni estado del bot.

Uso desde otros modulos:
    from src.telegram.bot import send_alert
    send_alert("⚠️ Error: timeout en la API")

Comandos disponibles:
    /start       — Bienvenida y lista de comandos
    /status      — Estado completo del bot (resumido)
    /balance     — Balance USDC, exposure, PnL diario
    /positions   — Posiciones detalladas con mid, share, PnL
    /pnl         — Repo rte de PnL dia / semana / mes
    /markets     — Mercados activos con inventory
    /stats       — Sharpe/Sortino/Calmar 30d
    /attribution — Top/bottom estrategias y mercados
    /drawdown    — Rolling drawdown 7/15/30d
    /stages      — Stage actual de cada estrategia
    /promote     — Promover estrategia al siguiente stage
    /demote      — Demotear estrategia al stage anterior
    /blacklist   — Mercados en blacklist activa
    /block       — Bloquear mercado temporalmente
    /unblock     — Desbloquear mercado de la blacklist
    /logs        — Ultimas N lineas del log general
    /strategies  — Lista estrategias con estado y PnL
    /config      — Mostrar configuracion
    /force_reconcile — Forzar reconciliacion on-chain
    /health      — Estado del WebSocket y sistema
    /errors      — Ultimos errores consecutivos
    /review      — Forzar self-review inmediato
    /pause       — Pausa instantanea del trading
    /resume      — Reanuda el trading
    /kill        — Para el bot completamente (requiere confirmacion)
    /confirm_kill — Confirmar detencion del bot
"""

import asyncio
import json
import logging
import os
import threading
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

load_dotenv()
logger = logging.getLogger("nachomarket.telegram")

TRADES_FILE = Path("data/trades.jsonl")
LOG_FILE = Path("data/nachomarket.log")

# ------------------------------------------------------------------
# Estado global del modulo — usado por send_alert() para comunicacion
# cross-thread con el event loop del bot
# ------------------------------------------------------------------
_bot_instance: "TelegramBot | None" = None
_event_loop: asyncio.AbstractEventLoop | None = None


def send_alert(message: str) -> None:
    """Envia una alerta de Telegram. Llamable desde cualquier modulo.

    Es sincrona y no bloqueante: delega el envio al event loop del
    thread de Telegram via run_coroutine_threadsafe.
    Si el bot no esta inicializado, la llamada es silenciosa.

    Args:
        message: Texto de la alerta (acepta Markdown de Telegram).
    """
    from tenacity import retry, stop_after_attempt, wait_exponential

    def _send() -> None:
        if _bot_instance is None or _event_loop is None or _event_loop.is_closed():
            return
        asyncio.run_coroutine_threadsafe(
            _bot_instance._send_message(message),
            _event_loop,
        )

    retry_cfg = retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10), reraise=True)
    try:
        retry_cfg(_send)()
    except Exception:
        logger.exception("Failed to send Telegram alert after retries")


class TelegramBot:
    """Bot de Telegram con comandos de control y notificaciones proactivas.

    Args:
        bot_controller: Instancia de NachoMarketBot (para get_status, pause, etc.)
                        Acepta cualquier objeto con esos metodos (duck typing).
    """

    def __init__(self, bot_controller: Any = None) -> None:
        self._token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
        self._chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
        self._authorized_user_id = os.environ.get("TELEGRAM_USER_ID", "")
        self._controller = bot_controller
        self._app: Application | None = None
        self._stop_event: asyncio.Event | None = None
        self._last_command_time: dict[str, float] = {}
        self._pending_kill_user_id: int | None = None
        self._pending_kill_time: float = 0.0
        self._pnl_cache: tuple[datetime, dict[str, float]] | None = None
        self._start_time = datetime.now(timezone.utc)

        # Auto-arrancar en thread daemon si hay token configurado
        if self._token:
            self.run_in_thread()
        else:
            logger.warning("TELEGRAM_BOT_TOKEN not set — Telegram bot disabled")

    # ------------------------------------------------------------------
    # Thread management
    # ------------------------------------------------------------------

    def run_in_thread(self) -> threading.Thread:
        """Inicia el bot de Telegram en un thread daemon separado.

        Registra _bot_instance y _event_loop globalmente para que
        send_alert() pueda funcionar desde cualquier modulo.

        Returns:
            El thread iniciado (daemon=True, muere con el proceso principal).
        """
        def _thread_main() -> None:
            global _bot_instance, _event_loop

            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            _event_loop = loop
            _bot_instance = self

            try:
                loop.run_until_complete(self._run_async())
            except Exception:
                logger.exception("Telegram bot thread crashed")
            finally:
                _event_loop = None
                _bot_instance = None
                loop.close()

        thread = threading.Thread(
            target=_thread_main,
            daemon=True,
            name="telegram-bot",
        )
        thread.start()
        logger.info("Telegram bot thread started")
        return thread

    async def _run_async(self) -> None:
        """Loop principal async del bot. Corre hasta que stop() sea llamado."""
        self._stop_event = asyncio.Event()
        self._app = Application.builder().token(self._token).build()

        # Middleware global de errores
        self._app.add_error_handler(self._error_handler)

        # Registrar handlers
        handlers = [
            ("start", self._cmd_start),
            ("status", self._cmd_status),
            ("balance", self._cmd_balance),
            ("pause", self._cmd_pause),
            ("resume", self._cmd_resume),
            ("kill", self._cmd_kill),
            ("confirm_kill", self._cmd_confirm_kill),
            ("review", self._cmd_review),
            ("markets", self._cmd_markets),
            ("pnl", self._cmd_pnl),
            ("block", self._cmd_block),
            ("unblock", self._cmd_unblock),
            ("drawdown", self._cmd_drawdown),
            ("stats", self._cmd_stats),
            ("attribution", self._cmd_attribution),
            ("promote", self._cmd_promote),
            ("demote", self._cmd_demote),
            ("stages", self._cmd_stages),
            ("blacklist", self._cmd_blacklist),
            ("positions", self._cmd_positions),
            ("health", self._cmd_health),
            ("errors", self._cmd_errors),
            ("logs", self._cmd_logs),
            ("strategies", self._cmd_strategies),
            ("config", self._cmd_config),
            ("force_reconcile", self._cmd_force_reconcile),
        ]
        for name, handler in handlers:
            self._app.add_handler(CommandHandler(name, handler))

        # Tareas de fondo
        asyncio.create_task(self._heartbeat_loop())
        asyncio.create_task(self._daily_summary_loop())

        async with self._app:
            await self._app.start()
            await self._app.updater.start_polling(drop_pending_updates=True)
            logger.info("Telegram bot polling started")
            await self._stop_event.wait()
            await self._app.updater.stop()
            await self._app.stop()

        logger.info("Telegram bot stopped")

    def stop(self) -> None:
        """Detiene el bot de Telegram de forma ordenada."""
        global _event_loop
        if self._stop_event and _event_loop and not _event_loop.is_closed():
            _event_loop.call_soon_threadsafe(self._stop_event.set)

    # ------------------------------------------------------------------
    # Envio de mensajes
    # ------------------------------------------------------------------

    async def _send_message(self, message: str, parse_mode: str = "Markdown") -> None:
        """Envia un mensaje al chat configurado (metodo interno async)."""
        if not self._app or not self._chat_id:
            return
        try:
            await self._app.bot.send_message(
                chat_id=self._chat_id,
                text=message,
                parse_mode=parse_mode,
            )
        except Exception:
            logger.exception("Failed to send Telegram message")

    async def send_trade_alert(self, trade: dict[str, Any]) -> None:
        """Envia alerta de trade ejecutado con formato estandar."""
        side = trade.get("side", "?")
        size = trade.get("size", 0)
        price = trade.get("price", 0)
        market = trade.get("market_id", "?")[:12]
        strategy = trade.get("strategy_name", "?")
        status = trade.get("status", "?")
        icon = "✅" if status not in ("error",) else "❌"
        msg = (
            f"{icon} *{side}* `{size} USDC` @ `{price:.4f}`\n"
            f"Mercado: `{market}...` | Estrategia: `{strategy}`"
        )
        await self._send_message(msg)

    # ------------------------------------------------------------------
    # Seguridad y rate-limiting
    # ------------------------------------------------------------------

    def _is_authorized(self, update: Update) -> bool:
        """Verifica que el mensaje viene del usuario autorizado por TELEGRAM_USER_ID."""
        if not self._authorized_user_id:
            # Sin restriccion configurada: solo loguear advertencia
            logger.warning("TELEGRAM_USER_ID not set — accepting all users (insecure)")
            return True

        user = update.effective_user
        if user is None:
            return False

        authorized = str(user.id) == self._authorized_user_id
        if not authorized:
            logger.warning(
                "Unauthorized Telegram access attempt from user_id=%s", user.id
            )
        return authorized

    async def _reject(self, update: Update) -> None:
        """Respuesta silenciosa para usuarios no autorizados."""
        if update.message:
            await update.message.reply_text("Acceso no autorizado.")

    def _check_rate_limit(self, user_id: str) -> bool:
        """Devuelve True si el usuario puede ejecutar el comando (>=1s desde el ultimo)."""
        now = time.time()
        last = self._last_command_time.get(user_id, 0.0)
        if now - last < 1.0:
            return False
        self._last_command_time[user_id] = now
        return True

    async def _error_handler(self, update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handler global de errores no capturados en handlers de Telegram."""
        logger.exception("Unhandled exception in Telegram handler")
        if isinstance(update, Update) and update.effective_message:
            await update.effective_message.reply_text("❌ Error interno. Revisá /errors.")

    # ------------------------------------------------------------------
    # Helpers de formato
    # ------------------------------------------------------------------

    @staticmethod
    def _fmt_money(val: float, decimals: int = 2) -> str:
        """Formatea valor monetario con signo y color implícito."""
        sign = "+" if val > 0 else ""
        return f"`${sign}{val:,.{decimals}f}`"

    @staticmethod
    def _fmt_pct(val: float, decimals: int = 1) -> str:
        """Formatea porcentaje."""
        sign = "+" if val > 0 else ""
        return f"`{sign}{val:.{decimals}f}%`"

    @staticmethod
    def _section_header(title: str, emoji: str = "📊") -> str:
        """Header de seccion con linea separadora."""
        return f"\n*{emoji} {title}*\n{'─' * 22}"

    # ------------------------------------------------------------------
    # Comandos
    # ------------------------------------------------------------------

    async def _cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Mensaje de bienvenida con lista de comandos disponibles."""
        if not self._is_authorized(update):
            await self._reject(update)
            return
        user_id = str(update.effective_user.id) if update.effective_user else "?"
        if not self._check_rate_limit(user_id):
            if update.message:
                await update.message.reply_text("⏳ Muy rápido. Esperá un segundo.")
            return

        text = (
            "🤖 *NachoMarket — Bot de Trading*\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "\n"
            "*📡 Monitoreo*\n"
            "  /status    → Estado resumido del bot\n"
            "  /balance   → Balance, exposure, PnL diario\n"
            "  /positions → Posiciones detalladas (mid, share, PnL)\n"
            "  /markets   → Mercados activos con inventory\n"
            "  /health    → Estado WebSocket y sistema\n"
            "  /logs      → Últimas líneas del log\n"
            "\n"
            "*📈 Rendimiento*\n"
            "  /pnl          → PnL día / semana / mes\n"
            "  /stats        → Sharpe / Sortino / Calmar 30d\n"
            "  /attribution  → Top/bottom estrategias y mercados\n"
            "  /drawdown     → Rolling drawdown 7/15/30d\n"
            "  /strategies   → Estado y PnL de estrategias\n"
            "\n"
            "*⚙️ Control*\n"
            "  /pause   → Pausar trading (cancela órdenes)\n"
            "  /resume  → Reanudar trading\n"
            "  /kill    → Parar el bot completamente\n"
            "  /confirm_kill → Confirmar detención\n"
            "  /review  → Forzar self-review inmediato\n"
            "  /config  → Ver configuración\n"
            "  /force_reconcile → Reconciliación on-chain\n"
            "\n"
            "*🔧 Estrategias*\n"
            "  /stages              → Stage actual de cada estrategia\n"
            "  /promote <estrategia> → Promover al siguiente stage\n"
            "  /demote  <estrategia> → Demotear al stage anterior\n"
            "\n"
            "*🛡️ Seguridad*\n"
            "  /blacklist              → Mercados en blacklist activa\n"
            "  /block <id> <horas>     → Bloquear mercado temporalmente\n"
            "  /unblock <id>           → Desbloquear mercado\n"
            "  /errors                 → Últimos errores consecutivos\n"
            "\n"
            "_Notificaciones automáticas: trades, errores, circuit breaker, reviews, stage changes_"
        )
        if update.message:
            await update.message.reply_text(text, parse_mode="Markdown")

    async def _cmd_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Estado resumido del bot — una pantalla rápida."""
        if not self._is_authorized(update):
            await self._reject(update)
            return
        user_id = str(update.effective_user.id) if update.effective_user else "?"
        if not self._check_rate_limit(user_id):
            if update.message:
                await update.message.reply_text("⏳ Muy rápido. Esperá un segundo.")
            return
        if not update.message:
            return

        if not self._controller:
            await update.message.reply_text("❌ Bot controller no conectado.")
            return

        status = self._controller.get_status()
        positions = self._controller.get_positions()
        today_trades = self._count_today_trades()

        # Estado del bot
        state = status.get("state", "?")
        state_icon = {"running": "🟢", "paused": "🟡", "stopped": "🔴"}.get(state, "⚪")
        cb_active = status.get("circuit_breaker", False)
        cb_icon = "🔴" if cb_active else "🟢"

        # Estrategias
        strategies = getattr(self._controller, "_strategies", [])
        active_strats = [s.name for s in strategies if getattr(s, "is_active", True)]
        inactive_strats = [s.name for s in strategies if not getattr(s, "is_active", True)]

        open_orders = status.get('open_orders', 0)
        open_exposure = status.get('open_exposure', 0.0)
        inventory_exposure = status.get('inventory_exposure', 0.0)

        lines = [
            f"{state_icon} *Estado:* `{state.upper()}`",
            f"💰 *Balance:* `${status.get('balance_usdc', 0):.2f}` USDC",
            f"📊 *Exposure:* `${status.get('total_exposure', 0):.2f}` USDC",
        ]
        if open_exposure > 0:
            lines.append(f"   └ En ordenes: `{open_exposure:.2f}` USDC")
        if inventory_exposure > 0:
            lines.append(f"   └ En posiciones: `{inventory_exposure:.2f}` USDC")
        lines.extend([
            f"📈 *PnL hoy:* `{status.get('daily_pnl', 0):+.4f}` USDC",
            f"📊 *Trades hoy:* `{today_trades}`",
            f"📋 *Ordenes abiertas:* `{open_orders}`",
            f"🏪 *Mercados:* `{len(positions)}` de `{status.get('active_markets', 0)}`",
            f"⚡ *Estrategias:* `{'`, `'.join(active_strats) if active_strats else 'ninguna'}`",
        ])
        if inactive_strats:
            lines.append(f"   ⏸️ Pausadas: `{'`, `'.join(inactive_strats)}`")

        lines.extend([
            f"🔁 *Errores:* `{status.get('consecutive_errors', 0)}`",
            f"⛔ *Circuit breaker:* {cb_icon} {'ACTIVO' if cb_active else 'OK'}",
        ])

        if status.get("trigger_reason"):
            lines.append(f"   _Razón: {status['trigger_reason']}_")

        # Resumen rápido de inventory (máx 3)
        if positions:
            lines.append("\n*Inventory (top 3):*")
            for i, (mid, pos) in enumerate(list(positions.items())[:3]):
                yes = pos.get("yes", 0)
                no = pos.get("no", 0)
                name = self._get_market_name(mid)
                lines.append(f"  `{name}` YES:`{yes:.1f}` NO:`{no:.1f}`")
            if len(positions) > 3:
                lines.append(f"  _...y {len(positions) - 3} más (ver /markets)_")

        await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

    async def _cmd_balance(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Balance detallado: USDC disponible, exposure, PnL, capital desplegado."""
        if not self._is_authorized(update):
            await self._reject(update)
            return
        user_id = str(update.effective_user.id) if update.effective_user else "?"
        if not self._check_rate_limit(user_id):
            if update.message:
                await update.message.reply_text("⏳ Muy rápido. Esperá un segundo.")
            return
        if not update.message:
            return

        if not self._controller:
            await update.message.reply_text("❌ Bot controller no conectado.")
            return

        status = self._controller.get_status()
        positions = self._controller.get_positions()

        # Calcular capital total desplegado
        total_deployed = 0.0
        for pos in positions.values():
            total_deployed += abs(pos.get("yes", 0)) + abs(pos.get("no", 0))

        balance = status.get("balance_usdc", 0.0)
        exposure = status.get("total_exposure", 0.0)
        open_exposure = status.get("open_exposure", 0.0)
        inventory_exposure = status.get("inventory_exposure", 0.0)
        daily_pnl = status.get("daily_pnl", 0.0)
        open_orders = status.get("open_orders", 0)

        # Colores según PnL
        pnl_icon = "🟢" if daily_pnl >= 0 else "🔴"

        lines = [
            "💰 *Balance Detail*",
            "━━━━━━━━━━━━━━━━━━━━━━",
            f"",
            f"🏦 *Balance pUSD:* `{balance:.2f}`",
            f"📊 *Exposure total:* `{exposure:.2f}`",
        ]
        if open_exposure > 0:
            lines.append(f"   └ En órdenes abiertas: `{open_exposure:.2f}`")
        if inventory_exposure > 0:
            lines.append(f"   └ En posiciones llenadas: `{inventory_exposure:.2f}`")
        lines.extend([
            f"💵 *Capital desplegado (fills):* `{total_deployed:.2f}`",
            f"📈 *PnL diario:* {pnl_icon} `{daily_pnl:+.4f}`",
            f"📋 *Órdenes abiertas:* `{open_orders}`",
            f"",
            f"*Utilización:* `{((exposure / balance) * 100):.1f}%`" if balance > 0 else "",
        ])

        await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

    async def _cmd_positions(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Posiciones detalladas con mid, participation share, inventory y horas activo."""
        if not self._is_authorized(update):
            await self._reject(update)
            return
        user_id = str(update.effective_user.id) if update.effective_user else "?"
        if not self._check_rate_limit(user_id):
            if update.message:
                await update.message.reply_text("⏳ Muy rápido. Esperá un segundo.")
            return
        if not update.message:
            return

        if not self._controller:
            await update.message.reply_text("❌ Bot controller no conectado.")
            return

        if not hasattr(self._controller, "get_positions_detail"):
            await update.message.reply_text("❌ Comando no disponible en esta versión.")
            return

        details = self._controller.get_positions_detail()
        if not details:
            await update.message.reply_text("📭 Sin posiciones activas.")
            return

        lines = ["📍 *Posiciones Activas*\n" + "━" * 22]

        for i, pos in enumerate(details, 1):
            share_pct = pos.get("participation_share", 0.0) * 100
            mid = pos.get("mid_price", 0.0)
            yes_inv = pos.get("yes_inventory", 0.0)
            no_inv = pos.get("no_inventory", 0.0)
            total = pos.get("total_inventory_usdc", 0.0)
            hours = pos.get("hours_since_last_order")
            rewards_icon = "💰" if pos.get("rewards_active") else ""
            question = pos.get("question", "?")[:35]

            hours_str = f"{hours:.1f}h" if hours is not None else "n/a"
            share_warn = " ⚠️ baja" if share_pct < 0.5 else ""

            lines.append(
                f"\n*{i}. {question}* {rewards_icon}\n"
                f"   ├ Mid: `{mid:.3f}`\n"
                f"   ├ Share: `{share_pct:.1f}%`{share_warn}\n"
                f"   ├ YES: `{yes_inv:.1f}` | NO: `{no_inv:.1f}`\n"
                f"   ├ Total: `${total:.1f}`\n"
                f"   └ Última orden: `{hours_str}`"
            )

        await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

    async def _cmd_markets(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Lista mercados activos con inventory y skew."""
        if not self._is_authorized(update):
            await self._reject(update)
            return
        user_id = str(update.effective_user.id) if update.effective_user else "?"
        if not self._check_rate_limit(user_id):
            if update.message:
                await update.message.reply_text("⏳ Muy rápido. Esperá un segundo.")
            return
        if not update.message:
            return

        if not self._controller:
            await update.message.reply_text("❌ Bot controller no conectado.")
            return

        positions = self._controller.get_positions()
        if not positions:
            await update.message.reply_text("📭 Sin mercados con posición abierta.")
            return

        lines = ["🏪 *Mercados Activos*\n" + "━" * 22]

        for i, (mid, pos) in enumerate(positions.items(), 1):
            yes = pos.get("yes", 0.0)
            no = pos.get("no", 0.0)
            total = abs(yes) + abs(no)
            skew = (yes - no) / total if total > 0 else 0.0
            name = self._get_market_name(mid)

            if skew > 0.3:
                skew_icon, skew_text = "🔼", "long YES"
            elif skew < -0.3:
                skew_icon, skew_text = "🔽", "long NO"
            else:
                skew_icon, skew_text = "➡️", "neutral"

            lines.append(
                f"\n*{i}. {name}* (`{mid[:10]}...`)\n"
                f"   ├ YES: `{yes:.1f}` | NO: `{no:.1f}`\n"
                f"   ├ Total: `${total:.1f}`\n"
                f"   └ Skew: {skew_icon} `{skew_text}`"
            )

        await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

    async def _cmd_pnl(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Reporte de PnL por períodos con formato claro."""
        if not self._is_authorized(update):
            await self._reject(update)
            return
        user_id = str(update.effective_user.id) if update.effective_user else "?"
        if not self._check_rate_limit(user_id):
            if update.message:
                await update.message.reply_text("⏳ Muy rápido. Esperá un segundo.")
            return
        if not update.message:
            return

        now = datetime.now(timezone.utc)
        periods = [
            ("📅 HOY", now - timedelta(hours=24)),
            ("📅 SEMANA", now - timedelta(days=7)),
            ("📅 MES", now - timedelta(days=30)),
        ]

        lines = ["📈 *Reporte de PnL*\n" + "━" * 22]

        for label, cutoff in periods:
            stats = self._compute_pnl_since(cutoff)
            pnl = stats['pnl']
            pnl_icon = "🟢" if pnl >= 0 else "🔴"

            lines.append(
                f"\n*{label}*\n"
                f"   ├ Trades: `{stats['count']}` | Errores: `{stats['errors']}`\n"
                f"   ├ Capital desplegado: `${stats['deployed']:.2f}`\n"
                f"   ├ PnL: {pnl_icon} `${pnl:+.4f}`\n"
                f"   ├ Fees: `${stats['fees']:.4f}`\n"
                f"   └ Rewards: `${stats['rewards']:.4f}`"
            )

        # PnL intradiario del circuit breaker
        if self._controller:
            cb_pnl = self._controller.get_status().get("daily_pnl", 0)
            lines.append(f"\n⚡ *PnL intradiario (CB):* `${cb_pnl:+.4f}`")

        # Top mercados por ROI
        if self._controller and hasattr(self._controller, "_profiler"):
            report = self._controller._profiler.get_report(top_n=3)
            if report:
                lines.append("\n🏆 *Top Mercados (ROI):*")
                for entry in report:
                    roi_pct = entry["roi"] * 100
                    icon = "🟢" if roi_pct >= 0 else "🔴"
                    lines.append(
                        f"   {icon} `{entry['question'][:25]}` "
                        f"ROI: `{roi_pct:.1f}%` PnL: `${entry['total_pnl']:.2f}`"
                    )

        await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

    async def _cmd_stats(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Métricas cuantitativas: Sharpe, Sortino, Calmar, max drawdown."""
        if not self._is_authorized(update):
            await self._reject(update)
            return
        user_id = str(update.effective_user.id) if update.effective_user else "?"
        if not self._check_rate_limit(user_id):
            if update.message:
                await update.message.reply_text("⏳ Muy rápido. Esperá un segundo.")
            return
        if not update.message:
            return

        try:
            from src.utils.performance_metrics import compute_metrics_from_trades_file  # noqa: PLC0415
            metrics = compute_metrics_from_trades_file(
                str(TRADES_FILE),
                window_days=30,
                risk_free_rate=0.04,
            )
        except Exception:
            await update.message.reply_text("❌ Error calculando métricas cuantitativas.")
            return

        if metrics.get("trade_count_30d", 0) == 0:
            await update.message.reply_text("📭 Sin trades suficientes en los últimos 30 días.")
            return

        sharpe = metrics.get("sharpe_ratio", 0.0)
        sortino = metrics.get("sortino_ratio", 0.0)
        calmar = metrics.get("calmar_ratio", 0.0)
        max_dd = metrics.get("max_drawdown", 0.0)
        total_return = metrics.get("total_return", 0.0)
        count = metrics.get("trade_count_30d", 0)

        # Benchmark icons
        def _grade(val: float, good: float, bad: float) -> str:
            if val >= good:
                return "🟢"
            if val >= bad:
                return "🟡"
            return "🔴"

        lines = [
            "📊 *Métricas Cuantitativas — 30 días*\n" + "━" * 22,
            f"",
            f"{_grade(sharpe, 1.5, 0.5)} Sharpe Ratio: `{sharpe:.3f}`",
            f"{_grade(sortino, 1.5, 0.5)} Sortino Ratio: `{sortino:.3f}`",
            f"{_grade(calmar, 2.0, 0.5)} Calmar Ratio: `{calmar:.3f}`",
            f"{_grade(-max_dd, -20, -50)} Max Drawdown: `${max_dd:.4f}`",
            f"💰 Return total: `${total_return:+.4f}`",
            f"🔢 Trades analizados: `{count}`",
        ]

        await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

    async def _cmd_attribution(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Top y bottom estrategias/mercados por PnL."""
        if not self._is_authorized(update):
            await self._reject(update)
            return
        user_id = str(update.effective_user.id) if update.effective_user else "?"
        if not self._check_rate_limit(user_id):
            if update.message:
                await update.message.reply_text("⏳ Muy rápido. Esperá un segundo.")
            return
        if not update.message:
            return

        try:
            from src.analysis.attribution import TradeAttribution  # noqa: PLC0415
            attr = TradeAttribution(str(TRADES_FILE))
            text = attr.format_telegram(top_n=3)
        except Exception:
            text = "❌ Error generando attribution report."

        await update.message.reply_text(text or "📭 Sin datos de attribution disponibles.")

    async def _cmd_drawdown(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Rolling drawdown 7/15/30d y estado de scale-down."""
        if not self._is_authorized(update):
            await self._reject(update)
            return
        user_id = str(update.effective_user.id) if update.effective_user else "?"
        if not self._check_rate_limit(user_id):
            if update.message:
                await update.message.reply_text("⏳ Muy rápido. Esperá un segundo.")
            return
        if not update.message:
            return

        if not self._controller:
            await update.message.reply_text("❌ Bot controller no conectado.")
            return

        cb = getattr(self._controller, "_circuit_breaker", None)
        if cb is None:
            await update.message.reply_text("❌ Circuit breaker no disponible.")
            return

        report = cb.get_drawdown_report()

        def _fmt_dd(val: float) -> str:
            icon = "🟢" if val >= 0 else "🔴"
            return f"{icon} `${val:+.2f}`"

        thresholds = {
            "7d": getattr(cb, "_drawdown_7d_threshold", 40.0),
            "15d": getattr(cb, "_drawdown_15d_threshold", 80.0),
            "30d": getattr(cb, "_drawdown_30d_threshold", 120.0),
        }

        scale_active = getattr(cb, "_scale_down_active", False)
        arb_paused = getattr(cb, "_arb_paused", False)

        lines = [
            "📉 *Rolling Drawdown*\n" + "━" * 22,
            f"",
            f"📅 7d:  {_fmt_dd(report['drawdown_7d'])}  (límite: `${thresholds['7d']:.0f}`)",
            f"📅 15d: {_fmt_dd(report['drawdown_15d'])}  (límite: `${thresholds['15d']:.0f}`)",
            f"📅 30d: {_fmt_dd(report['drawdown_30d'])}  (límite: `${thresholds['30d']:.0f}`)",
            f"",
            f"⚖️ Scale-down 7d: {'🔴 ACTIVO (size -50%)' if scale_active else '🟢 Normal'}",
            f"🛑 Arb/Directional 15d: {'🔴 PAUSADO' if arb_paused else '🟢 Activo'}",
        ]

        await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

    async def _cmd_health(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Estado de salud del sistema: WebSocket, API, último ciclo."""
        if not self._is_authorized(update):
            await self._reject(update)
            return
        user_id = str(update.effective_user.id) if update.effective_user else "?"
        if not self._check_rate_limit(user_id):
            if update.message:
                await update.message.reply_text("⏳ Muy rápido. Esperá un segundo.")
            return
        if not update.message:
            return

        if not self._controller:
            await update.message.reply_text("❌ Bot controller no conectado.")
            return

        status = self._controller.get_status()

        # WebSocket status
        feed = getattr(self._controller, "_feed", None)
        ws_connected = feed.is_connected() if feed else False
        ws_icon = "🟢" if ws_connected else "🔴"

        # API status
        client = getattr(self._controller, "_client", None)
        api_ok = False
        if client:
            try:
                client.test_connection()
                api_ok = True
            except Exception:
                api_ok = False
        api_icon = "🟢" if api_ok else "🔴"

        # Tiempo desde último trade
        last_trade_time = self._get_last_trade_time()
        last_trade_str = f"{last_trade_time:.0f}m atrás" if last_trade_time is not None else "n/a"

        # Uptime real del bot
        start_time = status.get("start_time", 0.0)
        if start_time > 0:
            uptime_sec = time.time() - start_time
            uptime_h = int(uptime_sec // 3600)
            uptime_m = int((uptime_sec % 3600) // 60)
            uptime_d = int(uptime_sec // 86400)
            uptime_str = f"{uptime_d}d {uptime_h}h {uptime_m}m" if uptime_d > 0 else f"{uptime_h}h {uptime_m}m"
        else:
            uptime_str = "n/a"

        lines = [
            "🏥 *Health Check*\n" + "━" * 22,
            f"",
            f"{ws_icon} WebSocket: {'Conectado' if ws_connected else 'DESCONECTADO'}",
            f"{api_icon} Polymarket API: {'OK' if api_ok else 'ERROR'}",
            f"🕐 Último trade: `{last_trade_str}`",
            f"⏱️ Uptime: `{uptime_str}`",
            f"",
            f"📊 Loop interval: `{getattr(self._controller, '_loop_interval', '?')}s`",
            f"🔄 Mercados activos: `{len(getattr(self._controller, '_active_markets', []))}`",
        ]

        await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

    async def _cmd_errors(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Muestra últimos errores consecutivos y errores recientes del log."""
        if not self._is_authorized(update):
            await self._reject(update)
            return
        user_id = str(update.effective_user.id) if update.effective_user else "?"
        if not self._check_rate_limit(user_id):
            if update.message:
                await update.message.reply_text("⏳ Muy rápido. Esperá un segundo.")
            return
        if not update.message:
            return

        if not self._controller:
            await update.message.reply_text("❌ Bot controller no conectado.")
            return

        status = self._controller.get_status()
        consecutive = status.get("consecutive_errors", 0)

        lines = [
            "⚠️ *Errores Recientes*\n" + "━" * 22,
            f"",
            f"🔁 Errores consecutivos: `{consecutive}`",
        ]

        if consecutive >= 5:
            lines.append(f"   🔴 *ALERTA: {consecutive} errores consecutivos*")
        elif consecutive >= 1:
            lines.append(f"   🟡 Precaución: errores acumulándose")
        else:
            lines.append(f"   🟢 Sin errores recientes")

        # Últimas líneas de error del log
        recent_errors = self._get_recent_log_errors(n=5)
        if recent_errors:
            lines.append(f"\n*Últimos errores del log:*")
            for i, err_line in enumerate(recent_errors, 1):
                # Truncar líneas largas
                err_trunc = err_line[:60] + "..." if len(err_line) > 60 else err_line
                lines.append(f"   `{i}. {err_trunc}`")

        await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

    async def _cmd_pause(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Pausa instantanea del trading."""
        if not self._is_authorized(update):
            await self._reject(update)
            return
        user_id = str(update.effective_user.id) if update.effective_user else "?"
        if not self._check_rate_limit(user_id):
            if update.message:
                await update.message.reply_text("⏳ Muy rápido. Esperá un segundo.")
            return
        if not update.message:
            return

        if not self._controller:
            await update.message.reply_text("❌ Bot controller no conectado.")
            return

        self._controller.pause()
        await update.message.reply_text(
            "⏸️ *Trading PAUSADO*\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "Usá `/resume` para reanudar.",
            parse_mode="Markdown",
        )
        user_id_val = update.effective_user.id if update.effective_user else "?"
        send_alert(f"⏸️ Bot PAUSADO por usuario `{user_id_val}` via Telegram")
        logger.info("Bot paused via Telegram")

    async def _cmd_resume(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Reanuda el trading."""
        if not self._is_authorized(update):
            await self._reject(update)
            return
        user_id = str(update.effective_user.id) if update.effective_user else "?"
        if not self._check_rate_limit(user_id):
            if update.message:
                await update.message.reply_text("⏳ Muy rápido. Esperá un segundo.")
            return
        if not update.message:
            return

        if not self._controller:
            await update.message.reply_text("❌ Bot controller no conectado.")
            return

        self._controller.resume()
        await update.message.reply_text(
            "▶️ *Trading REANUDADO*\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "Bot operando normalmente.",
            parse_mode="Markdown",
        )
        user_id_val = update.effective_user.id if update.effective_user else "?"
        send_alert(f"▶️ Bot REANUDADO por usuario `{user_id_val}` via Telegram")
        logger.info("Bot resumed via Telegram")

    async def _cmd_kill(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Para el bot completamente (requiere confirmacion)."""
        if not self._is_authorized(update):
            await self._reject(update)
            return
        user_id = str(update.effective_user.id) if update.effective_user else "?"
        if not self._check_rate_limit(user_id):
            if update.message:
                await update.message.reply_text("⏳ Muy rápido. Esperá un segundo.")
            return
        if not update.message:
            return

        if not self._controller:
            await update.message.reply_text("❌ Bot controller no conectado.")
            return

        if update.effective_user:
            self._pending_kill_user_id = update.effective_user.id
            self._pending_kill_time = time.time()

        await update.message.reply_text(
            "⚠️ ¿Estás seguro? Respondé /confirm_kill para detener el bot.",
            parse_mode="Markdown",
        )

    async def _cmd_confirm_kill(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Confirma la detencion del bot."""
        if not self._is_authorized(update):
            await self._reject(update)
            return
        user_id = str(update.effective_user.id) if update.effective_user else "?"
        if not self._check_rate_limit(user_id):
            if update.message:
                await update.message.reply_text("⏳ Muy rápido. Esperá un segundo.")
            return
        if not update.message:
            return

        if self._pending_kill_user_id is None:
            await update.message.reply_text("No hay kill pendiente. Usá /kill primero.")
            return

        if update.effective_user and update.effective_user.id != self._pending_kill_user_id:
            await update.message.reply_text("No sos el usuario que inició el kill.")
            return

        if time.time() - self._pending_kill_time > 60:
            self._pending_kill_user_id = None
            await update.message.reply_text("Expiró el tiempo de confirmación. Usá /kill de nuevo.")
            return

        await update.message.reply_text(
            "🛑 *Deteniendo NachoMarket...*\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "Cancelando todas las órdenes abiertas.",
            parse_mode="Markdown",
        )
        self._controller.kill()
        self._pending_kill_user_id = None
        logger.critical("Bot killed via Telegram")

    async def _cmd_review(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Fuerza un self-review inmediato con Claude Haiku."""
        if not self._is_authorized(update):
            await self._reject(update)
            return
        user_id = str(update.effective_user.id) if update.effective_user else "?"
        if not self._check_rate_limit(user_id):
            if update.message:
                await update.message.reply_text("⏳ Muy rápido. Esperá un segundo.")
            return
        if not update.message:
            return

        await update.message.reply_text("🔍 Ejecutando self-review... (~10s)")

        reviewer = getattr(self._controller, "_reviewer", None) if self._controller else None
        if not reviewer:
            await update.message.reply_text("❌ Reviewer no disponible.")
            return

        loop = asyncio.get_event_loop()
        state = self._controller.get_status() if self._controller else None
        try:
            result = await loop.run_in_executor(None, reviewer.run_review, state)
            status = result.get("status", "ok")
            if status == "no_trades":
                await update.message.reply_text("📭 Sin trades en las últimas 8h para revisar.")
            elif status == "error":
                await update.message.reply_text("❌ Error ejecutando el review.")
            else:
                analysis = result.get("analysis", {})
                if isinstance(analysis, dict):
                    risk = analysis.get("risk_level", "?")
                    summary = analysis.get("summary", "Sin resumen")
                    cost = result.get("estimated_cost_usd", 0)
                    risk_icon = "🟢" if risk in ("low", "bajo") else ("🟡" if risk in ("medium", "medio") else "🔴")
                    await update.message.reply_text(
                        f"✅ *Review Completado*\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"{risk_icon} Risk: `{risk}`\n"
                        f"💰 Costo: `${cost:.5f}`\n"
                        f"📝 _{summary}_",
                        parse_mode="Markdown",
                    )
                else:
                    await update.message.reply_text(
                        f"✅ Review completado: `{result.get('trade_count', 0)}` trades analizados."
                    )
        except Exception:
            logger.exception("Error in /review command")
            await update.message.reply_text("❌ Error inesperado durante el review.")

    async def _cmd_block(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Bloquea un mercado temporalmente."""
        if not self._is_authorized(update):
            await self._reject(update)
            return
        user_id = str(update.effective_user.id) if update.effective_user else "?"
        if not self._check_rate_limit(user_id):
            if update.message:
                await update.message.reply_text("⏳ Muy rápido. Esperá un segundo.")
            return
        if not update.message:
            return

        args = context.args or []
        if len(args) < 2:
            await update.message.reply_text(
                "🛡️ *Uso: /block*\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n"
                "`/block <condition_id> <horas>`\n"
                "Ejemplo: `/block 0xabc123 24`",
                parse_mode="Markdown",
            )
            return

        market_id = args[0]
        try:
            hours = float(args[1])
        except ValueError:
            await update.message.reply_text("❌ Horas debe ser un número.")
            return

        if self._controller and hasattr(self._controller, "_market_analyzer"):
            self._controller._market_analyzer.market_filter.block_market_until(
                market_id, hours
            )
            await update.message.reply_text(
                f"🚫 *Mercado bloqueado*\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"ID: `{market_id[:16]}...`\n"
                f"Duración: `{hours}h`",
                parse_mode="Markdown",
            )
        else:
            await update.message.reply_text("❌ Bot no tiene market analyzer activo.")

    async def _cmd_unblock(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Desbloquea un mercado de la blacklist."""
        if not self._is_authorized(update):
            await self._reject(update)
            return
        user_id = str(update.effective_user.id) if update.effective_user else "?"
        if not self._check_rate_limit(user_id):
            if update.message:
                await update.message.reply_text("⏳ Muy rápido. Esperá un segundo.")
            return
        if not update.message:
            return

        args = context.args or []
        if not args:
            await update.message.reply_text(
                "🛡️ *Uso: /unblock*\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n"
                "`/unblock <condition_id>`\n"
                "Ejemplo: `/unblock 0xabc123`",
                parse_mode="Markdown",
            )
            return

        market_id = args[0]
        blacklist = self._get_blacklist()
        if blacklist:
            bl_dict = getattr(blacklist, "_blacklisted", None)
            if bl_dict and market_id in bl_dict:
                bl_dict.pop(market_id, None)
                if hasattr(blacklist, "_save"):
                    blacklist._save()
                await update.message.reply_text(
                    f"✅ Mercado desbloqueado: `{market_id[:16]}...`"
                )
                return
        await update.message.reply_text("❌ Mercado no encontrado en blacklist o blacklist no disponible.")

    async def _cmd_logs(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Devuelve las últimas N líneas del log general."""
        if not self._is_authorized(update):
            await self._reject(update)
            return
        user_id = str(update.effective_user.id) if update.effective_user else "?"
        if not self._check_rate_limit(user_id):
            if update.message:
                await update.message.reply_text("⏳ Muy rápido. Esperá un segundo.")
            return
        if not update.message:
            return

        args = context.args or []
        n = int(args[0]) if args else 20
        n = max(1, min(n, 100))
        lines = self._get_recent_log_lines(n)
        text = "📋 *Últimas líneas del log*\n" + "━" * 22 + "\n\n" + "\n".join(f"`{l[:90]}`" for l in lines)
        await update.message.reply_text(text, parse_mode="Markdown")

    async def _cmd_strategies(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Lista estrategias con estado (RUNNING/PAUSED/KILLED) y PnL del día."""
        if not self._is_authorized(update):
            await self._reject(update)
            return
        user_id = str(update.effective_user.id) if update.effective_user else "?"
        if not self._check_rate_limit(user_id):
            if update.message:
                await update.message.reply_text("⏳ Muy rápido. Esperá un segundo.")
            return
        if not update.message:
            return

        if not self._controller:
            await update.message.reply_text("❌ Bot controller no conectado.")
            return

        strategies = getattr(self._controller, "_strategies", [])
        monitor = getattr(self._controller, "_strategy_monitor", None)

        if not strategies:
            await update.message.reply_text("📭 Sin estrategias registradas.")
            return

        lines = ["🧠 *Estrategias*\n" + "━" * 22]
        for s in strategies:
            name = getattr(s, "name", "?")
            is_active = getattr(s, "is_active", False)
            killed = monitor.is_killed(name) if monitor and hasattr(monitor, "is_killed") else False
            if killed:
                status = "🔴 KILLED"
            elif is_active:
                status = "🟢 RUNNING"
            else:
                status = "🟡 PAUSED"
            daily_pnl = getattr(s, "daily_pnl", 0.0)
            lines.append(f"`{name}`: {status} | PnL: `${daily_pnl:+.2f}`")

        await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

    async def _cmd_config(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Muestra valor de una key de config. Sin args → lista keys principales."""
        if not self._is_authorized(update):
            await self._reject(update)
            return
        user_id = str(update.effective_user.id) if update.effective_user else "?"
        if not self._check_rate_limit(user_id):
            if update.message:
                await update.message.reply_text("⏳ Muy rápido. Esperá un segundo.")
            return
        if not update.message:
            return

        args = context.args or []
        settings = getattr(self._controller, "_settings", {}) if self._controller else {}
        if not args:
            keys = ["mode", "capital_total", "signature_type", "max_risk_per_market", "main_loop_interval_sec"]
            text = "⚙️ *Config*\n" + "\n".join(f"`{k}`: `{settings.get(k, 'N/A')}`" for k in keys)
        else:
            key = args[0]
            val = settings.get(key, "N/A")
            text = f"⚙️ `{key}` = `{val}`"
        await update.message.reply_text(text, parse_mode="Markdown")

    async def _cmd_force_reconcile(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Fuerza reconciliacion on-chain inmediata."""
        if not self._is_authorized(update):
            await self._reject(update)
            return
        user_id = str(update.effective_user.id) if update.effective_user else "?"
        if not self._check_rate_limit(user_id):
            if update.message:
                await update.message.reply_text("⏳ Muy rápido. Esperá un segundo.")
            return
        if not update.message:
            return

        if not self._controller:
            await update.message.reply_text("❌ Bot controller no conectado.")
            return

        await update.message.reply_text("🔄 Forzando reconciliación...")
        if hasattr(self._controller, "_client"):
            try:
                result = self._controller._client.reconcile_state()
            except Exception:
                logger.exception("Error en reconciliacion")
                await update.message.reply_text("❌ Error forzando reconciliación.")
                return
            desync = result.get("desync", False)
            icon = "⚠️" if desync else "✅"
            text = (
                f"{icon} *Reconciliación*\n"
                f"Balance: `${result.get('balance_onchain', 0):.2f}`\n"
                f"Órdenes: `{result.get('open_orders_onchain', 0)}`\n"
                f"Desync: `{'SÍ' if desync else 'NO'}`"
            )
            await update.message.reply_text(text, parse_mode="Markdown")
        else:
            await update.message.reply_text("❌ Cliente no disponible.")

    async def _cmd_promote(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Promover estrategia al siguiente stage."""
        if not self._is_authorized(update):
            await self._reject(update)
            return
        user_id = str(update.effective_user.id) if update.effective_user else "?"
        if not self._check_rate_limit(user_id):
            if update.message:
                await update.message.reply_text("⏳ Muy rápido. Esperá un segundo.")
            return
        if not update.message:
            return

        args = context.args or []
        if not args:
            await update.message.reply_text(
                "⬆️ *Uso: /promote*\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n"
                "`/promote <estrategia>`\n"
                "Estrategias: `market_maker`, `multi_arb`, `stat_arb`, `directional`\n"
                "Ver stages actuales: `/stages`",
                parse_mode="Markdown",
            )
            return

        strategy_name = args[0].lower()
        stage_machine = self._get_stage_machine()
        if stage_machine is None:
            await update.message.reply_text("❌ Stage machine no disponible.")
            return

        promoted = stage_machine.promote(strategy_name)
        if promoted:
            new_stage = stage_machine.get_stage(strategy_name)
            mult = stage_machine.get_size_multiplier(strategy_name)
            await update.message.reply_text(
                f"⬆️ *{strategy_name} Promovida*\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"Nuevo stage: `{new_stage.value}`\n"
                f"Multiplicador: `{mult:.0%}`",
                parse_mode="Markdown",
            )
        else:
            current = stage_machine.get_stage(strategy_name)
            await update.message.reply_text(
                f"⛔ No se puede promover *{strategy_name}*\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"Stage actual: `{current.value}`\n"
                f"_No hay transición válida disponible._",
                parse_mode="Markdown",
            )
        logger.info("Promote command: strategy=%s promoted=%s", strategy_name, promoted)

    async def _cmd_demote(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Demotear estrategia al stage anterior."""
        if not self._is_authorized(update):
            await self._reject(update)
            return
        user_id = str(update.effective_user.id) if update.effective_user else "?"
        if not self._check_rate_limit(user_id):
            if update.message:
                await update.message.reply_text("⏳ Muy rápido. Esperá un segundo.")
            return
        if not update.message:
            return

        args = context.args or []
        if not args:
            await update.message.reply_text(
                "⬇️ *Uso: /demote*\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n"
                "`/demote <estrategia>`\n"
                "Estrategias: `market_maker`, `multi_arb`, `stat_arb`, `directional`\n"
                "Ver stages actuales: `/stages`",
                parse_mode="Markdown",
            )
            return

        strategy_name = args[0].lower()
        stage_machine = self._get_stage_machine()
        if stage_machine is None:
            await update.message.reply_text("❌ Stage machine no disponible.")
            return

        demoted = stage_machine.demote(strategy_name)
        if demoted:
            new_stage = stage_machine.get_stage(strategy_name)
            mult = stage_machine.get_size_multiplier(strategy_name)
            await update.message.reply_text(
                f"⬇️ *{strategy_name} Demoteada*\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"Nuevo stage: `{new_stage.value}`\n"
                f"Multiplicador: `{mult:.0%}`",
                parse_mode="Markdown",
            )
        else:
            current = stage_machine.get_stage(strategy_name)
            await update.message.reply_text(
                f"⛔ No se puede demotear *{strategy_name}*\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"Stage actual: `{current.value}`\n"
                f"_No hay transición válida disponible._",
                parse_mode="Markdown",
            )
        logger.info("Demote command: strategy=%s demoted=%s", strategy_name, demoted)

    async def _cmd_stages(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Ver stage actual de cada estrategia."""
        if not self._is_authorized(update):
            await self._reject(update)
            return
        user_id = str(update.effective_user.id) if update.effective_user else "?"
        if not self._check_rate_limit(user_id):
            if update.message:
                await update.message.reply_text("⏳ Muy rápido. Esperá un segundo.")
            return
        if not update.message:
            return

        stage_machine = self._get_stage_machine()
        if stage_machine is None:
            await update.message.reply_text("❌ Stage machine no disponible.")
            return

        stats = stage_machine.get_stats()
        if not stats:
            await update.message.reply_text("📭 Sin estrategias registradas.")
            return

        _stage_icons = {
            "SHADOW": "👁",
            "PAPER": "📄",
            "LIVE_SMALL": "🔸",
            "LIVE_FULL": "🟢",
        }

        lines = ["🎚️ *Stage Machine*\n" + "━" * 22]

        for name, info in stats.items():
            icon = _stage_icons.get(info["stage"], "❓")
            lines.append(
                f"\n*{icon} {name}*\n"
                f"   ├ Stage: `{info['stage']}` ({info['multiplier']:.0%})\n"
                f"   ├ Reviews: `{info['recent_positive']}/{info['review_window']}`\n"
                f"   ├ Para promover: `{info['reviews_to_promote']}` más\n"
                f"   └ Próximo: `{info['next_stage']}`"
            )

        await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

    async def _cmd_blacklist(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Ver mercados en blacklist activa."""
        if not self._is_authorized(update):
            await self._reject(update)
            return
        user_id = str(update.effective_user.id) if update.effective_user else "?"
        if not self._check_rate_limit(user_id):
            if update.message:
                await update.message.reply_text("⏳ Muy rápido. Esperá un segundo.")
            return
        if not update.message:
            return

        blacklist = self._get_blacklist()
        if blacklist is None:
            await update.message.reply_text("❌ Blacklist no disponible.")
            return

        active = blacklist.get_active()
        if not active:
            await update.message.reply_text("✅ Sin mercados en blacklist activa.")
            return

        now = time.time()
        lines = [f"🚫 *Blacklist ({len(active)} mercados)*\n" + "━" * 22]

        for mid, expire in sorted(active.items(), key=lambda x: x[1]):
            hours_left = (expire - now) / 3600
            lines.append(f"   `{mid[:16]}...` — expira en `{hours_left:.1f}h`")

        stats = blacklist.get_stats()
        lines.append(
            f"\n_Umbral WR: {stats['wr_threshold']:.0%} | "
            f"Min round-trips: {stats['min_trades']}_"
        )

        await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

    # ------------------------------------------------------------------
    # Loops de fondo
    # ------------------------------------------------------------------

    async def _heartbeat_loop(self) -> None:
        """Envia un heartbeat cada 30 minutos si no hubo alertas recientes."""
        while self._stop_event and not self._stop_event.is_set():
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=1800)
            except asyncio.TimeoutError:
                if self._controller:
                    try:
                        status = self._controller.get_status()
                        uptime_delta = datetime.now(timezone.utc) - self._start_time
                        uptime_h = int(uptime_delta.total_seconds() // 3600)
                        uptime_m = int((uptime_delta.total_seconds() % 3600) // 60)
                        uptime_d = int(uptime_delta.total_seconds() // 86400)
                        uptime_str = f"{uptime_d}d {uptime_h}h {uptime_m}m" if uptime_d > 0 else f"{uptime_h}h {uptime_m}m"
                        send_alert(
                            f"💓 *Heartbeat*\n"
                            f"Uptime: `{uptime_str}`\n"
                            f"Balance: `${status.get('balance_usdc', 0):.2f}`\n"
                            f"Estado: `{status.get('state', '?')}`"
                        )
                    except Exception:
                        logger.exception("Error en heartbeat loop")

    async def _daily_summary_loop(self) -> None:
        """Envia un resumen diario automatico a las 00:00 UTC."""
        while self._stop_event and not self._stop_event.is_set():
            now = datetime.now(timezone.utc)
            next_midnight = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            seconds_until = (next_midnight - now).total_seconds()
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=seconds_until)
            except asyncio.TimeoutError:
                try:
                    self._send_daily_summary()
                except Exception:
                    logger.exception("Error enviando resumen diario")

    def _send_daily_summary(self) -> None:
        """Calcula y envia resumen diario por Telegram."""
        cutoff = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        stats = self._compute_pnl_since(cutoff)
        trades_today = int(stats["count"])
        pnl_today = stats["pnl"]
        fees = stats["fees"]
        rewards = stats["rewards"]

        # Drawdown
        dd_str = "N/A"
        if self._controller:
            cb = getattr(self._controller, "_circuit_breaker", None)
            if cb and hasattr(cb, "get_drawdown_report"):
                dd_report = cb.get_drawdown_report()
                dd_7 = dd_report.get("drawdown_7d", 0.0)
                dd_str = f"${dd_7:+.2f}"

        msg = (
            f"📅 *Resumen Diario*\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"Trades: `{trades_today}`\n"
            f"PnL: `${pnl_today:+.4f}`\n"
            f"Fees: `${fees:.4f}`\n"
            f"Rewards: `${rewards:.4f}`\n"
            f"Drawdown 7d: `{dd_str}`"
        )
        send_alert(msg)

    # ------------------------------------------------------------------
    # Accesores a componentes
    # ------------------------------------------------------------------

    def _get_stage_machine(self) -> Any:
        """Accede a la StageMachine via el controller."""
        if self._controller and hasattr(self._controller, "_stage_machine"):
            return self._controller._stage_machine
        return None

    def _get_blacklist(self) -> Any:
        """Accede a la MarketBlacklist via el controller."""
        if self._controller and hasattr(self._controller, "_blacklist"):
            return self._controller._blacklist
        return None

    def _get_market_name(self, condition_id: str) -> str:
        """Busca el nombre (question) de un mercado por condition_id.

        Revisa self._controller._active_markets y retorna la pregunta truncada.
        Si no lo encuentra, retorna el condition_id truncado.
        """
        markets = getattr(self._controller, "_active_markets", []) if self._controller else []
        for market in markets:
            if market.get("condition_id") == condition_id:
                question = market.get("question", "")
                return question[:30] if question else condition_id[:12]
        return condition_id[:12]

    # ------------------------------------------------------------------
    # Helpers para calculo de PnL desde trades.jsonl
    # ------------------------------------------------------------------

    def _compute_pnl_since(self, cutoff: datetime) -> dict[str, float]:
        """Calcula estadisticas de PnL para trades posteriores a cutoff."""
        # Cache por 60 segundos para evitar re-leer el archivo en cada comando
        now = datetime.now(timezone.utc)
        if self._pnl_cache is not None:
            cached_time, cached_val = self._pnl_cache
            if (now - cached_time).total_seconds() < 60:
                return cached_val.copy()

        stats: dict[str, float] = {
            "count": 0, "errors": 0, "deployed": 0.0,
            "pnl": 0.0, "fees": 0.0, "rewards": 0.0,
        }

        if not TRADES_FILE.exists():
            self._pnl_cache = (now, stats.copy())
            return stats

        trades_by_market: dict[str, list[dict]] = {}

        with open(TRADES_FILE, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    t = json.loads(line)
                    ts_str = t.get("timestamp", "")
                    if ts_str:
                        ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                        if ts < cutoff:
                            continue
                    stats["count"] += 1
                    if t.get("status") == "error":
                        stats["errors"] += 1
                    if t.get("side") == "BUY":
                        stats["deployed"] += t.get("size", 0.0)
                    stats["fees"] += t.get("fee_paid", 0.0)
                    stats["rewards"] += t.get("rewards_earned", 0.0)

                    mid = t.get("market_id", "unknown")
                    trades_by_market.setdefault(mid, []).append(t)
                except (json.JSONDecodeError, ValueError):
                    continue

        # Calcular PnL por mercado via spread capturado
        gross_profit = 0.0
        gross_loss = 0.0
        for market_trades in trades_by_market.values():
            buys = [t for t in market_trades if t.get("side") == "BUY"]
            sells = [t for t in market_trades if t.get("side") == "SELL"]
            if not buys or not sells:
                continue
            avg_buy = sum(t.get("price", 0.0) for t in buys) / len(buys)
            avg_sell = sum(t.get("price", 0.0) for t in sells) / len(sells)
            matched = min(
                sum(t.get("size", 0.0) for t in buys),
                sum(t.get("size", 0.0) for t in sells),
            )
            pnl = (avg_sell - avg_buy) * matched
            if pnl >= 0:
                gross_profit += pnl
            else:
                gross_loss += abs(pnl)

        stats["pnl"] = round(gross_profit - gross_loss - stats["fees"], 4)
        stats["fees"] = round(stats["fees"], 4)
        stats["rewards"] = round(stats["rewards"], 4)
        stats["deployed"] = round(stats["deployed"], 2)
        self._pnl_cache = (now, stats.copy())
        return stats

    def _count_today_trades(self) -> int:
        """Cuenta trades desde el inicio del dia UTC."""
        if not TRADES_FILE.exists():
            return 0
        cutoff = datetime.now(timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        count = 0
        with open(TRADES_FILE, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    t = json.loads(line)
                    ts_str = t.get("timestamp", "")
                    if ts_str:
                        ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                        if ts >= cutoff:
                            count += 1
                except (json.JSONDecodeError, ValueError):
                    continue
        return count

    def _get_last_trade_time(self) -> float | None:
        """Retorna minutos desde el último trade exitoso."""
        if not TRADES_FILE.exists():
            return None
        last_ts = None
        with open(TRADES_FILE, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    t = json.loads(line)
                    if t.get("status") != "error":
                        ts_str = t.get("timestamp", "")
                        if ts_str:
                            ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                            if last_ts is None or ts > last_ts:
                                last_ts = ts
                except (json.JSONDecodeError, ValueError):
                    continue
        if last_ts:
            return (datetime.now(timezone.utc) - last_ts).total_seconds() / 60
        return None

    def _get_recent_log_errors(self, n: int = 5) -> list[str]:
        """Retorna las últimas N líneas de error del log."""
        if not LOG_FILE.exists():
            return []
        errors: deque[str] = deque(maxlen=n)
        try:
            with open(LOG_FILE, "r", encoding="utf-8", errors="ignore") as f:
                for line in f:
                    if "ERROR" in line or "CRITICAL" in line or "exception" in line.lower():
                        cleaned = line.strip()
                        if len(cleaned) > 10:
                            errors.append(cleaned)
        except OSError:
            pass
        return list(errors)

    def _get_recent_log_lines(self, n: int = 20) -> list[str]:
        """Retorna las últimas N líneas del log general."""
        if not LOG_FILE.exists():
            return []
        lines: deque[str] = deque(maxlen=n)
        try:
            with open(LOG_FILE, "r", encoding="utf-8", errors="ignore") as f:
                for line in f:
                    stripped = line.rstrip("\n")
                    if stripped:
                        lines.append(stripped)
        except OSError:
            pass
        return list(lines)

    # ------------------------------------------------------------------
    # Compatibilidad con main.py (API publica sync anterior)
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """API de compatibilidad: corre el bot directamente en el event loop actual."""
        await self._run_async()

    async def stop_async(self) -> None:
        """Detiene el bot si esta corriendo en el event loop actual."""
        if self._stop_event:
            self._stop_event.set()
