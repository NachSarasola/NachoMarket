"""Bot de Telegram para monitoreo y control del bot de trading.

Corre en un thread daemon separado dentro del proceso principal.
Expone send_alert() como funcion de modulo — llamable desde cualquier modulo
sin importar ciclos ni estado del bot.

Uso desde otros modulos:
    from src.telegram.bot import send_alert
    send_alert("⚠️ Error: timeout en la API")

Comandos disponibles:
    /start    — Bienvenida y lista de comandos
    /status   — Balance, PnL, trades, estrategias, mercados, inventory, proximo review
    /pause    — Pausa instantanea del trading
    /resume   — Reanuda el trading
    /kill     — Para el bot completamente
    /review   — Fuerza un self-review inmediato
    /markets  — Lista mercados actuales con inventory
    /pnl      — Reporte de PnL dia / semana / mes
"""

import asyncio
import json
import logging
import os
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

load_dotenv()
logger = logging.getLogger("nachomarket.telegram")

TRADES_FILE = Path("data/trades.jsonl")

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
    if _bot_instance is None or _event_loop is None or _event_loop.is_closed():
        return
    asyncio.run_coroutine_threadsafe(
        _bot_instance._send_message(message),
        _event_loop,
    )


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

        # Registrar handlers
        handlers = [
            ("start", self._cmd_start),
            ("status", self._cmd_status),
            ("pause", self._cmd_pause),
            ("resume", self._cmd_resume),
            ("kill", self._cmd_kill),
            ("review", self._cmd_review),
            ("markets", self._cmd_markets),
            ("pnl", self._cmd_pnl),
            ("block", self._cmd_block),
            ("drawdown", self._cmd_drawdown),
            ("stats", self._cmd_stats),
            ("attribution", self._cmd_attribution),
        ]
        for name, handler in handlers:
            self._app.add_handler(CommandHandler(name, handler))

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
    # Seguridad
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

    # ------------------------------------------------------------------
    # Comandos
    # ------------------------------------------------------------------

    async def _cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Mensaje de bienvenida con lista de comandos disponibles."""
        if not self._is_authorized(update):
            await self._reject(update)
            return

        text = (
            "*NachoMarket — Bot de Trading Polymarket* 🤖\n"
            "Capital: $400 USDC | Modo: market making + arbitraje\n\n"
            "*Comandos disponibles:*\n"
            "/status — Estado actual del bot\n"
            "/pnl — PnL dia / semana / mes\n"
            "/stats — Sharpe/Sortino/Calmar 30d\n"
            "/attribution — Top/bottom estrategias y mercados\n"
            "/markets — Mercados activos con inventory\n"
            "/review — Forzar self\\-review inmediato\n"
            "/pause — Pausar trading \\(cancela ordenes\\)\n"
            "/resume — Reanudar trading\n"
            "/kill — Parar el bot completamente\n"
            "/drawdown — Rolling drawdown 7/15/30d y scale-down status\n\n"
            "_Notificaciones automaticas: trades, errores, circuit breaker, reviews_"
        )
        if update.message:
            await update.message.reply_text(text, parse_mode="MarkdownV2")

    async def _cmd_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """/status — Balance, PnL, trades ejecutados, estrategias, inventory, proximo review."""
        if not self._is_authorized(update):
            await self._reject(update)
            return
        if not update.message:
            return

        if not self._controller:
            await update.message.reply_text("Bot controller no conectado.")
            return

        status = self._controller.get_status()
        positions = self._controller.get_positions()

        # Contar trades de hoy
        today_trades = self._count_today_trades()

        # Estrategias activas
        strategies = getattr(self._controller, "_strategies", [])
        active_strats = [s.name for s in strategies if getattr(s, "is_active", True)]

        # Mercados con posicion abierta
        markets_open = len(positions)

        # Circuit breaker
        cb_status = "🔴 ACTIVO" if status.get("circuit_breaker") else "🟢 OK"
        cb_reason = status.get("trigger_reason", "")

        state_icon = {"running": "▶️", "paused": "⏸️", "stopped": "🛑"}.get(
            status.get("state", ""), "❓"
        )

        lines = [
            f"*Estado NachoMarket* {state_icon} `{status.get('state', '?').upper()}`",
            "",
            f"💰 Exposure total: `${status.get('total_exposure', 0):.2f}` USDC",
            f"📈 PnL diario: `${status.get('daily_pnl', 0):.4f}` USDC",
            f"📊 Trades hoy: `{today_trades}`",
            f"📋 Ordenes abiertas: `{status.get('open_orders', 0)}`",
            f"🏪 Mercados con posicion: `{markets_open}`",
            f"⚡ Estrategias activas: `{', '.join(active_strats) or 'ninguna'}`",
            f"🔁 Errores consecutivos: `{status.get('consecutive_errors', 0)}`",
            f"⛔ Circuit breaker: {cb_status}",
        ]

        if cb_reason:
            lines.append(f"   Razon: `{cb_reason}`")

        if positions:
            lines.append("\n*Inventory:*")
            for mid, pos in list(positions.items())[:5]:
                yes = pos.get("yes", 0)
                no = pos.get("no", 0)
                lines.append(f"  `{mid[:10]}...` YES:`{yes:.1f}` NO:`{no:.1f}`")
            if len(positions) > 5:
                lines.append(f"  _(y {len(positions) - 5} mercados más)_")

        await update.message.reply_text("\n".join(lines))

    async def _cmd_pause(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """/pause — Pausa instantanea del trading."""
        if not self._is_authorized(update):
            await self._reject(update)
            return
        if not update.message:
            return

        if not self._controller:
            await update.message.reply_text("Bot controller no conectado.")
            return

        self._controller.pause()
        await update.message.reply_text(
            "⏸️ *Trading PAUSADO.*\nUsá /resume para reanudar.",
        )
        logger.info("Bot paused via Telegram")

    async def _cmd_resume(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """/resume — Reanuda el trading."""
        if not self._is_authorized(update):
            await self._reject(update)
            return
        if not update.message:
            return

        if not self._controller:
            await update.message.reply_text("Bot controller no conectado.")
            return

        self._controller.resume()
        await update.message.reply_text("▶️ *Trading REANUDADO.*")
        logger.info("Bot resumed via Telegram")

    async def _cmd_kill(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """/kill — Cancela todas las ordenes y detiene el bot."""
        if not self._is_authorized(update):
            await self._reject(update)
            return
        if not update.message:
            return

        if not self._controller:
            await update.message.reply_text("Bot controller no conectado.")
            return

        await update.message.reply_text(
            "🛑 *Deteniendo el bot...*\nCancelando todas las ordenes abiertas.",
        )
        self._controller.kill()
        logger.critical("Bot killed via Telegram")

    async def _cmd_review(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """/review — Fuerza un self-review inmediato con Claude Haiku."""
        if not self._is_authorized(update):
            await self._reject(update)
            return
        if not update.message:
            return

        await update.message.reply_text("🔍 Ejecutando self-review... (puede tardar ~10s)")

        reviewer = getattr(self._controller, "_reviewer", None) if self._controller else None
        if not reviewer:
            await update.message.reply_text("Reviewer no disponible.")
            return

        # Ejecutar en executor para no bloquear el event loop del bot
        loop = asyncio.get_event_loop()
        state = self._controller.get_status() if self._controller else None
        try:
            result = await loop.run_in_executor(None, reviewer.run_review, state)
            status = result.get("status", "ok")
            if status == "no_trades":
                await update.message.reply_text("Sin trades en las ultimas 8h para revisar.")
            elif status == "error":
                await update.message.reply_text("❌ Error ejecutando el review.")
            else:
                analysis = result.get("analysis", {})
                if isinstance(analysis, dict):
                    risk = analysis.get("risk_level", "?")
                    summary = analysis.get("summary", "Sin resumen")
                    cost = result.get("estimated_cost_usd", 0)
                    await update.message.reply_text(
                        f"✅ *Review completado*\n"
                        f"Risk: `{risk}` | Costo: `${cost:.5f}`\n"
                        f"_{summary}_",
                    )
                else:
                    await update.message.reply_text(f"✅ Review completado: {result.get('trade_count', 0)} trades analizados.")
        except Exception:
            logger.exception("Error in /review command")
            await update.message.reply_text("❌ Error inesperado durante el review.")

    async def _cmd_markets(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """/markets — Lista mercados activos con inventory actual."""
        if not self._is_authorized(update):
            await self._reject(update)
            return
        if not update.message:
            return

        if not self._controller:
            await update.message.reply_text("Bot controller no conectado.")
            return

        positions = self._controller.get_positions()
        if not positions:
            await update.message.reply_text("Sin mercados con posicion abierta.")
            return

        lines = ["*Mercados activos (inventory):*"]
        for i, (mid, pos) in enumerate(positions.items()):
            yes = pos.get("yes", 0.0)
            no = pos.get("no", 0.0)
            total = abs(yes) + abs(no)
            skew = (yes - no) / total if total > 0 else 0.0
            skew_icon = "🔼" if skew > 0.3 else ("🔽" if skew < -0.3 else "➡️")
            lines.append(
                f"{i+1}. `{mid[:14]}...`\n"
                f"   YES:`{yes:.1f}` NO:`{no:.1f}` Total:`${total:.1f}` {skew_icon}"
            )

        await update.message.reply_text("\n".join(lines))

    async def _cmd_pnl(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """/pnl — Reporte detallado de PnL dia / semana / mes."""
        if not self._is_authorized(update):
            await self._reject(update)
            return
        if not update.message:
            return

        now = datetime.now(timezone.utc)
        periods = {
            "Hoy": now - timedelta(hours=24),
            "Semana": now - timedelta(days=7),
            "Mes": now - timedelta(days=30),
        }

        lines = ["*Reporte de PnL*"]

        for label, cutoff in periods.items():
            stats = self._compute_pnl_since(cutoff)
            lines.append(
                f"\n*{label}:*\n"
                f"  Trades: `{stats['count']}` | Errores: `{stats['errors']}`\n"
                f"  Capital desplegado: `${stats['deployed']:.2f}`\n"
                f"  PnL estimado: `${stats['pnl']:.4f}`\n"
                f"  Fees pagados: `${stats['fees']:.4f}`\n"
                f"  Rewards: `${stats['rewards']:.4f}`"
            )

        # PnL del circuit breaker (sesion actual)
        if self._controller:
            cb_pnl = self._controller.get_status().get("daily_pnl", 0)
            lines.append(f"\n*PnL intradiario (circuit breaker):* `${cb_pnl:.4f}`")

        # Top mercados por rentabilidad
        if self._controller and hasattr(self._controller, "_profiler"):
            report = self._controller._profiler.get_report(top_n=5)
            if report:
                lines.append("\n*Top mercados (ROI):*")
                for entry in report:
                    roi_pct = entry["roi"] * 100
                    icon = "🟢" if roi_pct >= 0 else "🔴"
                    lines.append(
                        f"  {icon} `{entry['question'][:25]}` "
                        f"ROI:`{roi_pct:.1f}%` PnL:`${entry['total_pnl']:.2f}`"
                    )

        await update.message.reply_text("\n".join(lines))

    async def _cmd_block(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """/block <market_id> <hours> — Bloquea un mercado temporalmente."""
        if not self._is_authorized(update):
            await self._reject(update)
            return
        if not update.message:
            return

        args = context.args or []
        if len(args) < 2:
            await update.message.reply_text(
                "Uso: `/block <condition_id> <hours>`\n"
                "Ejemplo: `/block 0xabc123 24`"
            )
            return

        market_id = args[0]
        try:
            hours = float(args[1])
        except ValueError:
            await update.message.reply_text("Horas debe ser un numero")
            return

        if self._controller and hasattr(self._controller, "_market_analyzer"):
            self._controller._market_analyzer.market_filter.block_market_until(
                market_id, hours
            )
            await update.message.reply_text(
                f"Mercado `{market_id[:12]}...` bloqueado por {hours}h"
            )
        else:
            await update.message.reply_text("Bot no tiene market analyzer activo")

    async def _cmd_drawdown(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """/drawdown — Muestra drawdown rolling 7/15/30d y estado de scale-down."""
        if not self._is_authorized(update):
            await self._reject(update)
            return
        if not update.message:
            return

        if not self._controller:
            await update.message.reply_text("Bot controller no conectado.")
            return

        cb = getattr(self._controller, "_circuit_breaker", None)
        if cb is None:
            await update.message.reply_text("Circuit breaker no disponible.")
            return

        report = cb.get_drawdown_report()
        dd7 = report["drawdown_7d"]
        dd15 = report["drawdown_15d"]
        dd30 = report["drawdown_30d"]

        def fmt(val: float) -> str:
            icon = "🟢" if val >= 0 else "🔴"
            return f"{icon} `${val:+.2f}`"

        thresholds = {
            "7d": getattr(cb, "_drawdown_7d_threshold", 40.0),
            "15d": getattr(cb, "_drawdown_15d_threshold", 80.0),
            "30d": getattr(cb, "_drawdown_30d_threshold", 120.0),
        }

        scale_status = "🔴 ACTIVO (size -50%)" if getattr(cb, "_scale_down_active", False) else "🟢 normal"
        arb_status = "🔴 PAUSADO" if getattr(cb, "_arb_paused", False) else "🟢 activo"

        text = (
            "*Rolling Drawdown Report*\n\n"
            f"📅 7d: {fmt(dd7)} (límite: `${thresholds['7d']:.0f}`)\n"
            f"📅 15d: {fmt(dd15)} (límite: `${thresholds['15d']:.0f}`)\n"
            f"📅 30d: {fmt(dd30)} (límite: `${thresholds['30d']:.0f}`)\n\n"
            f"⚖️ Scale-down 7d: {scale_status}\n"
            f"🛑 Arb/Directional 15d: {arb_status}"
        )
        await update.message.reply_text(text)

    async def _cmd_stats(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """/stats — Muestra Sharpe/Sortino/Calmar ratios de los últimos 30 días."""
        if not self._is_authorized(update):
            await self._reject(update)
            return
        if not update.message:
            return

        try:
            from src.analysis.performance_metrics import compute_metrics_from_trades_file  # noqa: PLC0415
            metrics = compute_metrics_from_trades_file(
                str(TRADES_FILE),
                window_days=30,
                risk_free_rate=0.04,
            )
        except Exception:
            await update.message.reply_text("❌ Error calculando métricas cuantitativas.")
            return

        if metrics.get("trade_count_30d", 0) == 0:
            await update.message.reply_text("Sin trades suficientes en los últimos 30 días.")
            return

        sharpe = metrics.get("sharpe_ratio", 0.0)
        sortino = metrics.get("sortino_ratio", 0.0)
        calmar = metrics.get("calmar_ratio", 0.0)
        max_dd = metrics.get("max_drawdown", 0.0)
        total_return = metrics.get("total_return", 0.0)
        count = metrics.get("trade_count_30d", 0)

        sharpe_icon = "🟢" if sharpe > 1.5 else ("🟡" if sharpe > 0.5 else "🔴")
        calmar_icon = "🟢" if calmar > 2.0 else ("🟡" if calmar > 0.5 else "🔴")
        dd_icon = "🟢" if max_dd < 20 else ("🟡" if max_dd < 50 else "🔴")

        text = (
            "*Métricas Cuantitativas — 30 días*\n\n"
            f"📊 Sharpe Ratio: {sharpe_icon} `{sharpe:.3f}`\n"
            f"📉 Sortino Ratio: `{sortino:.3f}`\n"
            f"{calmar_icon} Calmar Ratio: `{calmar:.3f}`\n"
            f"{dd_icon} Max Drawdown: `${max_dd:.4f}`\n"
            f"💰 Return total 30d: `${total_return:.4f}`\n"
            f"🔢 Trades analizados: `{count}`\n\n"
            "_Benchmark: Sharpe>1.5 = 🟢, >0.5 = 🟡, <0.5 = 🔴_"
        )
        await update.message.reply_text(text)

    async def _cmd_attribution(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """/attribution — Top y bottom estrategias/mercados por PnL."""
        if not self._is_authorized(update):
            await self._reject(update)
            return
        if not update.message:
            return

        try:
            from src.analysis.attribution import TradeAttribution  # noqa: PLC0415
            attr = TradeAttribution(str(TRADES_FILE))
            text = attr.format_telegram(top_n=3)
        except Exception:
            text = "❌ Error generando attribution report."

        await update.message.reply_text(text or "Sin datos de attribution disponibles.")

    # ------------------------------------------------------------------
    # Helpers para calculo de PnL desde trades.jsonl
    # ------------------------------------------------------------------

    def _compute_pnl_since(self, cutoff: datetime) -> dict[str, float]:
        """Calcula estadisticas de PnL para trades posteriores a cutoff."""
        stats: dict[str, float] = {
            "count": 0, "errors": 0, "deployed": 0.0,
            "pnl": 0.0, "fees": 0.0, "rewards": 0.0,
        }

        if not TRADES_FILE.exists():
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
                    stats["rewards"] += t.get("rewards", 0.0)

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
