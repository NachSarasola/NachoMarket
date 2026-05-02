"""Bot de Telegram para monitoreo y control del bot de LP rewards farming.

Corre en un thread daemon separado dentro del proceso principal.
Expone send_alert() como funcion de modulo — llamable desde cualquier modulo
sin importar ciclos ni estado del bot.

Uso desde otros modulos:
    from src.telegram.bot import send_alert
    send_alert("Error: timeout en la API")

Comandos disponibles:
    /start          — Lista de comandos
    /status         — Dashboard principal: balance, rewards hoy, top mercados
    /balance        — Detalle financiero + desglose de rewards
    /markets        — Mercados activos: ¢/min, share%, daily_rate, ordenes
    /rewards        — Vista detallada del RewardTracker por mercado
    /orders         — Ordenes abiertas con mercado, precio, tamaño, edad
    /pnl            — Rewards acumulados hoy + stats de trades
    /health         — WS, API, uptime, ciclos, errores recientes
    /logs [n]       — Ultimas N lineas del log
    /pause          — Pausa instantanea del trading
    /resume         — Reanuda el trading
    /kill           — Para el bot (requiere confirmacion)
    /confirm_kill   — Confirmar detencion
    /block <id> <h> — Bloquear mercado temporalmente
    /unblock <id>   — Desbloquear mercado
    /blacklist      — Mercados en blacklist activa
    /review         — Forzar self-review inmediato
    /force_reconcile — Forzar reconciliacion on-chain
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

    Sincrona y no bloqueante: delega al event loop del thread de Telegram
    via run_coroutine_threadsafe. Si el bot no esta inicializado, es silenciosa.

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

    retry_cfg = retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
    )
    try:
        retry_cfg(_send)()
    except Exception:
        logger.exception("Failed to send Telegram alert after retries")


class TelegramBot:
    """Bot de Telegram con comandos de control y notificaciones proactivas.

    Args:
        bot_controller: Instancia de NachoMarketBot (duck typing).
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

        if self._token:
            self.run_in_thread()
        else:
            logger.warning("TELEGRAM_BOT_TOKEN not set — Telegram bot disabled")

    # ------------------------------------------------------------------
    # Thread management
    # ------------------------------------------------------------------

    def run_in_thread(self) -> threading.Thread:
        """Inicia el bot en un thread daemon separado."""
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

        thread = threading.Thread(target=_thread_main, daemon=True, name="telegram-bot")
        thread.start()
        logger.info("Telegram bot thread started")
        return thread

    async def _run_async(self) -> None:
        """Loop principal async. Corre hasta que stop() sea llamado."""
        self._stop_event = asyncio.Event()
        self._app = Application.builder().token(self._token).build()
        self._app.add_error_handler(self._error_handler)

        handlers = [
            ("start", self._cmd_start),
            ("status", self._cmd_status),
            ("balance", self._cmd_balance),
            ("markets", self._cmd_markets),
            ("rewards", self._cmd_rewards),
            ("orders", self._cmd_orders),
            ("pnl", self._cmd_pnl),
            ("health", self._cmd_health),
            ("logs", self._cmd_logs),
            ("pause", self._cmd_pause),
            ("resume", self._cmd_resume),
            ("kill", self._cmd_kill),
            ("confirm_kill", self._cmd_confirm_kill),
            ("block", self._cmd_block),
            ("unblock", self._cmd_unblock),
            ("blacklist", self._cmd_blacklist),
            ("review", self._cmd_review),
            ("force_reconcile", self._cmd_force_reconcile),
        ]
        for name, handler in handlers:
            self._app.add_handler(CommandHandler(name, handler))

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
        """Detiene el bot de forma ordenada."""
        global _event_loop
        if self._stop_event and _event_loop and not _event_loop.is_closed():
            _event_loop.call_soon_threadsafe(self._stop_event.set)

    # ------------------------------------------------------------------
    # Envio de mensajes
    # ------------------------------------------------------------------

    async def _send_message(self, message: str, parse_mode: str = "Markdown") -> None:
        """Envia un mensaje al chat configurado."""
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

    # ------------------------------------------------------------------
    # Seguridad y rate-limiting
    # ------------------------------------------------------------------

    def _is_authorized(self, update: Update) -> bool:
        """Verifica que el mensaje viene del usuario autorizado."""
        if not self._authorized_user_id:
            logger.warning("TELEGRAM_USER_ID not set — accepting all users (insecure)")
            return True
        user = update.effective_user
        if user is None:
            return False
        authorized = str(user.id) == self._authorized_user_id
        if not authorized:
            logger.warning("Unauthorized Telegram access attempt from user_id=%s", user.id)
        return authorized

    async def _reject(self, update: Update) -> None:
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
        logger.exception("Unhandled exception in Telegram handler")
        if isinstance(update, Update) and update.effective_message:
            await update.effective_message.reply_text("Error interno.")

    # ------------------------------------------------------------------
    # Helpers de formato
    # ------------------------------------------------------------------

    @staticmethod
    def _fmt_money(val: float, decimals: int = 2) -> str:
        sign = "+" if val > 0 else ""
        return f"`${sign}{val:,.{decimals}f}`"

    @staticmethod
    def _fmt_pct(val: float, decimals: int = 1) -> str:
        sign = "+" if val > 0 else ""
        return f"`{sign}{val:.{decimals}f}%`"

    @staticmethod
    def _section_header(title: str) -> str:
        return f"*{title}*\n{'─' * 22}"

    @staticmethod
    def _rate_bar(value: float, max_value: float, width: int = 5) -> str:
        """Barra visual de proporcion: ░░░█░"""
        if max_value <= 0:
            return "░" * width
        ratio = min(value / max_value, 1.0)
        filled = round(ratio * width)
        return "█" * filled + "░" * (width - filled)

    # ------------------------------------------------------------------
    # Guard helper para reducir boilerplate en comandos
    # ------------------------------------------------------------------

    async def _guard(self, update: Update) -> bool:
        """Valida autorizacion, rate limit y que update.message exista.
        Retorna True si el handler puede continuar."""
        if not self._is_authorized(update):
            await self._reject(update)
            return False
        user_id = str(update.effective_user.id) if update.effective_user else "?"
        if not self._check_rate_limit(user_id):
            if update.message:
                await update.message.reply_text("Muy rapido. Espera un segundo.")
            return False
        if not update.message:
            return False
        return True

    # ------------------------------------------------------------------
    # Accesores a RewardTracker
    # ------------------------------------------------------------------

    def _get_reward_tracker(self) -> Any:
        """Accede al RewardTracker via el controller."""
        return getattr(self._controller, "_reward_tracker", None) if self._controller else None

    def _get_today_rewards(self) -> tuple[float, dict[str, float], bool]:
        """Retorna (total_usd_today, {cid: usd_today}, is_real) usando datos de la API.

        Intenta obtener el valor real via GET /rewards/user/total (mismo que muestra
        la web en "Recompensas diarias"). Si falla, usa estimación basada en
        porcentajes y daily_rate como fallback.
        """
        client = getattr(self._controller, "_client", None) if self._controller else None
        if client is None:
            return 0.0, {}, False

        # Intentar obtener el valor real de la API
        try:
            from datetime import datetime, timezone
            date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            real_total = client.get_daily_real_rewards(date_str)
            if real_total > 0:
                logger = logging.getLogger("nachomarket.telegram")
                logger.info("_get_today_rewards: usando valor real de API: $%.4f", real_total)
                # Obtener desglose por mercado para compatibilidad
                try:
                    percentages: dict[str, float] = client.get_reward_percentages() or {}
                    rewards_map: dict = client.get_rewards() or {}
                    per_market: dict[str, float] = {}
                    for cid, share_pct in percentages.items():
                        if not share_pct:
                            continue
                        r = rewards_map.get(cid, {})
                        daily_rate = float(r.get("rewards_daily_rate", 0.0))
                        if daily_rate > 0:
                            per_market[cid] = float(share_pct) * daily_rate
                    return real_total, per_market, True
                except Exception:
                    return real_total, {}, True
        except Exception as e:
            logger = logging.getLogger("nachomarket.telegram")
            logger.warning("_get_today_rewards: API real falló, usando estimación: %s", e)

        # Fallback: estimación original
        try:
            percentages: dict[str, float] = client.get_reward_percentages() or {}
            rewards_map: dict = client.get_rewards() or {}
        except Exception:
            return 0.0, {}, False

        per_market: dict[str, float] = {}
        for cid, share_pct in percentages.items():
            if not share_pct:
                continue
            r = rewards_map.get(cid, {})
            daily_rate = float(r.get("rewards_daily_rate", 0.0))
            if daily_rate > 0:
                per_market[cid] = float(share_pct) * daily_rate
        return sum(per_market.values()), per_market, False

    # ------------------------------------------------------------------
    # Accesores generales
    # ------------------------------------------------------------------

    def _get_blacklist(self) -> Any:
        if self._controller and hasattr(self._controller, "_blacklist"):
            return self._controller._blacklist
        return None

    def _get_market_name(self, condition_id: str) -> str:
        """Busca el nombre (question) de un mercado por condition_id."""
        markets = getattr(self._controller, "_active_markets", []) if self._controller else []
        for market in markets:
            if market.get("condition_id") == condition_id:
                question = market.get("question", "")
                return question[:32] if question else condition_id[:12]
        return condition_id[:12]

    # ------------------------------------------------------------------
    # Comandos
    # ------------------------------------------------------------------

    async def _cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._guard(update):
            return

        text = (
            "*NachoMarket — LP Rewards Farmer*\n"
            "─────────────────────\n"
            "\n"
            "*Monitoreo*\n"
            "· /status   — Dashboard principal\n"
            "· /balance  — Detalle financiero\n"
            "· /markets  — Mercados activos\n"
            "· /rewards  — RewardTracker por mercado\n"
            "· /orders   — Ordenes abiertas\n"
            "· /pnl      — Rewards + trades del dia\n"
            "· /health   — Sistema, WS, errores\n"
            "· /logs     — Ultimas lineas del log\n"
            "\n"
            "*Control*\n"
            "· /pause    — Pausar trading\n"
            "· /resume   — Reanudar trading\n"
            "· /kill     — Parar el bot\n"
            "· /review   — Self-review inmediato\n"
            "· /force\\_reconcile — Reconciliacion on-chain\n"
            "\n"
            "*Seguridad*\n"
            "· /blacklist          — Ver blacklist\n"
            "· /block <id> <horas> — Bloquear mercado\n"
            "· /unblock <id>       — Desbloquear\n"
        )
        await update.message.reply_text(text, parse_mode="Markdown")

    async def _cmd_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Dashboard principal: balance, rewards hoy, top mercados por ¢/min."""
        if not await self._guard(update):
            return
        if not self._controller:
            await update.message.reply_text("Bot controller no conectado.")
            return

        status = self._controller.get_status()
        total_rewards, per_market, is_real = self._get_today_rewards()
        today_trades = self._count_today_trades()

        state = status.get("state", "?")
        state_icon = {"running": "●", "paused": "○", "stopped": "×"}.get(state, "?")
        cb_active = status.get("circuit_breaker", False)

        balance = status.get("balance_usdc", 0.0)
        open_orders = status.get("open_orders", 0)
        active_markets = getattr(self._controller, "_active_markets", [])

        # Top 3 mercados por rewards estimados hoy
        rt = self._get_reward_tracker()
        snap = rt.snapshot() if rt else {}

        real_label = "(real)" if is_real else "(estimado)"
        lines = [
            f"*NachoMarket* [{state_icon} {state.upper()}{'  CB ACTIVO' if cb_active else ''}]",
            "─────────────────────",
            f"· Balance:  `${balance:.2f}` USDC",
            f"· Rewards hoy {real_label}: `${total_rewards:.4f}`",
            f"· Mercados activos: `{len(active_markets)}`",
            f"· Ordenes abiertas: `{open_orders}`",
            f"· Trades hoy: `{today_trades}`",
        ]

        if snap:
            # Ordenar por ¢/min descendente
            ranked = sorted(
                snap.items(),
                key=lambda kv: kv[1].get("cents_per_min") or 0.0,
                reverse=True,
            )
            max_cpm = (ranked[0][1].get("cents_per_min") or 0.0) if ranked else 0.0
            lines.append("\n*Top mercados (¢/min)*")
            for cid, data in ranked[:4]:
                cpm = data.get("cents_per_min") or 0.0
                share = data.get("last_share_pct") or 0.0
                bar = self._rate_bar(cpm, max_cpm)
                name = self._get_market_name(cid)
                lines.append(f"· `{bar}` {name[:26]}  `{cpm:.2f}¢`  `{share:.1f}%`")

        if cb_active and status.get("trigger_reason"):
            lines.append(f"\n_CB: {status['trigger_reason']}_")

        await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

    async def _cmd_balance(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Detalle financiero: USDC, exposure, rewards breakdown."""
        if not await self._guard(update):
            return
        if not self._controller:
            await update.message.reply_text("Bot controller no conectado.")
            return

        status = self._controller.get_status()
        total_rewards, per_market, is_real = self._get_today_rewards()

        balance = status.get("balance_usdc", 0.0)
        exposure = status.get("total_exposure", 0.0)
        open_exposure = status.get("open_exposure", 0.0)
        inventory_exposure = status.get("inventory_exposure", 0.0)
        daily_pnl = status.get("daily_pnl", 0.0)
        open_orders = status.get("open_orders", 0)
        util_pct = (exposure / balance * 100) if balance > 0 else 0.0

        real_label = "(real)" if is_real else "(estimado)"
        lines = [
            self._section_header("Balance"),
            f"· USDC disponible: `${balance:.2f}`",
            f"· Exposure total:  `${exposure:.2f}`  (`{util_pct:.1f}%`)",
        ]
        if open_exposure > 0:
            lines.append(f"  ↳ en ordenes:    `${open_exposure:.2f}`")
        if inventory_exposure > 0:
            lines.append(f"  ↳ en inventory:  `${inventory_exposure:.2f}`")
        lines.extend([
            f"· Ordenes abiertas: `{open_orders}`",
            f"· PnL intradiario:  `${daily_pnl:+.4f}`",
            "",
            self._section_header("Rewards hoy"),
            f"· Total {real_label}: `${total_rewards:.4f}`",
        ])

        # Top 5 por rewards
        if per_market:
            ranked = sorted(per_market.items(), key=lambda kv: kv[1], reverse=True)
            for cid, usd in ranked[:5]:
                name = self._get_market_name(cid)
                lines.append(f"  · {name[:28]}  `${usd:.4f}`")

        await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

    async def _cmd_markets(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Mercados activos: ¢/min observado, share%, daily_rate, ordenes abiertas."""
        if not await self._guard(update):
            return
        if not self._controller:
            await update.message.reply_text("Bot controller no conectado.")
            return

        active_markets = getattr(self._controller, "_active_markets", [])
        if not active_markets:
            await update.message.reply_text("Sin mercados activos.")
            return

        rt = self._get_reward_tracker()
        snap = rt.snapshot() if rt else {}
        status = self._controller.get_status()
        open_orders_total = status.get("open_orders", 0)

        lines = [self._section_header(f"Mercados activos ({len(active_markets)})")]

        for m in active_markets:
            cid = m.get("condition_id", "?")
            question = (m.get("question") or cid)[:34]
            rewards_rate = m.get("rewards_rate") or m.get("rewards_min_size", 0)
            max_spread = m.get("rewards_max_spread", 0)

            data = snap.get(cid, {})
            cpm = data.get("cents_per_min") or 0.0
            share = data.get("last_share_pct") or 0.0
            daily_rate = data.get("last_daily_rate") or 0.0
            samples = data.get("sample_count") or 0

            lines.append(
                f"\n· *{question}*\n"
                f"  ¢/min: `{cpm:.2f}`  share: `{share:.1f}%`  rate: `${daily_rate:.2f}/d`\n"
                f"  spread max: `{max_spread}`  samples: `{samples}`"
            )

        await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

    async def _cmd_rewards(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Vista detallada del RewardTracker por mercado."""
        if not await self._guard(update):
            return
        if not self._controller:
            await update.message.reply_text("Bot controller no conectado.")
            return

        rt = self._get_reward_tracker()
        if rt is None:
            await update.message.reply_text("RewardTracker no disponible.")
            return

        snap = rt.snapshot()
        if not snap:
            await update.message.reply_text("Sin datos en RewardTracker aun.")
            return

        total_rewards, per_market, is_real = self._get_today_rewards()

        # Ordenar por ¢/min
        ranked = sorted(
            snap.items(),
            key=lambda kv: kv[1].get("cents_per_min") or 0.0,
            reverse=True,
        )
        max_cpm = (ranked[0][1].get("cents_per_min") or 0.0) if ranked else 0.0

        real_label = "(real)" if is_real else "(estimado)"
        lines = [
            self._section_header("RewardTracker"),
            f"· Total hoy {real_label}: `${total_rewards:.4f}`",
            "",
        ]

        for cid, data in ranked:
            cpm = data.get("cents_per_min") or 0.0
            share = data.get("last_share_pct") or 0.0
            daily_rate = data.get("last_daily_rate") or 0.0
            samples = data.get("sample_count") or 0
            est_today = per_market.get(cid, 0.0)
            bar = self._rate_bar(cpm, max_cpm)
            name = self._get_market_name(cid)

            lines.append(
                f"*{name[:32]}*\n"
                f"  `{bar}` `{cpm:.2f}¢/min`  share `{share:.1f}%`\n"
                f"  rate `${daily_rate:.2f}/d`  est. hoy `${est_today:.4f}`  ({samples} samples)\n"
            )

        # Mercados bloqueados
        blacklist = self._get_blacklist()
        if blacklist:
            active_bl = getattr(blacklist, "get_active", lambda: {})()
            if active_bl:
                lines.append(f"*Bloqueados ({len(active_bl)}):*")
                now = time.time()
                for mid, exp in active_bl.items():
                    h = (exp - now) / 3600
                    lines.append(f"  · `{mid[:14]}...`  expira `{h:.1f}h`")

        await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

    async def _cmd_orders(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Ordenes abiertas con mercado, precio, tamaño y edad."""
        if not await self._guard(update):
            return
        if not self._controller:
            await update.message.reply_text("Bot controller no conectado.")
            return

        client = getattr(self._controller, "_market_analyzer", None)
        if client:
            client = getattr(client, "_client", None)
        if client is None:
            client = getattr(self._controller, "_client", None)

        if client is None:
            await update.message.reply_text("Cliente no disponible.")
            return

        try:
            loop = asyncio.get_event_loop()
            positions = await loop.run_in_executor(None, client.get_positions)
        except Exception:
            logger.exception("Error obteniendo posiciones")
            await update.message.reply_text("Error obteniendo ordenes abiertas.")
            return

        if not positions:
            await update.message.reply_text("Sin ordenes abiertas.")
            return

        now_ts = time.time()
        lines = [self._section_header(f"Ordenes abiertas ({len(positions)})")]

        for pos in positions[:20]:
            cid = pos.get("asset_id") or pos.get("condition_id") or "?"
            side = pos.get("side", "?").upper()
            price = pos.get("price") or pos.get("average_price") or 0.0
            size = pos.get("size") or pos.get("remaining") or 0.0
            created = pos.get("created_at") or pos.get("timestamp")
            age_str = "?"
            if created:
                try:
                    if isinstance(created, (int, float)):
                        age_sec = now_ts - created
                    else:
                        ts = datetime.fromisoformat(str(created).replace("Z", "+00:00"))
                        age_sec = (datetime.now(timezone.utc) - ts).total_seconds()
                    age_min = age_sec / 60
                    age_str = f"{age_min:.0f}m" if age_min < 60 else f"{age_min/60:.1f}h"
                except (ValueError, TypeError):
                    pass
            name = self._get_market_name(cid)
            lines.append(
                f"· *{name[:28]}*\n"
                f"  {side}  `{size:.1f}` @ `{price:.4f}`  edad `{age_str}`"
            )

        if len(positions) > 20:
            lines.append(f"_...y {len(positions) - 20} mas_")

        await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

    async def _cmd_pnl(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Rewards acumulados hoy + estadisticas de trades."""
        if not await self._guard(update):
            return

        now = datetime.now(timezone.utc)
        today_cutoff = now.replace(hour=0, minute=0, second=0, microsecond=0)

        total_rewards, per_market, is_real = self._get_today_rewards()
        stats_today = self._compute_pnl_since(today_cutoff)
        stats_week = self._compute_pnl_since(now - timedelta(days=7))

        real_label = "(real)" if is_real else "(estimado)"
        lines = [
            self._section_header("PnL / Rewards"),
            "",
            f"*Rewards hoy {real_label}*",
            f"· Total: `${total_rewards:.4f}`",
        ]

        if per_market:
            ranked = sorted(per_market.items(), key=lambda kv: kv[1], reverse=True)
            for cid, usd in ranked[:5]:
                name = self._get_market_name(cid)
                lines.append(f"  · {name[:28]}  `${usd:.4f}`")

        lines.extend([
            "",
            "*Trades — hoy*",
            f"· Cantidad: `{stats_today['count']}`  errores: `{stats_today['errors']}`",
            f"· Fees pagados: `${stats_today['fees']:.4f}`",
            f"· Rewards en trades.jsonl: `${stats_today['rewards']:.4f}`",
            "",
            "*Trades — 7 dias*",
            f"· Cantidad: `{stats_week['count']}`  errores: `{stats_week['errors']}`",
            f"· Fees pagados: `${stats_week['fees']:.4f}`",
            f"· Rewards en trades.jsonl: `${stats_week['rewards']:.4f}`",
        ])

        if self._controller:
            cb_pnl = self._controller.get_status().get("daily_pnl", 0.0)
            lines.append(f"\n· PnL intradiario (CB): `${cb_pnl:+.4f}`")

        await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

    async def _cmd_health(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Estado del sistema: WS, API, uptime, ciclos, errores recientes."""
        if not await self._guard(update):
            return
        if not self._controller:
            await update.message.reply_text("Bot controller no conectado.")
            return

        status = self._controller.get_status()

        feed = getattr(self._controller, "_feed", None)
        ws_connected = feed.is_connected() if feed else False

        client = getattr(self._controller, "_client", None)
        api_ok = False
        if client:
            try:
                client.test_connection()
                api_ok = True
            except Exception:
                api_ok = False

        last_trade_min = self._get_last_trade_time()
        last_trade_str = f"{last_trade_min:.0f}m" if last_trade_min is not None else "n/a"

        start_ts = status.get("start_time", 0.0)
        if start_ts > 0:
            up_sec = time.time() - start_ts
            up_d = int(up_sec // 86400)
            up_h = int((up_sec % 86400) // 3600)
            up_m = int((up_sec % 3600) // 60)
            uptime_str = f"{up_d}d {up_h}h {up_m}m" if up_d > 0 else f"{up_h}h {up_m}m"
        else:
            uptime_str = "n/a"

        consecutive = status.get("consecutive_errors", 0)
        err_icon = "●" if consecutive >= 5 else ("○" if consecutive >= 1 else "·")

        recent_errors = self._get_recent_log_errors(n=3)

        lines = [
            self._section_header("Health"),
            f"· WS:      {'conectado' if ws_connected else 'DESCONECTADO'}",
            f"· API:     {'ok' if api_ok else 'ERROR'}",
            f"· Uptime:  `{uptime_str}`",
            f"· Ultimo trade: `{last_trade_str}` atras",
            f"· Errores consec: {err_icon} `{consecutive}`",
            f"· Mercados activos: `{len(getattr(self._controller, '_active_markets', []))}`",
            f"· Loop interval: `{getattr(self._controller, '_loop_interval', '?')}s`",
        ]

        if recent_errors:
            lines.append("\n*Ultimos errores:*")
            for err in recent_errors:
                lines.append(f"  `{err[:70]}`")

        await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

    async def _cmd_logs(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Ultimas N lineas del log."""
        if not await self._guard(update):
            return

        args = context.args or []
        n = int(args[0]) if args else 20
        n = max(1, min(n, 100))
        lines_raw = self._get_recent_log_lines(n)
        text = (
            self._section_header(f"Log (ultimas {n} lineas)")
            + "\n\n"
            + "\n".join(f"`{ln[:90]}`" for ln in lines_raw)
        )
        await update.message.reply_text(text, parse_mode="Markdown")

    async def _cmd_pause(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._guard(update):
            return
        if not self._controller:
            await update.message.reply_text("Bot controller no conectado.")
            return

        self._controller.pause()
        uid = update.effective_user.id if update.effective_user else "?"
        await update.message.reply_text("*Trading PAUSADO*\nUsa /resume para reanudar.", parse_mode="Markdown")
        send_alert(f"Bot PAUSADO por `{uid}` via Telegram")
        logger.info("Bot paused via Telegram")

    async def _cmd_resume(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._guard(update):
            return
        if not self._controller:
            await update.message.reply_text("Bot controller no conectado.")
            return

        self._controller.resume()
        uid = update.effective_user.id if update.effective_user else "?"
        await update.message.reply_text("*Trading REANUDADO*", parse_mode="Markdown")
        send_alert(f"Bot REANUDADO por `{uid}` via Telegram")
        logger.info("Bot resumed via Telegram")

    async def _cmd_kill(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._guard(update):
            return
        if not self._controller:
            await update.message.reply_text("Bot controller no conectado.")
            return

        if update.effective_user:
            self._pending_kill_user_id = update.effective_user.id
            self._pending_kill_time = time.time()

        await update.message.reply_text(
            "Confirma con /confirm\\_kill para detener el bot.",
            parse_mode="Markdown",
        )

    async def _cmd_confirm_kill(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._guard(update):
            return

        if self._pending_kill_user_id is None:
            await update.message.reply_text("No hay kill pendiente. Usa /kill primero.")
            return
        if update.effective_user and update.effective_user.id != self._pending_kill_user_id:
            await update.message.reply_text("No sos el usuario que inicio el kill.")
            return
        if time.time() - self._pending_kill_time > 60:
            self._pending_kill_user_id = None
            await update.message.reply_text("Expiro el tiempo. Usa /kill de nuevo.")
            return

        await update.message.reply_text("Deteniendo NachoMarket...")
        self._controller.kill()
        self._pending_kill_user_id = None
        logger.critical("Bot killed via Telegram")

    async def _cmd_review(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Fuerza un self-review inmediato con Claude Haiku."""
        if not await self._guard(update):
            return

        await update.message.reply_text("Ejecutando self-review... (~10s)")

        reviewer = getattr(self._controller, "_reviewer", None) if self._controller else None
        if not reviewer:
            await update.message.reply_text("Reviewer no disponible.")
            return

        loop = asyncio.get_event_loop()
        state = self._controller.get_status() if self._controller else None
        try:
            result = await loop.run_in_executor(None, reviewer.run_review, state)
            status_r = result.get("status", "ok")
            if status_r == "no_trades":
                await update.message.reply_text("Sin trades en las ultimas 8h para revisar.")
            elif status_r == "error":
                await update.message.reply_text("Error ejecutando el review.")
            else:
                analysis = result.get("analysis", {})
                if isinstance(analysis, dict):
                    risk = analysis.get("risk_level", "?")
                    summary = analysis.get("summary", "Sin resumen")
                    cost = result.get("estimated_cost_usd", 0)
                    await update.message.reply_text(
                        f"*Review completado*\n"
                        f"· Risk: `{risk}`\n"
                        f"· Costo: `${cost:.5f}`\n"
                        f"· _{summary}_",
                        parse_mode="Markdown",
                    )
                else:
                    await update.message.reply_text(
                        f"Review completado: `{result.get('trade_count', 0)}` trades."
                    )
        except Exception:
            logger.exception("Error in /review command")
            await update.message.reply_text("Error inesperado durante el review.")

    async def _cmd_block(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._guard(update):
            return

        args = context.args or []
        if len(args) < 2:
            await update.message.reply_text(
                "Uso: `/block <condition\\_id> <horas>`",
                parse_mode="Markdown",
            )
            return

        market_id = args[0]
        try:
            hours = float(args[1])
        except ValueError:
            await update.message.reply_text("Horas debe ser un numero.")
            return

        if self._controller and hasattr(self._controller, "_market_analyzer"):
            self._controller._market_analyzer.market_filter.block_market_until(market_id, hours)
            await update.message.reply_text(
                f"Mercado bloqueado: `{market_id[:16]}...` por `{hours}h`",
                parse_mode="Markdown",
            )
        else:
            await update.message.reply_text("Market analyzer no disponible.")

    async def _cmd_unblock(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._guard(update):
            return

        args = context.args or []
        if not args:
            await update.message.reply_text(
                "Uso: `/unblock <condition\\_id>`",
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
                await update.message.reply_text(f"Mercado desbloqueado: `{market_id[:16]}...`")
                return
        await update.message.reply_text("Mercado no encontrado en blacklist.")

    async def _cmd_blacklist(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._guard(update):
            return

        blacklist = self._get_blacklist()
        if blacklist is None:
            await update.message.reply_text("Blacklist no disponible.")
            return

        active = blacklist.get_active()
        if not active:
            await update.message.reply_text("Sin mercados en blacklist activa.")
            return

        now = time.time()
        lines = [self._section_header(f"Blacklist ({len(active)} mercados)")]
        for mid, expire in sorted(active.items(), key=lambda x: x[1]):
            h = (expire - now) / 3600
            lines.append(f"· `{mid[:16]}...`  expira `{h:.1f}h`")

        await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

    async def _cmd_force_reconcile(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._guard(update):
            return
        if not self._controller:
            await update.message.reply_text("Bot controller no conectado.")
            return

        await update.message.reply_text("Forzando reconciliacion...")
        if not hasattr(self._controller, "_client"):
            await update.message.reply_text("Cliente no disponible.")
            return

        try:
            result = self._controller._client.reconcile_state()
        except Exception:
            logger.exception("Error en reconciliacion")
            await update.message.reply_text("Error forzando reconciliacion.")
            return

        desync = result.get("desync", False)
        icon = "DESYNC" if desync else "OK"
        await update.message.reply_text(
            f"*Reconciliacion {icon}*\n"
            f"· Balance: `${result.get('balance_onchain', 0):.2f}`\n"
            f"· Ordenes: `{result.get('open_orders_onchain', 0)}`",
            parse_mode="Markdown",
        )

    # ------------------------------------------------------------------
    # Loops de fondo
    # ------------------------------------------------------------------

    async def _heartbeat_loop(self) -> None:
        """Heartbeat cada 30 minutos con balance y rewards del dia."""
        while self._stop_event and not self._stop_event.is_set():
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=1800)
            except asyncio.TimeoutError:
                if self._controller:
                    try:
                        status = self._controller.get_status()
                        total_rewards, _, _ = self._get_today_rewards()
                        delta = datetime.now(timezone.utc) - self._start_time
                        up_h = int(delta.total_seconds() // 3600)
                        up_m = int((delta.total_seconds() % 3600) // 60)
                        up_d = int(delta.total_seconds() // 86400)
                        uptime = f"{up_d}d {up_h}h {up_m}m" if up_d > 0 else f"{up_h}h {up_m}m"
                        send_alert(
                            f"Heartbeat\n"
                            f"· Uptime: `{uptime}`\n"
                            f"· Balance: `${status.get('balance_usdc', 0):.2f}`\n"
                            f"· Rewards hoy: `${total_rewards:.4f}`\n"
                            f"· Estado: `{status.get('state', '?')}`"
                        )
                    except Exception:
                        logger.exception("Error en heartbeat loop")

    async def _daily_summary_loop(self) -> None:
        """Resumen diario automatico a las 00:00 UTC."""
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
        total_rewards, _, _ = self._get_today_rewards()

        msg = (
            f"*Resumen Diario*\n"
            f"─────────────────────\n"
            f"· Rewards LP: `${total_rewards:.4f}`\n"
            f"· Trades: `{int(stats['count'])}`  errores: `{int(stats['errors'])}`\n"
            f"· Fees pagados: `${stats['fees']:.4f}`\n"
            f"· Rewards en log: `${stats['rewards']:.4f}`"
        )
        send_alert(msg)

    # ------------------------------------------------------------------
    # Helpers para calculo de PnL desde trades.jsonl
    # ------------------------------------------------------------------

    def _compute_pnl_since(self, cutoff: datetime) -> dict[str, float]:
        """Estadisticas de trades desde cutoff. Cache de 60s."""
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
        if not TRADES_FILE.exists():
            return 0
        cutoff = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
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
        """Retorna minutos desde el ultimo trade exitoso."""
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
    # API de compatibilidad con main.py
    # ------------------------------------------------------------------

    async def start(self) -> None:
        await self._run_async()

    async def stop_async(self) -> None:
        if self._stop_event:
            self._stop_event.set()
