import json
import logging
import os
from typing import Any

from dotenv import load_dotenv
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

load_dotenv()
logger = logging.getLogger("nachomarket.telegram")


class TelegramBot:
    """Bot de Telegram para alertas y control del bot."""

    def __init__(self, bot_controller: Any = None) -> None:
        self._token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
        self._chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
        self._controller = bot_controller
        self._app: Application | None = None

    async def start(self) -> None:
        """Inicia el bot de Telegram."""
        if not self._token:
            logger.warning("Telegram bot token not configured, skipping")
            return

        self._app = Application.builder().token(self._token).build()

        # Registrar comandos
        self._app.add_handler(CommandHandler("status", self._cmd_status))
        self._app.add_handler(CommandHandler("pause", self._cmd_pause))
        self._app.add_handler(CommandHandler("resume", self._cmd_resume))
        self._app.add_handler(CommandHandler("kill", self._cmd_kill))
        self._app.add_handler(CommandHandler("pnl", self._cmd_pnl))
        self._app.add_handler(CommandHandler("positions", self._cmd_positions))

        await self._app.initialize()
        await self._app.start()
        await self._app.updater.start_polling()
        logger.info("Telegram bot started")

    async def stop(self) -> None:
        """Detiene el bot de Telegram."""
        if self._app:
            await self._app.updater.stop()
            await self._app.stop()
            await self._app.shutdown()
            logger.info("Telegram bot stopped")

    async def send_alert(self, message: str) -> None:
        """Envia una alerta al chat configurado."""
        if not self._app or not self._chat_id:
            return
        try:
            await self._app.bot.send_message(
                chat_id=self._chat_id,
                text=f"🤖 NachoMarket\n{message}",
                parse_mode="Markdown",
            )
        except Exception:
            logger.exception("Failed to send Telegram alert")

    async def send_trade_alert(self, trade: dict[str, Any]) -> None:
        """Envia alerta de trade ejecutado."""
        side = trade.get("side", "?")
        size = trade.get("size", 0)
        price = trade.get("price", 0)
        token = trade.get("token_id", "?")[:8]
        msg = f"*Trade* {side} ${size} @ {price}\nToken: `{token}...`"
        await self.send_alert(msg)

    async def _cmd_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handler para /status."""
        if not self._is_authorized(update):
            return
        if self._controller:
            status = self._controller.get_status()
            await update.message.reply_text(
                f"Estado: {status.get('state', 'unknown')}\n"
                f"PnL diario: ${status.get('daily_pnl', 0):.2f}\n"
                f"Ordenes abiertas: {status.get('open_orders', 0)}\n"
                f"Circuit breaker: {'ACTIVO' if status.get('circuit_breaker') else 'OK'}"
            )
        else:
            await update.message.reply_text("Bot controller not connected")

    async def _cmd_pause(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handler para /pause — pausa instantanea."""
        if not self._is_authorized(update):
            return
        if self._controller:
            self._controller.pause()
            await update.message.reply_text("Bot PAUSADO. Usa /resume para reanudar.")
            logger.info("Bot paused via Telegram")
        else:
            await update.message.reply_text("Bot controller not connected")

    async def _cmd_resume(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handler para /resume."""
        if not self._is_authorized(update):
            return
        if self._controller:
            self._controller.resume()
            await update.message.reply_text("Bot REANUDADO.")
            logger.info("Bot resumed via Telegram")
        else:
            await update.message.reply_text("Bot controller not connected")

    async def _cmd_kill(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handler para /kill — cancela todo y para."""
        if not self._is_authorized(update):
            return
        if self._controller:
            self._controller.kill()
            await update.message.reply_text("Bot DETENIDO. Todas las ordenes canceladas.")
            logger.critical("Bot killed via Telegram")
        else:
            await update.message.reply_text("Bot controller not connected")

    async def _cmd_pnl(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handler para /pnl."""
        if not self._is_authorized(update):
            return
        if self._controller:
            status = self._controller.get_status()
            await update.message.reply_text(f"PnL diario: ${status.get('daily_pnl', 0):.2f}")
        else:
            await update.message.reply_text("Bot controller not connected")

    async def _cmd_positions(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handler para /positions."""
        if not self._is_authorized(update):
            return
        if self._controller:
            positions = self._controller.get_positions()
            if positions:
                msg = "\n".join(
                    f"• {tid[:8]}: ${val:.2f}"
                    for tid, val in positions.items()
                )
                await update.message.reply_text(f"Posiciones:\n{msg}")
            else:
                await update.message.reply_text("Sin posiciones abiertas")
        else:
            await update.message.reply_text("Bot controller not connected")

    def _is_authorized(self, update: Update) -> bool:
        """Verifica que el mensaje viene del chat autorizado."""
        if not self._chat_id:
            return True
        return str(update.effective_chat.id) == self._chat_id
