import argparse
import asyncio
import logging
import signal
import sys
import time
from pathlib import Path
from typing import Any

import schedule
import yaml
from dotenv import load_dotenv

from src.polymarket.client import PolymarketClient
from src.polymarket.markets import MarketAnalyzer
from src.risk.circuit_breaker import CircuitBreaker
from src.risk.inventory import InventoryManager
from src.risk.position_sizer import PositionSizer
from src.review.self_review import SelfReviewer
from src.strategy.market_maker import MarketMakerStrategy
from src.strategy.multi_arb import MultiArbStrategy
from src.strategy.directional import DirectionalStrategy
from src.telegram.bot import TelegramBot
from src.utils.logger import setup_logger

load_dotenv()


class NachoMarketBot:
    """Entry point principal del bot de trading."""

    def __init__(self, paper_mode: bool = True) -> None:
        # Cargar configuracion
        self._settings = self._load_yaml("config/settings.yaml")
        self._markets_config = self._load_yaml("config/markets.yaml")
        self._risk_config = self._load_yaml("config/risk.yaml")

        # Override paper mode si se pasa por argumento
        if paper_mode:
            self._settings["mode"] = "paper"
        is_paper = self._settings.get("mode", "paper") == "paper"

        # Setup logger
        self._logger = setup_logger(
            "nachomarket",
            log_file=self._settings.get("log_file", "data/nachomarket.log"),
            level=self._settings.get("log_level", "INFO"),
        )

        self._logger.info(f"NachoMarket starting in {'PAPER' if is_paper else 'LIVE'} mode")

        # Inicializar componentes
        self._client = PolymarketClient(paper_mode=is_paper)
        self._circuit_breaker = CircuitBreaker(self._risk_config)
        self._position_sizer = PositionSizer(self._risk_config)
        self._inventory = InventoryManager(self._risk_config)
        self._market_analyzer = MarketAnalyzer(self._client, self._markets_config)
        self._reviewer = SelfReviewer(
            model=self._settings.get("review_model", "claude-haiku-4-5-20251001")
        )
        self._telegram = TelegramBot(bot_controller=self)

        # Estrategias
        self._strategies = [
            MarketMakerStrategy(self._client, self._circuit_breaker, self._risk_config),
            MultiArbStrategy(self._client, self._circuit_breaker, self._risk_config),
            DirectionalStrategy(self._client, self._circuit_breaker, self._risk_config),
        ]

        # Estado
        self._state = "running"  # running | paused | stopped
        self._loop_interval = self._settings.get("main_loop_interval_sec", 10)

    def run(self) -> None:
        """Loop principal del bot."""
        # Configurar self-review periodico
        review_hours = self._settings.get("review_interval_hours", 8)
        schedule.every(review_hours).hours.do(self._run_review)

        # Resetear contadores diarios a medianoche
        schedule.every().day.at("00:00").do(self._circuit_breaker.reset_daily)

        # Signal handlers para shutdown graceful
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)

        self._logger.info("Bot loop started")

        while self._state != "stopped":
            try:
                schedule.run_pending()

                if self._state == "paused":
                    time.sleep(1)
                    continue

                self._trading_cycle()
                time.sleep(self._loop_interval)

            except KeyboardInterrupt:
                self._logger.info("Keyboard interrupt received")
                self.kill()
            except Exception:
                self._logger.exception("Error in main loop")
                self._circuit_breaker.record_error()
                time.sleep(self._loop_interval)

    def _trading_cycle(self) -> None:
        """Un ciclo de trading: escanear mercados y ejecutar estrategias."""
        if self._circuit_breaker.is_triggered():
            self._logger.warning("Circuit breaker active, skipping cycle")
            return

        markets = self._market_analyzer.scan_markets()

        for market in markets:
            for strategy in self._strategies:
                try:
                    results = strategy.execute(market)
                    for trade in results:
                        self._inventory.add_position(
                            token_id=trade.get("token_id", ""),
                            side=trade.get("side", ""),
                            price=trade.get("price", 0),
                            size=trade.get("size", 0),
                        )
                except Exception:
                    self._logger.exception(
                        f"Error executing {strategy.name} on market"
                    )

    def _run_review(self) -> None:
        """Ejecuta self-review con Claude Haiku."""
        try:
            state = self.get_status()
            review = self._reviewer.run_review(state=state)
            self._logger.info(f"Self-review completed: {review.get('status', 'ok')}")
        except Exception:
            self._logger.exception("Self-review failed")

    def _handle_shutdown(self, signum: int, frame: Any) -> None:
        """Maneja shutdown graceful."""
        self._logger.info(f"Shutdown signal received ({signum})")
        self.kill()

    # --- Metodos de control (usados por Telegram) ---

    def get_status(self) -> dict[str, Any]:
        """Retorna estado actual del bot."""
        cb_status = self._circuit_breaker.get_status()
        return {
            "state": self._state,
            "daily_pnl": cb_status["daily_pnl"],
            "open_orders": cb_status["open_orders"],
            "circuit_breaker": cb_status["triggered"],
            "total_exposure": self._inventory.get_total_exposure(),
        }

    def get_positions(self) -> dict[str, float]:
        """Retorna posiciones abiertas."""
        positions = {}
        for token_id in self._inventory._positions:
            positions[token_id] = self._inventory.get_inventory(token_id)
        return positions

    def pause(self) -> None:
        """Pausa el bot instantaneamente."""
        self._state = "paused"
        for strategy in self._strategies:
            strategy.pause()
        self._logger.info("Bot PAUSED")

    def resume(self) -> None:
        """Reanuda el bot."""
        self._state = "running"
        for strategy in self._strategies:
            strategy.resume()
        self._logger.info("Bot RESUMED")

    def kill(self) -> None:
        """Cancela todo y detiene el bot."""
        self._state = "stopped"
        try:
            self._client.cancel_all_orders()
        except Exception:
            self._logger.exception("Error cancelling orders during kill")
        for strategy in self._strategies:
            strategy.pause()
        self._logger.critical("Bot KILLED — all orders cancelled")

    @staticmethod
    def _load_yaml(path: str) -> dict[str, Any]:
        """Carga un archivo YAML."""
        file_path = Path(path)
        if not file_path.exists():
            return {}
        with open(file_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}


def main() -> None:
    parser = argparse.ArgumentParser(description="NachoMarket Trading Bot")
    parser.add_argument("--paper", action="store_true", help="Run in paper trading mode")
    args = parser.parse_args()

    paper_mode = args.paper or True  # Default to paper mode for safety
    bot = NachoMarketBot(paper_mode=paper_mode)
    bot.run()


if __name__ == "__main__":
    main()
