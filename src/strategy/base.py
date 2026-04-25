"""
Clase abstracta base para todas las estrategias de trading.

Define el contrato Signal → evaluate → execute → Trade → log_trade
que cada estrategia concreta debe implementar.

Template Method (GoF): run() orquesta el pipeline completo con hooks
sobreescribibles (should_trade, should_act, evaluate, execute).
"""

import json
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

from src.polymarket.client import PolymarketClient

if TYPE_CHECKING:
    from src.risk.blacklist import MarketBlacklist

TRADES_FILE = Path("data/trades.jsonl")


@dataclass
class Signal:
    """Senal de trading generada por evaluate().

    Representa una intencion de operar, antes de colocar la orden real.
    """

    market_id: str        # condition_id del mercado
    token_id: str         # token_id del outcome especifico
    side: str             # "BUY" | "SELL"
    price: float          # Precio limite deseado
    size: float           # Tamano en USDC
    confidence: float     # 0.0 - 1.0 (que tan segura es la senal)
    strategy_name: str    # Nombre de la estrategia que la genero
    metadata: dict[str, Any] = field(default_factory=dict)  # Datos extra opcionales


@dataclass
class Trade:
    """Resultado de una orden ejecutada.

    Se loguea a data/trades.jsonl despues de cada ejecucion.
    """

    timestamp: str            # ISO 8601 UTC
    market_id: str            # condition_id
    token_id: str             # token_id
    side: str                 # "BUY" | "SELL"
    price: float              # Precio al que se coloco
    size: float               # Tamano en USDC
    order_id: str             # ID de la orden retornado por la API
    status: str               # "submitted" | "filled_paper" | "error" | ...
    strategy_name: str        # Estrategia que origino el trade
    fee_paid: float = 0.0     # Fee pagado en USDC (0 si post_only maker)
    # Tip 17: campos de analisis de patrones (default 0/empty para no romper logs viejos)
    mid_at_entry: float = 0.0                   # Mid price del mercado al colocar la orden
    participation_share_at_entry: float = 0.0   # % de share estimado al entrar
    category: str = ""                          # Categoria del mercado
    time_to_exit_sec: float = 0.0              # Segundos hasta que se filleo el exit (round-trip)
    rewards_earned: float = 0.0                # Rewards LP acumulados en este trade


class BaseStrategy(ABC):
    """Clase abstracta base para todas las estrategias de trading.

    Flujo:
        1. evaluate(market_data) → genera Signal's (intenciones)
        2. execute(signals) → convierte Signal's en Trade's reales
        3. log_trade(trade) → persiste cada trade a trades.jsonl

    Las subclases DEBEN implementar evaluate() y execute().
    Opcionalmente pueden override should_act() para filtrado rapido.
    """

    def __init__(
        self,
        name: str,
        client: PolymarketClient,
        config: dict[str, Any],
        logger: logging.Logger | None = None,
    ) -> None:
        self.name = name
        self._client = client
        self._config = config
        self._logger = logger or logging.getLogger(f"nachomarket.strategy.{name}")
        self._active = True
        self._blacklist: "MarketBlacklist | None" = None

        # Asegurar que el directorio de trades existe
        TRADES_FILE.parent.mkdir(parents=True, exist_ok=True)

        self._logger.info(f"Estrategia '{name}' inicializada")

    # ------------------------------------------------------------------
    # Metodos abstractos — cada estrategia los implementa
    # ------------------------------------------------------------------

    @abstractmethod
    def evaluate(self, market_data: dict[str, Any]) -> list[Signal]:
        """Analiza datos de mercado y genera senales de trading.

        Args:
            market_data: Dict con datos del mercado (tokens, precios, spread, etc.)

        Returns:
            Lista de Signal con las operaciones sugeridas.
        """
        ...

    @abstractmethod
    def execute(self, signals: list[Signal]) -> list[Trade]:
        """Convierte senales en ordenes reales via el cliente.

        Cada estrategia decide como ejecutar: post_only, GTC, etc.
        DEBE llamar self.log_trade(trade) por cada trade ejecutado.

        Args:
            signals: Lista de Signal generadas por evaluate().

        Returns:
            Lista de Trade con los resultados de ejecucion.
        """
        ...

    # ------------------------------------------------------------------
    # Metodos concretos — compartidos por todas las estrategias
    # ------------------------------------------------------------------

    def set_blacklist(self, blacklist: "MarketBlacklist") -> None:
        """Inyecta la blacklist para filtrado previo al ciclo.

        Dependency Injection: evita acoplamiento directo al módulo de blacklist.
        """
        self._blacklist = blacklist

    def should_trade(self, market_data: dict[str, Any]) -> bool:
        """Gate previo — comprueba blacklist antes de evaluar señales.

        Template Method hook: sobreescribible, pero la lógica de blacklist
        se aplica siempre si hay una blacklist inyectada.

        Returns:
            False si el mercado está en blacklist activa; True en caso contrario.
        """
        if self._blacklist is None:
            return True
        market_id = market_data.get("condition_id", market_data.get("market_id", ""))
        if market_id and self._blacklist.is_blacklisted(market_id):
            self._logger.debug(
                "Mercado %s en blacklist — saltando ciclo de '%s'",
                market_id[:14], self.name,
            )
            return False
        return True

    def should_act(self, market_data: dict[str, Any]) -> bool:
        """Filtro rapido antes de evaluate(). Override en subclases.

        Returns:
            True si la estrategia debe analizar este mercado.
        """
        return True

    def run(self, market_data: dict[str, Any]) -> list[Trade]:
        """Pipeline completo: filtrar → evaluar → ejecutar → loguear.

        Este es el metodo que llama main.py en cada ciclo de trading.

        Returns:
            Lista de trades ejecutados (vacia si la estrategia esta pausada,
            no hay senales, o should_act() retorna False).
        """
        if not self._active:
            self._logger.debug(f"Estrategia '{self.name}' pausada, saltando")
            return []

        if not self.should_trade(market_data):
            return []

        if not self.should_act(market_data):
            return []

        signals = self.evaluate(market_data)
        if not signals:
            return []

        self._logger.info(f"'{self.name}': {len(signals)} senales generadas")
        trades = self.execute(signals)

        return trades

    def log_trade(self, trade: Trade) -> None:
        """Persiste un trade a data/trades.jsonl (append-only).

        SIEMPRE loguear cada decision de trading (regla INQUEBRANTABLE).
        """
        record = asdict(trade)
        try:
            with open(TRADES_FILE, "a", encoding="utf-8") as f:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
        except OSError:
            self._logger.exception("No se pudo escribir en trades.jsonl")

        self._logger.info(
            f"Trade: {trade.side} {trade.size} USDC @ {trade.price} "
            f"| {trade.strategy_name} | status={trade.status} "
            f"| order_id={trade.order_id}"
        )

    def pause(self) -> None:
        """Pausa la estrategia instantaneamente."""
        self._active = False
        self._logger.info(f"Estrategia '{self.name}' PAUSADA")

    def resume(self) -> None:
        """Reanuda la estrategia."""
        self._active = True
        self._logger.info(f"Estrategia '{self.name}' REANUDADA")

    @property
    def is_active(self) -> bool:
        """Indica si la estrategia esta activa."""
        return self._active

    # ------------------------------------------------------------------
    # Helpers para subclases
    # ------------------------------------------------------------------

    def _make_signal(
        self,
        market_id: str,
        token_id: str,
        side: str,
        price: float,
        size: float,
        confidence: float = 0.5,
    ) -> Signal:
        """Factory de Signal con strategy_name pre-rellenado."""
        return Signal(
            market_id=market_id,
            token_id=token_id,
            side=side,
            price=price,
            size=size,
            confidence=confidence,
            strategy_name=self.name,
        )

    def _make_trade(
        self,
        signal: Signal,
        order_id: str,
        status: str,
        fee_paid: float = 0.0,
    ) -> Trade:
        """Factory de Trade a partir de un Signal y resultado de ejecucion."""
        return Trade(
            timestamp=datetime.now(timezone.utc).isoformat(),
            market_id=signal.market_id,
            token_id=signal.token_id,
            side=signal.side,
            price=signal.price,
            size=signal.size,
            order_id=order_id,
            status=status,
            strategy_name=signal.strategy_name,
            fee_paid=fee_paid,
        )
