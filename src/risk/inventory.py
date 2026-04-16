"""Gestion de inventario de shares por mercado.

Trackea posiciones YES/NO separadamente por market_id.
Provee skew, merge detection y quote adjustment para market making.
"""

import json
import logging
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any

logger = logging.getLogger("nachomarket.inventory")

STATE_FILE = Path("data/state.json")

_SKEW_THRESHOLD = 0.3      # Umbral de skew para ajustar quotes
_QUOTE_ADJ = 0.005         # 0.5 cents de ajuste por nivel de skew


@dataclass
class MarketInventory:
    """Inventario YES/NO de un mercado especifico.

    Los valores representan USDC equivalente en shares de cada lado.
    Positivo = long shares, negativo = short (raro pero posible).
    """
    yes: float = 0.0
    no: float = 0.0

    def total(self) -> float:
        """USDC total invertido en este mercado."""
        return abs(self.yes) + abs(self.no)

    def skew(self) -> float:
        """Skew normalizado de inventario: (yes - no) / (yes + no).

        Retorna 0.0 si no hay posicion.
        Rango: -1.0 (todo NO) a +1.0 (todo YES).
        """
        total = abs(self.yes) + abs(self.no)
        if total == 0:
            return 0.0
        return (self.yes - self.no) / total


class InventoryManager:
    """Gestion de inventario de posiciones por mercado.

    Estructura interna: {market_id: MarketInventory(yes, no)}

    Soporta:
    - Tracking YES/NO por mercado
    - Calculo de skew para informar a market maker
    - Deteccion de cuando mergear YES+NO → USDC
    - Ajuste de quotes segun sesgo de inventario
    """

    def __init__(
        self,
        config: dict[str, Any],
        state_file: Path | None = None,
    ) -> None:
        inv = config.get("inventory_management", {})
        self._max_per_market = inv.get("max_inventory_per_market_usdc", 20.0)
        self._merge_threshold = inv.get("merge_threshold_usdc", 20.0)
        self._state_file = state_file or STATE_FILE
        self._markets: dict[str, MarketInventory] = {}
        self._load_state()

    # ------------------------------------------------------------------
    # Actualizar inventario
    # ------------------------------------------------------------------

    def add_trade(
        self,
        market_id: str,
        token_type: str,
        side: str,
        size: float,
    ) -> None:
        """Registra un trade y actualiza el inventario del mercado.

        Args:
            market_id: condition_id del mercado.
            token_type: 'yes' o 'no' (YES = clobTokenIds[0], NO = clobTokenIds[1]).
            side: 'BUY' o 'SELL'.
            size: Tamano en USDC.
        """
        if market_id not in self._markets:
            self._markets[market_id] = MarketInventory()

        inv = self._markets[market_id]
        delta = size if side == "BUY" else -size

        if token_type == "yes":
            inv.yes = round(inv.yes + delta, 4)
        elif token_type == "no":
            inv.no = round(inv.no + delta, 4)
        else:
            logger.warning(f"Unknown token_type '{token_type}' for market {market_id[:8]}...")
            return

        self._save_state()
        logger.info(
            f"Inventory updated: {side} {size:.2f} {token_type.upper()} "
            f"@ {market_id[:8]}... → yes={inv.yes:.2f}, no={inv.no:.2f}"
        )

    def clear_market(self, market_id: str) -> None:
        """Limpia el inventario de un mercado (tras merge o expiracion)."""
        self._markets.pop(market_id, None)
        self._save_state()
        logger.info(f"Cleared inventory for market {market_id[:8]}...")

    # ------------------------------------------------------------------
    # Consultas
    # ------------------------------------------------------------------

    def get_market_inventory(self, market_id: str) -> MarketInventory:
        """Retorna el inventario actual de un mercado."""
        return self._markets.get(market_id, MarketInventory())

    def get_skew(self, market_id: str) -> float:
        """Retorna el skew normalizado: (yes - no) / (yes + no).

        Valores:
          +1.0 → solo YES en inventario
          -1.0 → solo NO en inventario
           0.0 → balanceado o sin posicion
        """
        return self.get_market_inventory(market_id).skew()

    def should_merge(self, market_id: str) -> bool:
        """True si ambos lados superan el umbral de merge.

        Logica: si min(yes, no) > merge_threshold, tenemos suficiente de
        ambos lados para mergear YES+NO → USDC (cada par vale $1).

        Args:
            market_id: condition_id del mercado.

        Returns:
            True si vale la pena ejecutar un merge.
        """
        inv = self.get_market_inventory(market_id)
        return min(inv.yes, inv.no) > self._merge_threshold

    def adjust_quotes(
        self,
        base_bid: float,
        base_ask: float,
        skew: float,
    ) -> tuple[float, float]:
        """Ajusta bid/ask segun el skew de inventario.

        Estrategia de ajuste (inventory-aware market making):
          skew > 0.3  (mucho YES, querer vender):
            - Bajar ambas cotizaciones: ask mas competitivo para vender,
              bid mas bajo para evitar acumular mas YES.
          skew < -0.3 (mucho NO, querer comprar YES para merge):
            - Subir ambas cotizaciones: bid mas competitivo para comprar YES,
              ask mas alto para no perder shares.

        Args:
            base_bid: Precio bid base del market maker.
            base_ask: Precio ask base del market maker.
            skew: Valor entre -1 y +1 de get_skew().

        Returns:
            (adjusted_bid, adjusted_ask) redondeados a 4 decimales.
        """
        if skew > _SKEW_THRESHOLD:
            # Mucho YES: widen ask (sube), tighten bid (baja hacia mid)
            adj = _QUOTE_ADJ * (1 + (skew - _SKEW_THRESHOLD))
            new_ask = round(base_ask + adj, 4)
            new_bid = round(base_bid - adj, 4)
            logger.debug(f"Skew={skew:.3f} (long YES): ask {base_ask:.4f}→{new_ask:.4f}, bid {base_bid:.4f}→{new_bid:.4f}")
            return (new_bid, new_ask)

        elif skew < -_SKEW_THRESHOLD:
            # Mucho NO: widen bid (sube hacia mid), tighten ask (baja)
            adj = _QUOTE_ADJ * (1 + (abs(skew) - _SKEW_THRESHOLD))
            new_bid = round(base_bid + adj, 4)
            new_ask = round(base_ask - adj, 4)
            logger.debug(f"Skew={skew:.3f} (long NO): bid {base_bid:.4f}→{new_bid:.4f}, ask {base_ask:.4f}→{new_ask:.4f}")
            return (new_bid, new_ask)

        return (base_bid, base_ask)

    def get_total_exposure(self) -> float:
        """Retorna la exposicion total en USDC (suma de todos los mercados)."""
        return sum(inv.total() for inv in self._markets.values())

    def get_positions(self) -> dict[str, dict[str, float]]:
        """Retorna todas las posiciones como dict serializable."""
        return {
            market_id: {"yes": inv.yes, "no": inv.no}
            for market_id, inv in self._markets.items()
        }

    def can_add_position(self, market_id: str, size: float) -> bool:
        """Verifica si se puede agregar posicion sin exceder el limite por mercado."""
        current = self.get_market_inventory(market_id).total()
        return (current + size) <= self._max_per_market

    # ------------------------------------------------------------------
    # Persistencia
    # ------------------------------------------------------------------

    def _load_state(self) -> None:
        """Carga estado desde disco."""
        if not self._state_file.exists():
            return
        try:
            data = json.loads(self._state_file.read_text(encoding="utf-8"))
            for market_id, inv_data in data.get("markets", {}).items():
                self._markets[market_id] = MarketInventory(
                    yes=inv_data.get("yes", 0.0),
                    no=inv_data.get("no", 0.0),
                )
        except (json.JSONDecodeError, KeyError, TypeError):
            logger.warning("Could not load inventory state, starting fresh")

    def _save_state(self) -> None:
        """Guarda estado a disco."""
        self._state_file.parent.mkdir(parents=True, exist_ok=True)
        data = {"markets": self.get_positions()}
        self._state_file.write_text(json.dumps(data, indent=2), encoding="utf-8")
