"""Gestion de inventario de shares por mercado.

Trackea posiciones por token_id por market_id.
Provee skew, merge detection y quote adjustment para market making.
Soporta mercados binarios (YES/NO) y multi-outcome (N tokens).
"""

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

logger = logging.getLogger("nachomarket.inventory")

STATE_FILE = Path("data/state.json")

_SKEW_THRESHOLD = 0.3      # Umbral de skew para ajustar quotes
_QUOTE_ADJ = 0.005         # 0.5 cents de ajuste por nivel de skew


@dataclass
class MarketInventory:
    """Inventario de un mercado especifico.

    Las keys de positions son token_id (o 'yes'/'no' para compatibilidad
    con estado previo). Los valores representan USDC equivalente en shares.
    Positivo = long shares, negativo = short (raro pero posible).
    """

    positions: dict[str, float] = field(default_factory=dict)

    @property
    def yes(self) -> float:
        return self.positions.get("yes", 0.0)

    @property
    def no(self) -> float:
        return self.positions.get("no", 0.0)

    def total(self) -> float:
        """USDC total invertido en este mercado."""
        return sum(abs(v) for v in self.positions.values())

    def skew(self) -> float:
        """Skew normalizado para mercados binarios: (yes - no) / (yes + no).

        Retorna 0.0 si no hay posicion o si hay mas de 2 lados.
        Rango: -1.0 (todo NO) a +1.0 (todo YES).
        """
        keys = set(self.positions.keys())
        if not keys.issubset({"yes", "no"}):
            return 0.0
        y = self.positions.get("yes", 0.0)
        n = self.positions.get("no", 0.0)
        total = abs(y) + abs(n)
        if total == 0:
            return 0.0
        return (y - n) / total


class InventoryManager:
    """Gestion de inventario de posiciones por mercado.

    Estructura interna: {market_id: MarketInventory}

    Soporta:
    - Tracking por token_id para mercados de N outcomes
    - Calculo de skew para informar a market maker (binario-only)
    - Deteccion de merge YES+NO -> USDC (binario-only)
    - Ajuste de quotes segun sesgo de inventario (binario-only)
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
        token_id: str | None = None,
    ) -> None:
        """Registra un trade y actualiza el inventario del mercado.

        Args:
            market_id: condition_id del mercado.
            token_type: 'yes' o 'no' (legacy compat).
            side: 'BUY' o 'SELL'.
            size: Tamano en USDC.
            token_id: token_id real del outcome. Si se provee, se usa como key.
        """
        if market_id not in self._markets:
            self._markets[market_id] = MarketInventory()

        inv = self._markets[market_id]
        key = token_id or token_type
        delta = size if side == "BUY" else -size

        inv.positions[key] = round(inv.positions.get(key, 0.0) + delta, 4)

        self._save_state()
        logger.info(
            f"Inventory updated: {side} {size:.2f} {key[:12]}... "
            f"@ {market_id[:8]}... -> total={inv.total():.2f}"
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
          +1.0 -> solo YES en inventario
          -1.0 -> solo NO en inventario
           0.0 -> balanceado o sin posicion / multi-outcome
        """
        return self.get_market_inventory(market_id).skew()

    def should_merge(self, market_id: str) -> bool:
        """True si ambos lados superan el umbral de merge (binario-only).

        Para mercados multi-outcome retorna False (no hay merge nativo).
        """
        inv = self.get_market_inventory(market_id)
        if len(inv.positions) != 2:
            return False
        vals = list(inv.positions.values())
        return min(abs(vals[0]), abs(vals[1])) > self._merge_threshold

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
            logger.debug(f"Skew={skew:.3f} (long YES): ask {base_ask:.4f}->{new_ask:.4f}, bid {base_bid:.4f}->{new_bid:.4f}")
            return (new_bid, new_ask)

        elif skew < -_SKEW_THRESHOLD:
            # Mucho NO: widen bid (sube hacia mid), tighten ask (baja)
            adj = _QUOTE_ADJ * (1 + (abs(skew) - _SKEW_THRESHOLD))
            new_bid = round(base_bid + adj, 4)
            new_ask = round(base_ask - adj, 4)
            logger.debug(f"Skew={skew:.3f} (long NO): bid {base_bid:.4f}->{new_bid:.4f}, ask {base_ask:.4f}->{new_ask:.4f}")
            return (new_bid, new_ask)

        return (base_bid, base_ask)

    def get_total_exposure(self) -> float:
        """Retorna la exposicion total en USDC (suma de todos los mercados)."""
        return sum(inv.total() for inv in self._markets.values())

    def get_positions(self) -> dict[str, dict[str, float]]:
        """Retorna todas las posiciones como dict serializable."""
        return {
            market_id: dict(inv.positions)
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
                positions: dict[str, float] = {}
                # Compatibilidad con formato viejo yes/no
                if "yes" in inv_data or "no" in inv_data:
                    if inv_data.get("yes", 0.0) != 0.0:
                        positions["yes"] = inv_data["yes"]
                    if inv_data.get("no", 0.0) != 0.0:
                        positions["no"] = inv_data["no"]
                else:
                    positions = {k: float(v) for k, v in inv_data.items()}
                self._markets[market_id] = MarketInventory(positions=positions)
        except (json.JSONDecodeError, KeyError, TypeError):
            logger.warning("Could not load inventory state, starting fresh")

    def _save_state(self) -> None:
        """Guarda estado a disco."""
        self._state_file.parent.mkdir(parents=True, exist_ok=True)
        data = {"markets": self.get_positions()}
        self._state_file.write_text(json.dumps(data, indent=2), encoding="utf-8")
