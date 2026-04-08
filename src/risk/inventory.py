import json
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger("nachomarket.inventory")

STATE_FILE = Path("data/state.json")


class InventoryManager:
    """Gestion de inventario de posiciones + merging."""

    def __init__(self, config: dict[str, Any]) -> None:
        inv_config = config.get("inventory_management", {})
        self._max_inventory = inv_config.get("max_inventory_per_market_usdc", 20.0)
        self._rebalance_threshold = inv_config.get("rebalance_threshold_pct", 30.0)
        self._merge_threshold = inv_config.get("merge_threshold", 5)
        self._positions: dict[str, list[dict[str, Any]]] = {}
        self._load_state()

    def add_position(self, token_id: str, side: str, price: float, size: float) -> None:
        """Registra una nueva posicion."""
        if token_id not in self._positions:
            self._positions[token_id] = []

        self._positions[token_id].append({
            "side": side,
            "price": price,
            "size": size,
        })

        # Auto-merge si hay muchas posiciones
        if len(self._positions[token_id]) >= self._merge_threshold:
            self._merge_positions(token_id)

        self._save_state()
        logger.info(f"Position added: {side} {size} @ {price} for {token_id[:8]}...")

    def get_inventory(self, token_id: str) -> float:
        """Retorna el inventario neto en USDC para un token."""
        positions = self._positions.get(token_id, [])
        net = 0.0
        for pos in positions:
            value = pos["price"] * pos["size"]
            if pos["side"] == "BUY":
                net += value
            else:
                net -= value
        return net

    def get_total_exposure(self) -> float:
        """Retorna la exposicion total en USDC."""
        return sum(abs(self.get_inventory(tid)) for tid in self._positions)

    def can_add_position(self, token_id: str, size_usdc: float) -> bool:
        """Verifica si se puede agregar una posicion sin exceder limites."""
        current = abs(self.get_inventory(token_id))
        return (current + size_usdc) <= self._max_inventory

    def needs_rebalance(self, token_id: str) -> bool:
        """Verifica si el inventario necesita rebalanceo."""
        inventory = abs(self.get_inventory(token_id))
        if inventory == 0:
            return False
        deviation_pct = (inventory / self._max_inventory) * 100
        return deviation_pct > self._rebalance_threshold

    def clear_position(self, token_id: str) -> None:
        """Limpia todas las posiciones de un token."""
        self._positions.pop(token_id, None)
        self._save_state()
        logger.info(f"Cleared positions for {token_id[:8]}...")

    def _merge_positions(self, token_id: str) -> None:
        """Merge multiples posiciones en una sola (promedio ponderado)."""
        positions = self._positions.get(token_id, [])
        if len(positions) < 2:
            return

        buys = [p for p in positions if p["side"] == "BUY"]
        sells = [p for p in positions if p["side"] == "SELL"]

        merged: list[dict[str, Any]] = []
        for side_positions, side in [(buys, "BUY"), (sells, "SELL")]:
            if not side_positions:
                continue
            total_size = sum(p["size"] for p in side_positions)
            avg_price = sum(p["price"] * p["size"] for p in side_positions) / total_size
            merged.append({"side": side, "price": round(avg_price, 4), "size": total_size})

        self._positions[token_id] = merged
        logger.info(f"Merged {len(positions)} positions into {len(merged)} for {token_id[:8]}...")

    def _load_state(self) -> None:
        """Carga estado desde disco."""
        if STATE_FILE.exists():
            try:
                data = json.loads(STATE_FILE.read_text(encoding="utf-8"))
                self._positions = data.get("positions", {})
            except (json.JSONDecodeError, KeyError):
                logger.warning("Could not load state file, starting fresh")

    def _save_state(self) -> None:
        """Guarda estado a disco."""
        STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        data = {"positions": self._positions}
        STATE_FILE.write_text(json.dumps(data, indent=2), encoding="utf-8")
