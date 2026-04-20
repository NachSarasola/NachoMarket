"""Tests para PolymarketClient.reconcile_state() (TODO 1.2)."""

import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.polymarket.client import PolymarketClient


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_paper_client() -> PolymarketClient:
    return PolymarketClient(paper_mode=True)


def make_live_client() -> PolymarketClient:
    """Crea cliente live con _client mockeado para evitar llamadas reales."""
    with patch("src.polymarket.client.PolymarketClient._build_client", return_value=MagicMock()):
        client = PolymarketClient(paper_mode=False)
    return client


def write_state(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f)


# ---------------------------------------------------------------------------
# Paper mode
# ---------------------------------------------------------------------------

class TestReconcilePaperMode:
    def test_returns_simulated_balance(self):
        client = make_paper_client()
        result = client.reconcile_state()
        assert result["balance_onchain"] == 400.0

    def test_no_desync_in_paper(self):
        client = make_paper_client()
        result = client.reconcile_state()
        assert result["desync"] is False

    def test_state_not_updated_in_paper(self):
        client = make_paper_client()
        result = client.reconcile_state()
        # Paper mode retorna inmediatamente sin escribir
        assert result["state_updated"] is False


# ---------------------------------------------------------------------------
# Live mode — sin desync
# ---------------------------------------------------------------------------

class TestReconcileLiveSync:
    def test_balance_matches_no_desync(self):
        client = make_live_client()
        client.get_balance = MagicMock(return_value=395.50)
        client.get_positions = MagicMock(return_value=[{"id": "order_1"}, {"id": "order_2"}])

        with tempfile.TemporaryDirectory() as tmp:
            state_path = str(Path(tmp) / "state.json")
            write_state(Path(state_path), {"balance_usdc": 395.50, "open_orders_count": 2})

            result = client.reconcile_state(state_path=state_path)

        assert result["balance_onchain"] == 395.50
        assert result["balance_delta"] == pytest.approx(0.0, abs=0.001)
        assert result["desync"] is False
        assert result["open_orders_onchain"] == 2

    def test_state_updated_with_ground_truth(self):
        client = make_live_client()
        client.get_balance = MagicMock(return_value=300.0)
        client.get_positions = MagicMock(return_value=[])

        with tempfile.TemporaryDirectory() as tmp:
            state_path = Path(tmp) / "state.json"
            write_state(state_path, {"balance_usdc": 300.0})

            result = client.reconcile_state(state_path=str(state_path))

            assert result["state_updated"] is True
            # Verificar que se escribió el ground truth
            with open(state_path) as f:
                saved = json.load(f)
            assert saved["balance_usdc"] == 300.0
            assert saved["open_orders_count"] == 0
            assert "last_reconcile" in saved

    def test_missing_state_file_no_crash(self):
        """Si state.json no existe, no falla — crea uno nuevo."""
        client = make_live_client()
        client.get_balance = MagicMock(return_value=250.0)
        client.get_positions = MagicMock(return_value=[])

        with tempfile.TemporaryDirectory() as tmp:
            state_path = str(Path(tmp) / "nonexistent" / "state.json")
            result = client.reconcile_state(state_path=state_path)

        assert result["balance_onchain"] == 250.0
        assert result["state_updated"] is True
        # Sin balance local previo: no hay desync ni delta
        assert result["balance_local"] is None
        assert result["balance_delta"] == 0.0


# ---------------------------------------------------------------------------
# Live mode — desync detectado
# ---------------------------------------------------------------------------

class TestReconcileDesync:
    def test_desync_when_delta_exceeds_threshold(self):
        client = make_live_client()
        client.get_balance = MagicMock(return_value=200.0)
        client.get_positions = MagicMock(return_value=[])

        with tempfile.TemporaryDirectory() as tmp:
            state_path = str(Path(tmp) / "state.json")
            # Local dice 210 pero on-chain es 200 → delta = 10 > 1
            write_state(Path(state_path), {"balance_usdc": 210.0})

            result = client.reconcile_state(state_path=state_path, alert_delta_threshold=1.0)

        assert result["desync"] is True
        assert result["balance_delta"] == pytest.approx(10.0)

    def test_no_desync_when_delta_below_threshold(self):
        client = make_live_client()
        client.get_balance = MagicMock(return_value=200.50)
        client.get_positions = MagicMock(return_value=[])

        with tempfile.TemporaryDirectory() as tmp:
            state_path = str(Path(tmp) / "state.json")
            write_state(Path(state_path), {"balance_usdc": 200.0})

            # Delta es 0.5, threshold es 1.0 → sin desync
            result = client.reconcile_state(state_path=state_path, alert_delta_threshold=1.0)

        assert result["desync"] is False
        assert result["balance_delta"] == pytest.approx(0.5)

    def test_desync_still_updates_state(self):
        """Incluso con desync, el state.json se actualiza con ground truth."""
        client = make_live_client()
        client.get_balance = MagicMock(return_value=100.0)
        client.get_positions = MagicMock(return_value=[{"id": "x"}])

        with tempfile.TemporaryDirectory() as tmp:
            state_path = Path(tmp) / "state.json"
            write_state(state_path, {"balance_usdc": 150.0})

            result = client.reconcile_state(state_path=str(state_path))

            assert result["desync"] is True
            assert result["state_updated"] is True
            with open(state_path) as f:
                saved = json.load(f)
            assert saved["balance_usdc"] == 100.0  # Ground truth aplicado


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------

class TestReconcileErrors:
    def test_get_balance_failure_returns_partial(self):
        client = make_live_client()
        client.get_balance = MagicMock(side_effect=RuntimeError("API down"))

        with tempfile.TemporaryDirectory() as tmp:
            state_path = str(Path(tmp) / "state.json")
            result = client.reconcile_state(state_path=state_path)

        # Retorna resultado parcial sin crashear
        assert result["balance_onchain"] == 0.0
        assert result["desync"] is False

    def test_get_positions_failure_continues(self):
        """Si get_positions falla, aún se puede reconciliar el balance."""
        client = make_live_client()
        client.get_balance = MagicMock(return_value=300.0)
        client.get_positions = MagicMock(side_effect=RuntimeError("orders API error"))

        with tempfile.TemporaryDirectory() as tmp:
            state_path = str(Path(tmp) / "state.json")
            result = client.reconcile_state(state_path=state_path)

        assert result["balance_onchain"] == 300.0
        assert result["open_orders_onchain"] == 0  # Fallback a 0 en error

    def test_corrupt_state_json_no_crash(self):
        """state.json corrupto no rompe la reconciliación."""
        client = make_live_client()
        client.get_balance = MagicMock(return_value=150.0)
        client.get_positions = MagicMock(return_value=[])

        with tempfile.TemporaryDirectory() as tmp:
            state_path = Path(tmp) / "state.json"
            state_path.write_text("NOT VALID JSON{{{")

            result = client.reconcile_state(state_path=str(state_path))

        assert result["balance_onchain"] == 150.0
        # Sin state local → no hay delta
        assert result["balance_local"] is None
