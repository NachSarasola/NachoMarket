"""Kalshi API client with RSA-PSS authentication."""
from __future__ import annotations

import base64
import json
import logging
import os
import time
from typing import Any

import requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding as asym_padding

logger = logging.getLogger("nachomarket.kalshi")

KALSHI_BASE = "https://trading-api.kalshi.com/trade-api/v2"


class KalshiClient:
    """Minimal Kalshi API client for weather temperature markets."""

    def __init__(self, paper: bool = False) -> None:
        self._paper = paper
        self._api_key_id = os.environ.get("KALSHI_API_KEY_ID", "")
        self._private_key_path = os.environ.get("KALSHI_PRIVATE_KEY_PATH", "")

        if not paper and (not self._api_key_id or not self._private_key_path):
            logger.warning("KalshiClient: sin credenciales API. Usar paper mode o setear KALSHI_API_KEY_ID + KALSHI_PRIVATE_KEY_PATH en .env")

        self._private_key = None
        if self._private_key_path and os.path.exists(self._private_key_path):
            try:
                with open(self._private_key_path, "rb") as f:
                    self._private_key = serialization.load_ssh_private_key(
                        f.read(), password=None
                    )
            except Exception:
                try:
                    with open(self._private_key_path, "rb") as f:
                        self._private_key = serialization.load_pem_private_key(
                            f.read(), password=None
                        )
                except Exception as e:
                    logger.error("KalshiClient: no se pudo cargar private key: %s", e)

        self._base = KALSHI_BASE

    def _sign(self, timestamp_ms: int, method: str, path: str) -> str:
        """Genera firma RSA-PSS para Kalshi API."""
        if self._private_key is None:
            return ""
        msg = f"{timestamp_ms}{method}{path}".encode()
        signature = self._private_key.sign(msg, asym_padding.PSS(
            mgf=asym_padding.MGF1(hashes.SHA256()),
            salt_length=asym_padding.PSS.DIGEST_LENGTH,
        ), hashes.SHA256())
        return base64.b64encode(signature).decode()

    def _request(self, method: str, path: str, params: dict | None = None, json_body: dict | None = None) -> dict[str, Any]:
        """HTTP request con auth Kalshi."""
        url = f"{self._base}{path}"
        timestamp_ms = int(time.time() * 1000)
        signature = self._sign(timestamp_ms, method, path)

        headers = {
            "KALSHI-API-KEY": self._api_key_id,
            "KALSHI-TIMESTAMP": str(timestamp_ms),
            "KALSHI-SIGNATURE": signature,
            "Content-Type": "application/json",
        }

        try:
            if method == "GET":
                r = requests.get(url, params=params, headers=headers, timeout=15)
            elif method == "POST":
                r = requests.post(url, json=json_body, headers=headers, timeout=15)
            else:
                return {}
            r.raise_for_status()
            return r.json() if r.text.strip() else {}
        except Exception as e:
            logger.debug("Kalshi API error %s %s: %s", method, path, e)
            return {}

    # --- Public API ---

    def get_markets(
        self,
        series_ticker: str | None = None,
        status: str = "open",
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Obtiene mercados abiertos."""
        params: dict[str, Any] = {"status": status, "limit": limit}
        if series_ticker:
            params["series_ticker"] = series_ticker
        result = self._request("GET", "/markets", params=params)
        return result.get("markets", []) if isinstance(result, dict) else []

    def get_series(self, category: str = "Climate and Weather", limit: int = 100) -> list[dict[str, Any]]:
        """Obtiene series de una categoria."""
        result = self._request("GET", "/series", params={"category": category, "limit": limit})
        return result.get("series", []) if isinstance(result, dict) else []

    def get_orderbook(self, ticker: str) -> dict[str, Any]:
        """Obtiene orderbook de un mercado."""
        return self._request("GET", f"/markets/{ticker}/orderbook")

    def place_order(
        self,
        ticker: str,
        side: str,
        count: int,
        order_type: str = "limit",
        price: float | None = None,
    ) -> dict[str, Any]:
        """Coloca una orden (limit o market)."""
        body = {
            "ticker": ticker,
            "action": side.lower(),
            "type": order_type,
            "count": count,
            "client_order_id": f"nachomarket_{int(time.time() * 1000)}",
        }
        if price is not None:
            body["price"] = int(price * 100)  # Kalshi usa centavos
        return self._request("POST", "/portfolio/orders", json_body=body)

    def get_balance(self) -> float:
        """Consulta balance disponible."""
        result = self._request("GET", "/portfolio/balance")
        return float(result.get("balance", 0)) / 100.0 if isinstance(result, dict) else 0.0

    def get_positions(self) -> list[dict[str, Any]]:
        """Obtiene posiciones abiertas."""
        result = self._request("GET", "/portfolio/positions")
        return result.get("positions", []) if isinstance(result, dict) else []
