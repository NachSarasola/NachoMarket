"""Validacion de acceso geografico a Polymarket.

Verifica que la IP de salida no este geobloqueada antes de arrancar el bot.
Usa el endpoint oficial /api/geoblock como fuente primaria, y valida
acceso al CLOB como fallback.
Solo usa urllib (stdlib) para no depender de requests.
"""

import json
import logging
import urllib.request
import urllib.error

logger = logging.getLogger("nachomarket.geo")

CLOB_URL = "https://clob.polymarket.com/"
GEOBLOCK_URL = "https://polymarket.com/api/geoblock"
GEO_BLOCKED_CODES = {403, 451}
TIMEOUT_SEC = 15


def _check_geoblock_api() -> dict | None:
    """Consulta el endpoint oficial de geoblock de Polymarket.

    Returns:
        dict con keys 'blocked', 'ip', 'country', 'region' si disponible.
        None si el endpoint no responde.
    """
    try:
        req = urllib.request.Request(
            GEOBLOCK_URL,
            headers={"User-Agent": "NachoMarket/1.0"},
        )
        with urllib.request.urlopen(req, timeout=TIMEOUT_SEC) as response:
            data = json.loads(response.read().decode())
            return data
    except Exception as e:
        logger.warning("No se pudo consultar /api/geoblock: %s", e)
        return None


def verify_geo_access() -> bool:
    """Verifica que la IP actual puede acceder a Polymarket.

    Usa dos metodos:
    1. Endpoint oficial /api/geoblock (fuente primaria)
    2. GET al CLOB (fallback)

    Returns:
        True si el acceso esta permitido.

    Raises:
        ConnectionError si la IP esta geobloqueada.
        OSError si hay un problema de red.
    """
    # --- Check 1: Endpoint oficial ---
    geo_data = _check_geoblock_api()
    if geo_data is not None:
        blocked = geo_data.get("blocked", False)
        ip = geo_data.get("ip", "desconocida")
        country = geo_data.get("country", "??")

        if blocked:
            raise ConnectionError(
                f"Polymarket geobloqueo esta IP ({ip}, pais={country}). "
                "El bot debe correr desde un VPS en una region permitida."
            )
        logger.info(
            "Geo-check OK via /api/geoblock: IP=%s, pais=%s, blocked=false",
            ip, country,
        )
        return True

    # --- Check 2: Fallback al CLOB ---
    logger.info("Fallback: verificando acceso directo al CLOB...")
    try:
        req = urllib.request.Request(
            CLOB_URL,
            headers={"User-Agent": "NachoMarket/1.0"},
        )
        with urllib.request.urlopen(req, timeout=TIMEOUT_SEC) as response:
            code = response.getcode()
            if code in GEO_BLOCKED_CODES:
                raise ConnectionError(
                    f"Polymarket geobloqueo esta IP (HTTP {code}). "
                    "El bot debe correr desde un VPS en una region permitida."
                )
            logger.info("Geo-check OK: Polymarket accesible (HTTP %d)", code)
            return True
    except urllib.error.HTTPError as e:
        if e.code in GEO_BLOCKED_CODES:
            raise ConnectionError(
                f"Polymarket geobloqueo esta IP (HTTP {e.code}). "
                "El bot debe correr desde un VPS en una region permitida."
            ) from e
        # Otros errores HTTP no son geobloqueo
        logger.warning("Polymarket retorno HTTP %d (no es geobloqueo)", e.code)
        return True
    except urllib.error.URLError as e:
        logger.warning("No se pudo verificar acceso geo: %s", e.reason)
        # Si no hay conexion, dejamos que el bot intente arrancar
        # y falle naturalmente en test_connection()
        return True
