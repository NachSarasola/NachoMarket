"""Validación de acceso geográfico a Polymarket.

Verifica que la IP de salida no esté geobloqueada antes de arrancar el bot.
Usa solo urllib (stdlib) para no depender de requests.
"""

import logging
import urllib.request
import urllib.error

logger = logging.getLogger("nachomarket.geo")

CLOB_URL = "https://clob.polymarket.com/"
GEO_BLOCKED_CODES = {403, 451}
TIMEOUT_SEC = 15


def verify_geo_access() -> bool:
    """Verifica que la IP actual puede acceder a Polymarket CLOB.

    Returns:
        True si el acceso está permitido.

    Raises:
        ConnectionError si la IP está geobloqueada.
        OSError si hay un problema de red.
    """
    try:
        req = urllib.request.Request(
            CLOB_URL,
            headers={"User-Agent": "NachoMarket/1.0"},
        )
        with urllib.request.urlopen(req, timeout=TIMEOUT_SEC) as response:
            code = response.getcode()
            if code in GEO_BLOCKED_CODES:
                raise ConnectionError(
                    f"Polymarket geobloqueó esta IP (HTTP {code}). "
                    "El bot debe correr desde un VPS en una región permitida."
                )
            logger.info("Geo-check OK: Polymarket accesible (HTTP %d)", code)
            return True
    except urllib.error.HTTPError as e:
        if e.code in GEO_BLOCKED_CODES:
            raise ConnectionError(
                f"Polymarket geobloqueó esta IP (HTTP {e.code}). "
                "El bot debe correr desde un VPS en una región permitida."
            ) from e
        # Otros errores HTTP no son geobloqueo
        logger.warning("Polymarket retornó HTTP %d (no es geobloqueo)", e.code)
        return True
    except urllib.error.URLError as e:
        logger.warning("No se pudo verificar acceso geo: %s", e.reason)
        # Si no hay conexión, dejamos que el bot intente arrancar
        # y falle naturalmente en test_connection()
        return True
