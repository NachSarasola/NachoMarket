#!/usr/bin/env python3
"""Verifica que la IP actual puede acceder a Polymarket CLOB.

Uso:
    python scripts/check_geo.py

Sale con exit code 0 si el acceso está OK, 1 si está geobloqueado.
Diseñado para usarse como ExecStartPre en el systemd service.
"""

import sys
import urllib.request
import urllib.error


CLOB_URL = "https://clob.polymarket.com/"
# Códigos HTTP que indican geobloqueo
GEO_BLOCKED_CODES = {403, 451}
TIMEOUT_SEC = 15


def check_geo_access() -> bool:
    """Verifica acceso a Polymarket CLOB API.

    Returns:
        True si el acceso está permitido, False si está geobloqueado.

    Raises:
        SystemExit si hay error de conexión.
    """
    try:
        req = urllib.request.Request(
            CLOB_URL,
            headers={"User-Agent": "NachoMarket-GeoCheck/1.0"},
        )
        with urllib.request.urlopen(req, timeout=TIMEOUT_SEC) as response:
            code = response.getcode()
            if code in GEO_BLOCKED_CODES:
                return False
            print(f"✅ Polymarket CLOB accesible (HTTP {code})")
            return True
    except urllib.error.HTTPError as e:
        if e.code in GEO_BLOCKED_CODES:
            return False
        # Otros errores HTTP (500, 502, etc.) no son geobloqueo
        print(f"⚠️  Polymarket retornó HTTP {e.code} (no es geobloqueo, puede ser temporal)")
        return True
    except urllib.error.URLError as e:
        print(f"❌ Error de conexión a Polymarket: {e.reason}")
        print("   Verificá la conexión a internet del VPS.")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error inesperado: {e}")
        sys.exit(1)


def main() -> None:
    """Entry point."""
    print("🔍 Verificando acceso geográfico a Polymarket CLOB...")

    if not check_geo_access():
        print("🚫 GEOBLOQUEADO: Polymarket bloquea esta IP.")
        print("   Esta IP está en una jurisdicción restringida.")
        print("   El bot NO puede operar desde esta ubicación.")
        print("   → Usá un VPS en una región permitida (ej: US-Ashburn, Ireland).")
        sys.exit(1)

    print("✅ Geo-check OK — esta IP puede acceder a Polymarket.")
    sys.exit(0)


if __name__ == "__main__":
    main()
