#!/usr/bin/env python3
"""Verifica que la IP actual puede acceder a Polymarket CLOB.

Uso:
    python scripts/check_geo.py

Sale con exit code 0 si el acceso esta OK, 1 si esta geobloqueado.
Disenado para usarse como ExecStartPre en el systemd service.
"""

import json
import sys
import urllib.request
import urllib.error


CLOB_URL = "https://clob.polymarket.com/"
GEOBLOCK_URL = "https://polymarket.com/api/geoblock"
# Codigos HTTP que indican geobloqueo
GEO_BLOCKED_CODES = {403, 451}
TIMEOUT_SEC = 15


def check_geoblock_api() -> dict | None:
    """Consulta el endpoint oficial de geoblock de Polymarket.

    Returns:
        dict con keys 'blocked', 'ip', 'country', 'region' si disponible.
        None si el endpoint no responde.
    """
    try:
        req = urllib.request.Request(
            GEOBLOCK_URL,
            headers={"User-Agent": "NachoMarket-GeoCheck/1.0"},
        )
        with urllib.request.urlopen(req, timeout=TIMEOUT_SEC) as response:
            data = json.loads(response.read().decode())
            return data
    except Exception as e:
        print(f"  No se pudo consultar /api/geoblock: {e}")
        return None


def check_clob_access() -> bool:
    """Verifica acceso a Polymarket CLOB API.

    Returns:
        True si el acceso esta permitido, False si esta geobloqueado.

    Raises:
        SystemExit si hay error de conexion.
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
            print(f"  Polymarket CLOB accesible (HTTP {code})")
            return True
    except urllib.error.HTTPError as e:
        if e.code in GEO_BLOCKED_CODES:
            return False
        # Otros errores HTTP (500, 502, etc.) no son geobloqueo
        print(f"  Polymarket retorno HTTP {e.code} (no es geobloqueo, puede ser temporal)")
        return True
    except urllib.error.URLError as e:
        print(f"  Error de conexion a Polymarket: {e.reason}")
        print("  Verifica la conexion a internet del VPS.")
        sys.exit(1)
    except Exception as e:
        print(f"  Error inesperado: {e}")
        sys.exit(1)


def main() -> None:
    """Entry point."""
    print("Verificando acceso geografico a Polymarket...")
    print()

    # --- Check 1: Endpoint oficial de geoblock ---
    print("[1/2] Consultando /api/geoblock ...")
    geo_data = check_geoblock_api()

    if geo_data is not None:
        blocked = geo_data.get("blocked", False)
        ip = geo_data.get("ip", "desconocida")
        country = geo_data.get("country", "??")
        region = geo_data.get("region", "??")
        print(f"  IP: {ip} | Pais: {country} | Region: {region}")

        if blocked:
            print()
            print("GEOBLOQUEADO: El endpoint /api/geoblock reporta blocked=true.")
            print(f"  Esta IP ({ip}) esta en {country}, una jurisdiccion restringida.")
            print("  El bot NO puede operar desde esta ubicacion.")
            sys.exit(1)
        else:
            print(f"  /api/geoblock: blocked=false (OK)")
    else:
        print("  No se pudo verificar /api/geoblock, continuando con check CLOB...")

    # --- Check 2: Acceso directo al CLOB ---
    print()
    print("[2/2] Verificando acceso al CLOB API ...")
    if not check_clob_access():
        print()
        print("GEOBLOQUEADO: Polymarket CLOB bloquea esta IP.")
        print("  Esta IP esta en una jurisdiccion restringida.")
        print("  El bot NO puede operar desde esta ubicacion.")
        sys.exit(1)

    print()
    print("Geo-check OK -- esta IP puede acceder a Polymarket.")
    sys.exit(0)


if __name__ == "__main__":
    main()
