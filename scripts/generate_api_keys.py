"""
Genera las credenciales API de Polymarket a partir de tu clave privada.

Uso:
    python scripts/generate_api_keys.py

Requiere que POLYMARKET_PRIVATE_KEY esté en tu archivo .env
"""

import os
import sys
from pathlib import Path

# Cargar .env manualmente
env_path = Path(__file__).parent.parent / ".env"
if env_path.exists():
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, _, value = line.partition("=")
            os.environ.setdefault(key.strip(), value.strip())

private_key = os.environ.get("POLYMARKET_PRIVATE_KEY", "")

if not private_key:
    print()
    print("ERROR: No encontré POLYMARKET_PRIVATE_KEY en tu archivo .env")
    print()
    print("Pasos:")
    print("  1. Abre el archivo .env en la carpeta NachoMarket/")
    print("  2. Pon tu clave privada en la línea: POLYMARKET_PRIVATE_KEY=0xtu_clave_aqui")
    print("  3. Vuelve a ejecutar este script")
    print()
    sys.exit(1)

print()
print("Conectando con Polymarket para generar credenciales API...")
print()

try:
    from py_clob_client.client import ClobClient

    client = ClobClient(
        host="https://clob.polymarket.com",
        chain_id=137,
        key=private_key,
        signature_type=1,
    )

    creds = client.create_or_derive_api_creds()

    print("=" * 60)
    print("CREDENCIALES GENERADAS CORRECTAMENTE")
    print("=" * 60)
    print()
    print("Copia estas 3 líneas a tu archivo .env:")
    print()
    print(f"POLYMARKET_API_KEY={creds.api_key}")
    print(f"POLYMARKET_SECRET={creds.api_secret}")
    print(f"POLYMARKET_PASSPHRASE={creds.api_passphrase}")
    print()
    print("=" * 60)
    print()
    print("IMPORTANTE: Guarda estas credenciales de forma segura.")
    print("Si las pierdes, puedes volver a generarlas con este script.")
    print()

    # Actualizar automáticamente (sin preguntar)
    if True:
        import re
        contenido = env_path.read_text()
        # Usar regex para reemplazar toda la línea, evitando duplicados
        contenido = re.sub(
            r"^POLYMARKET_API_KEY=.*$",
            f"POLYMARKET_API_KEY={creds.api_key}",
            contenido,
            count=1,
            flags=re.MULTILINE,
        )
        contenido = re.sub(
            r"^POLYMARKET_SECRET=.*$",
            f"POLYMARKET_SECRET={creds.api_secret}",
            contenido,
            count=1,
            flags=re.MULTILINE,
        )
        contenido = re.sub(
            r"^POLYMARKET_PASSPHRASE=.*$",
            f"POLYMARKET_PASSPHRASE={creds.api_passphrase}",
            contenido,
            count=1,
            flags=re.MULTILINE,
        )
        env_path.write_text(contenido)
        print()
        print(".env actualizado correctamente.")
        print()

except ImportError:
    print("ERROR: Falta instalar dependencias.")
    print("Ejecuta: pip install -r requirements.txt")
    sys.exit(1)
except Exception as e:
    print(f"ERROR al conectar con Polymarket: {e}")
    print()
    print("Posibles causas:")
    print("  - Clave privada incorrecta o mal formateada")
    print("  - Sin conexión a internet")
    print("  - Polymarket API temporalmente caída")
    sys.exit(1)
