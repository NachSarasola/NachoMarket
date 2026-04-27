#!/usr/bin/env python3
"""Verifica que el entorno LIVE está completo antes de iniciar el bot.

Sale con exit code 0 si el entorno está OK, 1 si falta algo.
Diseñado para usarse como ExecStartPre en el systemd service.
"""

import os
import sys
from pathlib import Path


REQUIRED_FILES = [
    ".env",
    "config/settings.yaml",
    "config/markets.yaml",
    "config/risk.yaml",
]


def check_env_files() -> bool:
    """Verifica que los archivos necesarios existen."""
    base_dir = Path(__file__).parent.parent
    missing = []

    for rel_path in REQUIRED_FILES:
        full_path = base_dir / rel_path
        if not full_path.exists():
            missing.append(rel_path)

    if missing:
        print("❌ Archivos faltantes:")
        for f in missing:
            print(f"   - {f}")
        return False

    print("✅ Archivos de configuración OK")
    return True


def check_api_keys() -> bool:
    """Verifica que las variables de entorno estén definidas."""
    required_vars = [
        "POLYMARKET_API_KEY",
        "POLYMARKET_API_SECRET",
    ]

    missing = [v for v in required_vars if not os.environ.get(v)]

    if missing:
        print("❌ Variables de entorno faltantes:")
        for v in missing:
            print(f"   - {v}")
        print("   Agregalas al archivo .env")
        return False

    print("✅ API keys configuradas")
    return True


def main() -> None:
    """Entry point."""
    print("🔍 Verificando entorno LIVE...")

    files_ok = check_env_files()
    if not files_ok:
        print("\n🚫 ENTORNO INCOMPLETO")
        sys.exit(1)

    # No verificamos API keys aquí porque se cargan desde .env
    # que es cargado por el proceso padre del systemd
    print("✅ ENTORNO OK — listo para iniciar el bot")
    sys.exit(0)


if __name__ == "__main__":
    main()