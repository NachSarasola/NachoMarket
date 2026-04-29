#!/usr/bin/env python3
"""MCP Server — Polymarket Documentation.

Expone la documentacion oficial de Polymarket como recursos y herramientas
para consulta contextual dentro de cualquier cliente MCP (Cursor, Claude Desktop, etc.).

Uso:
    python scripts/mcp_polymarket_docs.py

Instalacion en Cursor:
    1. Settings > MCP Servers > Add
    2. Name: polymarket-docs
    3. Command: python "C:/Users/Usuario/Desktop/NachoMarket/scripts/mcp_polymarket_docs.py"

Instalacion en Claude Desktop:
    Agregar a %APPDATA%\Claude\settings.json:
    {
      "mcpServers": {
        "polymarket-docs": {
          "command": "python",
          "args": ["C:/Users/Usuario/Desktop/NachoMarket/scripts/mcp_polymarket_docs.py"]
        }
      }
    }
"""

import json
from pathlib import Path

from mcp.server.fastmcp import FastMCP

DOCS_FILE = Path(__file__).parent.parent / "docs" / "polymarket_docs.md"

mcp = FastMCP("polymarket-docs")


def _load_docs() -> str:
    if DOCS_FILE.exists():
        return DOCS_FILE.read_text(encoding="utf-8")
    return "Documentacion no disponible. Ejecutar scripts/download_polymarket_docs.py primero."


@mcp.tool()
def search_docs(query: str) -> str:
    """Busca un termino en la documentacion de Polymarket.

    Args:
        query: Palabra clave o frase a buscar (ej: 'liquidity rewards', 'Q_min', 'post_orders').

    Returns:
        Bloques de texto relevantes encontrados en la documentacion.
    """
    docs = _load_docs()
    lines = docs.splitlines()
    matches: list[str] = []
    query_lower = query.lower()
    for i, line in enumerate(lines):
        if query_lower in line.lower():
            start = max(0, i - 2)
            end = min(len(lines), i + 5)
            matches.append("\n".join(lines[start:end]))
    if not matches:
        return f"No se encontraron coincidencias para '{query}'."
    return "\n\n---\n\n".join(matches[:20])


@mcp.resource("docs://polymarket")
def get_full_docs() -> str:
    """Retorna la documentacion completa de Polymarket."""
    return _load_docs()


@mcp.resource("docs://liquidity-rewards")
def get_liquidity_rewards_doc() -> str:
    """Retorna la seccion de Liquidity Rewards de Polymarket."""
    docs = _load_docs()
    # Buscar la seccion de Liquidity Rewards
    start = docs.find("## Liquidity Rewards")
    if start == -1:
        return "Seccion no encontrada."
    end = docs.find("\n## ", start + 1)
    if end == -1:
        end = len(docs)
    return docs[start:end]


if __name__ == "__main__":
    mcp.run(transport="stdio")
