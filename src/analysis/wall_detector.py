"""
Deteccion de walls grandes en el orderbook.

Tip 9: una wall de x10 el minimo de shares es grande. Posicionarse
por encima (mismo precio) o 1c mas barato si se puede actuar rapido
como vendedor limite post-fill.

Uso:
    book_bids = [(0.47, 300), (0.46, 50), (0.45, 20)]  # (price, size)
    found, wall_price = is_large_wall(book_bids, min_share=20.0)
    # → (True, 0.47)  — wall de 300 shares es 15x el minimo
"""

from typing import Sequence


def is_large_wall(
    book_side: Sequence[tuple[float, float]],
    min_share: float,
    multiplier: float = 10.0,
) -> tuple[bool, float]:
    """Detecta si existe una wall grande en un lado del orderbook.

    Una wall cuenta como "grande" cuando su size >= multiplier * min_share.
    Retorna la primera wall encontrada (mejor precio = primer nivel).

    Args:
        book_side: Lista de (price, size) ordenada por precio (bids: desc, asks: asc).
        min_share: Tamaño minimo del mercado en shares.
        multiplier: Factor de amplificacion (default 10x).

    Returns:
        (True, wall_price) si hay wall; (False, 0.0) si no.
    """
    if min_share <= 0 or not book_side:
        return False, 0.0

    threshold = multiplier * min_share

    for price, size in book_side:
        if size >= threshold:
            return True, price

    return False, 0.0


def best_price_near_wall(
    book_side: Sequence[tuple[float, float]],
    min_share: float,
    is_bid: bool,
    tick_size: float = 0.01,
    multiplier: float = 10.0,
) -> float:
    """Calcula el precio optimo relativo a la wall detectada.

    Si hay wall: retorna wall_price (mismo nivel para acumular rewards junto a la wall).
    Si no hay wall: retorna 0.0 (caller usa su propia logica de spread).

    Args:
        book_side: Lista de (price, size).
        min_share: Tamaño minimo del mercado.
        is_bid: True si es el lado bid (BUY), False si es ask (SELL).
        tick_size: Tick size del mercado para redondeo.
        multiplier: Factor de amplificacion de wall.

    Returns:
        Precio sugerido float, o 0.0 si no hay wall.
    """
    found, wall_price = is_large_wall(book_side, min_share, multiplier)
    if not found or wall_price <= 0:
        return 0.0

    _ = is_bid  # Reservado para logica futura de offset por lado
    return round(wall_price, 4)
