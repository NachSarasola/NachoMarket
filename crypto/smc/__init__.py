"""SMC detection primitives (pure pandas/numpy, exchange-agnostic).

Este paquete NO depende de freqtrade ni de ningun cliente de exchange, para que
la logica de senales sea testeable de forma aislada. Las estrategias de freqtrade
en ``crypto/user_data/strategies`` importan estas funciones y las envuelven en la
interfaz ``IStrategy``.

Principio rector: CAUSALIDAD ESTRICTA. Toda columna de senal en la barra ``t`` usa
exclusivamente informacion disponible al cierre de la barra ``t`` (o anterior). Los
swings fractales de 5 velas se "confirman" con ``right`` barras de retraso, de modo
que un nivel de liquidez solo esta disponible a partir de su barra de confirmacion.
Esto elimina el look-ahead estructural de las librerias SMC genericas.
"""

from crypto.smc.signals import (
    add_atr,
    confirmed_swings,
    donchian_bms_signals,
    fair_value_gap,
    flow_momentum_signals,
    ma_timing_signals,
    regime_labels,
    smc_sweep_signals,
    swing_mask,
    taker_buy_ratio,
    volume_zscore,
)

__all__ = [
    "add_atr",
    "confirmed_swings",
    "donchian_bms_signals",
    "fair_value_gap",
    "flow_momentum_signals",
    "ma_timing_signals",
    "regime_labels",
    "smc_sweep_signals",
    "swing_mask",
    "taker_buy_ratio",
    "volume_zscore",
]
