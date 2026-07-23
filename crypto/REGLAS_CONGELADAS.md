# Reglas congeladas — SMC Sweep (v1)

> **Gate 1 del proceso anti-overfitting.** Este documento se escribe ANTES de mirar
> resultados de backtest. Toda variante de parametros que se pruebe se anota en el
> registro del final (multiple-testing accounting, López de Prado / Deflated Sharpe).
> Si empezás a cambiar reglas para "mejorar" el backtest, estás haciendo curve-fitting.

## Contexto

Pivot desde el bot de Polymarket (que se fundió por un fill tóxico sin tope de riesgo)
a un bot de crypto direccional. Fuente: ebooks Smart Money (BEIG vol 1 y 2). Restricción
del usuario: **sin overfitting, sin sobre-ingeniería**. Prioridad heredada:
**preservación de capital**.

## Hipótesis (falsable)

El único componente SMC con fundamento microestructural documentado es el **clustering de
stops** tras máximos/mínimos previos (Osler, NY Fed Staff Report 150). Un **falso breakout
que barre esa liquidez y revierte** (turtle soup / stop hunt del libro) debería tener
expectativa positiva neta de costos en 4h sobre BTC/ETH. Si NO bate a un buy&hold ni a un
cruce de media móvil out-of-sample, la hipótesis se rechaza y no se opera.

## Setup congelado — `SmcSweep` (SPOT long-only)

Sobre velas 4h CERRADAS de BTC/USDT y ETH/USDT:

1. **Nivel SSL** = mínimo de las `lookback=20` barras previas (Donchian low, `shift(1)` = causal).
2. **Sweep alcista**: `low[t] < SSL` (la mecha barre) **y** `close[t] > SSL` (el cuerpo cierra
   de vuelta adentro). Regla del libro: mecha = liquidity grab, cuerpo = ruptura.
3. **Filtro PDA**: solo entrar en zona *discount* (close < mid del rango de `pda_lookback=60`).
4. **Entrada**: al open de la barra siguiente al cierre de la señal (sin look-ahead).
5. **Stop**: `low[t] - 0.5*ATR14`. Si el riesgo resultante > **4%** del precio de entrada, la
   señal se DESCARTA (no se acerca el stop — mover el stop está prohibido). Stop SIEMPRE presente.
6. **Target único**: medio del rango `(BSL+SSL)/2`. Salida completa (sin parciales en v1).
7. **Time-stop**: cerrar si tras `12` velas (48h) no se alcanzó el target.
8. **Riesgo**: 1% del equity por trade (por distancia al stop). Máx 2 trades abiertos (1/par).
   Sin apalancamiento.

Parámetros libres (presupuesto ≤ 6): `lookback`, `sl_buffer_atr`, `sl_cap_pct`, `pda_lookback`,
`time_stop_bars`, `use_pda_filter`. **Valores base congelados**:

| parámetro        | valor |
|------------------|-------|
| lookback         | 20    |
| swing_left/right | 2 / 2 |
| atr_period       | 14    |
| sl_buffer_atr    | 0.5   |
| sl_cap_pct       | 0.04  |
| pda_lookback     | 60    |
| use_pda_filter   | true  |
| require_equal    | false |
| time_stop_bars   | 12    |
| single_target    | true  |

Estos valores deben coincidir en tres lugares (si divergen, es un bug):
`crypto/scripts/validate.py` (`DEFAULT_PARAMS['sweep']`),
`crypto/user_data/strategies/smc_sweep.py` (`SWEEP_PARAMS`), y esta tabla.

## Control / benchmark — `DonchianControl`

BMS/breakout Donchian (cierre de cuerpo sobre el máximo de N). Trend following con evidencia.
Si `SmcSweep` no lo bate out-of-sample neto de costos, se **descarta `SmcSweep`** y queda este.

## Gates de validación (en orden; no se salta ninguno)

1. **Reglas congeladas** (este archivo) antes de mirar datos. ✅
2. **Backtest in-sample 2019-2023** con costos pesimistas (fee 10 bps + slippage 5 bps por lado).
3. **≥ 100-200 trades** cruzando bull/bear/rango. Si no los genera → no validable → descartar.
4. **Walk-forward** (folds) + heatmap de parámetros: aceptar solo mesetas, nunca picos.
5. **OOS 2024-2026, UNA sola pasada.** Sharpe OOS < 50% del in-sample → overfit → descartar.
6. **Batir buy&hold BTC y MA diaria** neto de costos. Si no → no desplegar.
7. **Dry-run 4-8 semanas** (freqtrade); fills/fees reales deben coincidir con lo simulado.
8. **Live con $10-20/posición** + kill-switch. Objetivo de la etapa: un setup validado, no ingresos.

## Registro de variantes probadas (multiple-testing)

> Cada fila cuenta para el sesgo de selección. Cuantas más variantes, más alto debe ser el
> Sharpe para ser creíble. Anotar ANTES de mirar el OOS.

| # | fecha | cambio vs base | motivo | resultado IS (Sharpe/PF/trades) |
|---|-------|----------------|--------|---------------------------------|
| 0 | (base)| —              | hipótesis inicial congelada | (pendiente: correr con datos reales) |
