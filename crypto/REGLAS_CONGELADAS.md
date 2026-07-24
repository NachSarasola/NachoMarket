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

## Fuera de v1 a propósito (anti-overfitting)

- **FVG** (`fair_value_gap` existe y está testeado) NO se cablea al sweep: el FVG solo tiene
  edge documentado como *pullback en tendencia*, no como filtro de sweep, y acoplarlo sumaría
  parámetros libres sin fundamento. Queda para un eventual `fvg_pullback` v2 SOLO si v1 valida.
- Order blocks, armónicos, EMAs/conteo de niveles, MMBM/MMSM: descartados (discreción o
  explosión de parámetros). Ver `crypto/README.md` y el análisis del pivot.

## Verificación del motor (controles)

- **Positivo**: `crypto/smc/synthetic.py::sweep_market_ohlcv` → el sweep DEBE ser rentable.
- **Negativo**: `random_walk_ohlcv` → DEBE perder por costos.
- **DSR / Monte Carlo**: `crypto/smc/stats.py` (multiple-testing + prob. de ruina).
- Todos ejercitados en `crypto/tests/` (57 tests: causalidad, fills, controles, stats, régimen,
  veredicto, multi-par, review).

## Gates de validación (en orden; no se salta ninguno)

1. **Reglas congeladas** (este archivo) antes de mirar datos. ✅
2. **Backtest in-sample 2019-2023** con costos pesimistas (fee 10 bps + slippage 5 bps por lado).
3. **≥ 100-200 trades** cruzando bull/bear/rango. Si no los genera → no validable → descartar.
4. **Walk-forward** (folds) + heatmap de parámetros: aceptar solo mesetas, nunca picos.
5. **OOS 2024-2026, UNA sola pasada.** Sharpe OOS < 50% del in-sample → overfit → descartar.
6. **Batir buy&hold BTC y MA diaria** neto de costos. Si no → no desplegar.
7. **Dry-run 4-8 semanas** (freqtrade); fills/fees reales deben coincidir con lo simulado.
8. **Live con $10-20/posición** + kill-switch. Objetivo de la etapa: un setup validado, no ingresos.

Los gates 2-6 los aplica MECÁNICAMENTE `crypto/scripts/decide.py` sobre los reportes JSON de
`validate.py` — el veredicto no se interpreta a mano.

## GATE 1 — validación con datos reales (decide.py)

Correr `crypto/scripts/vps_validate_all.sh` en el VPS → `decide.py` sobre los reportes. Umbrales:
`trades≥100 · walk-forward ≥50% folds Sharpe>0 y peor fold≥-1.0 · OOS/IS≥0.5 · DSR≥0.95 ·
batir buy&hold y MA`. Veredictos: `GO_DRY_RUN` / `AJUSTE_UNICO` / `DESCARTAR_SWEEP_QUEDA_DONCHIAN`
/ `NO_OPERAR`. **El veredicto se anota como fila en el registro de abajo.**

## GATE 2 — pasar de dry-run a live (checklist)

- [ ] ≥ 4 semanas de dry-run (freqtrade) completadas.
- [ ] Fills/fees reales dentro de ±20% de lo simulado (paridad backtest↔dry-run).
- [ ] `weekly_review.py` sin `KILL` ni `ALERTA` sostenida en las últimas 4 semanas.
- [ ] Kill-switch probado en vivo (drill: `/stop` de Telegram y verificar cancelación de órdenes).
- [ ] Live arranca con $10-20/posición, riesgo 1%, y el otro bot del VPS intacto.

## Criterios de muerte en live (weekly_review.py → KILL)

- Drawdown realizado peor que **2× el p95 del Monte Carlo** del backtest.
- Racha de pérdidas **>> la esperada** (backtest + 3 y > 1.5×).
- (ALERTA, no KILL) winrate vivo < 70% del backtest → edge decayendo, investigar.

## Registro de variantes probadas (multiple-testing)

> Cada fila cuenta para el sesgo de selección. Cuantas más variantes, más alto debe ser el
> Sharpe para ser creíble. Anotar ANTES de mirar el OOS.

> `param_sweep.py` prueba una grilla de 108 combinaciones — ESO cuenta como 108 trials para
> el Deflated Sharpe. Al validar con datos reales, pasar `--deflated-sharpe 108` (o el nº real
> de variantes acumuladas) a `validate.py`.

| # | fecha | cambio vs base | motivo | resultado IS (Sharpe/PF/trades) |
|---|-------|----------------|--------|---------------------------------|
| 0 | (base)| —              | hipótesis inicial congelada | (pendiente: correr con datos reales) |
| — | —     | grilla param_sweep (108 combos) | robustez/meseta | cuenta para multiple-testing |
