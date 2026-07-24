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
| 0 | 2026-07-24 | — (base congelada) | GATE 1 con datos reales BTC+ETH 4h 2019→2026 | **FAIL → NO_OPERAR** (ver abajo) |
| — | 2026-07-24 | grilla param_sweep (108 combos) | robustez/meseta | cuenta para multiple-testing |
| 1 | 2026-07-24 | H1 ma_timing (SMA100 1d, vol-target 0.30) BTC+ETH | programa v2, spec congelada | **FAIL → NO_OPERAR** (2 runs) |
| 2 | 2026-07-24 | H2 flow (taker q0.80/0.50) BTC+ETH 4h y 1d | programa v2, spec congelada | **FAIL → NO_OPERAR** (4 runs) |

## RESULTADO GATE H1/H2 — 2026-07-24 — VEREDICTO: NO_OPERAR ❌ (trials acumulados ≈ 119)

| run | trades | WF folds>0 | OOS/IS | DSR | benchmarks | nota |
|-----|--------|-----------|--------|-----|------------|------|
| H1 ma_timing BTC 1d | 28 | 75% | **0.09** | 0.457 | **bate IS**, no OOS | el edge de los papers existió… hasta 2023 |
| H1 ma_timing ETH 1d | 37 | 75% | -0.50 | 0.082 | no IS / sí OOS | — |
| H2 flow BTC 4h | 369 | 75% | **-2.21** | 0.036 | no | OOS fuertemente negativo |
| H2 flow ETH 4h | 409 | 50% | 0.31 | 0.088 | no IS / sí OOS | — |
| H2 flow BTC 1d | 37 | 33% | -0.57 | 0.141 | no | — |
| H2 flow ETH 1d | 46 | 67% | **0.56 ✅** | 0.228 | no IS / sí OOS | "lo menos muerto": pasó OOS/IS pero 46 trades no alcanzan para el DSR |

Lecciones (pre-registradas como formato, escritas al ver los datos):
1. **Tercera confirmación independiente del quiebre de régimen 2024+**: el trend lento
   (ma_timing 1d) batió benchmarks IS con WF sólido y colapsó OOS — igual que donchian 4h y
   que el propio benchmark. El edge de tendencia de la literatura (muestras ≤2022) **decayó
   en la era ETF**, como predice el factor zoo. No es un bug nuestro: es el mercado.
2. **El flow tiene un pulso débil** (ETH 1d pasó OOS/IS y batió B&H OOS) pero 37-46 trades en
   1d no dan poder estadístico frente a ~119 trials: DSR hard-fail. Insuficiente evidencia ≠
   evidencia. NO se ajusta (los fallos DSR/benchmarks son HARD por diseño).
3. **Metodológica**: estrategias lentas en 1d generan 30-50 señales en 7 años → casi
   in-validables con nuestro estándar. Correcto no operarlas; anotar que el gate de trades
   limita estructuralmente el espacio 1d (consecuencia aceptada, no defecto).

Siguiente paso según el árbol (MAPA_EDGES §A, rama AMBAS FAIL): **correr H3a (funding
extremo)**, ya congelada e implementada. Si H3a también muere → checkpoint estratégico.

## RESULTADO GATE 1 — 2026-07-24 — VEREDICTO: NO_OPERAR ❌

Corrido en el VPS con datos reales (Binance, 4h, 2019→2026), costos 10+5 bps/lado.
`decide.py` (mecánico, sin interpretación):

| par | estrategia | trades | walk-forward | OOS/IS | DSR | benchmarks | veredicto |
|-----|-----------|--------|--------------|--------|-----|------------|-----------|
| BTC | sweep     | 228 | 25% folds >0 | **-243.75** | 0.006 | no bate | FAIL |
| BTC | donchian  | 299 | 75% folds >0 | **-0.57** | 0.464 | no bate | FAIL |
| ETH | sweep     | 176 | 50% folds >0 | IS no rentable | 0.000 | no bate | FAIL |
| ETH | donchian  | 303 | 75% folds >0 | **-0.35** | 0.197 | no bate OOS sí/IS no | FAIL |

Conclusiones pre-registradas que aplican:
1. **El sweep SMC mecanizado NO tiene edge** en majors 4h neto de costos. Se descarta como
   estrategia operable (la hipótesis era falsable y fue falsada — eso es un resultado válido).
2. **El control Donchian muestra estructura IS pero muere en el OOS 2024+** (régimen chop) y
   no bate al buy&hold. Tampoco se opera.
3. **Nada batió a holdear BTC** en el periodo. Para exposición a crypto con este capital,
   holdear/DCA es la posición honesta; un bot direccional no se justifica con esta evidencia.
4. Fallos HARD → **no hay "ajuste único"**. Toda hipótesis nueva pasa el pipeline COMPLETO
   desde cero y se anota acá como trial (el contador de multiple-testing sigue corriendo).
5. Según TESIS ("qué invalida la tesis", escrito de antemano): NO se opera direccional; se
   preserva capital y aprendizaje; nuevas hipótesis solo en la ventana de review trimestral.

**Post-mortem con datos finos** (journals + param_sweep + slices, 2026-07-24):
- Grilla del sweep 100% roja (mejor combo Sharpe -0.32, y era pico, no meseta) → familia
  falsada, no des-tuneada.
- Asimetría de régimen consistente en las 4 combinaciones: PnL positivo SOLO en regímenes
  `up_*`; long con precio < SMA 30d destruyó valor siempre.
- Única celda viva del sweep: BTC `up_hi` (n=20, wr 0.80, avgR +0.47) → semilla de H2, n<30.
- `ma_cross` (benchmark SMA200 long/flat) batió a TODO in-sample (Sharpe 1.45/1.56; 16x/42x).
- El programa de investigación v2 (familias H1-H6, reglas y RIP) vive en `crypto/HIPOTESIS.md`.
  Aclaración de cadencia: **investigar es continuo** (nada está live); la regla trimestral
  aplica a cambios de parámetros EN VIVO.
