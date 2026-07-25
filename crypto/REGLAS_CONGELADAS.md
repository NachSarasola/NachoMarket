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

## RESULTADO GATE H3a — 2026-07-24 — VEREDICTO: NO_OPERAR ❌ (trials acumulados ≈ 125)

| run | trades | WF folds>0 | OOS/IS | DSR | benchmarks | nota |
|-----|--------|-----------|--------|-----|------------|------|
| funding BTC 4h | 37 | 75% | -0.35 | 0.288 | no | IS: PF 2.68, maxDD -1.9%, Sharpe 0.87 |
| funding ETH 4h | 48 | **100%** | -0.55 | 0.048 | no IS / sí OOS | el WF más consistente del programa entero |

Misma firma que las 4 familias anteriores: estructura IS (2019-2023), muerte OOS (2024+).

## ★ CHECKPOINT ESTRATÉGICO — 2026-07-24 (rama terminal del árbol pre-registrado)

**Meta-hallazgo del programa** (5 familias × 2 pares, ~125 trials, $0 perdidos): precio,
tendencia, order flow y posicionamiento — TODAS muestran estructura in-sample hasta 2023 y
colapso out-of-sample 2024+. Conclusión honesta: **no hay edge direccional validable para
nosotros en majors, swing, con datos gratis, en el régimen actual.** No es un fallo del
método: es LA respuesta del método. El mercado post-ETF arbitró el rincón donde mira el retail.

**Resolución (las tres salidas pre-escritas en MAPA_EDGES §A):**
1. **Exposición pasiva** (si se quiere exposición a crypto): DCA/hold — lo único que
   sobrevivió en nuestros propios datos. Es una posición del usuario, no un bot.
2. **Acumular capital → Etapa 3 (carry delta-neutral, $5k+)** — el único edge "grande" con
   evidencia peer-reviewed que no depende de predecir dirección. Dato honesto de HOY: el
   funding medio 30d está bajo (BTC ~6.2%, ETH ~3.5% anualizado) → el carry es cíclico y
   paga en mercados calientes; el capital se acumula MIENTRAS se espera el ciclo.
3. **Investigación nueva SOLO con mecanismo distinto + datos nuevos**: única candidata viva
   con diseño listo = **H3b (proxy de cascadas de liquidación, evento-driven en horas)** —
   mecanismo diferente (overshoot intra-evento, no posicionamiento lento). Requiere construir
   el dataset de ΔOI. Cualquier otra idea: prior ≥ MEDIO + fuente de datos nueva + pipeline
   completo + trial contado. **Prohibido re-minar las 5 familias muertas.**

**El programa NO se detiene: cambia de modo.** La máquina de validación (81 tests, gates,
DSR, decide.py) es el activo construido. Revisión trimestral como manda TESIS; el contador
de trials sigue corriendo para siempre.

**RESOLUCIÓN DEL CHECKPOINT — elegida por el usuario (2026-07-24): salidas 2 + 3**
(acumular capital hacia carry $5k+ Y cargar solo mecanismos nuevos), más el mandato
"encontrar otras aristas: cómo ganan los bots que ganan de verdad".

**Enmienda por evidencia (mismo día, investigación de 19 búsquedas — RECURSOS §8):**
- Dato central (on-chain, infalsificable): de 2.396 vaults de Hyperliquid, ~16% rentable a
  30d / ~20% lifetime. Los ganadores: MM/HFT con control de riesgo brutal, **winrate BAJO**
  y asimetría avg-win≫avg-loss. NINGUNO con señal técnica direccional en majors → tercera
  confirmación independiente del meta-hallazgo. La frase "buen winrate ⇒ alguna forma hay"
  queda invertida: WR 80-95% es la firma de martingala/grid/short-vol con cola letal
  (incidente JELLY: −27% del vault en horas).
- **H3b (cascadas) degradada MEDIO→BAJO**: cero papers/backtests públicos del rebote en 19
  búsquedas. EN PAUSA (la salida 3 la nombraba "candidata viva"; la evidencia manda).
- Entran con soporte estadístico y datos gratis: **H7 short-unlocks-cliff (MEDIO-ALTO)** y
  **H8 listing-fade (MEDIO)** — specs propuestas SIN congelar en HIPOTESIS.md; **0 trials
  gastados**. Congelar y correr requiere go explícito del usuario (gasta los próximos trials
  del presupuesto DSR).
- Paralelo no-backtest permitido (B6 del mapa): cosecha de incentivos con caps duros +
  presupuesto fijo de fees; HLP pasivo solo con sizing que tolere −30% en un día.

## ★ SPECS CONGELADAS H7/H8 — 2026-07-24 (tesis INVERTIDA: PnL, no winrate)

**Mandato del usuario (textual): "continua e invierte mi tesis, encuentra la forma de armar
algún bot que gane tradeando, no importa el winrate sino el pnl. Cualquier crypto, metodo,
etc."** → go explícito para gastar los próximos trials en H7/H8. Congeladas ANTES de bajar
un solo dato de eventos. El winrate se REPORTA pero no participa de ningún gate.

### H7 — short de unlocks tipo cliff (`event_validate.py --strategy h7_unlock`)
- **Universo**: unlocks cliff ≥ **2.0%** del supply (basis `max_supply`, estática → sin
  lookahead y conservadora vs circulante), token con perp USDT en Binance.
- **Regla**: short al open de la primera vela 4h ≥ T−48h (calendario público con semanas de
  anticipación → causal); cover al open de la primera vela ≥ T+24h; stop 4% (cap
  inquebrantable, conservador intrabarra); riesgo 1%; sin re-entradas; eventos solapados del
  mismo símbolo → solo el primero.
- **Costos**: 6 bps fee + 10 bps slippage POR LADO + **funding acumulado** del período (el
  corto paga el funding negativo — asesino documentado del trade).
- **Variante ÚNICA permitida** (pre-registrada): umbral 1.0% (palanca de muestra). Nada más.

### H8 — listing fade Binance (`event_validate.py --strategy h8_listing`)
- **Universo**: listings spot Binance desde 2023 (fecha = primera vela 1d, mecánico, sin
  scraping de anuncios) con perp USDT disponible.
- **Regla**: short al open de la primera vela 4h ≥ T+24h (posterior al evento → causal);
  salida por tiempo en T+168h (día 7) o stop 4%; riesgo 1%.
- **Costos**: idénticos a H7 (funding incluido).
- **Variante ÚNICA permitida** (pre-registrada): salida T+72h (día 3). Nada más.

### Gates del event-study (congelados; en `crypto/smc/events.py::classify_event_study`)
| Gate | Umbral | Severidad |
|---|---|---|
| Muestra total | ≥ 60 eventos ejecutables | soft → MUESTRA_INSUFICIENTE |
| Muestra OOS | ≥ 15 eventos | soft |
| IS con efecto | expectancy neta > 0 **y** p(bootstrap, media≤0) < 0.05 | **hard** |
| OOS sostiene | expectancy OOS > 0 **y** OOS/IS ≥ 0.5 (una sola pasada, IS≤2024-12-31) | **hard** |
| DSR | ≥ 0.95 sobre retornos POR EVENTO (n_trials acumulados) | **hard** |
| Ruina | P(ruina MC) ≤ 10% a riesgo 1% | soft |
| Datos | eventos truncados por fin de datos ≤ 10% | soft |

**Sesgos pre-registrados (ambos CONSERVADORES para el short)**: universos de sobrevivientes
(DeFiLlama y exchangeInfo no listan tokens muertos/delistados → faltan los mejores shorts);
% de supply subestimado por basis max_supply.

**Contador de trials**: base acumulada ~125 + H7 base + H7 variante + H8 base + H8 variante
→ correr con `--deflated-sharpe 130` (redondeado en contra nuestra). La variante solo se
corre TRAS registrar el veredicto base, y se anota como fila acá.

**Alcance**: esto es INVESTIGACIÓN sobre alts/perps. La regla v1 ("spot, solo BTC/ETH")
sigue vigente para operar: si H7/H8 validan, el pase a ejecución real es un cambio de
alcance que se registra acá (perps = Etapa 2: Hyperliquid/Binance futures, shorts, dry-run
4-8 semanas y GATE 2 completo ANTES de un dólar real). Nada de esto salta el pipeline.

## RESULTADO H8 (listing fade) — 2026-07-24 — VEREDICTO: NO_OPERAR ❌

Corrido en el VPS con datos reales (178 listings 2023-2026 con perp; universo mecánico =
primera vela spot; costos 6+10 bps/lado + funding acumulado). `event_validate.py`:

| segmento | n | expectancy neta | mediana | WR (informativo) | p(media≤0) | DSR (130 trials) |
|---|---|---|---|---|---|---|
| IS 2023-2024 | 62 | **+1.50%** | −4.32% | 0.21 | 0.172 | 0.06 |
| OOS 2025-2026 | 116 | **−0.23%** | −4.32% | 0.15 | 0.592 | 0.00 |

Fallos HARD: IS sin significancia (p=0.17) + DSR 0.06. **Familia muerta** (HARD fail = sin
ajuste; la variante pre-registrada NO se corre — sería curve-fitting sobre un hard fail y
queda sin gastar). Trials: +1 → acumulado ~126 (seguir usando `--deflated-sharpe 130`).

Autopsia (journal `eventos_20260724_2007/journal_h8_listing.csv`): 82.6% de los eventos
salió por stop (la vol post-listing cruza 4% casi siempre); el 17% restante capturó caídas
grandes — el PERFIL invertido (WR bajo + asimetría) apareció, pero no supera al azar con
esta ejecución. Funding: −0.48%/evento en contra del short. Lección estructural registrada
en HIPOTESIS.md → RIP: el cap de stop 4% es incompatible con vol de eventos en alts; un
event-short futuro exigiría stop escalado por vol = **enmienda de regla INQUEBRANTABLE que
solo el usuario puede autorizar** y contaría como spec nueva.

**H7 (unlocks): PENDIENTE** — el API clásico de DeFiLlama pasó a ser PAGO (HTTP 402) y el
sitio quedó tras challenge de Cloudflare (403), ambos verificados en el VPS 2026-07-24. No
se elude el challenge (ToS + carrera armamentista). Mitigación definitiva implementada:
**fuente = el repo open-source que GENERA esos datos** (github.com/DefiLlama/
emissions-adapters, clonado en el VPS) — `fetch_unlocks.py --source adapters` parsea los
cronogramas TS declarativos con un mini-evaluador seguro (consts, fechas, periodToSeconds
leído del propio repo). Supuestos registrados: % sobre suma de secciones declaradas;
`manualStep` = un evento por escalón en start+k·duración (categoría "step"); helpers no
parseables → `pct_basis=sum_parcial`. Mapeo gecko_id→ticker vía CoinGecko coins/list
(cacheado; `COINGECKO_API_KEY` demo opcional). Fuentes secundarias: sitio y API clásico
quedan como fallback en cascada. El resto del pipeline (klines/funding de 202 perps) ya
quedó cacheado en el VPS.

## RESULTADO H7 (short unlocks cliff) — 2026-07-25 — VEREDICTO: NO_OPERAR ❌

Fuente de eventos: **plan C supply-step** (pre-registrada; salto ≥2% del circulante,
CoinGecko): 457 eventos ≥1% → 150 tras el umbral congelado 2% → **57 ejecutables** (93 sin
perp vivo al momento del evento — Binance listó el perp después). Costos 6+10 bps/lado +
funding. `event_validate.py --strategy h7_unlock --deflated-sharpe 130`:

| segmento | n | expectancy neta | mediana | WR (informativo) | p(media≤0) | DSR |
|---|---|---|---|---|---|---|
| IS 2023-2024 | 26 | **+0.0000** | −4.26% | 0.35 | 0.530 | 0.004 |
| OOS 2025-2026 | 31 | **+0.0009** | −4.30% | 0.29 | 0.511 | 0.005 |

Fallos HARD (IS sin efecto + DSR≈0) y SOFT (57<60 eventos). **Familia muerta; la variante
pre-registrada (umbral 1%) NO se corre** — regla del programa: hard fail = sin ajuste, y
con expectancy IS literalmente cero, agrandar la muestra con eventos más chicos es
forking-paths puro. Trials: +1 → acumulado ~127 (seguir con `--deflated-sharpe 130`).

**Autopsia — distinta a la de H8 y eso importa:**
- En H8 el bruto EXISTÍA (+1.17%/evento) y lo mató la ejecución (83% stopped-out). En H7
  el bruto es ~cero (+0.41% vs 0.32% de costos) en NUESTRA ventana T−48h→T+24h: el efecto
  de la literatura (~90% negativo) vive en horizontes ±30 días (drift lento), NO en un
  short ejecutable de 72h alrededor del evento. La ventana congelada no lo captura — y esa
  ventana era la única compatible con nuestros caps de riesgo.
- Stops 63% (cap 4% de nuevo mordiendo alts) pero NO fue lo decisivo acá: sin bruto no hay
  nada que el stop pueda estar destruyendo.
- Funding −0.04%/evento (menor de lo temido a 72h).
- Caveats de la fuente (pre-registrados): blur ±12h del timestamp y eventos falsos que
  diluyen. Con IS en cero exacto, ni el escenario más generoso con esos caveats sugiere un
  edge ejecutable escondido.

**Cierre del brazo event-driven**: H7 y H8 muertas bajo los caps del programa. Re-testear
event-shorts exigiría (a) enmienda de la regla INQUEBRANTABLE del stop 4% (solo el usuario)
y (b) una tesis de ventana distinta (drift ±30d ≠ evento 72h) = familia NUEVA con spec
nueva y trial contado. No se recomienda: el drift a 30 días con shorts de alts es carry
negativo + borrow + 30 días de riesgo de squeeze por 1-2% esperado.

**Actualización 2026-07-25: GitHub upstream TAMBIÉN privatizado** (404 en codeload/api;
clone anónimo pide credenciales; grep.app sin forks; Software Heritage sin el origen).
DeFiLlama cerró calendario por las 3 vías — señal de que el dato VALE. **PLAN C activado y
PRE-REGISTRADO antes de ver resultado alguno** (`fetch_unlocks.py --source supply-step`):
reconstrucción de cliffs desde saltos del supply circulante (circulante = mcap/precio
diario, CoinGecko gratis). Supuestos congelados: (1) conocibilidad ex-ante — los cliffs de
vesting son contractuales y públicos desde el TGE, por lo que un salto realizado era
anticipable; los mints NO programados que se cuelen generan eventos falsos que DILUYEN el
efecto medido → sesgo conservador; (2) timestamp del evento = punto medio entre muestras
diarias (blur ±12h, mismas ventanas congeladas); (3) filtros de MEDICIÓN congelados (no son
parámetros de trading): persistencia 0.7, ruido 0.2, separación 5 días; (4) pct_basis =
salto % del CIRCULANTE (base más fiel que max_supply). El instalador de adapters
(`vps_get_adapters.sh`: forks vía búsqueda oficial de GitHub + Software Heritage) queda
como vía A si alguna reaparece; UNA sola fuente alimenta el run que se registre (sin dobles
miradas al OOS).

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
