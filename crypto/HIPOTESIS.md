# Programa de investigación de edge — v2 (post GATE 1)

> Objetivo: **encontrar el edge y aprender de lo que no lo es** — con el sistema que acaba de
> falsar la primera hipótesis por $0. Método: familias de hipótesis, UNA a la vez, reglas
> congeladas antes de tocar datos, pipeline completo (IS → walk-forward → OOS una pasada →
> DSR → benchmarks → decide.py), y cada intento se anota. **Investigar es continuo; operar
> en LIVE sigue gateado.** Trials acumulados hasta hoy: **~113** (base + grilla 108 + runs).

## Lo que los datos reales ya nos enseñaron (post-mortem GATE 1, 2026-07-24)

1. **Sweep-reversal long-only (SMC turtle soup): FALSADO en toda la grilla.** 108 combos, el
   mejor dio Sharpe -0.32 (y era un pico, no meseta). No es tuning: la familia como se
   especificó no captura edge neto de costos en majors 4h. RIP, y a mucha honra: eso era SMC
   mecanizado sin adornos.
2. **Asimetría de régimen brutal y consistente** (la lección #1): las 4 combinaciones
   estrategia×par solo ganaron en regímenes `up_*` (precio > SMA 30d) y perdieron en `dn_*`.
   Operar señales long con precio bajo la media de 30d destruyó valor SIEMPRE.
3. **La celda `up_hi` del sweep en BTC** (n=20, winrate 0.80, avgR +0.47) es la única vida del
   sweep → semilla de la hipótesis H2, pero n<30: no es evidencia todavía.
4. **`ma_cross` (SMA200 long/flat) batió a todo** — Sharpe IS 1.45 (BTC) / 1.56 (ETH), 16x/42x.
   El edge real observado en NUESTROS datos es el filtro de tendencia, coincidente con la
   literatura (time-series momentum). Su OOS todavía NO fue mirado → es testeable limpio.
5. **Trend-following 4h (donchian) murió en OOS 2024+** (chop post-ETF): cualquier hipótesis
   de tendencia debe demostrar que sobrevive ese régimen o que su filtro la saca a tiempo.

## Reality-check de la lista de deseos (por qué el programa es este y no "todo junto")

- **Scalping (<1h): DESCARTADO por aritmética.** Movimiento medio por vela 1-5m en BTC: 5-15
  bps; round-trip real: 11-20+ bps. Los costos superan el edge antes de empezar. Piso de
  investigación: **1h con fills maker**; el terreno principal: **4h-1d (swing)**.
- **Volumen/order-flow: LA adición más fuerte.** Éramos price-only. Binance regala en las
  klines el *taker buy volume* (historia completa) → agresión compradora/vendedora medible.
- **"Whales": la versión útil son los proxies gratis** — taker-flow, funding rate (historia
  completa gratis), open interest (histórico corto). Wallet-tracking on-chain: infra pesada,
  evidencia retail floja → FUERA por ahora (se revisa con la investigación de vanguardia).
- **Armónicos**: sin un solo backtest público riguroso conocido; ratios fijos o nada; prior
  MUY BAJO → al final de la cola, y solo si sobra presupuesto de trials.
- **Estructuras/SMC**: vuelven SOLO como hipótesis condicionadas (régimen + flujo), no como
  fe. El sweep desnudo ya fue falsado.

## Priors calibrados con evidencia (investigación 2026-07-24; fuentes en RECURSOS.md §8)

| Señal | Evidencia clave | Prior |
|---|---|---|
| Vol-targeting (overlay) | peer-reviewed, robusto; mejora Sharpe/Calmar sin nueva señal | **ALTO** |
| MA-timing diario lento / momentum 1-8 sem | Detzel et al. (FM 2021) bate B&H; señales lentas > rápidas (RM 2026); Liu-Tsyvinski (NBER) | **MEDIO-ALTO** |
| Taker buy/sell imbalance (por vela) | JFM 2026: +1σ → +0.2%/día, +0.9%/sem; horizonte 1d-1sem; dato GRATIS en klines | **MEDIO-ALTO** |
| Funding extremo (contrarian/filtro) | BIS WP1087/Mgmt Sci: carry >10% anual = posicionamiento saturado; señal = cola extrema, semanas | **MEDIO** |
| Reversión post-cascada de liquidaciones | mecanismo documentado (Osler→perps) pero 19 búsquedas dirigidas (2026-07-24) no hallaron NI UN paper/backtest público del rebote — solo anécdota | **BAJO — degradado 2026-07-24** |
| **Short de unlocks tipo cliff (H7)** | Keyrock: ~90% de 16.000 eventos con impacto negativo a 30d; SSRN: 88,5% negativo en 72h (52 eventos Binance). Contras pre-registrados: parcialmente priced-in (estudio de 236 eventos); el funding/borrow del short puede comerse el edge | **MEDIO-ALTO** |
| **Listing fade Binance (H8)** | 2025: 24/27 listings con retorno negativo (media −44%); el "Binance effect" pasó de descubrimiento a distribución | **MEDIO** |
| Funding/basis en venues jóvenes (HIP-3, DEXs nuevos) | estructura two-tier del funding (MDPI): venues chicos se desvían más y por más tiempo; fábrica de mercados nuevos desde oct-2025. Alimenta la Etapa 3 | **MEDIO** |
| Cosecha de incentivos (points/airdrops/HLP pasivo) | HYPE: ~$7B a 94k wallets, on-chain. NO backtesteable: contabilidad de EV + caps duros de riesgo (la versión corregida del bot de Polymarket) | **ALTO (hecho) / MEDIO (forward)** |
| MM con rebate en pares long-tail (Kraken 650+) | subsidio explícito vigente 2026, pero Hummingbot Miner murió (mar-2026) y el adverse selection está medido; solo medible EN VIVO (markouts por fill) | **MEDIO-BAJO (solo live)** |
| Cross-section alts long-short a escala retail | señal gross en papers; neta de costos retail muere (Chen & Welch 2026: 7bp/mes las ~200 anomalías) | **BAJO** |
| Copy trading / leaderboards | 100.236 resultados en 90d: el copiador rinde < líder (delay de fill) + survivorship del leaderboard | **MITO — 0 trials** |
| Scalping "maker gratis" en majors | adverse selection documentado (Tiniç&Sensoy; arXiv 2602.00776): sin velocidad, tu fill maker ES la mala noticia | **MITO — 0 trials** |
| Seasonality horaria (21-23 UTC etc.) | Quantpedia ~33-40% anual BRUTO; 2 trades/día → los fees se lo comen; solo como overlay | **MEDIO (overlay)** |
| OI standalone / MVRV / stablecoin flows | mixta, dato parcial o de pago, horizonte equivocado | **BAJO** |
| **Armónicos** | CERO backtests con fees en toda la literatura; detección ambigua = overfitting | **MITO — descartada, 0 trials gastados** |
| **Whale/wallet-following** | 97% líderes rentables pero solo 44% de copiadores; datos buenos = de pago | **MITO — descartada, 0 trials** |
| **CVD-divergencia / OFI de libro para swing** | el OFI real decae en minutos (HFT); la versión visual no tiene un solo backtest | **MITO — descartada, 0 trials** |

Base rate del factor zoo (crypto y equities): **~70-80% de las hipótesis mueren OOS**. Falsar
5/5 hasta ahora ES la tasa base, no mala suerte. Confirmación on-chain independiente
(2026-07-24): de 2.396 vaults de Hyperliquid con PnL público e infalsificable, **solo ~16%
rentable a 30d / ~20% lifetime** — y los ganadores NO usan señal técnica direccional (fuentes
en RECURSOS §8). Presupuestar en consecuencia.

## SPECS CONGELADAS — listas para correr (reglas fijadas ANTES de tocar datos reales)

### H1 — `ma_timing` (MA-timing diario long/flat, Detzel) — IMPLEMENTADA ✅
- Datos: velas **1d** BTC/USDT y ETH/USDT 2019→hoy. IS 2019-2023, OOS 2024+ una pasada.
- Reglas: long al cierre que cruza sobre SMA(**100**); flat al cruce inverso (fill al open
  siguiente); stop paracaídas close−3·ATR14; **overlay vol-target 30% anual** (solo reduce).
- Variantes permitidas: UNA (window=50). Total trials nuevos: 2 por par.
- Peek declarado: el IS de la familia ya fue "visto" como benchmark en GATE 1 → el juicio
  real de H1 es OOS + DSR (el gate de "batir ma_cross" es contra su propia familia: informativo).
- Código: `signals.ma_timing_signals` + `validate.py --strategy ma_timing --vol-target 0.30`.

### H2 — `flow` (taker-imbalance momentum, JFM 2026) — IMPLEMENTADA ✅
- Datos: velas **4h y 1d CON flujo** (`fetch_data --with-flow`) BTC/ETH 2019→hoy.
- Reglas: flow = media 6 velas de `taker_buy_ratio`; long cuando flow > cuantil 0.80 rolling
  (ventana 360, historia previa → causal); flat bajo cuantil 0.50 (histéresis); stop 3·ATR.
- Variantes permitidas: UNA tanda (enter_q 0.85 o flow_window 12) si la base falla SOFT.
- Código: `signals.flow_momentum_signals` + `validate.py --strategy flow`.

**Cómo correr en el VPS — UN comando (recomendado):**
```bash
cd ~/nacho-crypto && bash crypto/scripts/vps_run_hipotesis.sh
```
(hace git pull, tests, baja los 4 datasets con flujo, corre H1+H2 con las specs congeladas,
aplica decide.py y empaqueta `crypto/data/hipotesis_<fecha>.tar.gz`)

O a mano:
```bash
cd ~/nacho-crypto && git pull && source .venv-crypto/bin/activate && python -m pytest crypto/tests -q
python crypto/scripts/fetch_data.py --with-flow --symbol BTC/USDT --timeframe 1d --since 2019-01-01 --out crypto/data/BTC_USDT-1d.csv
python crypto/scripts/fetch_data.py --with-flow --symbol ETH/USDT --timeframe 1d --since 2019-01-01 --out crypto/data/ETH_USDT-1d.csv
python crypto/scripts/fetch_data.py --with-flow --symbol BTC/USDT --timeframe 4h --since 2019-01-01 --out crypto/data/BTC_USDT-4h-flow.csv
python crypto/scripts/fetch_data.py --with-flow --symbol ETH/USDT --timeframe 4h --since 2019-01-01 --out crypto/data/ETH_USDT-4h-flow.csv
for d in crypto/data/BTC_USDT-1d.csv crypto/data/ETH_USDT-1d.csv; do
  python crypto/scripts/validate.py --data $d --strategy ma_timing --vol-target 0.30 --compare --deflated-sharpe 120 --out ${d%.csv}-rep_ma.json
done
for d in crypto/data/BTC_USDT-4h-flow.csv crypto/data/ETH_USDT-4h-flow.csv crypto/data/BTC_USDT-1d.csv crypto/data/ETH_USDT-1d.csv; do
  python crypto/scripts/validate.py --data $d --strategy flow --compare --deflated-sharpe 120 --out ${d%.csv}-rep_flow.json
done
python crypto/scripts/decide.py crypto/data/*-rep_*.json
```
(`--deflated-sharpe 120` = trials acumulados ~113 + los nuevos, redondeado en contra nuestra.)

### H3a — `funding` (funding extremo contrarian, BIS WP1087) — IMPLEMENTADA ✅, EN COLA
**Spec CONGELADA (2026-07-24, antes de mirar cualquier dato de funding real):**
- Datos: velas **4h** BTC/ETH + funding adjunto (`validate.py --funding <csv>` con el CSV de
  `fetch_funding.py`; ffill causal del último funding conocido al open de cada barra).
- Reglas: long cuando funding ≤ su cuantil **0.02** rolling (ventana **1095 barras 4h ≈ 6
  meses**, umbral sobre historia previa → causal); flat cuando funding ≥ cuantil **0.50**
  (histéresis); stop paracaídas close − 3·ATR14. Sin target/time-stop.
- Parámetros libres: enter_pct, exit_pct, lookback, sl_atr (4). Variantes permitidas: UNA
  (enter_pct 0.05) si la base falla SOFT.
- **REGLA DE ORDEN: NO se corre hasta registrar el veredicto de H1/H2** (una familia a la
  vez). Al correrla, usar `--deflated-sharpe 124` (trials acumulados + H1/H2 + esta).
- Código: `signals.funding_extreme_signals` + controles en `synthetic.funding_market_ohlcv`
  (capitulación→rebote vs no-informativo) — 5 tests.
- Comandos (POST-veredicto H1/H2):
```bash
python crypto/scripts/fetch_funding.py --symbol BTCUSDT --out crypto/data/BTCUSDT-funding.csv
python crypto/scripts/fetch_funding.py --symbol ETHUSDT --out crypto/data/ETHUSDT-funding.csv
python crypto/scripts/validate.py --data crypto/data/BTC_USDT-4h-flow.csv --funding crypto/data/BTCUSDT-funding.csv --strategy funding --compare --deflated-sharpe 124 --out crypto/data/rep_funding_btc.json
python crypto/scripts/validate.py --data crypto/data/ETH_USDT-4h-flow.csv --funding crypto/data/ETHUSDT-funding.csv --strategy funding --compare --deflated-sharpe 124 --out crypto/data/rep_funding_eth.json
python crypto/scripts/decide.py crypto/data/rep_funding_*.json
```

### H3b — proxy de cascadas de liquidación (DISEÑADA, no implementada — EN PAUSA)
Requiere construir el dataset de proxies (ΔOI de los dumps de métricas + volumen z + wick +
flip de funding). Se implementa SOLO si H3a resuelve y el programa sigue en pie (MAPA_EDGES B1).
**Actualización 2026-07-24 (post-investigación de bots verificados):** 19 búsquedas dirigidas
no encontraron NI UN paper ni backtest público del rebote post-cascada — solo anécdota. Prior
degradado **MEDIO → BAJO**. Queda en cola DETRÁS de H7/H8 (que sí tienen soporte estadístico
y datos gratis). No se implementa por ahora; 0 trials gastados.

### H7 — short de unlocks tipo cliff — CONGELADA ✅ 2026-07-24, IMPLEMENTADA
**Go del usuario (2026-07-24): "continua e invierte mi tesis... no importa el winrate sino
el pnl. Cualquier crypto, metodo, etc."** Spec EXACTA y gates congelados en
REGLAS_CONGELADAS.md → "SPECS CONGELADAS H7/H8" (winrate: se reporta, JAMÁS decide).
Código: `crypto/smc/events.py` (simulador por evento + gates) + `event_validate.py`
(specs) + `fetch_unlocks.py` (DeFiLlama) + `fetch_listings.py` + `fetch_data --futures`
+ 23 tests (controles positivo/negativo incluidos).

**Correr en el VPS — UN comando (H7+H8 juntos, son la misma pasada de datos):**
```bash
cd ~/nacho-crypto && bash crypto/scripts/vps_run_eventos.sh
```
Variante única pre-registrada de cada una: `event_validate.py ... --variant` — SOLO tras
registrar el veredicto base (cuenta como trial). Detalle original de la propuesta (histórico):
La arista con mejor evidencia de la investigación 2026-07-24 (RECURSOS §8): ~90% de 16.000
unlocks con impacto negativo (Keyrock); 88,5% negativo en 72h en 52 eventos de Binance (SSRN).
Mecanismo: oferta programada e IGNORADA por holders — el vendedor es forzoso y conocido con
semanas de anticipación. Contraparte: quien compra sin mirar el calendario de emisión.
- **Datos**: calendario histórico de unlocks (Tokenomist/CryptoRank/DropsTab, gratis) +
  klines del PERP (Binance/Bybit, gratis) + funding del perp (`fetch_funding.py`).
- **Universo**: unlocks cliff ≥ 2% del circulante, token con perp en Binance/Bybit.
- **Regla propuesta** (se congela EXACTA antes de bajar el calendario): short al open de la
  primera vela 4h posterior a T−48h; cover al open de la primera vela posterior a T+24h;
  stop 4% (cap inquebrantable del programa); riesgo 1%; SIN re-entradas.
- **Costos modelados**: taker 2 lados + slippage + **funding horario ACUMULADO** del período
  short (asesino documentado de este trade — un backtest independiente murió por esto).
- **Parámetros libres**: umbral % circulante, ventana de entrada, ventana de salida (3).
  Variantes permitidas: UNA tanda.
- **Gates**: event study IS 2023-2024 / OOS 2025-2026 (una pasada), ≥60 eventos para validar
  (menos → informativo, no operable), DSR con trials acumulados, Monte Carlo. Muerte
  pre-registrada: si el retorno medio del evento neto de costos ≤ 0 en OOS, o si el efecto
  vive solo en tokens sin perp líquido (no ejecutable).
- **Por qué puede fallar** (escrito HOY): priced-in creciente (mercado ya mira calendarios),
  funding negativo de alts come el short, muestra OOS corta.

### H8 — listing fade Binance — CONGELADA ✅ 2026-07-24, IMPLEMENTADA
Misma maquinaria y gates que H7 (`event_validate.py --strategy h8_listing`); se corre en la
misma pasada del VPS. Variante única: salida día 3 (`--variant`). Detalle original (histórico):
Evidencia: 24/27 listings 2025 con retorno negativo (media −44%); el pump es pre/intra-listing
y el retail que compra el día 1 es la contraparte. Mecanismo: distribución programada (insiders/
farmers venden el evento de liquidez).
- **Datos**: lista de listings spot Binance 2023-2026 (anuncios públicos) + klines del perp
  desde el listing + funding.
- **Regla propuesta**: short al open de la vela 4h siguiente al cierre del día 1 post-listing
  spot (si existe perp); salida por tiempo al día 7 o stop 4% (cap del programa); riesgo 1%.
- **Parámetros libres**: día de entrada, día de salida (2). UNA tanda de variantes.
- **Gates**: mismos que H7. Nota honesta pre-registrada: la vol del día 1 puede hacer que el
  stop 4% corte la mayoría de los trades — si eso pasa, es un FAIL del diseño a registrar,
  NO una licencia para aflojar el cap.

**Orden de la cola CONFIRMADO por el usuario (2026-07-24, "continua e invierte mi tesis"):
H7 → H8 → (recién después, si algo pasa) H3b. Trials: correr con `--deflated-sharpe 130`.**

## Backlog de hipótesis (prioridad = prior × dato disponible × costo de test)

| ID | Familia (reglas a congelar antes de correr) | Prior | Datos | Estado |
|----|---------------------------------------------|-------|-------|--------|
| **H1** | **Tendencia diaria long/flat** (SMA/Donchian 1d + vol-targeting; la formalización del benchmark que ganó) | ALTO (evidencia académica + NUESTROS datos IS) | ya los tenemos | LISTA para congelar reglas. OJO: su IS ya fue "visto" como benchmark → cuenta ese peek; el juicio real es OOS+DSR |
| **H2** | **Sweep 2.0 condicionado**: solo régimen `up_*` + confirmación de flujo (volume climax / taker-sell exhaustion en el barrido) | MEDIO (semilla up_hi n=20 + Osler + flow) | requiere fetch de taker-volume (gratis) | requiere infra F0 |
| **H3** | **Funding/OI extremos** → mean-reversion o squeeze (la versión perp del stop-hunt: cascadas de liquidación) | H3a corrida → RIP; H3b degradada a BAJO (2026-07-24) | funding gratis historia completa; OI corto | H3b EN PAUSA |
| **H4** | **Short side en régimen `dn_*`** (donchian/sweep espejo; investigación con `direction='both'`; live solo etapa perps) | MEDIO-BAJO | ya los tenemos | tras H1/H2 |
| **H5** | Estructuras BOS/CHoCH multi-TF (1h piso, maker-only, con filtro de régimen) | BAJO | ya + 1h fetch | cola |
| **H6** | Armónicos (Gartley/Bat, ratios FIJOS de la tabla del ebook, tolerancia única) | MUY BAJO | ya los tenemos | cola, opcional |
| **H7** | **Short de unlocks tipo cliff** (event-driven; spec congelada arriba) | ~~MEDIO-ALTO~~ | supply-step (plan C) | **RIP 2026-07-25 (NO_OPERAR, hard fail)** — ver RIP |
| **H8** | **Listing fade Binance** (event-driven; spec congelada arriba) | ~~MEDIO~~ | gratis | **RIP 2026-07-24 (NO_OPERAR, hard fail)** — ver RIP |
| **H9** | **Long contrarian post-purga de OI** (cascadas; spec congelada en REGLAS → H9; 10 majors, evento observado al cierre → causal puro) | BAJO (sin backtest público; costo de test mínimo) | OI 5m gratis (dumps binance.vision) | **CONGELADA ✅ 2026-07-25 — lista: `vps_run_cascadas.sh`** |

**Reglas del programa** (innegociables):
1. UNA familia por vez. Reglas y parámetros congelados en este archivo ANTES de tocar datos.
2. Presupuesto por familia: ≤5 parámetros, ≤2 tandas de variantes (todas anotadas).
3. Veredicto por `decide.py`; HARD fail = familia muerta y documentada (sección RIP).
4. El contador de trials es ACUMULATIVO entre familias → el `--deflated-sharpe N` crece
   siempre. Encontrar edge tarde exige que sea más fuerte. Así funciona la honestidad.
5. Nada pasa a LIVE sin GATE completo + dry-run. Investigar ≠ operar.
6. Cada familia muerta se escribe en el RIP con SU lección (qué aprendimos que NO es el edge).

## Infra F0 (habilita H1-H3; se construye ya, testeable con sintéticos)

- `fetch_data.py`: modo raw-Binance que además del OHLCV guarde `taker_buy_volume`
  (klines campo 9, gratis) — y `fetch_funding.py` para la historia completa de funding.
- `signals.py`: features de flujo (`volume_zscore`, `taker_buy_ratio`) causales + tests.
- `validate.py`: inferir el timeframe del CSV (1h/4h/1d) para anualizar bien las métricas.
- H1 como estrategia formal (`trend_daily_signals`): SMA/Donchian diario long/flat +
  vol-targeting opcional; control positivo/negativo propio; MISMOS gates.

## RIP — lo que ya sabemos que NO es el edge (se alimenta con cada kill)

- **Sweep-reversal long-only 4h price-only** (108 combos, 2019-2026, BTC+ETH): sin edge neto
  de costos. Lección: el patrón de entrada sin contexto de régimen/flujo no vale nada.
- **Donchian 4h long-only sin filtro de régimen**: estructura IS pero muere en chop 2024+ y
  no bate hold. Lección: la tendencia existe pero el timeframe/gestión importan más que la
  señal de ruptura.
- **H1 ma_timing (SMA100 1d + vol-target)** — 2026-07-24, NO_OPERAR: batió benchmarks IS en
  BTC con WF 75% y colapsó OOS 2024+ (ratio 0.09). Lección: TERCERA confirmación del quiebre
  de régimen 2024+ para trend en majors; el edge de los papers (muestras ≤2022) decayó.
- **H2 flow (taker imbalance q0.80/0.50)** — 2026-07-24, NO_OPERAR: en 4h el OOS fue
  negativo; en 1d ETH pasó OOS/IS (0.56) y batió B&H OOS pero 46 trades no sobreviven al
  DSR con ~119 trials. Lección: si el flujo tiene señal, vive en 1d y necesita un diseño con
  más señales por año (o más pares/historia) para ser validable — no torturar esta spec.
- **H3a funding extremo (q0.02/0.50, 4h)** — 2026-07-24, NO_OPERAR: el WF más consistente
  del programa (ETH 100% folds >0, BTC PF 2.68 IS) y aun así misma muerte OOS 2024+
  (ratios -0.35/-0.55, DSR 0.29/0.05). Lección: ni el posicionamiento escapa al quiebre de
  régimen; 37-48 trades tampoco dan poder. QUINTA familia con la misma firma → el hallazgo
  es el régimen, no las estrategias. Ver CHECKPOINT en REGLAS_CONGELADAS.
- **H7 short unlocks cliff (T−48h→T+24h, stop 4%, fuente supply-step)** — 2026-07-25,
  NO_OPERAR con 57 eventos ejecutables: IS expectancy +0.0000 (p=0.53, DSR 0.004), OOS
  +0.0009. Lección: el efecto documentado de unlocks (~90% negativo) vive en horizontes de
  ±30 DÍAS (drift lento), no en una ventana ejecutable de 72h alrededor del evento — que
  era la única compatible con nuestros caps. A diferencia de H8, acá NO hubo bruto que la
  ejecución matara: la ventana estaba vacía. Séptima familia; variante sin gastar; $0.
- **H8 listing fade (short T+24h→T+168h, stop 4%)** — 2026-07-24, NO_OPERAR con 178 eventos
  reales 2023-2026: IS expectancy +1.5% pero p=0.17 y DSR 0.06; OOS (n=116) expectancy
  −0.2%. La autopsia importa: **el 82.6% de los trades murió por el stop de 4%** (mediana
  −4.32% = stop+costos) — la vol post-listing de una alt cruza 4% casi siempre; los pocos
  que sobreviven ganan enorme (WR 21%, perfil invertido CORRECTO) pero no alcanzan
  significancia y el funding se lleva −0.48%/evento. Lección doble: (1) el "listing fade"
  de los estudios es de buy&hold a meses, no de un short ejecutable a 7 días; (2) **el cap
  de stop 4% —diseñado para majors 4h— es estructuralmente incompatible con vol de eventos
  en alts**: convierte una asimetría real en moneda cargada en contra. Cualquier futuro
  event-short necesitaría stop escalado por vol (cambio de regla INQUEBRANTABLE → decisión
  registrada del usuario, no un ajuste). El fallo fue pre-escrito en la spec ("si el stop
  4% corta la mayoría, es FAIL del diseño, NO licencia para aflojar el cap") — y se cumplió.
- **Scalping <1h con capital chico**: muerto por aritmética de costos, sin necesidad de test.
