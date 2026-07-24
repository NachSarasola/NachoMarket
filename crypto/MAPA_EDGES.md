# Mapa de edges explotables + árbol de decisión del programa

> Complemento de `HIPOTESIS.md` (qué corremos AHORA) y `ROADMAP.md` (etapas por capital).
> Este documento responde: ¿cuál es el universo COMPLETO de edges accesibles para nosotros,
> por qué existiría cada uno (quién pierde del otro lado), y qué hacemos ante CADA resultado
> posible? Un edge sin contraparte identificable es un mito por definición.

---

## A. Árbol de decisión inmediato (tras correr H1/H2 en el VPS)

```
decide.py sobre reportes H1 (ma_timing 1d) y H2 (flow 4h/1d), BTC+ETH
│
├─ H1 PASS y H2 PASS
│   → portfolio.py: correlación de equities H1↔H2
│     ├─ corr < 0.7 → mini-cartera: dry-run de AMBAS (riesgo 0.5%/trade cada una)
│     └─ corr ≥ 0.7 → dry-run SOLO de la de mejor OOS; la otra al banco de suplentes
├─ H1 PASS, H2 FAIL   → dry-run H1 · H2 al RIP con lección · H3 se congela y corre (research)
├─ H1 FAIL, H2 PASS   → dry-run H2 · H1 al RIP · H3 idem
├─ AJUSTE_UNICO (soft) → la ÚNICA variante permitida de esa familia → re-correr → veredicto final
└─ AMBAS FAIL
    → RIP con lecciones · congelar y correr H3 (funding/cascadas)
      ├─ H3 PASS → dry-run H3 (nota: lado live requiere perps → Hyperliquid, $10 min)
      └─ H3 FAIL → **strategic checkpoint**: precio+flujo+posicionamiento agotados en swing.
         Decisión honesta pre-escrita: (a) exposición pasiva DCA/hold (lo único que batió
         todo), (b) acumular capital → Etapa 3 carry ($5k+), (c) nuevas familias SOLO de la
         sección B con prior ≥ MEDIO y datos nuevos (no re-minar las muertas).
```

**Pasar un PASS a dry-run implica** (trabajo mío, solo tras el GO): wrapper freqtrade de la
estrategia ganadora (hoy solo existen SmcSweep/DonchianControl), config 1d o 4h, baseline
para `weekly_review.py` (report JSON del validate ganador), y checklist GATE 2 de
REGLAS_CONGELADAS. Nada de esto se construye antes del veredicto (código muerto = deuda).

**Cadencia**: research continuo (nada vivo) · dry-run 4-8 semanas mínimo · live $10-20/posición
· cambios de parámetros en vivo SOLO trimestrales. El contador de trials nunca se resetea.

---

## B. Atlas de familias de edge (universo completo, en profundidad)

Formato: **mecanismo → contraparte (quién paga) → evidencia → datos → piso de capital →
nuestro plan → estado**.

### B1. Conductuales / microestructura de stops
*Contraparte: retail apalancado que pone stops en lugares obvios y persigue precio.*

| Edge | Detalle | Estado |
|---|---|---|
| Stop-clustering spot (turtle soup) | Osler documenta el clustering, pero NUESTRO test dice: en 4h price-only NO sobrevive costos | **RIP (GATE 1)** — lección: el patrón sin contexto no monetiza |
| **Cascadas de liquidación en perps** | La versión moderna y VIOLENTA del mismo fenómeno: 3.5%/día de longs liquidados (BitMEX, arXiv 2102.04591); overshoot → reversión en horas-días. Sin backtest público: el que lo valide, lo cobra. Datos: proxies gratis (ΔOI + volumen z + wick + flip de funding; el feed directo de liquidaciones está truncado por los exchanges) | **H3b — siguiente en cola** tras H1/H2. Research: gratis. Live: perps ($10 Hyperliquid) |
| Failed breakouts condicionados a flujo | Solo si H2 demuestra que el taker-flow tiene información: re-testear el sweep EXIGIENDO agotamiento de flujo en el barrido (la semilla up_hi wr80% n=20 del GATE 1) | **H2b — condicional a H2 PASS** |

### B2. Tendencia / momentum
*Contraparte: under-reaction — el dinero entra lento y en manada; nadie descuenta todo de golpe.*

| Edge | Detalle | Estado |
|---|---|---|
| **MA-timing diario lento (long/flat)** | Detzel FM2021 bate B&H; nuestras 4 corridas del GATE 1 lo confirmaron como benchmark. Señal lenta > rápida (RM 2026) | **H1 — CORRIENDO** (spec congelada) |
| TSMOM semanal 1-8 sem | Liu-Tsyvinski NBER. Variante más lenta de H1; solo vía ventana trimestral si H1 da señales de vida pero falla borde | Suplente de H1 |
| Momentum cross-sectional (BTC vs ETH vs SOL) | Long el más fuerte / short el más débil. Contra: correlación ~0.8 entre majors deja poco spread; necesita perps para el short; los costos del rebalanceo muerden | Prior MEDIO-BAJO — cola, tras Etapa 2 |
| Breakout Donchian 4h | Estructura IS real (75% folds >0) pero murió en OOS 2024+ | **RIP** — lección: el timeframe/gestión importan más que la señal |

### B3. Posicionamiento / flujos forzados
*Contraparte: flujos insensibles a precio — liquidados forzosos, hedgers, emisores, horarios.*

| Edge | Detalle | Estado |
|---|---|---|
| **Taker-imbalance momentum** | JFM 2026: el flujo agresor agregado predice 1d-1sem. Dato gratis en klines | **H2 — CORRIENDO** (spec congelada) |
| **Funding extremo contrarian** | BIS WP1087: carry alto = posicionamiento saturado → desapalancamiento. Señal = cola extrema (funding <0 profundo → rebotes 30-180d). Pocas observaciones: usarlo como FILTRO/condición, no sistema | **H3a — spec a congelar tras H1/H2** (datos ya bajables con fetch_funding.py) |
| OI como estado de fragilidad | OI alto + funding alto = mercado frágil (gasolina para cascadas). No señal standalone (dato misquoteado, arXiv 2310.14973): variable de estado de H3 | Integrado en H3 |
| Seasonality horaria/semanal | Quantpedia: 21-23 UTC ~33-40% anual BRUTO — 2 trades/día = los fees se lo comen standalone. Como OVERLAY de ejecución (cronometrar entradas/salidas que ya ibas a hacer) cuesta CERO fees extra | Overlay de ejecución — se agrega a cualquier estrategia PASS en su segunda iteración trimestral |
| Stablecoin inflows | 1 paper (arXiv 2411.06327), horizonte horas; fuente gratis fiable dudosa | Watchlist — BAJO |
| Flujos de ETF (2024+) | Dato público diario, pero solo ~2.5 años de historia → NO puede pasar nuestro diseño IS/OOS todavía | Watchlist — revisitar 2027 con 4+ años |

### B4. Primas de riesgo (cobrar por dar un servicio, no por predecir)
*Contraparte: apalancados dispuestos a PAGAR por su posición (funding) o por seguro (opciones).*

| Edge | Detalle | Estado |
|---|---|---|
| **Carry delta-neutral (funding arb)** | El único edge "grande" con paper de Management Science detrás (>10% anual promedio, picos >40%). No predice nada: cobra el peaje del apalancamiento ajeno. Riesgos: funding negativo sostenido, basis, liquidación de la pata short, contraparte | **Etapa 3 ($5k+)** — EL DESTINO del programa. Diseño en ROADMAP.md |
| Venta de volatilidad (opciones) | Prima real pero cola catastrófica: exactamente el perfil short-vol que te fundió en Polymarket. Con capital chico = ruina esperada | **DESCARTADO permanente** para este programa |

### B5. Estructuralmente fuera de alcance (saberlo también es edge)
*Por qué NO perseguimos esto — cada línea nos ahorra meses:*

- **HFT / latencia / arb triangular / MEV**: carrera de infraestructura; el retail es la liquidez, no el competidor.
- **Market making con inventario**: adverse selection — la lección de $162 de Polymarket, medida académicamente (VPIN). No se vuelve.
- **Arb cross-exchange**: capital dividido, riesgo de retiro/contraparte, spreads que desaparecen en ms.
- **Whale/wallet-following**: 97% de líderes ganan, 44% de copiadores. Estructuralmente anti-retail. MITO (0 trials).
- **Armónicos / CVD-divergencia**: cero evidencia con fees; grados de libertad ocultos. MITO (0 trials).
- **ML end-to-end (predecir precio con redes)**: fábrica de overfitting con nuestro tamaño de muestra; el factor zoo crypto muere 70-80% OOS incluso con features "significativas".
- **News/NLP/sentimiento**: latencia y datos de pago; el técnico ya adelanta al fundamental en nuestros horizontes.

---

## C. Secuencia maestra (de hoy a $5k+)

1. **AHORA** → correr H1/H2 en el VPS (bloque en HIPOTESIS.md) → `decide.py` → rama del árbol A.
2. **+1-2 semanas** → si hay PASS: wrapper freqtrade + dry-run (GATE 2 en marcha). En paralelo,
   congelar spec H3 (funding extremo + proxy cascadas) y correrla (research no espera al dry-run).
3. **+4-8 semanas** → GATE 2: fills reales ≈ simulados + weekly_review sin KILL → live $10-20.
4. **Trimestral** → ventana única de cambios: overlay de seasonality a lo que esté vivo,
   variantes suplentes (TSMOM semanal), tercer par si pasa los criterios de TESIS.
5. **$500-2k** → Etapa 2 (ROADMAP): shorts Hyperliquid (funding modelado), H2b/fvg si validan,
   cartera de 2 estrategias descorrelacionadas máx.
6. **$5k+** → Etapa 3: carry delta-neutral como segunda pata. La curva del programa pasa de
   "una estrategia direccional" a "dirección + prima de riesgo" — ahí recién hay un negocio.

**Métricas de éxito del PROGRAMA** (no del PnL): hipótesis resueltas por trimestre (PASS o RIP
con lección), trials gastados vs presupuesto, capital preservado, y divergencia dry-run↔backtest.
Un trimestre que mata 3 mitos con $0 perdidos es un buen trimestre.

## D. Registro de decisiones de este mapa

- 2026-07-24: creado post-GATE 1 con priors calibrados por evidencia (RECURSOS §7bis).
  Descartes permanentes: venta de vol, MM con inventario, HFT/MEV, whales, armónicos, CVD,
  ML end-to-end, news. Toda reversión de un descarte exige evidencia nueva peer-reviewed y
  cuenta como trial doble (castigo por revivir muertos).
