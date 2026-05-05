# poly-maker — Análisis Completo

> Repo: https://github.com/warproxxx/poly-maker  
> 1.1k ★ | 426 forks | MIT | 77.5% Python · 22.5% JS

---

## 1. ESTRUCTURA COMPLETA DE ARCHIVOS

```
poly-maker/
├── main.py                     # Entry point principal
├── trading.py                  # Toda la lógica de trading (core engine)
├── update_markets.py           # Scheduler que actualiza datos cada 1h
├── update_stats.py             # Stats de cuenta (PnL, earnings) cada 3h
├── pyproject.toml              # UV project config
├── uv.lock                     # Lockfile
├── .env.example                # Template de credenciales
├── .python-version             # Python 3.9.10
├── .gitignore
├── LICENSE                     # MIT
├── README.md
│
├── poly_data/                  ★ Core: datos y lógica
│   ├── __init__.py
│   ├── CONSTANTS.py            # MIN_MERGE_SIZE = 20
│   ├── abis.py                 # ABIs: ERC20, NegRiskAdapter, ConditionalToken
│   ├── global_state.py         # Estado global mutable (dicts compartidos)
│   ├── polymarket_client.py    # Cliente REST + on-chain (Web3)
│   ├── trading_utils.py        # get_best_bid_ask_deets, get_order_prices, sizing
│   ├── data_utils.py           # update_positions, update_orders, get/set_position
│   ├── data_processing.py      # Procesamiento de mensajes WebSocket
│   ├── utils.py                # get_sheet_df (Google Sheets → DataFrame)
│   └── websocket_handlers.py   # Conexiones WS (market + user)
│
├── poly_stats/                 ★ Stats de cuenta y earnings
│   ├── __init__.py
│   └── account_stats.py        # Posiciones, órdenes, earnings diarios
│
├── poly_utils/                 ★ Utilidades
│   ├── __init__.py
│   └── google_utils.py         # Auth Google Sheets (gspread + fallback read-only)
│
├── data_updater/               ★ Recolección de datos de mercado (separado)
│   ├── find_markets.py         # Escaneo de TODOS los mercados de Polymarket
│   ├── google_utils.py         # Google Sheets (misma lógica con credentials.json)
│   ├── trading_utils.py        # ClobClient + approveContracts()
│   └── erc20ABI.json           # ABI ERC20
│
└── poly_merger/                ★ Merging on-chain (Node.js)
    ├── README.md
    ├── merge.js                # Script de merge on-chain via Safe wallet
    ├── safe-helpers.js         # SignAndExecuteSafeTransaction
    ├── safeAbi.js              # Gnosis Safe ABI
    ├── package.json
    └── package-lock.json
```

---

## 2. TODAS LAS ESTRATEGIAS IMPLEMENTADAS

### 2.1 Market Making Puro (estrategia principal)

**Archivo:** `trading.py` → función `perform_trade(market)`

Es una estrategia de **market making con gestión de inventario** que opera ambos lados (YES/NO) del orderbook:

#### Flujo completo:

```
[1] POSITION MERGING
    Si YES > MIN_MERGE_SIZE (20) y NO > MIN_MERGE_SIZE → merge_positions()
    Esto quema el par YES+NO y recupera USDC al instante (sin perder spread)

[2] ORDERBOOK ANALYSIS (por cada outcome YES/NO)
    get_best_bid_ask_deets(market, token, min_size=100)
    Extrae: best_bid, best_ask, 2nd_best, top, bid_sum_within_n%, ask_sum_within_n%

[3] PRICE CALCULATION
    get_order_prices(): 
      - bid_price = best_bid + tick_size
      - ask_price = best_ask - tick_size
      - Si best_bid_size < min_size*1.5 → bid_price = best_bid (sin tuck)
      - Si best_ask_size < 250*1.5 → ask_price = best_ask (sin tuck)
      - Clamps: bid debe ser < top_ask, ask debe ser > top_bid
      - Si ask <= avgPrice → ask = avgPrice (nunca vender a pérdida)

[4] SIZE CALCULATION — get_buy_sell_amount()
    Dos fases:
    FASE ACUMULACIÓN (position < max_size):
      - buy_amount = min(trade_size, max_size - position)
      - Solo vende si position >= trade_size
    FASE DISTRIBUCIÓN (position >= max_size):
      - Siempre ofrece vender trade_size
      - Sigue comprando si total_exposure < max_size*2 (flexibilidad)
    Multiplicador: si bid_price < 0.10 → buy_amount *= multiplier (del sheet)

[5] SELL ORDER — con TAKE-PROFIT
    Si sell_amount > 0 y avgPrice > 0:
      - tp_price = avgPrice + (avgPrice * take_profit_threshold/100)
      - ask_price = max(tp_price, ask_price calculado)
      - Reprice si diff > 2% o si size < position*0.97

[6] BUY ORDER — con múltiples gates
    Si position < max_size y position < 250 y buy_amount >= min_size:
      - Gate de risk-off: si hay archivo positions/{market}.json → respetar sleep_till
      - Gate de volatilidad: si 3_hour > volatility_threshold → NO comprar
      - Gate de precio: si abs(price - sheet_value) >= 0.05 → cancelar todo
      - Gate de posición inversa: si tenemos rev_pos > min_size → no comprar
      - Gate de ratio: si overall_ratio < 0 → no comprar
      - Reprice si: best_bid > current_order_price | position+orders < 0.95*max_size | orders > size*1.01
```

### 2.2 Stop-Loss en caliente

**Antes de cada SELL**, el bot evalúa:
```
if (pnl < stop_loss_threshold AND spread <= spread_threshold) 
   OR (3_hour_volatility > volatility_threshold):
    → SELL a best_bid inmediato
    → Cancelar TODAS las órdenes del mercado (cancel_all_market)
    → Guardar positions/{market}.json con sleep_till = now + sleep_period horas
    → Durante sleep_period: NO comprar en ese mercado
```

### 2.3 Position Merging on-chain (Node.js)

**`poly_merger/merge.js`** ejecuta mergePositions en los smart contracts de Polymarket:

- **Regular markets**: `ConditionalTokens.mergePositions(collateral, HashZero, conditionId, [1,2], amount)`
- **Neg risk markets**: `NegRiskAdapter.mergePositions(conditionId, amount)`
- Usa **Gnosis Safe** wallet (signAndExecuteSafeTransaction)
- Se llama desde Python via `subprocess.run()`

### 2.4 Reward Maximizer (implícito en la selección de mercados)

**No es una estrategia separada de "solo rewards"**, pero el `data_updater/find_markets.py` calcula:

- `bid_reward_per_100`, `ask_reward_per_100` → qué reward genera cada nivel de precio
- `sm_reward_per_100` = media aritmética
- `gm_reward_per_100` = media geométrica (más conservadora, premia balance)
- Fórmula Q = ((max_spread - |p - mid|) / max_spread)² * (size + 100/p) → reward proporcional
- Ranking compuesto: `gm_reward_per_100` normalizado - volatility_sum normalizado + proximity_score

---

## 3. CÓMO SE CONECTA A POLYMARKET

### 3.1 REST API (py-clob-client v0.28.0)
```
PolymarketClient.__init__():
  host = "https://clob.polymarket.com"
  ClobClient(host, key=PK, chain_id=POLYGON, funder=BROWSER_ADDRESS, signature_type=2)
  creds = client.create_or_derive_api_creds()
  client.set_api_creds(creds)
```

### 3.2 WebSockets (doble conexión)
- **Market WS**: `wss://ws-subscriptions-clob.polymarket.com/ws/market`
  - Suscripción: `{"assets_ids": [token_ids]}`
  - Recibe: `book` (snapshot) y `price_change` (deltas)
  - Cada update dispara `asyncio.create_task(perform_trade(asset))`

- **User WS**: `wss://ws-subscriptions-clob.polymarket.com/ws/user`
  - Auth: `{"type":"user", "auth": {"apiKey", "secret", "passphrase"}}`
  - Recibe: `trade` (MATCHED/CONFIRMED/FAILED) y `order` (cambios)
  - Procesa fills y reconcilia inventario en tiempo real

### 3.3 On-chain (Web3.py + ethers.js)
- **Polygon RPC**: `https://polygon-rpc.com`
- **Contratos**: USDC (0x2791...), ConditionalTokens (0x4D97...), NegRiskAdapter (0xd91E...)
- **Merge**: subprocess llama a merge.js que usa ethers.js v5 para interactuar con Gnosis Safe

### 3.4 Google Sheets (Config dinámica)
- `poly_utils/google_utils.py`: Autenticación con `credentials.json` (Service Account)
- Fallback read-only via CSV export público sin credenciales
- Pestañas: `Selected Markets`, `All Markets`, `Volatility Markets`, `Hyperparameters`, `Full Markets`, `Summary`

### 3.5 Data Updater (proceso separado)
- Escanea TODOS los mercados de Polymarket via `client.get_sampling_markets()` (paginado)
- Para CADA mercado: get_order_book, calcula rewards potenciales, volatilidad histórica
- Volatilidad: `https://clob.polymarket.com/prices-history?interval=1m&market={token}&fidelity=10`
- Calcula volatilidad anualizada en 8 ventanas: 1h, 3h, 6h, 12h, 24h, 7d, 14d, 30d
- Escribe resultados a Google Sheets (All Markets, Volatility Markets)
- Corre cada 1 hora

---

## 4. GESTIÓN DE RIESGO

### 4.1 Stop-Loss paramétrico
| Parámetro | Significado |
|-----------|-------------|
| `stop_loss_threshold` | % de pérdida que dispara el stop (ej: -2%) |
| `spread_threshold` | Spread máximo para que el stop-loss sea ejecutable |
| `volatility_threshold` | Volatilidad 3h máxima antes de liquidar posición |
| `take_profit_threshold` | % de ganancia para ajustar ask (ej: 1%) |
| `sleep_period` | Horas que se bloquea la compra tras stop-loss |

### 4.2 Límites de posición
- **max_size**: tope por mercado (configurable por fila en el sheet)
- **Absolute cap**: 250 shares máximo (hardcodeado)
- **Exposure total**: position + other_token_position no debe superar max_size*2
- **min_size**: tamaño mínimo de orden para calificar a rewards

### 4.3 Risk-off period
- Tras un stop-loss, se bloquea la compra en ese mercado por `sleep_period` horas
- Se persiste en `positions/{market_id}.json`
- El bloqueo se chequea antes de cada buy order

### 4.4 Estados de trades (tracking en memoria)
- `performing`: set de trade_ids MATCHED pero no confirmados
- `performing_timestamps`: timestamps para detectar stale trades (>15s)
- Cancela órdenes duplicadas automáticamente
- `last_trade_update`: evita race conditions entre WS y API

### 4.5 Posición inversa
- Si `rev_pos['size'] > min_size` → no comprar más del token (ya tenemos el opuesto)
- Esta es una forma implícita de limitar exposure

---

## 5. SISTEMA DE ÓRDENES

### 5.1 Colocación
```
client.create_order(token_id, side, price, size, neg_risk=bool)
  → OrderArgs(token_id, price, size, side)
  → client.create_order(order_args, options=PartialCreateOrderOptions(neg_risk=True) si aplica)
  → client.post_order(signed_order)
```

### 5.2 Cancelación
- `cancel_all_asset(token_id)`: cancela todas las órdenes de un token
- `cancel_all_market(market_id)`: cancela todas las órdenes de un mercado completo
- Se cancela cuando: price_diff > 0.005, size_diff > 10%, o no hay orden existente

### 5.3 Política de repricing
- Solo cancela y recoloca si el cambio es significativo (>0.5¢ o >10% size)
- Esto minimiza gas y llamadas API innecesarias
- Compra: repricing si best_bid mejoró, si position+orders < 95% max_size, o si orden actual > 101% target
- Venta: repricing si diff > 2% vs tp_price o si sell_size < 97% position

### 5.4 Post-only implícito
- No usa explícitamente `post_only=True` en el SDK
- Pero la estrategia de pricing (siempre detrás del BBO) logra el mismo efecto

---

## 6. QUÉ TIENE POLY-MAKER QUE NachoMarket NO TIENE

### 6.1 Diferencias FUNDAMENTALES (arquitectura)

| Feature | poly-maker | NachoMarket |
|---------|-----------|-------------|
| Config dinámica | Google Sheets (editable sin redeploy) | YAML estático |
| Data pipeline | `data_updater` = proceso separado que escanea TODOS los mercados | `MarketAnalyzer` escanea solo los que cumplen filtros iniciales |
| WS por token | Una conexión market + una user, suscripción a lista de tokens | Una conexión market + health callbacks |
| Estado global | `global_state.py`: dicts mutables compartidos sin locks (salvo 1 para trading) | Objetos con estado interno, dataclasses, persistencia JSON |
| Lenguajes | Python + Node.js (merge) | Python puro (merge via Web3.py + NegRiskAdapter) |
| Merge on-chain | Gnosis Safe + ethers.js v5 (compatible con multisig) | Web3.py directo + fallback a FOK sell |

### 6.2 TÉCNICAS QUE NachoMarket NO TIENE

#### a) **Market Making con gestión activa de inventario bidireccional**
poly-maker mantiene órdenes BUY y SELL en AMBOS tokens (YES y NO) simultáneamente del mismo mercado, con:
- Sizing adaptativo: acumula hasta `max_size`, luego distribuye
- Repricing inteligente solo cuando el cambio es significativo (>0.5¢)
- Gate de posición inversa: si tenés YES no compres NO (y viceversa)

NachoMarket tiene `rewards_farmer.py` que es shadow quoting (solo rewards), NO market making real.

#### b) **Stop-loss por PnL + volatilidad + spread**
Evalúa TRES dimensiones simultáneas antes de liquidar:
- `pnl < stop_loss_threshold` → estás perdiendo
- `spread <= spread_threshold` → podés salir sin slippage excesivo
- `3_hour_volatility > volatility_threshold` → riesgo de gap

NachoMarket tiene `circuit_breaker.py` con drawdown diario y límites de órdenes, pero no stop-loss por mercado individual con esta granularidad.

#### c) **Risk-off period con persistencia local**
Tras un stop-loss, bloquea compras en ese mercado por N horas, guardando el timer en `positions/{market_id}.json`. Esto evita re-entrar en un mercado que acaba de liquidar.

NachoMarket no tiene este mecanismo.

#### d) **Cálculo de recompensas por nivel de precio**
`add_formula_params()` en `find_markets.py` calcula exactamente cuánto reward genera cada tick de precio dentro del max_spread usando la fórmula Q:
```
Q = ((max_spread - |price - mid|) / max_spread)² * (order_size + 100/price)
reward_per_100 = (Q / ΣQ) * daily_reward / 2 / order_size * 100/price
```
Y usa media geométrica (`gm_reward_per_100`) que penaliza el desbalance (si solo un lado da rewards, la geométrica es baja).

NachoMarket usa la API de Polymarket para `rewards_daily_rate` pero no desglosa por nivel de precio.

#### e) **Volatilidad anualizada multi-ventana**
Calcula volatilidad en 8 horizontes temporales: 1h, 3h, 6h, 12h, 24h, 7d, 14d, 30d usando log-returns y la fórmula:
```
σ_annualized = σ_log_returns * √(60 * 24 * 252)
```
El trading usa `3_hour` como señal principal.

NachoMarket no calcula volatilidad.

#### f) **Ratio de liquidez bid/ask**
```
overall_ratio = bid_sum_within_n_percent / ask_sum_within_n_percent
```
Si hay más vendedores que compradores (ratio < 0), no coloca buy orders. Es un filtro de presión de mercado.

#### g) **Ordermanager con detección de stale trades**
El sistema trackea trades por `(token_side)` con timestamps. Si un trade queda en estado MATCHED >15s sin confirmarse, lo remueve para destrabar el sistema.

#### h) **Orderbook en memoria con SortedDict**
Usa `sortedcontainers.SortedDict` para mantener el orderbook ordenado por precio y poder consultar `find_best_price_with_size()` en O(log n). Esto permite buscar el mejor precio que tenga al menos X size (crucial para market making).

#### i) **Two-sided quoting por token (NO solo por mercado)**
El bot coloca BUY+SELL en el MISMO token (ej: BUY YES a 0.48, SELL YES a 0.52), no solo BUY YES + BUY NO. Esto maximiza Q_min en la fórmula de Polymarket (min(Q_bid, Q_ask) vs Q_bid/3).

NachoMarket también hace two-sided por token en `rewards_farmer.py`, pero solo para rewards, no para market making.

#### j) **Multiplicador para precios bajos**
Si `bid_price < 0.10` y hay un `multiplier` configurado, multiplica el buy_amount. Esto compensa que shares baratos requieren más unidades para el mismo nocional.

#### k) **Aprobación on-chain de contratos**
`data_updater/trading_utils.py` tiene `approveContracts()` que aprueba USDC y ConditionalTokens para 3 direcciones de contrato distintas. NachoMarket no tiene aprobación automática.

---

## 7. PARÁMETROS Y THRESHOLDS CLAVE

### 7.1 Hyperparameters (Google Sheet)

| Parámetro | Descripción | Impacto |
|-----------|-------------|---------|
| `stop_loss_threshold` | % PnL negativo que dispara stop | Controla pérdida máxima por posición |
| `spread_threshold` | Spread máximo para ejecutar stop-loss | Evita slippage en mercados ilíquidos |
| `volatility_threshold` | Volatilidad 3h máxima tolerada | Filtro de riesgo sistémico |
| `take_profit_threshold` | % ganancia para ask price | Define cuándo vender con ganancia |
| `sleep_period` | Horas de bloqueo post stop-loss | Evita re-entrada prematura |

### 7.2 Por mercado (Selected Markets sheet)

| Columna | Uso | Valor típico |
|---------|-----|-------------|
| `trade_size` | Tamaño base de orden | 10-50 USDC |
| `max_size` | Tope de posición acumulada | 50-200 USDC |
| `min_size` | Mínimo para rewards | Dado por Polymarket |
| `max_spread` | Spread máximo para rewards | Dado por Polymarket (ej: 3.5 → 3.5%) |
| `tick_size` | Tick mínimo del mercado | 0.01 o 0.001 |
| `multiplier` | Multiplicador para precios < 0.10 | 2-5 |
| `neg_risk` | Si es mercado neg risk | TRUE/FALSE |
| `1_hour` - `30_day` | Volatilidad en 8 ventanas | Calculado automático |
| `gm_reward_per_100` | Reward medio geométrico por 100 shares | Calculado automático |
| `param_type` | Tipo de hyperparámetros a aplicar | string (key de Hyperparameters) |

### 7.3 Constantes en código

| Constante | Archivo | Valor |
|-----------|---------|-------|
| `MIN_MERGE_SIZE` | CONSTANTS.py | 20 (shares) |
| Stale trade timeout | main.py | 15 segundos |
| Update positions/orders | main.py | cada 5 segundos |
| Update markets | main.py | cada 30 segundos |
| Price diff para cancelar | trading.py | 0.005 (>0.5¢) |
| Size diff para cancelar | trading.py | 10% |
| Cap absoluto de posición | trading.py | 250 shares |
| Rango de precios aceptable | trading.py | [0.1, 0.9] |
| TP diff para recolocar | trading.py | 2% |
| Data updater interval | update_markets.py | 1 hora |
| Stats updater interval | update_stats.py | 3 horas |
| Gas limit merge | merge.js | 10,000,000 |
| Min size threshold | find_markets.py | 0.75 (maker_reward) |

---

## 8. DEPENDENCIAS (pyproject.toml)

```
py-clob-client==0.28.0     # SDK oficial (NachoMarket usa py_clob_client también)
python-dotenv==1.2.1
pandas==2.3.3
gspread==6.2.1             # Google Sheets (NACHOMARKET NO TIENE)
gspread-dataframe==4.0.0
sortedcontainers==2.4.0   # SortedDict para orderbook (NACHOMARKET NO TIENE)
eth-account==0.13.7
eth-utils==5.3.1
poly_eip712_structs==0.0.1
py_order_utils==0.3.2
requests==2.32.5
websockets==15.0.1         # WebSocket client (misma lib que NachoMarket)
cryptography==46.0.3
google-auth==2.42.1        # (NACHOMARKET NO TIENE)
web3==7.14.0               # Web3.py (NachoMarket también lo usa)
```

---

## 9. RESUMEN: LO MÁS VALIOSO PARA NachoMarket

### Técnicas de ALTO impacto (fácil de implementar):

1. **Stop-loss por volatilidad 3h + spread**: poly-maker no solo mira PnL, sino que liquida preventivamente si la volatilidad explota (aunque el PnL no sea negativo aún). Esto es un "circuit breaker por mercado" que NachoMarket no tiene.

2. **Risk-off period con sleep_till**: simple y efectivo. Archivo JSON por mercado con timestamp de re-entrada. Evita el "whipsaw" de recomprar justo después de un stop-loss.

3. **Ratio bid_sum / ask_sum**: si la presión vendedora es dominante, no compres. Filtro sencillo de microestructura de mercado.

4. **SortedDict para orderbook**: NachoMarket usa listas de tuplas. Con `sortedcontainers` tendrías búsqueda O(log n) para find_best_price_with_size, que es la operación más frecuente.

5. **Volatilidad anualizada multi-ventana**: la API de Polymarket ya tiene el endpoint `/prices-history`. Calcular log-returns y anualizar es ~15 líneas de código. Te da 8 métricas de riesgo por mercado.

### Técnicas de MEDIO impacto (requiere más refactor):

6. **Google Sheets como config dinámica**: permite ajustar parámetros sin redeploy. poly-maker ya tiene el fallback read-only implementado.

7. **Market making real (no solo rewards farming)**: poly-maker mantiene spreads tight y gestiona inventario activamente. NachoMarket podría agregar un modo "hybrid" que haga shadow quoting + market making en los mercados más líquidos.

8. **Data updater como proceso independiente**: escanear TODOS los mercados cada 1h y rankearlos por reward/volatilidad permite descubrir oportunidades que un filtro estático perdería.

### Técnicas que NO recomendaría copiar:

- **Gnosis Safe merge via Node.js**: NachoMarket ya tiene su propio merger en Python con NegRiskAdapter y Web3.py, que es más mantenible.
- **Estado global mutable sin locks**: poly-maker usa dicts globales compartidos entre threads. NachoMarket ya tiene una arquitectura más limpia con dataclasses y locks explícitos.
