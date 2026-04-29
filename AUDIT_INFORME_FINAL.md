# INFORME FINAL DE AUDITORIA — NachoMarket Bot

> Fecha: 2026-04-29 | Capital: $166 USDC | Estrategia activa: `rewards_farmer` | Modo: live/paper

---

## 1. RESUMEN EJECUTIVO

El bot está **sobredimensionado por un factor de ~5x** para su capital y estrategia actual. El código productivo realmente necesario para operar RF (rewards farming) con $166 es aproximadamente **~400 líneas**; el resto son módulos hedge-fund-grade (backtest, DuckDB warehouse, FRED, whale tracking, regime detection, toxic flow, VaR, correlation, stage machines, bandit allocators, A/B testing) que **nunca se ejecutan** o que se ejecutan pero **nadie consume sus outputs**.

**Problemas críticos activos:**
- Inventario de RF corrupto por bug en `record_fill` (BUY en NO se registra como YES).
- Leak masivo de órdenes: el tracking popea órdenes "live" como si estuvieran cerradas.
- `merge_positions` no mergea; vende al mercado perdiendo spread.
- Límite de 50 órdenes abiertas del circuit breaker nunca se aplica (`order_placed()` nunca se llama).
- Exposure hardcodeada al 70% del capital, contradiciendo la regla del 5%.

**Veredicto:** El bot no es seguro para live hasta corregir los bugs de inventario y orden tracking. Una vez corregidos, la estrategia RF es viable pero requiere simplificación drástica para reducir superficie de error y latencia.

---

## 2. CODIGO MUERTO / OVER-ENGINEERED

### A. Estrategias deshabilitadas (instanciadas pero nunca usadas)

| Archivo | Estado | Problema |
|---------|--------|----------|
| `src/strategy/market_maker.py` | Deshabilitada en `config/settings.yaml:26` | Toda la lógica de `_check_fills_and_reposition` en `main.py:748-857` es específica de MM y nunca corre. |
| `src/strategy/multi_arb.py` | Deshabilitada | Comentada en settings. |
| `src/strategy/stat_arb.py` | TODO 4.1, deshabilitada | `execute(self, signals, market_data)` tiene firma incorrecta (`market_data` como 2do arg). BaseStrategy no le pasa `market_data` a `execute`. Si se habilitara, crashea por argumentos inesperados. |
| `src/strategy/directional.py` | Deshabilitada | Comentada en settings. |
| `src/strategy/event_driven.py` | TODO 4.3, deshabilitada | `execute(self, signals, market_data)` misma firma rota que `stat_arb`. FREDClient no tiene API key configurada; `_is_high_impact_window()` siempre retorna `False`. |
| `src/strategy/copy_trade.py` | TODO 4.5, deshabilitada | Llama a `self._client.place_order()` en `copy_trade.py:175` y `244`. **Ese método NO EXISTE** en `PolymarketClient` (solo existen `place_limit_order` y `place_fok_order`). Crashearía inmediatamente si se habilitara. |

### B. Módulos enteros sin uso (ni siquiera se importan)

| Archivo | Líneas | Justificación |
|---------|--------|---------------|
| `src/backtest/engine.py` | 271 | TODO 3.2. Ningún archivo del proyecto lo importa. |
| `src/data/warehouse.py` | 365 | TODO 6.1. Ningún archivo lo importa. DuckDB se importa condicionalmente dentro de un método. |

### C. Módulos instanciados pero puros "calentadores de CPU"

En `main.py:210-215` se crean:

```python
self._regime_detector = MarketRegimeDetector()
self._toxic_flow = ToxicFlowDetector(...)
self._correlation = CorrelationTracker()
self._var_calc = VaRCalculator(...)
```

Y en `main.py:440-458` se actualizan **cada ciclo** (cada 30s):

```python
self._regime_detector.update(token_id, mid)
self._correlation.update(token_id, mid)
market_data["toxic_flow"] = self._toxic_flow.is_toxic(token_id)
```

**Nadie lee estos valores para la estrategia activa (RF).** El toxic flow se usa para saltear estrategias no-RF (línea 467). El regime detector solo afecta a MM (línea 464). Con RF como única estrategia, estos 4 objetos hacen cálculos y escrituras en vano cada 30 segundos.

También:
- `src/analysis/wall_detector.py` — importado solo por `market_maker.py` (deshabilitado).
- `src/analysis/performance_metrics.py` — importado por `backtest/engine.py` (muerto) y `self_review.py` (solo para métricas de MM).
- `src/analysis/cost_model.py`, `attribution.py`, `var.py` — muertos.

### D. External data (pura sobrecarga)

| Archivo | Uso real |
|---------|----------|
| `src/external/fred.py` | Solo importado por `event_driven.py` (deshabilitado). El calendario es estimado (no usa API real). |
| `src/external/polyscan.py` | Instanciado en `main.py:234` y poll cada 60s (`main.py:283`), pero `copy_trade` (único consumidor) está deshabilitado. Genera I/O a disco (`data/whale_trades.jsonl`) sin utilidad. |

### E. Risk/Strategy over-engineering

- `src/risk/strategy_monitor.py` — Kill switch por Calmar ratio. Con una sola estrategia y $166, matarla significa detener todo el bot. Overkill.
- `src/strategy/allocator.py` — Bandit allocator. Se evalúa diariamente (`main.py:287`), pero con una sola estrategia las allocations son triviales (100% a RF).
- `src/strategy/stages.py` — Stage machine. Se instancia en `main.py:176` pero no afecta la ejecución de RF.
- `src/strategy/repositioner.py` — Solo usado por MM (deshabilitado).
- `src/risk/blacklist.py` — Se instancia en `main.py:182` pero no se consulta en el ciclo de RF.

### F. Telegram bot inflado

`src/telegram/bot.py` tiene ~1700 líneas con comandos para estrategias muertas (`/arb`, `/copy`, `/event`, `/stages`, `/backtest`, etc.). La mayoría de los handlers no tienen efecto real porque las estrategias asociadas no están activas.

---

## 3. ESTRATEGIA DE REWARDS FARMING ACTUAL

**Archivo clave:** `src/strategy/rewards_farmer.py` (450 líneas)
**Orquestador:** `src/main.py` (1741 líneas)

### Pipeline por ciclo (cada 30s)

1. **Market discovery** (`main.py:1261-1284`):
   - `MarketAnalyzer.discover_markets()` → Gamma API (cache 15 min).
   - `enrich_with_rewards()` → CLOB API `/rewards/markets/current`.
   - `select_top_markets(n=5)` → scoring con peso 45% a rewards.

2. **Enriquecimiento WS** (`main.py:1119-1187`):
   - Para cada token del mercado, lee `OrderbookFeed.get_midpoint()` y `get_orderbook()`.
   - Inyecta `token_data` con mids, spreads y orderbook por token.

3. **RF.should_act** (`rewards_farmer.py:77-136`):
   - Filtra: `rewards_rate >= 0.0005`, `mid_price > 0`, `|mid - 0.50| <= 0.35`.
   - Detecta fase de mercado (`pre_game` vs `live`). Si es `live`, skip (salvo que tenga inventario).
   - Verifica `required_usd = min_size * mid` vs `side_capital = max_capital / 2`. Si no alcanza, fallback a single-sided.
   - Cap de mercados simultáneos: 5.

4. **RF.evaluate** (`rewards_farmer.py:138-215`):
   - Lee `rewards_max_spread` (en centavos de la API) y lo divide por 100 → `max_spread_cents`.
   - Calcula `global_mid = (yes_mid + no_mid) / 2`.
   - Calcula `distance = max_spread_cents * (1 - sqrt(target_score))` donde `target_score` depende del `reward_share` actual.
   - Calcula `side_size_usd = max(side_capital * 0.5, min_size * mid, 4.0)`.
   - Para cada token, calcula `bid_price = mid - distance`, ajusta por `best_ask` y `max_spread_cents`.
   - Genera `Signal` con `side="BUY"`, `post_only=True` implícito.
   - Si `use_two_sided` es False, hace `break` después del primer token.

5. **RF.execute** (`rewards_farmer.py:217-295`):
   - Cancela órdenes previas por `(market_id, token_id)` vía `cancel_market_orders`.
   - Coloca nueva orden `place_limit_order(token_id, side="BUY", price, size, post_only=True)`.
   - En paper mode, simula trade con status `"paper"`.
   - Si la orden se coloca OK, guarda `order_id → signal` en `self._pending_orders`.

6. **Post-ejecución en main.py**:
   - `_check_rf_inventory` (`main.py:955-1021`): itera `rf._pending_orders`, llama `get_order_status()` por cada una. Si está `ORDER_STATUS_MATCHED`, registra fill en inventario RF y notifica por Telegram.
   - `_check_merges` (`main.py:862-949`): si `min(yes_inv, no_inv) >= 5`, llama `client.merge_positions(yes_token_id, merge_size)`.

7. **Monitoreo de reward %** (`main.py:1022-1063`):
   - Cada 5 minutos consulta `/rewards/user/percentages`.
   - Si share < 5% en un mercado activo, loguea warning de rotación.

---

## 4. PROBLEMAS DE LA ESTRATEGIA DE FARMING

### Bug crítico #1 — Inventario RF corrupto

**Archivo:** `src/strategy/rewards_farmer.py:301-309`

```python
def record_fill(self, token_id: str, side: str, size: float, market_id: str) -> None:
    if market_id not in self._fill_inventory:
        self._fill_inventory[market_id] = {"yes": 0.0, "no": 0.0}
    inv = self._fill_inventory[market_id]
    if side == "BUY":
        inv["yes"] = inv.get("yes", 0.0) + size
    elif side == "SELL":
        inv["no"] = inv.get("no", 0.0) + size
```

**Problema:** `record_fill` ignora `token_id`. Cuando two-sided BID en NO se llena, `side == "BUY"`, por lo que suma a `"yes"` en vez de `"no"`. El inventario queda invertido. Como `_check_merges` depende de `min(yes, no)`, nunca detectará que tenemos ambos lados, o detectará merges fantasmas.

**Fix:** comparar `token_id` contra `tokens[0]["token_id"]` (YES) y `tokens[1]["token_id"]` (NO).

### Bug crítico #2 — Leak de órdenes en tracking

**Archivo:** `src/main.py:987`

```python
if status.get("status") in ("ORDER_STATUS_MATCHED", "live"):
    is_filled = status.get("status") == "ORDER_STATUS_MATCHED"
    if is_filled:
        ...
    filled_ids.append(order_id)
```

**Problema:** `"live"` está en la tupla de condiciones. Cualquier orden que aún esté viva en el CLOB se saca de `rf._pending_orders` y nunca más se revisa. Si se llena 2 minutos después, el bot no se entera. Esto es un **leak masivo**.

**Fix:** solo popear si el status es `ORDER_STATUS_MATCHED` o `CANCELLED` / `ORDER_STATUS_CANCELLED`. Las órdenes `LIVE` deben permanecer en tracking.

### Bug #3 — `merge_positions` no mergea

**Archivo:** `src/polymarket/client.py:856-910`

```python
def merge_positions(self, token_id: str, size: float) -> dict[str, Any]:
    """NOTA: Este metodo NO realiza un merge on-chain real ...
    En su lugar, coloca una orden GTC SELL (taker-friendly)"""
```

**Problema:** El método se llama `merge_positions` pero **vende al mercado**. Un merge on-chain de YES+NO en Polymarket da $1.00 por par. Vender YES a 0.49 y NO a 0.49 da ~$0.98 menos fees. Con $166 de capital, cada centavo cuenta. Además, si no hay comprador inmediato, la orden SELL taker-friendly puede no ejecutarse rápido.

**Fix:** usar el endpoint real de merge del SDK (`NegRiskAdapter`) o renombrar el método y ajustar expectativas.

### Bug #4 — Fórmula Q_min no implementada correctamente

**Archivo:** `src/strategy/rewards_farmer.py:26-28`

```python
SINGLE_SIDED_DIVISOR = 3.0  # Scaling factor c en la formula Q_min
```

El comentario dice que la fórmula es `Q_min = max(min(Q_one, Q_two), max(Q_one/3, Q_two/3))`, pero en ningún lado del código se calcula `Q_min` real. La estrategia solo calcula una distancia al mid y coloca BIDs. **No verifica que las órdenes efectivamente califiquen para rewards según la fórmula oficial.**

**Consecuencia:** Podríamos estar desplegando capital en órdenes que no scorean porque:
- La distancia es demasiado grande (S bajo).
- Un lado tiene mucho más size que el otro (Q_min dominado por el mínimo).

### Bug #5 — Mid global de NO incorrecto

**Archivo:** `src/strategy/rewards_farmer.py:163`

```python
global_mid = round((yes_mid + no_mid) / 2, 4) if (yes_mid > 0 and no_mid > 0) else float(market_data.get("mid_price", 0.5))
```

**Problema:** En mercados binarios, `no_mid` debería ser `1.0 - yes_mid`. Si el WS entrega `yes_mid=0.70` y `no_mid=0.30`, el promedio es `0.50`, lo que distorsiona el sizing y la distancia. El sizing se calcula sobre `global_mid=0.50` en vez de 0.70/0.30.

### Bug #6 — Estructura inconsistente en `_active_farms`

**Archivo:** `src/strategy/rewards_farmer.py:112` y `rewards_farmer.py:275-281`

En `should_act` (single-sided fallback):
```python
self._active_farms[condition_id] = {"mode": "single"}
```

En `execute` (normal):
```python
self._active_farms[signal.market_id][signal.token_id] = {"side": signal.side, "size": signal.size, "price": signal.price}
```

El mismo dict tiene estructuras de tipos distintos. No crashea Python, pero es una bomba de mantenimiento.

### Bug #7 — Distancia adaptativa invertida

**Archivo:** `src/strategy/rewards_farmer.py:342-375`

```python
if reward_share > 0.20:
    target_score = 0.72
elif reward_share > 0.10:
    target_score = 0.60
...
distance = max_spread_cents * (1.0 - math.sqrt(target_score))
```

**Problema:** Si ya tenemos 20%+ de share, ponemos target_score=0.72 (distancia corta: ~15% del max_spread). Esto nos pega al mid, donde la competencia es mayor y S puede bajar. La lógica debería ser: **alta share → podemos alejarnos un poco del mid sin perder participación**, manteniendo S alto. O mejor: calcular Q_min directamente y ajustar size, no solo distancia.

---

## 5. GESTION DE ORDENES

### ¿Sostiene órdenes correctamente?

**No.** El bot cancela y recoloca demasiado agresivamente.

En `rewards_farmer.py:228-235`:
```python
self._client.cancel_market_orders(
    condition_id=signal.market_id,
    token_id=signal.token_id,
)
```

Esto cancela **todas** las órdenes previas de ese token antes de colocar una nueva. En market making para rewards, la continuidad en el book es clave: cancelar y recolocar te saca de la cola FIFO y reduce el tiempo acumulado scoreando. La estrategia debería:
1. Consultar órdenes abiertas reales (`get_positions()`).
2. Solo recolocar si el precio cambió significativamente o la orden desapareció.

### ¿Hay leaks?

**Sí, masivo**, por el bug de `main.py:987` ya documentado en sección 4. Las órdenes vivas se pierden del tracking cada ciclo.

### ¿Cancela y recoloca bien?

Parcialmente. El `cancel_market_orders` por `condition_id + token_id` es granular y correcto, pero la política de "siempre cancelar antes de colocar" es ineficiente. Además:

- `_calc_bid_price` (`rewards_farmer.py:397-411`) hace un clamp post-only evitando cruzar el best_ask, pero **no re-verifica justo antes de `place_limit_order`**. Si el book se mueve entre `evaluate` y `execute` (pocos ms), la orden puede cruzar y ser rechazada como taker.
- No hay manejo de `ORDER_STATUS_CANCELLED` o `EXPIRED` en `_check_rf_inventory`. Las órdenes canceladas por el exchange (ej. por inactividad sin heartbeat) quedan huérfanas en `_pending_orders` hasta que... bueno, en realidad se las saca en el próximo ciclo por el bug del `"live"`.

### Heartbeat

**Correcto.** `client.py:117-158` envía heartbeat cada 5s. Esto evita que Polymarket cancele órdenes GTC por inactividad.

---

## 6. RIESGO Y CIRCUIT BREAKERS

### Circuit Breaker: ¿funciona?

**Parcialmente.** La lógica de drawdown diario, errores consecutivos y rolling drawdown está bien implementada en `src/risk/circuit_breaker.py`. Los thresholds lee de `config/risk.yaml`:
- `max_daily_loss_usdc: 8.3` (5% de $166) ✅
- `max_market_loss_1h_usdc: 2.075` ✅
- `loss_reserve_usdc: 20.0` ✅

**Pero hay dos fallas graves operativas:**

#### Falla #1 — Límite de órdenes abiertas inoperante

**Archivo:** `src/risk/circuit_breaker.py:249-255`

```python
def order_placed(self) -> None:
    self._open_orders += 1

def order_closed(self) -> None:
    self._open_orders = max(0, self._open_orders - 1)
```

**Nadie en todo el codebase llama a estos métodos.** Busqué en `main.py`, `client.py`, y todas las estrategias. `can_place_order()` (`circuit_breaker.py:220-224`) chequea `self._open_orders < self._max_open_orders` (50), pero `_open_orders` siempre es 0.

**Consecuencia:** El bot puede colocar ilimitadas órdenes. Con 5 mercados × 2 tokens = 10 órdenes, no es crítico ahora, pero si se aumenta `max_markets` o se habilitan estrategias adicionales, el bot puede saturar la API y su propio balance.

#### Falla #2 — Exposure total del PositionSizer al 70%

**Archivo:** `src/risk/position_sizer.py:156-167`

```python
def can_trade(self, current_exposure: float, capital: float, new_size: float = 0.0) -> bool:
    limit = capital * 0.70
    return (current_exposure + new_size) <= limit
```

El docstring dice "60%" pero el código dice `0.70` (70%). `_filter_signals` en `main.py:596` lo llama así:

```python
if not self._position_sizer.can_trade(projected_exposure, self._cached_balance, 0.0):
```

Con $166, permite **$116 de exposición**. La regla INQUEBRANTABLE del proyecto dice "Nunca arriesgar más del 5% del capital en un solo mercado". El límite por señal individual de `$20` en `main.py:617` amortigua un poco, pero la exposición agregada es excesiva.

#### Falla #3 — PnL estimation solo en SELLs

**Archivo:** `src/main.py:713-742`

```python
if trade.side == "BUY":
    return None  # BUY no genera PnL aun
```

El circuit breaker solo ve PnL cuando hay SELLs (o merges). Como RF acumula BUYs (BIDs), el CB no registra pérdidas durante la acumulación de inventario. Si el mercado se mueve en contra, el CB no se entera hasta que vendemos.

---

## 7. DEPENDENCIAS

### Librerías en `requirements.txt` que no se usan en runtime

| Librería | Estado | Evidencia |
|----------|--------|-----------|
| `duckdb>=0.10.0` | **MUERTA** | Solo importada condicionalmente dentro de `warehouse.py:connect()`. `warehouse.py` nunca se importa. |
| `streamlit>=1.32.0` | **MUERTA** | Ningún `import streamlit` en todo `src/`. |
| `aiohttp>=3.9.0` | **MUERTA** | Ningún `import aiohttp`. Se usa `requests` (en `markets.py`) y `urllib.request` (en `polyscan.py`). |
| `pytest>=8.0.0` | **Dev-only** | No se importa en runtime. Debería estar en `requirements-dev.txt`. |
| `pytest-asyncio>=0.23.0` | **Dev-only** | Ídem. |

### Falta en requirements.txt (pero se usa)

| Librería | Dónde se usa |
|----------|--------------|
| `requests` | `src/polymarket/markets.py:22` | Funciona por dependencia transitiva, pero debería listarse explícitamente. |

### Usadas correctamente

`py-clob-client-v2`, `websockets`, `anthropic`, `python-telegram-bot`, `pyyaml`, `tenacity`, `schedule`, `python-dotenv`.

---

## 8. RECOMENDACIONES DE SIMPLIFICACION

### Acción inmediata: borrar código muerto

Esto reduce la superficie de error y el tiempo de arranque:

```bash
# Estrategias muertas
rm src/strategy/market_maker.py
rm src/strategy/multi_arb.py
rm src/strategy/stat_arb.py
rm src/strategy/directional.py
rm src/strategy/event_driven.py
rm src/strategy/copy_trade.py
rm src/strategy/repositioner.py
rm src/strategy/ab_tester.py
rm src/strategy/allocator.py
rm src/strategy/stages.py

# Análisis puro overhead
rm -r src/analysis/

# External muerto
rm src/external/fred.py
rm src/external/polyscan.py

# Backtest / warehouse
rm -r src/backtest/
rm src/data/warehouse.py

# Risk overkill
rm src/risk/strategy_monitor.py
rm src/risk/blacklist.py
rm src/risk/market_profitability.py
```

**Nota:** `src/analysis/performance_metrics.py` se usa en `self_review.py`. Si se borra todo `src/analysis/`, hay que mover `performance_metrics.py` a `src/utils/` o integrarlo en `self_review.py`.

### Acción inmediata: limpiar main.py

Eliminar los siguientes bloques enteros:

- Líneas 26-29: imports de `correlation`, `regime_detector`, `toxic_flow`, `var`.
- Líneas 30-31: imports de `FREDClient`, `WhaleTracker`.
- Líneas 41-48: imports de estrategias muertas (dejar solo `RewardsFarmerStrategy`).
- Líneas 49-50: imports de `StageMachine`, `MarketBlacklist`.
- Líneas 155-165: factory de estrategias. Reemplazar por instanciación directa de `RewardsFarmerStrategy`.
- Líneas 176-181: `StageMachine`.
- Líneas 191-193: `PositionSizer` e `InventoryManager` (si se simplifica el risk).
- Líneas 210-215: regime, toxic, correlation, var.
- Líneas 219-231: `StrategyMonitor` y `StrategyAllocator`.
- Líneas 233-235: `WhaleTracker` y `FREDClient`.
- Líneas 283-283: `schedule.every(60).seconds.do(self._poll_whale_tracker)`.
- Líneas 287-287: `schedule.every(1).days.do(self._run_allocator_evaluation)`.
- Líneas 414-414: `self._check_fills_and_reposition(markets)` (específico de MM).
- Líneas 440-458: updates de regime/toxic/correlation.
- Líneas 748-857: método `_check_fills_and_reposition` completo.

### Simplificar Telegram bot

Reducir `telegram/bot.py` a los comandos que funcionan con RF:
- `/status`, `/pause`, `/resume`, `/kill`, `/balance`, `/positions`, `/review`.
- Eliminar handlers de `/arb`, `/copy`, `/event`, `/stages`, `/backtest`, `/whales`.

### Consolidar risk

Fusionar `PositionSizer` e `InventoryManager` en una sola clase `RiskManager` de ~100 líneas con:
- Límite de 5% por mercado (hardcodeado).
- Límite de 50% exposure total (ajustable).
- Tracking de órdenes abiertas (llamando `order_placed()` / `order_closed()`).

---

## 9. RECOMENDACIONES PARA FARMING OPTIMO

### Correcciones de bugs (obligatorias antes de live)

1. **Corregir `record_fill`** (`rewards_farmer.py:301`):
   ```python
   def record_fill(self, token_id: str, side: str, size: float, market_id: str, tokens: list[dict]) -> None:
       yes_id = tokens[0].get("token_id", "")
       key = "yes" if token_id == yes_id else "no"
       inv = self._fill_inventory.setdefault(market_id, {"yes": 0.0, "no": 0.0})
       if side == "BUY":
           inv[key] += size
       elif side == "SELL":
           inv[key] -= size  # o ajustar según convención de inventario
   ```

2. **Corregir `_check_rf_inventory`** (`main.py:987`):
   ```python
   status_val = status.get("status", "")
   if status_val == "ORDER_STATUS_MATCHED":
       ...
       filled_ids.append(order_id)
   elif status_val in ("ORDER_STATUS_CANCELLED", "CANCELLED"):
       filled_ids.append(order_id)  # sacar del tracking
   # NO popear si está LIVE
   ```

3. **Implementar tracking real de órdenes abiertas**:
   - En `execute`, guardar `order_id` con timestamp de colocación.
   - En cada ciclo, consultar `get_positions()` (1 llamada REST para TODAS las órdenes) en vez de `get_order_status()` N veces.
   - Cruzar la lista de órdenes abiertas del CLOB con `rf._pending_orders`. Las que desaparecieron del CLOB sin estar en "matched" se consideran canceladas.

4. **No cancelar a ciegas** (`rewards_farmer.py:228`):
   - Antes de `cancel_market_orders`, obtener órdenes abiertas del token.
   - Solo cancelar si el nuevo precio difiere del anterior en más de 1 tick, o si la orden ya no existe.
   - Esto preserva la cola FIFO y maximiza tiempo scoreando.

### Mejoras de la lógica de farming

5. **Implementar Q_min real**:
   ```python
   def calc_q_min(q_yes: float, q_no: float) -> float:
       return max(min(q_yes, q_no), max(q_yes / 3.0, q_no / 3.0))
   ```
   - Calcular `q_yes = size_yes * S(yes_mid, distance)` y `q_no = size_no * S(no_mid, distance)`.
   - Ajustar `size_yes` y `size_no` para que `calc_q_min(q_yes, q_no)` sea maximizado dado el capital disponible.

6. **Usar `1 - yes_mid` para NO**:
   - Reemplazar `global_mid = (yes_mid + no_mid) / 2` por cálculo por-token.
   - El sizing por lado debería ser `size_usd / token_mid`.

7. **Alejarse del mid de forma inteligente**:
   - La fórmula `S(v,s) = ((v-s)/v)^2` depende del precio del token `v`.
   - Para un token caro (v=0.80), una distancia de 2¢ da S = (0.78/0.80)^2 = 0.95. Para un token barato (v=0.20), la misma distancia da S = (0.18/0.20)^2 = 0.81.
   - La distancia óptima debería normalizarse por `v`: `distance = v * (1 - sqrt(target_score))`, no `max_spread_cents * constante`.

8. **Implementar merge on-chain real**:
   - Investigar `py-clob-client-v2` para `merge` via `NegRiskAdapter` o Relayer.
   - Si no existe en el SDK, documentar explícitamente que `merge_positions` es una venta de mercado y ajustar la expectativa de PnL.

9. **Exposición por mercado al 5%**:
   - En `_filter_signals` (`main.py:596`), cambiar la llamada a:
   ```python
   limit = self._cached_balance * 0.05
   if projected_exposure > limit:
       ...
   ```
   - O modificar `PositionSizer.can_trade` para aceptar `max_risk_pct` y pasar `0.05`.

10. **Llamar `order_placed()` / `order_closed()`**:
    - En `main.py:_handle_trade`, cuando una orden es colocada exitosamente: `self._circuit_breaker.order_placed()`.
    - Cuando una orden es cancelada o matched: `self._circuit_breaker.order_closed()`.
    - Esto activa el límite de 50 órdenes abiertas.

### Configuración óptima sugerida

Dado $166 y farming de rewards:

- `max_capital_per_market`: **$8.3** (5% de $166), no $30.
- `max_markets_simultaneous`: **4** (4 × $8.3 × 2 lados ≈ $66 desplegado, dejando cash para fees y slippage).
- `two_sided`: **True** (3× Q_min es correcto conceptualmente).
- `spread_pct_of_max`: **0.50** (estar en el medio del max_spread maximiza S sin perder participación).
- `refresh_seconds`: **300** (5 minutos). En rewards farming, el tiempo en el book importa más que el precio exacto. Solo recolocar si el mid se movió > 2 ticks o si la orden desapareció.
- `min_rewards_rate`: **0.0002** (0.02% diario). Con $8.3 por lado, 0.02% = $0.00166/día. Es marginal, pero evita mercados sin rewards reales.

---

*Fin del informe.*
