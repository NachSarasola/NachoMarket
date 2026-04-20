"""Dashboard Streamlit para NachoMarket (TODO 6.2).

Ejecutar con: streamlit run dashboard/app.py
Acceder en:   http://localhost:8501

Muestra:
- Equity curve (PnL acumulado)
- PnL por estrategia
- Mercados activos con ROI
- Estado del bot (balance, drawdown, circuit breaker)
- Alertas recientes

Refresca automaticamente cada 30 segundos.
"""

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Agregar el directorio raiz al path para importar src/
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    import streamlit as st
    STREAMLIT_AVAILABLE = True
except ImportError:
    STREAMLIT_AVAILABLE = False
    print("Streamlit no instalado. Instalar con: pip install streamlit")
    sys.exit(1)

TRADES_FILE = Path("data/trades.jsonl")
STATE_FILE = Path("data/state.json")
WHALE_FILE = Path("data/whale_trades.jsonl")

# ──────────────────────────────────────────────
# Config de la pagina
# ──────────────────────────────────────────────
st.set_page_config(
    page_title="NachoMarket Dashboard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("📈 NachoMarket — Trading Dashboard")
st.caption(f"Ultima actualizacion: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC")

# Auto-refresh cada 30 segundos
st.markdown(
    """<meta http-equiv="refresh" content="30">""",
    unsafe_allow_html=True,
)

# ──────────────────────────────────────────────
# Helpers de carga
# ──────────────────────────────────────────────

@st.cache_data(ttl=30)
def load_trades(days: int = 30) -> list[dict]:
    """Carga trades.jsonl con cache de 30s."""
    if not TRADES_FILE.exists():
        return []
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    trades = []
    try:
        with open(TRADES_FILE, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    t = json.loads(line)
                    ts_str = t.get("timestamp", "")
                    if ts_str:
                        ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                        if ts >= cutoff:
                            trades.append(t)
                except Exception:
                    pass
    except OSError:
        pass
    return trades


@st.cache_data(ttl=30)
def load_state() -> dict:
    if not STATE_FILE.exists():
        return {}
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except Exception:
        return {}


@st.cache_data(ttl=60)
def load_whale_trades(hours: float = 6.0) -> list[dict]:
    if not WHALE_FILE.exists():
        return []
    import time
    cutoff = time.time() - hours * 3600
    whales = []
    try:
        with open(WHALE_FILE) as f:
            for line in f:
                try:
                    t = json.loads(line.strip())
                    if float(t.get("timestamp", 0)) >= cutoff:
                        whales.append(t)
                except Exception:
                    pass
    except OSError:
        pass
    return whales


# ──────────────────────────────────────────────
# Sidebar
# ──────────────────────────────────────────────
with st.sidebar:
    st.header("⚙️ Filtros")
    days_filter = st.slider("Dias a mostrar", 1, 30, 7)
    show_paper = st.checkbox("Incluir trades paper", value=True)
    st.divider()

    state = load_state()
    balance = state.get("balance_usdc", 400.0)
    st.metric("💰 Balance USDC", f"${balance:.2f}")
    st.metric("📅 Ultimo reconcile", state.get("last_reconcile", "N/A")[:10] if state.get("last_reconcile") else "N/A")

# ──────────────────────────────────────────────
# Cargar datos
# ──────────────────────────────────────────────
trades = load_trades(days=days_filter)
if not show_paper:
    trades = [t for t in trades if t.get("status") != "paper"]

# ──────────────────────────────────────────────
# Row 1 — Metricas principales
# ──────────────────────────────────────────────
col1, col2, col3, col4 = st.columns(4)

total_pnl = sum(t.get("pnl", 0.0) or 0.0 for t in trades)
total_trades = len(trades)
wins = sum(1 for t in trades if (t.get("pnl", 0) or 0) > 0)
win_rate = wins / total_trades if total_trades > 0 else 0.0

col1.metric("💵 PnL Total", f"${total_pnl:+.4f}", delta=None)
col2.metric("📊 Trades", total_trades)
col3.metric("🏆 Win Rate", f"{win_rate:.1%}")
col4.metric("🔄 Estrategias activas", len(set(t.get("strategy_name", "") for t in trades)))

# ──────────────────────────────────────────────
# Row 2 — Equity Curve
# ──────────────────────────────────────────────
st.subheader("📈 Equity Curve")

if trades:
    # Agrupar PnL por dia
    from collections import defaultdict
    daily_pnl_map: dict[str, float] = defaultdict(float)
    for t in sorted(trades, key=lambda x: x.get("timestamp", "")):
        ts_str = t.get("timestamp", "")
        if ts_str:
            day = ts_str[:10]
            daily_pnl_map[day] += t.get("pnl", 0.0) or 0.0

    if daily_pnl_map:
        dates = sorted(daily_pnl_map.keys())
        cumulative = []
        cum = 0.0
        for d in dates:
            cum += daily_pnl_map[d]
            cumulative.append({"date": d, "cumulative_pnl": cum, "daily_pnl": daily_pnl_map[d]})

        col_curve, col_bar = st.columns(2)

        with col_curve:
            st.line_chart(
                data={row["date"]: row["cumulative_pnl"] for row in cumulative},
                use_container_width=True,
            )

        with col_bar:
            bar_data = {row["date"]: row["daily_pnl"] for row in cumulative}
            st.bar_chart(bar_data, use_container_width=True)
else:
    st.info("Sin trades en el periodo seleccionado.")

# ──────────────────────────────────────────────
# Row 3 — PnL por Estrategia + Mercados Top
# ──────────────────────────────────────────────
col_strat, col_markets = st.columns(2)

with col_strat:
    st.subheader("🎯 PnL por Estrategia")
    if trades:
        from collections import defaultdict
        by_strat: dict[str, dict] = defaultdict(lambda: {"pnl": 0.0, "count": 0, "wins": 0})
        for t in trades:
            s = t.get("strategy_name", "unknown")
            p = t.get("pnl", 0.0) or 0.0
            by_strat[s]["pnl"] += p
            by_strat[s]["count"] += 1
            if p > 0:
                by_strat[s]["wins"] += 1

        rows = []
        for s, data in sorted(by_strat.items(), key=lambda x: x[1]["pnl"], reverse=True):
            wr = data["wins"] / data["count"] if data["count"] > 0 else 0
            rows.append({
                "Estrategia": s,
                "PnL": f"${data['pnl']:+.4f}",
                "Trades": data["count"],
                "Win%": f"{wr:.0%}",
            })
        st.dataframe(rows, use_container_width=True, hide_index=True)
    else:
        st.info("Sin datos.")

with col_markets:
    st.subheader("🏪 Top Mercados por PnL")
    if trades:
        from collections import defaultdict
        by_market: dict[str, float] = defaultdict(float)
        for t in trades:
            mid = t.get("market_id", "?")[:16]
            by_market[mid] += t.get("pnl", 0.0) or 0.0

        top_markets = sorted(by_market.items(), key=lambda x: x[1], reverse=True)[:10]
        rows = [{"Mercado": m, "PnL": f"${p:+.4f}"} for m, p in top_markets]
        st.dataframe(rows, use_container_width=True, hide_index=True)
    else:
        st.info("Sin datos.")

# ──────────────────────────────────────────────
# Row 4 — Whale Trades
# ──────────────────────────────────────────────
st.subheader("🐳 Whale Trades (ultimas 6h)")
whales = load_whale_trades()
if whales:
    whale_rows = []
    for w in sorted(whales, key=lambda x: x.get("timestamp", 0), reverse=True)[:20]:
        import datetime as _dt
        ts = _dt.datetime.fromtimestamp(w.get("timestamp", 0), tz=timezone.utc)
        whale_rows.append({
            "Hora": ts.strftime("%H:%M"),
            "Lado": w.get("side", "?"),
            "Tamaño": f"${w.get('size', 0):,.0f}",
            "Precio": f"{w.get('price', 0):.4f}",
            "Mercado": w.get("market_id", "?")[:16],
        })
    st.dataframe(whale_rows, use_container_width=True, hide_index=True)
else:
    st.info("Sin whale trades recientes. Instalar polyscan tracker para activar.")

# ──────────────────────────────────────────────
# Row 5 — Trades recientes
# ──────────────────────────────────────────────
with st.expander("📋 Trades Recientes (ultimos 20)"):
    if trades:
        recent = sorted(trades, key=lambda x: x.get("timestamp", ""), reverse=True)[:20]
        display = []
        for t in recent:
            ts = t.get("timestamp", "")[:19]
            pnl = t.get("pnl", 0.0) or 0.0
            display.append({
                "Hora": ts,
                "Estrategia": t.get("strategy_name", "?"),
                "Lado": t.get("side", "?"),
                "Precio": f"{t.get('price', 0):.4f}",
                "Size": f"${t.get('size', 0):.2f}",
                "PnL": f"${pnl:+.4f}",
                "Status": t.get("status", "?"),
            })
        st.dataframe(display, use_container_width=True, hide_index=True)
    else:
        st.info("Sin trades.")

st.divider()
st.caption("NachoMarket v2.0 — Hedge-Fund Grade Trading Bot for Polymarket")
