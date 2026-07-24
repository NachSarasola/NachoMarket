"""Estrategia freqtrade: SMC Sweep-Reversal (turtle soup) — SPOT long-only.

Runtime de PRODUCCION (dry-run / live) para el VPS. La deteccion de senales es
EXACTAMENTE la misma que valida ``crypto/scripts/validate.py`` porque ambas importan
``crypto.smc.signals`` — no puede haber divergencia de senales entre backtest y live.

FLUJO DE VALIDACION OBLIGATORIO antes de arriesgar $1 (ver crypto/REGLAS_CONGELADAS.md):
  1. ``freqtrade backtesting`` sobre 2019-2023 con esta estrategia.
  2. Comparar con ``crypto/scripts/validate.py`` (deben coincidir las senales).
  3. OOS 2024+ una sola pasada; batir buy&hold y MA diaria.
  4. ``freqtrade trade`` en dry-run 4-8 semanas.
  5. Live con tamaño minimo y kill-switch.

Riesgo (la leccion de la fundida): stop OBLIGATORIO en toda posicion, riesgo 1% por
trade por distancia al stop, max_open_trades bajo, SIN apalancamiento (spot), y
protections (StoplossGuard/MaxDrawdown/CooldownPeriod) definidas en el config.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
from freqtrade.strategy import IStrategy, stoploss_from_absolute

# Poner la raiz del repo en el path para importar la deteccion compartida.
_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from crypto.smc.signals import smc_sweep_signals  # noqa: E402

# Parametros CONGELADOS (deben coincidir con DEFAULT_PARAMS['sweep'] de validate.py).
SWEEP_PARAMS = dict(
    lookback=20,
    swing_left=2,
    swing_right=2,
    atr_period=14,
    sl_buffer_atr=0.5,
    sl_cap_pct=0.04,
    require_equal=False,
    pda_lookback=60,
    use_pda_filter=True,
    time_stop_bars=12,
    single_target=True,
)
RISK_PER_TRADE = 0.01  # 1% del equity por trade (distancia al stop)


class SmcSweep(IStrategy):
    INTERFACE_VERSION = 3

    timeframe = "4h"
    can_short = False            # SPOT: solo largos (barrido de sell-side liquidity).
    process_only_new_candles = True
    use_custom_stoploss = True
    # BACKSTOP DURO: si custom_stoploss no puede leer la vela de entrada (devuelve None),
    # este stop estatico igual limita la perdida al nivel del cap de riesgo (~4%) + margen.
    # Nunca dejar -0.99 aca: seria quedarse sin red exactamente como en la fundida anterior.
    stoploss = -0.05
    minimal_roi = {}             # sin ROI fijo; salidas por custom_exit (target + time-stop).
    startup_candle_count = 120   # >= max(lookback, pda_lookback, atr_period) + margen.
    trailing_stop = False

    # ------------------------------------------------------------------ #
    # Indicadores / senales
    # ------------------------------------------------------------------ #
    def populate_indicators(self, dataframe: pd.DataFrame, metadata: dict) -> pd.DataFrame:
        sig = smc_sweep_signals(dataframe, **SWEEP_PARAMS)
        dataframe["smc_enter_long"] = sig["enter_long"].astype(int)
        dataframe["smc_sl"] = sig["sl_price"]
        dataframe["smc_tp"] = sig["tp1_price"]
        dataframe["smc_atr"] = sig["atr"]
        dataframe["smc_tstop"] = sig["time_stop_bars"]
        return dataframe

    def populate_entry_trend(self, dataframe: pd.DataFrame, metadata: dict) -> pd.DataFrame:
        dataframe.loc[dataframe["smc_enter_long"] == 1, ["enter_long", "enter_tag"]] = (
            1,
            "smc_sweep_long",
        )
        return dataframe

    def populate_exit_trend(self, dataframe: pd.DataFrame, metadata: dict) -> pd.DataFrame:
        # Salidas gestionadas por custom_stoploss + custom_exit.
        return dataframe

    # ------------------------------------------------------------------ #
    # Helpers de estado por trade
    # ------------------------------------------------------------------ #
    def _entry_row(self, pair: str, trade) -> pd.Series | None:
        """Devuelve la fila de la vela de entrada (con sl/tp congelados)."""
        df, _ = self.dp.get_analyzed_dataframe(pair, self.timeframe)
        if df is None or df.empty:
            return None
        prior = df.loc[df["date"] <= trade.open_date_utc]
        if prior.empty:
            return None
        return prior.iloc[-1]

    # ------------------------------------------------------------------ #
    # Sizing por riesgo (1% por distancia al stop)
    # ------------------------------------------------------------------ #
    def custom_stake_amount(
        self, pair: str, current_time, current_rate: float, proposed_stake: float,
        min_stake: float | None, max_stake: float, leverage: float, entry_tag: str | None,
        side: str, **kwargs,
    ) -> float:
        df, _ = self.dp.get_analyzed_dataframe(pair, self.timeframe)
        if df is None or df.empty:
            return proposed_stake
        row = df.iloc[-1]
        sl = row.get("smc_sl")
        if sl is None or pd.isna(sl) or current_rate <= 0:
            return proposed_stake
        risk_per_unit = current_rate - float(sl)  # long
        if risk_per_unit <= 0:
            return proposed_stake
        equity = self.wallets.get_total_stake_amount() if self.wallets else max_stake
        risk_budget = equity * RISK_PER_TRADE
        qty = risk_budget / risk_per_unit
        stake = qty * current_rate
        stake = min(stake, max_stake)
        if min_stake:
            stake = max(stake, min_stake)
        return stake

    # ------------------------------------------------------------------ #
    # Stop absoluto por trade (nunca se mueve en contra)
    # ------------------------------------------------------------------ #
    def custom_stoploss(
        self, pair: str, trade, current_time, current_rate: float, current_profit: float,
        **kwargs,
    ) -> float | None:
        row = self._entry_row(pair, trade)
        if row is None:
            return None
        sl = row.get("smc_sl")
        if sl is None or pd.isna(sl):
            return None
        # stoploss_from_absolute traduce el precio de stop a ratio relativo al rate actual.
        return stoploss_from_absolute(
            float(sl), current_rate, is_short=trade.is_short, leverage=trade.leverage
        )

    # ------------------------------------------------------------------ #
    # Salida por target unico + time-stop
    # ------------------------------------------------------------------ #
    def custom_exit(
        self, pair: str, trade, current_time, current_rate: float, current_profit: float,
        **kwargs,
    ) -> str | None:
        row = self._entry_row(pair, trade)
        if row is None:
            return None
        tp = row.get("smc_tp")
        if tp is not None and not pd.isna(tp) and current_rate >= float(tp):
            return "smc_target"
        # time-stop: cerrar si pasaron >= smc_tstop velas sin alcanzar target.
        tstop = row.get("smc_tstop")
        if tstop is not None and not pd.isna(tstop):
            bars = (current_time - trade.open_date_utc).total_seconds() / (4 * 3600)
            if bars >= float(tstop):
                return "smc_time_stop"
        return None
