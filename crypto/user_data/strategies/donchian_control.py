"""Estrategia freqtrade: Donchian/BMS breakout — CONTROL (benchmark).

Es el "break in market structure" del libro operado como continuacion = breakout de
canal Donchian (trend following con evidencia academica). Sirve de piso: si SmcSweep no
lo bate out-of-sample neto de costos, se descarta el sweep y se conserva este.

Misma deteccion compartida (``crypto.smc.signals.donchian_bms_signals``) y mismo esquema
de riesgo que SmcSweep. SPOT long-only.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
from freqtrade.strategy import IStrategy, stoploss_from_absolute

_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from crypto.smc.signals import donchian_bms_signals  # noqa: E402

DONCHIAN_PARAMS = dict(
    lookback=20,
    atr_period=14,
    sl_buffer_atr=1.5,
    sl_cap_pct=0.06,
    tp_r_multiple=2.0,
    time_stop_bars=18,
)
RISK_PER_TRADE = 0.01


class DonchianControl(IStrategy):
    INTERFACE_VERSION = 3

    timeframe = "4h"
    can_short = False
    process_only_new_candles = True
    use_custom_stoploss = True
    stoploss = -0.08  # backstop duro (cap de riesgo ~6% + margen) si custom_stoploss devuelve None
    minimal_roi = {}
    startup_candle_count = 60
    trailing_stop = False

    def populate_indicators(self, dataframe: pd.DataFrame, metadata: dict) -> pd.DataFrame:
        sig = donchian_bms_signals(dataframe, **DONCHIAN_PARAMS)
        dataframe["bms_enter_long"] = sig["enter_long"].astype(int)
        dataframe["bms_sl"] = sig["sl_price"]
        dataframe["bms_tp"] = sig["tp1_price"]
        dataframe["bms_tstop"] = sig["time_stop_bars"]
        return dataframe

    def populate_entry_trend(self, dataframe: pd.DataFrame, metadata: dict) -> pd.DataFrame:
        dataframe.loc[dataframe["bms_enter_long"] == 1, ["enter_long", "enter_tag"]] = (
            1,
            "donchian_bms_long",
        )
        return dataframe

    def populate_exit_trend(self, dataframe: pd.DataFrame, metadata: dict) -> pd.DataFrame:
        return dataframe

    def _entry_row(self, pair: str, trade) -> pd.Series | None:
        df, _ = self.dp.get_analyzed_dataframe(pair, self.timeframe)
        if df is None or df.empty:
            return None
        prior = df.loc[df["date"] <= trade.open_date_utc]
        return prior.iloc[-1] if not prior.empty else None

    def custom_stake_amount(
        self, pair: str, current_time, current_rate: float, proposed_stake: float,
        min_stake: float | None, max_stake: float, leverage: float, entry_tag: str | None,
        side: str, **kwargs,
    ) -> float:
        df, _ = self.dp.get_analyzed_dataframe(pair, self.timeframe)
        if df is None or df.empty:
            return proposed_stake
        sl = df.iloc[-1].get("bms_sl")
        if sl is None or pd.isna(sl) or current_rate <= 0:
            return proposed_stake
        risk_per_unit = current_rate - float(sl)
        if risk_per_unit <= 0:
            return proposed_stake
        equity = self.wallets.get_total_stake_amount() if self.wallets else max_stake
        stake = (equity * RISK_PER_TRADE / risk_per_unit) * current_rate
        stake = min(stake, max_stake)
        if min_stake:
            stake = max(stake, min_stake)
        return stake

    def custom_stoploss(
        self, pair: str, trade, current_time, current_rate: float, current_profit: float,
        **kwargs,
    ) -> float | None:
        row = self._entry_row(pair, trade)
        if row is None or pd.isna(row.get("bms_sl")):
            return None
        return stoploss_from_absolute(
            float(row["bms_sl"]), current_rate, is_short=trade.is_short, leverage=trade.leverage
        )

    def custom_exit(
        self, pair: str, trade, current_time, current_rate: float, current_profit: float,
        **kwargs,
    ) -> str | None:
        row = self._entry_row(pair, trade)
        if row is None:
            return None
        tp = row.get("bms_tp")
        if tp is not None and not pd.isna(tp) and current_rate >= float(tp):
            return "bms_target"
        tstop = row.get("bms_tstop")
        if tstop is not None and not pd.isna(tstop):
            bars = (current_time - trade.open_date_utc).total_seconds() / (4 * 3600)
            if bars >= float(tstop):
                return "bms_time_stop"
        return None
