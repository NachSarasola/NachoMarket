"""Tests de la validacion multi-par / correlacion (portfolio.py)."""

from __future__ import annotations

import importlib.util
import pathlib

import numpy as np
import pandas as pd

_P = pathlib.Path(__file__).resolve().parents[1] / "scripts" / "portfolio.py"
_spec = importlib.util.spec_from_file_location("portfolio", _P)
portfolio = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(portfolio)


def _equity(rets: np.ndarray, start: float = 500.0) -> pd.Series:
    idx = pd.date_range("2021-01-01", periods=len(rets) + 1, freq="4h", tz="UTC")
    eq = start * np.cumprod(np.concatenate([[1.0], 1 + rets]))
    return pd.Series(eq, index=idx)


def test_identical_curves_are_highly_correlated() -> None:
    rng = np.random.default_rng(0)
    r = rng.normal(0, 0.01, 2000)
    eqs = {"BTC": _equity(r), "ETH": _equity(r.copy())}
    corr = portfolio.correlation_matrix(eqs)
    assert corr.loc["BTC", "ETH"] > 0.95
    highs = portfolio.high_corr_pairs(corr)
    assert ("BTC", "ETH", corr.loc["BTC", "ETH"].round(3)) in highs or highs


def test_independent_curves_low_correlation() -> None:
    rng = np.random.default_rng(1)
    eqs = {"BTC": _equity(rng.normal(0, 0.01, 3000)),
           "ETH": _equity(rng.normal(0, 0.01, 3000))}
    corr = portfolio.correlation_matrix(eqs)
    assert abs(corr.loc["BTC", "ETH"]) < 0.2
    assert portfolio.high_corr_pairs(corr) == []


def test_daily_returns_downsamples() -> None:
    r = np.random.default_rng(2).normal(0, 0.01, 600)  # 100 dias (6 velas 4h/dia)
    dr = portfolio.daily_returns(_equity(r))
    assert 90 < len(dr) < 110  # ~100 retornos diarios


def test_combined_equity_diversifies_independent() -> None:
    rng = np.random.default_rng(3)
    eqs = {"A": _equity(rng.normal(0.0002, 0.02, 3000)),
           "B": _equity(rng.normal(0.0002, 0.02, 3000))}
    comb = portfolio.combined_equity(eqs, 500.0)
    assert not comb.empty
    # La vol de la cartera equal-weight de 2 series independientes es menor que la de cada una.
    comb_vol = comb.pct_change().dropna().std()
    a_vol = portfolio.daily_returns(eqs["A"]).std()
    assert comb_vol < a_vol
