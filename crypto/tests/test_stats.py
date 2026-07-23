"""Tests de la estadistica anti-overfitting y Monte Carlo de ruina."""

from __future__ import annotations

import math

import numpy as np

from crypto.smc.stats import (
    _norm_cdf,
    _norm_ppf,
    deflated_sharpe_ratio,
    expected_max_sharpe,
    monte_carlo_ruin,
    probabilistic_sharpe_ratio,
)


def test_norm_ppf_cdf_roundtrip() -> None:
    for p in (0.01, 0.1, 0.25, 0.5, 0.75, 0.9, 0.99):
        assert abs(_norm_cdf(_norm_ppf(p)) - p) < 1e-6
    assert abs(_norm_ppf(0.975) - 1.959963985) < 1e-4  # z_0.975 conocido


def test_psr_higher_for_better_returns() -> None:
    rng = np.random.default_rng(0)
    good = rng.normal(0.02, 0.05, 500)   # Sharpe alto
    meh = rng.normal(0.001, 0.05, 500)   # Sharpe ~0
    assert probabilistic_sharpe_ratio(good) > 0.99
    assert probabilistic_sharpe_ratio(meh) < 0.9


def test_expected_max_sharpe_grows_with_trials() -> None:
    # Mas variantes probadas => el maximo esperado por azar sube.
    s10 = expected_max_sharpe(10, sharpe_std=0.1)
    s100 = expected_max_sharpe(100, sharpe_std=0.1)
    assert s100 > s10 > 0
    assert expected_max_sharpe(1, 0.1) == 0.0


def test_deflated_sharpe_penalizes_many_trials() -> None:
    rng = np.random.default_rng(1)
    rets = rng.normal(0.01, 0.05, 400)  # Sharpe por periodo ~0.2
    dsr_few = deflated_sharpe_ratio(rets, n_trials=1)["dsr"]
    dsr_many = deflated_sharpe_ratio(rets, n_trials=200, sharpe_std=0.1)["dsr"]
    assert dsr_few >= dsr_many, "mas trials => DSR menor (mas exigente)"


def test_monte_carlo_ruin_basic() -> None:
    # Estrategia con expectativa positiva: baja prob de ruina.
    r_good = [2.0, -1.0, 2.0, -1.0, 1.5, -1.0, 2.0, 1.0] * 20
    out = monte_carlo_ruin(r_good, n_sims=2000, risk_pct=0.01, initial=500, seed=0)
    assert out["trades"] == len(r_good)
    assert 0.0 <= out["prob_ruin"] <= 1.0
    assert out["final_return_p50"] > out["final_return_p5"]

    # Estrategia con expectativa negativa y riesgo alto: mas prob de ruina.
    r_bad = [-1.0, -1.0, -1.0, 1.0] * 20
    out_bad = monte_carlo_ruin(r_bad, n_sims=2000, risk_pct=0.05, initial=500, seed=0)
    assert out_bad["prob_ruin"] >= out["prob_ruin"]


def test_monte_carlo_empty() -> None:
    assert monte_carlo_ruin([], n_sims=100)["trades"] == 0
