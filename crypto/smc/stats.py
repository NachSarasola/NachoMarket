"""Estadistica anti-overfitting y de riesgo de ruina (numpy puro, sin scipy).

- Probabilistic / Deflated Sharpe Ratio (Bailey & Lopez de Prado): corrige el Sharpe por
  la longitud de la muestra, el sesgo/curtosis de los retornos y el NUMERO DE VARIANTES
  probadas (multiple-testing). Un Sharpe alto tras probar 30 configuraciones vale mucho
  menos que el mismo Sharpe en la primera.
- Monte Carlo bootstrap del orden/seleccion de trades -> distribucion de drawdown, retorno
  final y PROBABILIDAD DE RUINA. Al servicio de "preservacion de capital".
"""

from __future__ import annotations

import math

import numpy as np

_EULER = 0.5772156649015329


def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _norm_ppf(p: float) -> float:
    """Inversa de la CDF normal estandar (algoritmo de Acklam, error < 1e-9)."""
    if p <= 0.0:
        return -math.inf
    if p >= 1.0:
        return math.inf
    a = [-3.969683028665376e+01, 2.209460984245205e+02, -2.759285104469687e+02,
         1.383577518672690e+02, -3.066479806614716e+01, 2.506628277459239e+00]
    b = [-5.447609879822406e+01, 1.615858368580409e+02, -1.556989798598866e+02,
         6.680131188771972e+01, -1.328068155288572e+01]
    c = [-7.784894002430293e-03, -3.223964580411365e-01, -2.400758277161838e+00,
         -2.549732539343734e+00, 4.374664141464968e+00, 2.938163982698783e+00]
    d = [7.784695709041462e-03, 3.224671290700398e-01, 2.445134137142996e+00,
         3.754408661907416e+00]
    plow, phigh = 0.02425, 1 - 0.02425
    if p < plow:
        q = math.sqrt(-2 * math.log(p))
        return (((((c[0]*q+c[1])*q+c[2])*q+c[3])*q+c[4])*q+c[5]) / \
               ((((d[0]*q+d[1])*q+d[2])*q+d[3])*q+1)
    if p > phigh:
        q = math.sqrt(-2 * math.log(1 - p))
        return -(((((c[0]*q+c[1])*q+c[2])*q+c[3])*q+c[4])*q+c[5]) / \
                ((((d[0]*q+d[1])*q+d[2])*q+d[3])*q+1)
    q = p - 0.5
    r = q * q
    return (((((a[0]*r+a[1])*r+a[2])*r+a[3])*r+a[4])*r+a[5])*q / \
           (((((b[0]*r+b[1])*r+b[2])*r+b[3])*r+b[4])*r+1)


def _sr_moments(returns: np.ndarray) -> tuple[float, float, float, int]:
    """Sharpe por periodo (no anualizado), skew, kurtosis (Fisher+3=normal), n."""
    r = np.asarray(returns, dtype=float)
    r = r[np.isfinite(r)]
    n = r.size
    if n < 3 or r.std(ddof=1) == 0:
        return 0.0, 0.0, 3.0, n
    mu = r.mean()
    sd = r.std(ddof=1)
    sr = mu / sd
    z = (r - mu) / sd
    skew = float((z ** 3).mean())
    kurt = float((z ** 4).mean())  # 3.0 para normal
    return float(sr), skew, kurt, n


def probabilistic_sharpe_ratio(returns: np.ndarray, sr_benchmark: float = 0.0) -> float:
    """PSR: prob. de que el Sharpe verdadero supere ``sr_benchmark`` (por periodo).

    Corrige por longitud de muestra, sesgo y curtosis (colas gordas bajan la confianza).
    """
    sr, skew, kurt, n = _sr_moments(returns)
    if n < 3:
        return float("nan")
    denom = math.sqrt(max(1e-12, 1 - skew * sr + ((kurt - 1) / 4.0) * sr * sr))
    stat = (sr - sr_benchmark) * math.sqrt(n - 1) / denom
    return _norm_cdf(stat)


def expected_max_sharpe(n_trials: int, sharpe_std: float) -> float:
    """Sharpe maximo ESPERADO por puro azar tras ``n_trials`` variantes (por periodo).

    ``sharpe_std`` = desvio de los Sharpe entre variantes. Formula de Bailey/LdP.
    """
    if n_trials < 1:
        return 0.0
    if n_trials == 1:
        return 0.0
    term = (1 - _EULER) * _norm_ppf(1 - 1.0 / n_trials) + _EULER * _norm_ppf(
        1 - 1.0 / (n_trials * math.e)
    )
    return sharpe_std * term


def deflated_sharpe_ratio(
    returns: np.ndarray, n_trials: int, sharpe_std: float | None = None
) -> dict:
    """DSR: PSR contra el Sharpe-max esperado por azar dado ``n_trials``.

    Si no se pasa ``sharpe_std`` (desvio de Sharpe entre variantes probadas), se usa la
    varianza asintotica de un solo estimador de Sharpe como fallback conservador:
        Var(SR) ~ (1 - skew*SR + (kurt-1)/4 * SR^2) / (n-1).
    DSR < 0.95 => el Sharpe NO es distinguible del mejor de N intentos aleatorios.
    """
    sr, skew, kurt, n = _sr_moments(returns)
    if n < 3:
        return {"dsr": float("nan"), "psr_vs_0": float("nan"), "sr0": float("nan")}
    if sharpe_std is None:
        var_sr = (1 - skew * sr + ((kurt - 1) / 4.0) * sr * sr) / max(1, n - 1)
        sharpe_std = math.sqrt(max(0.0, var_sr))
    sr0 = expected_max_sharpe(n_trials, sharpe_std)
    dsr = probabilistic_sharpe_ratio(returns, sr_benchmark=sr0)
    return {
        "dsr": dsr,
        "psr_vs_0": probabilistic_sharpe_ratio(returns, 0.0),
        "sr0_per_period": sr0,
        "sr_per_period": sr,
        "n_obs": n,
        "n_trials": n_trials,
    }


def monte_carlo_ruin(
    r_multiples: list[float] | np.ndarray,
    *,
    n_sims: int = 5000,
    risk_pct: float = 0.01,
    initial: float = 500.0,
    ruin_frac: float = 0.5,
    seed: int = 0,
) -> dict:
    """Bootstrap del riesgo de secuencia: reordena/remuestrea los R de los trades.

    Modelo de sizing fraccional: cada trade multiplica el equity por (1 + risk_pct*R).
    Para cada simulacion se remuestrea la secuencia de R con reemplazo, se reconstruye la
    curva y se mide drawdown, retorno final y si el equity cruzo ``initial*ruin_frac``.

    Returns percentiles (p5/p50/p95) de retorno final y maxDD, y la prob. de ruina.
    """
    r = np.asarray(list(r_multiples), dtype=float)
    r = r[np.isfinite(r)]
    if r.size == 0:
        return {"trades": 0}
    rng = np.random.default_rng(seed)
    finals = np.empty(n_sims)
    maxdds = np.empty(n_sims)
    ruined = 0
    ruin_level = initial * ruin_frac
    for s in range(n_sims):
        sample = rng.choice(r, size=r.size, replace=True)
        mult = 1.0 + risk_pct * sample
        mult = np.clip(mult, 0.0, None)  # no equity negativo por trade
        eq = initial * np.cumprod(mult)
        eq = np.concatenate([[initial], eq])
        peak = np.maximum.accumulate(eq)
        dd = (eq - peak) / peak
        finals[s] = eq[-1]
        maxdds[s] = dd.min()
        if eq.min() <= ruin_level:
            ruined += 1

    def _pct(a: np.ndarray, q: float) -> float:
        return float(np.percentile(a, q))

    return {
        "trades": int(r.size),
        "sims": n_sims,
        "risk_pct": risk_pct,
        "prob_ruin": ruined / n_sims,
        "ruin_level": ruin_level,
        "final_return_p5": round(_pct(finals, 5) / initial - 1, 4),
        "final_return_p50": round(_pct(finals, 50) / initial - 1, 4),
        "final_return_p95": round(_pct(finals, 95) / initial - 1, 4),
        "max_drawdown_p50": round(_pct(maxdds, 50), 4),
        "max_drawdown_p95": round(_pct(maxdds, 5), 4),  # p5 de dd (mas negativo) = peor 5%
    }
