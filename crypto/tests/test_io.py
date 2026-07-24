"""Regresion: load_csv debe aceptar timestamps ISO (string) y epoch-ms (numerico).

El bug: pandas 3.0 lee columnas string como StringDtype, que rompe np.issubdtype.
"""

from __future__ import annotations

import pandas as pd

from crypto.scripts.validate import load_csv


def _write(path, rows_iso: bool) -> None:
    idx = pd.date_range("2021-01-01", periods=5, freq="4h", tz="UTC")
    df = pd.DataFrame({
        "open": [1, 2, 3, 4, 5], "high": [2, 3, 4, 5, 6],
        "low": [0.5, 1, 2, 3, 4], "close": [1.5, 2.5, 3.5, 4.5, 5.5],
        "volume": [10, 10, 10, 10, 10],
    })
    if rows_iso:
        df.insert(0, "timestamp", idx.astype(str))
    else:
        df.insert(0, "timestamp", [int(t.timestamp() * 1000) for t in idx])  # epoch ms
    df.to_csv(path, index=False)


def test_load_csv_iso_timestamps(tmp_path) -> None:
    p = tmp_path / "iso.csv"
    _write(p, rows_iso=True)
    df = load_csv(str(p))
    assert len(df) == 5
    assert str(df.index[0]).startswith("2021-01-01")
    assert list(df.columns) == ["open", "high", "low", "close", "volume"]


def test_load_csv_epoch_ms(tmp_path) -> None:
    p = tmp_path / "ms.csv"
    _write(p, rows_iso=False)
    df = load_csv(str(p))
    assert len(df) == 5
    assert str(df.index[0]).startswith("2021-01-01")
