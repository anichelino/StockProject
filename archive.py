"""
Archivio permanente delle barre a 15 minuti, dentro il repository GitHub (cartella data/bars_15m).

Yahoo da' solo gli ultimi 59 giorni a 15 minuti. Per far crescere lo storico negli anni, ogni
giorno learn.py salva qui i giorni COMPLETI non ancora archiviati: un file compresso per giorno
(data/bars_15m/2026/2026-11-21.csv.gz, circa 50-80 KB). I file non vengono mai modificati ne'
cancellati, quindi tra due anni le statistiche in ore useranno tutto da oggi al 2028.
"""

import glob
import os
from datetime import datetime, timezone

import numpy as np
import pandas as pd

DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "bars_15m")
COLS = ["ticker", "ts", "day", "close"]


def frame_from_series(series_by_ticker):
    """{ticker: Series a 15 min} -> tabella ticker / ts (secondi UTC) / day (giorno locale della borsa) / close."""
    parts = []
    for t, s in series_by_ticker.items():
        if not len(s):
            continue
        idx = s.index if s.index.tz is not None else s.index.tz_localize("UTC")
        day = np.asarray(idx.year * 10000 + idx.month * 100 + idx.day, dtype=np.int64)
        ts = np.asarray(idx.tz_convert("UTC").as_unit("s").asi8, dtype=np.int64)
        parts.append(pd.DataFrame({"ticker": t, "ts": ts, "day": day, "close": s.to_numpy(float)}))
    if not parts:
        return pd.DataFrame(columns=COLS)
    return pd.concat(parts, ignore_index=True)


def _path(date_str):
    return os.path.join(DIR, date_str[:4], f"{date_str}.csv.gz")


def save_new_days(df, now=None):
    """Salva i giorni (UTC) gia' conclusi che non sono ancora nell'archivio. Ritorna i giorni salvati."""
    if df.empty:
        return []
    now = now or datetime.now(timezone.utc)
    today = now.strftime("%Y-%m-%d")
    utc_day = pd.to_datetime(df["ts"], unit="s", utc=True).dt.strftime("%Y-%m-%d")
    saved = []
    for d, part in df.groupby(utc_day):
        if d >= today:                       # giornata non ancora finita: la salvo domani
            continue
        p = _path(d)
        if os.path.exists(p):                # gia' archiviato: non si tocca mai piu'
            continue
        os.makedirs(os.path.dirname(p), exist_ok=True)
        part.sort_values(["ticker", "ts"])[COLS].to_csv(p, index=False, float_format="%.8g",
                                                        compression="gzip")
        saved.append(d)
    return sorted(saved)


def load_archive():
    files = sorted(glob.glob(os.path.join(DIR, "*", "*.csv.gz")))
    if not files:
        return pd.DataFrame(columns=COLS)
    return pd.concat((pd.read_csv(f) for f in files), ignore_index=True)


def history(fresh_df, tickers, min_bars=300):
    """Archivio + ultimo download, senza doppioni -> {ticker: (prezzi, giorno, orari UTC)}."""
    df = pd.concat([load_archive(), fresh_df], ignore_index=True)
    if df.empty:
        return {}
    df = df[df["ticker"].isin(tickers)]
    df = df.drop_duplicates(["ticker", "ts"], keep="last").sort_values(["ticker", "ts"])
    out = {}
    for t, g in df.groupby("ticker"):
        if len(g) < min_bars:
            continue
        times = pd.DatetimeIndex(pd.to_datetime(g["ts"].to_numpy(np.int64), unit="s", utc=True))
        out[t] = (g["close"].to_numpy(float), g["day"].to_numpy(np.int64), times)
    return out
