"""
Regole "se il titolo scende (o sale) di X% entro un intervallo, quante volte poi risale (o riscende)?"

Per OGNI titolo, separatamente, e per intervalli sia in ORE (barre da 15 minuti, ultimi 59 giorni)
sia in GIORNI (barre giornaliere, ultimi 10 anni):

  1. si calcola la variazione % sulla finestra (es. ultime 2 ore, o ultimi 3 giorni);
  2. un "evento" e' una finestra in cui il titolo ha perso (o guadagnato) almeno la soglia,
     che e' K volte la variazione normale di QUEL titolo su QUELLA finestra;
  3. si guarda cosa e' successo dopo l'orizzonte (es. 1 ora dopo, o 3 giorni dopo);
  4. si conta la % di volte in cui e' salito.

Per non farsi ingannare dal caso una regola e' "validata" solo se:
  - ha abbastanza eventi, contati senza sovrapposizioni;
  - nel periodo di scoperta (primo 70% dello storico) la % e' significativamente diversa da quella
    di un momento qualunque;
  - nel periodo di verifica (ultimo 30%, mai usato per scegliere) va nella stessa direzione ed e'
    ancora significativa;
  - il guadagno medio sui dati mai visti supera i costi di transazione.
"""

import math
import time
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import yfinance as yf

import config
import model

TF_NAME = {"h": "ore", "d": "giorni"}


def label(tf, n):
    return f"{n * 15 / 60:g}h" if tf == "h" else f"{n}g"


# ---------------------------------------------------------------- dati

def download(tickers, period, interval, prepost, chunk_size=40):
    """Scarica {ticker: Series dei prezzi di chiusura}."""
    out = {}
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i:i + chunk_size]
        try:
            df = yf.download(chunk, period=period, interval=interval, group_by="ticker",
                             auto_adjust=True, prepost=prepost, threads=True, progress=False)
        except Exception as e:
            print(f"Errore download {interval} blocco {i // chunk_size + 1}: {e}")
            continue
        for t in chunk:
            s = None
            for get in (lambda: df[t]["Close"], lambda: df["Close"] if len(chunk) == 1 else None):
                try:
                    s = get()
                except (KeyError, TypeError):
                    s = None
                if s is not None:
                    break
            if isinstance(s, pd.DataFrame):      # versione di yfinance con colonne a due livelli
                s = s.iloc[:, 0]
            if s is None:
                print(f"  nessun dato {interval} per {t}")
                continue
            s = s.dropna().astype(float)
            if len(s):
                out[t] = s
        time.sleep(1)
    return out


def to_arrays(s):
    """Series -> (prezzi, giorno locale come intero, orari UTC)."""
    idx = s.index
    dates = np.asarray(idx.year * 10000 + idx.month * 100 + idx.day)
    times = idx.tz_convert("UTC") if idx.tz is not None else idx.tz_localize("UTC")
    return s.to_numpy(float), dates, times


# ---------------------------------------------------------------- calcolo

def window_arrays(close, dates, L, H, intraday):
    """chg = variazione % nelle ultime L barre; fwd = variazione % nelle H barre successive."""
    n = len(close)
    chg = np.full(n, np.nan)
    fwd = np.full(n, np.nan)
    if n > L:
        chg[L:] = (close[L:] / close[:n - L] - 1) * 100
    if n > H:
        fwd[:n - H] = (close[H:] / close[:n - H] - 1) * 100
    ok = np.isfinite(chg) & np.isfinite(fwd)
    if intraday:        # finestra ed esito nella stessa giornata (niente salti notturni)
        same_w = np.zeros(n, dtype=bool)
        same_f = np.zeros(n, dtype=bool)
        same_w[L:] = dates[L:] == dates[:n - L]
        same_f[:n - H] = dates[:n - H] == dates[H:]
        ok &= same_w & same_f
    return chg, fwd, ok


def thin(idx_list, gap):
    keep, last = [], -10 ** 9
    for i in idx_list:
        if i - last >= gap:
            keep.append(i)
            last = i
    return np.array(keep, dtype=int)


def learn_timeframe(tf, data, combos):
    """data: {ticker: (close, dates, times)}. Ritorna (rows, events)."""
    rows, events = [], []
    intraday = tf == "h"
    for ticker, (close, dates, times) in data.items():
        n = len(close)
        split = int(n * config.TRAIN_FRAC)
        idx = np.arange(n)
        for L, H in combos:
            chg, fwd, ok = window_arrays(close, dates, L, H, intraday)
            is_base = ok & (idx + H < split)
            oos_base = ok & (idx >= split)
            if is_base.sum() < 50 or oos_base.sum() < 20:
                continue
            sigma = float(np.std(chg[is_base]))      # variazione "normale" di questo titolo (solo periodo di scoperta)
            if not sigma > 0:
                continue
            b_is, b_oos = fwd[is_base], fwd[oos_base]
            for k in config.K_LIST:
                thr = k * sigma
                for kind in ("drop", "rise"):
                    hit = ok & ((chg <= -thr) if kind == "drop" else (chg >= thr))
                    kept = np.zeros(n, dtype=bool)
                    kept[thin(np.flatnonzero(hit), H)] = True
                    i_is = np.flatnonzero(kept & (idx + H < split))
                    i_oos = np.flatnonzero(kept & (idx >= split))
                    if len(i_is) < config.RULE_MIN_ROW_IS or len(i_oos) < config.RULE_MIN_ROW_OOS:
                        continue
                    s_is, s_oos = model._stats(fwd[i_is], b_is), model._stats(fwd[i_oos], b_oos)
                    if s_is is None or s_oos is None:
                        continue
                    d = 1 if s_is["p"] >= s_is["p0"] else -1       # direzione scelta guardando SOLO il periodo di scoperta
                    excess = d * (s_oos["mean_ev"] - s_oos["mean_base"])
                    net = excess - config.COST_PCT
                    validated = bool(
                        s_is["n"] >= config.RULE_MIN_EVENTS_IS and s_oos["n"] >= config.RULE_MIN_EVENTS_OOS
                        and s_is["pval"] < config.RULE_P_IS_MAX
                        and d * (s_oos["p"] - s_oos["p0"]) > 0 and s_oos["pval"] < config.RULE_P_OOS_MAX
                        and net > 0
                    )
                    all_ev = np.concatenate([fwd[i_is], fwd[i_oos]])
                    p_all = float(np.mean(all_ev > 0))
                    prob_dir = p_all if d == 1 else 1 - p_all
                    rows.append(dict(
                        ticker=ticker, timeframe=tf, window_len=int(L), horizon_len=int(H), kind=kind,
                        k=float(k), threshold_pct=float(thr),
                        window_label=label(tf, L), horizon_label=label(tf, H),
                        direction=int(d), prob_dir=prob_dir,
                        n_is=int(s_is["n"]), p_up_is=s_is["p"], base_is=s_is["p0"], pval_is=s_is["pval"],
                        n_oos=int(s_oos["n"]), p_up_oos=s_oos["p"], base_oos=s_oos["p0"], pval_oos=s_oos["pval"],
                        n_all=int(len(all_ev)), p_up_all=p_all, mean_after_pct=float(np.mean(all_ev)),
                        edge_oos_pct=float(excess), net_oos_pct=float(net), validated=validated,
                    ))
                    n_all = len(all_ev)
                    if validated or (prob_dir >= 0.55 and n_all >= config.CANDIDATE_MIN_N):
                        for segment, ii in (("is", i_is), ("oos", i_oos)):
                            for i in ii:
                                events.append(dict(
                                    ticker=ticker, timeframe=tf, window_len=int(L), horizon_len=int(H),
                                    kind=kind, k=float(k), bar_time=times[i].isoformat(),
                                    price_start=float(close[i - L]), price0=float(close[i]),
                                    price_after=float(close[i + H]), change_pct=float(chg[i]),
                                    outcome_pct=float(fwd[i]), went_up=bool(fwd[i] > 0), segment=segment,
                                ))
    return rows, events


def learn_all(hour_data, day_data):
    rows, events = [], []
    for tf, data, combos in (("h", hour_data, config.HOUR_RULES), ("d", day_data, config.DAY_RULES)):
        if data:
            r, e = learn_timeframe(tf, data, combos)
            rows += r
            events += e
    return rows, events


# ---------------------------------------------------------------- uso dal vivo

def horizon_minutes(tf, H):
    """Tempo di attesa prima di verificare l'esito (i giorni di borsa ~ 7/5 di giorni di calendario)."""
    return H * 15 if tf == "h" else int(math.ceil(H * 7 / 5)) * 1440


def triggered(rule, close, dates):
    """Variazione % attuale se la regola scatta ora, altrimenti None."""
    L, n = rule["window_len"], len(close)
    if n <= L:
        return None
    if rule["timeframe"] == "h" and dates[-1] != dates[-1 - L]:
        return None
    chg = (close[-1] / close[-1 - L] - 1) * 100
    thr = float(rule["threshold_pct"])
    if rule["kind"] == "drop":
        return chg if chg <= -thr else None
    return chg if chg >= thr else None


def rule_key(r):
    return f"{r['timeframe']}|{r['window_len']}|{r['horizon_len']}|{r['kind']}|{float(r['k']):g}"


def describe(r, chg, price):
    """Riga di messaggio in italiano."""
    tf = r["timeframe"]
    d, kind = r["direction"], r["kind"]
    p = float(r["prob_dir"])
    moved = "Calo" if kind == "drop" else "Rialzo"
    word = "salito" if d == 1 else "sceso"
    again = "ancora " if (kind == "drop") == (d == -1) else ""
    after = float(r["mean_after_pct"])
    live = (f" Dati mai visti: {float(r['p_up_oos']) * 100 if d == 1 else (1 - float(r['p_up_oos'])) * 100:.0f}% "
            f"(n={r['n_oos']}).") if r.get("n_oos") else ""
    return (
        f"- {moved} {chg:+.2f}% in {r['window_label']} (soglia {float(r['threshold_pct']):.2f}%). "
        f"In passato, dopo {r['horizon_label']}, e' {again}{word} nel {p * 100:.0f}% dei casi "
        f"(n={r['n_all']}, variazione media {after:+.2f}%).{live}"
    )
