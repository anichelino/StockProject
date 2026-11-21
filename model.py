"""
Cuore statistico del sistema, calcolato SEPARATAMENTE PER OGNI TITOLO.

Idea: su ogni finestra di W barre da 15 minuti si adatta un polinomio di secondo
grado   y = a*x^2 + b*x + c   ai prezzi (normalizzati sulla volatilita' del titolo).
I due parametri che contano sono
    b = pendenza alla fine della finestra  (il prezzo sta salendo o scendendo?)
    a = curvatura                          (sta accelerando o rallentando?)
Non si usa il polinomio per "estrapolare" il futuro (sarebbe inaffidabile): si usano
b e a per classificare la situazione in 15 "casi" (bucket) e si misura nello STORICO
di quel titolo cosa e' successo dopo, ogni volta che si trovava in quel caso.

Un caso diventa "segnale validato" solo se supera TUTTI questi controlli:
  - abbastanza eventi, contati senza sovrapposizioni
  - differenza significativa rispetto a un momento qualunque, nel periodo di scoperta
  - la stessa direzione, ancora significativa, su dati MAI visti (periodo di verifica)
  - vantaggio medio sui dati mai visti maggiore dei costi di transazione

ATTENZIONE: per singolo titolo i dati sono pochi (59 giorni a 15 minuti), quindi serve
un effetto molto netto per essere validato. E' normale che quasi nessun caso lo sia.
"""

import math

import numpy as np
import pandas as pd
from numpy.lib.stride_tricks import sliding_window_view

import config

SLOPE_EDGES = np.array([-1.5, -0.5, 0.5, 1.5])   # 5 classi di pendenza
CURV_EDGES = np.array([-0.5, 0.5])               # 3 classi di curvatura
N_CURV = len(CURV_EDGES) + 1
N_BUCKETS = (len(SLOPE_EDGES) + 1) * N_CURV      # 15 casi

VOL_BARS = 96    # barre usate per stimare la volatilita' tipica
VOL_MIN = 40     # barre minime per stimarla

SLOPE_LABELS = ["in forte calo", "in calo", "laterale", "in salita", "in forte salita"]
CURV_LABELS = ["con accelerazione negativa", "a ritmo costante", "con accelerazione positiva"]

_PINV_CACHE = {}


def bucket_label(bucket):
    return f"andamento {SLOPE_LABELS[bucket // N_CURV]}, {CURV_LABELS[bucket % N_CURV]}"


def _pinv(W):
    if W not in _PINV_CACHE:
        x = np.linspace(-1.0, 0.0, W)
        X = np.column_stack([x ** 2, x, np.ones(W)])
        _PINV_CACHE[W] = np.linalg.pinv(X)    # (3, W)
    return _PINV_CACHE[W]


def poly_features(close, dates, W):
    """Per ogni barra: pendenza b e curvatura a della finestra che finisce li'.

    Restituisce due array (NaN dove la finestra non e' valida: poche barre,
    volatilita' non stimabile, o finestra che attraversa il cambio di giornata).
    """
    n = len(close)
    b = np.full(n, np.nan)
    a = np.full(n, np.nan)
    if n < W + VOL_MIN + 1:
        return b, a

    logp = np.log(close)
    ret = np.full(n, np.nan)
    ret[1:] = np.diff(logp)
    ret[1:][dates[1:] != dates[:-1]] = np.nan          # niente rendimenti "notturni"
    sig = pd.Series(ret).rolling(VOL_BARS, min_periods=VOL_MIN).std().shift(1).to_numpy()

    win = sliding_window_view(logp, W)                 # (n-W+1, W)
    end = np.arange(W - 1, n)
    scale = sig[end] * math.sqrt(W)
    with np.errstate(divide="ignore", invalid="ignore"):
        y = (win - win[:, -1:]) / scale[:, None]
    coef = y @ _pinv(W).T                              # colonne: a, b, c

    ok = np.isfinite(scale) & (scale > 0) & (dates[end - (W - 1)] == dates[end])
    ok &= np.isfinite(coef).all(axis=1)
    a[end[ok]] = coef[ok, 0]
    b[end[ok]] = coef[ok, 1]
    return b, a


def bucket_ids(b, a):
    return np.digitize(b, SLOPE_EDGES) * N_CURV + np.digitize(a, CURV_EDGES)


def current_bucket(close, dates, W):
    """Caso in cui si trova l'ultima barra (None se non calcolabile)."""
    b, a = poly_features(np.asarray(close, float), np.asarray(dates), W)
    if len(b) == 0 or not (np.isfinite(b[-1]) and np.isfinite(a[-1])):
        return None
    return int(bucket_ids(b[-1:], a[-1:])[0])


def observations(close, dates, W, F):
    """Per ogni barra: caso, rendimento a F barre, validita', e se tenerla (senza sovrapposizioni)."""
    n = len(close)
    b, a = poly_features(close, dates, W)
    fwd = np.full(n, np.nan)
    same = np.zeros(n, dtype=bool)
    if n > F:
        fwd[:n - F] = (close[F:] / close[:n - F] - 1) * 100
        same[:n - F] = dates[:n - F] == dates[F:]       # l'esito deve restare nella stessa giornata
    ok = np.isfinite(b) & np.isfinite(a) & np.isfinite(fwd) & same
    bid = np.full(n, -1, dtype=int)
    bid[ok] = bucket_ids(b[ok], a[ok])

    keep = np.zeros(n, dtype=bool)
    last = np.full(N_BUCKETS, -10 ** 9)
    for i in np.flatnonzero(ok):                        # eventi dello stesso caso distanziati di F barre
        k = bid[i]
        if i - last[k] >= F:
            keep[i] = True
            last[k] = i
    return bid, fwd, ok, keep


def _stats(ev, base):
    n = len(ev)
    p = float(np.mean(ev > 0))
    p0 = float(np.mean(base > 0))
    if not 0 < p0 < 1:
        return None
    z = (p - p0) / math.sqrt(p0 * (1 - p0) / n)
    return dict(
        n=n, p=p, p0=p0, pval=math.erfc(abs(z) / math.sqrt(2)),
        mean_ev=float(np.mean(ev)), mean_base=float(np.mean(base)),
    )


def learn(series_by_ticker, windows, forwards):
    """series_by_ticker: {ticker: (close ndarray, dates ndarray, times DatetimeIndex UTC)}.

    Ritorna (rows, events):
      rows   = una riga per titolo/finestra/orizzonte/caso, per la tabella signal_stats
      events = elenco degli eventi (data e ora esatte) dei casi validati o promettenti,
               per la tabella signal_events, cosi' puoi controllarli a mano su un grafico
    """
    rows, events = [], []
    for ticker, (close, dates, times) in series_by_ticker.items():
        n = len(close)
        split = int(n * config.TRAIN_FRAC)
        idx = np.arange(n)
        for W in windows:
            for F in forwards:
                bid, fwd, ok, keep = observations(close, dates, W, F)
                is_ok = ok & (idx + F < split)
                oos_ok = ok & (idx >= split)
                b_is, b_oos = fwd[is_ok], fwd[oos_ok]
                if len(b_is) == 0 or len(b_oos) == 0:
                    continue
                for k in range(N_BUCKETS):
                    m = keep & (bid == k)
                    i_is, i_oos = np.flatnonzero(m & is_ok), np.flatnonzero(m & oos_ok)
                    if len(i_is) < config.MIN_ROW_IS or len(i_oos) < config.MIN_ROW_OOS:
                        continue
                    ev_is, ev_oos = fwd[i_is], fwd[i_oos]
                    s_is, s_oos = _stats(ev_is, b_is), _stats(ev_oos, b_oos)
                    if s_is is None or s_oos is None:
                        continue

                    d = 1 if s_is["p"] >= s_is["p0"] else -1   # direzione scelta guardando SOLO il periodo di scoperta
                    excess = d * (s_oos["mean_ev"] - s_oos["mean_base"])
                    net = excess - config.COST_PCT
                    validated = bool(
                        s_is["n"] >= config.MIN_EVENTS_IS and s_oos["n"] >= config.MIN_EVENTS_OOS
                        and s_is["pval"] < config.P_IS_MAX
                        and d * (s_oos["p"] - s_oos["p0"]) > 0 and s_oos["pval"] < config.P_OOS_MAX
                        and net > 0
                    )
                    all_ev = np.concatenate([ev_is, ev_oos])
                    shape = bucket_label(k)
                    rows.append(dict(
                        ticker=ticker, window_bars=int(W), fwd_bars=int(F), bucket=int(k), shape=shape,
                        direction=int(d),
                        n_is=int(s_is["n"]), p_up_is=s_is["p"], base_is=s_is["p0"], pval_is=s_is["pval"],
                        n_oos=int(s_oos["n"]), p_up_oos=s_oos["p"], base_oos=s_oos["p0"], pval_oos=s_oos["pval"],
                        p_up_all=float(np.mean(all_ev > 0)), n_all=int(len(all_ev)),
                        edge_oos_pct=float(excess), net_oos_pct=float(net), validated=validated,
                    ))

                    if validated or (s_is["pval"] < config.CANDIDATE_P and s_is["n"] >= config.CANDIDATE_MIN_N):
                        for segment, ii in (("is", i_is), ("oos", i_oos)):
                            for i in ii:
                                events.append(dict(
                                    ticker=ticker, window_bars=int(W), fwd_bars=int(F), bucket=int(k),
                                    shape=shape, bar_time=times[i].isoformat(),
                                    price0=float(close[i]), price_after=float(close[i + F]),
                                    outcome_pct=float(fwd[i]), went_up=bool(fwd[i] > 0), segment=segment,
                                ))
    return rows, events
