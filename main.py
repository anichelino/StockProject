"""
Raccolta prezzi + avvisi (gira ogni 15 minuti su GitHub Actions).

Ad ogni giro:
  1. scarica le ultime barre a 15 minuti, pre-market e after-hours inclusi;
  2. tiene solo i titoli con dati "freschi" (a mercato chiuso non cambia nulla);
  3. salva i prezzi su Supabase e manda gli avvisi di calo / rialzo;
  4. controlla se la situazione del polinomio corrisponde a un segnale statistico
     validato (tabella signal_stats, aggiornata ogni giorno da learn.py);
  5. verifica come sono andati i segnali inviati in precedenza.
"""

import time
import traceback
from datetime import datetime, timedelta, timezone

import numpy as np
import yfinance as yf

import config
import model
from common import get_supabase, send_telegram

supabase = None   # creato in main()
STOCKS = config.STOCKS


def fetch_bars():
    """Ultimi 5 giorni a 15 minuti, con pre/after-market. Ritorna {ticker: Series dei prezzi}."""
    bars = {}
    for i in range(0, len(STOCKS), config.CHUNK_SIZE):
        chunk = STOCKS[i:i + config.CHUNK_SIZE]
        try:
            df = yf.download(
                chunk, period="5d", interval="15m", group_by="ticker",
                threads=True, progress=False, auto_adjust=True, prepost=True,
            )
        except Exception as e:
            print(f"Errore download blocco {i // config.CHUNK_SIZE + 1}: {e}")
            continue
        for t in chunk:
            try:
                s = df[t]["Close"].dropna().astype(float)
            except (KeyError, TypeError):
                print(f"Nessun dato per {t}.")
                continue
            if len(s):
                bars[t] = s
        time.sleep(2)
    return bars


def fresh_prices(bars):
    """Solo i titoli con una barra recente: a mercato chiuso il prezzo e' fermo e non serve."""
    now = datetime.now(timezone.utc)
    prices = {}
    for t, s in bars.items():
        ts = s.index[-1]
        ts = ts.tz_convert("UTC") if ts.tzinfo else ts.tz_localize("UTC")
        age_min = (now - ts.to_pydatetime()).total_seconds() / 60
        if age_min <= config.STALE_MINUTES:
            prices[t] = float(s.iloc[-1])
    print(f"Prezzi aggiornati per {len(prices)} titoli su {len(STOCKS)} "
          f"(gli altri hanno il mercato chiuso o nessun dato).")
    return prices


def store_prices(prices):
    if not prices:
        return
    now = datetime.now(tz=timezone.utc).isoformat()
    rows = [{"ticker": t, "price": p, "timestamp": now} for t, p in prices.items()]
    supabase.table("stock_prices").insert(rows).execute()


def fetch_recent_records(since_iso):
    rows, start, page = [], 0, 1000
    while True:
        res = (
            supabase.table("stock_prices")
            .select("ticker,price,timestamp")
            .gte("timestamp", since_iso)
            .order("timestamp")
            .range(start, start + page - 1)
            .execute()
        )
        rows.extend(res.data)
        if len(res.data) < page:
            break
        start += page
    return rows


def check_dropdowns(current_prices, by_ticker):
    """Cali dal massimo della finestra."""
    existing = {}
    res = supabase.table("dropdowns").select("*").order("calculated_at", desc=True).execute()
    for row in res.data:
        existing.setdefault(row["ticker"], row)

    now = datetime.now(timezone.utc).isoformat()

    for ticker, recs in by_ticker.items():
        final_price = current_prices.get(ticker)
        if final_price is None:
            continue

        prices = [r["price"] for r in recs]
        timestamps = [r["timestamp"] for r in recs]
        max_price, min_price, initial_price = max(prices), min(prices), prices[0]
        dropdown = (max_price - final_price) / max_price * 100

        payload = {
            "ticker": ticker,
            "initial_price": initial_price,
            "final_price": final_price,
            "max_price": max_price,
            "min_price": min_price,
            "dropdown": dropdown,
            "start_timestamp": timestamps[0],
            "end_timestamp": timestamps[-1],
            "calculated_at": now,
        }

        old = existing.get(ticker)
        if old is None:
            supabase.table("dropdowns").insert(payload).execute()
            print(f"{ticker}: nuovo record dropdown {dropdown:.2f}%")
            updated = True
        elif dropdown > old["dropdown"] or dropdown >= config.ALERT_THRESHOLD:
            supabase.table("dropdowns").update(payload).eq("id", old["id"]).execute()
            print(f"{ticker}: record dropdown aggiornato a {dropdown:.2f}%")
            updated = True
        else:
            updated = False

        if updated and dropdown >= config.ALERT_THRESHOLD:
            send_telegram(
                f"\U0001F53B CALO - {ticker}\n"
                f"Prezzo iniziale: {initial_price}\n"
                f"Prezzo attuale: {final_price}\n"
                f"Massimo ({config.LOOKBACK_HOURS}h): {max_price}\n"
                f"Minimo: {min_price}\n"
                f"Calo dal massimo: {dropdown:.2f}%\n"
                f"Da: {timestamps[0]}\n"
                f"A: {timestamps[-1]}"
            )


def check_rises(current_prices, by_ticker):
    """Rialzi dal minimo della finestra."""
    existing = {}
    res = supabase.table("risings").select("*").order("calculated_at", desc=True).execute()
    for row in res.data:
        existing.setdefault(row["ticker"], row)

    now = datetime.now(timezone.utc).isoformat()

    for ticker, recs in by_ticker.items():
        final_price = current_prices.get(ticker)
        if final_price is None:
            continue

        prices = [r["price"] for r in recs]
        timestamps = [r["timestamp"] for r in recs]
        max_price, min_price, initial_price = max(prices), min(prices), prices[0]
        rise = (final_price - min_price) / min_price * 100

        payload = {
            "ticker": ticker,
            "initial_price": initial_price,
            "final_price": final_price,
            "max_price": max_price,
            "min_price": min_price,
            "rise": rise,
            "start_timestamp": timestamps[0],
            "end_timestamp": timestamps[-1],
            "calculated_at": now,
        }

        old = existing.get(ticker)
        if old is None:
            supabase.table("risings").insert(payload).execute()
            print(f"{ticker}: nuovo record rise {rise:.2f}%")
            updated = True
        elif rise > old["rise"] or rise >= config.RISE_THRESHOLD:
            supabase.table("risings").update(payload).eq("id", old["id"]).execute()
            print(f"{ticker}: record rise aggiornato a {rise:.2f}%")
            updated = True
        else:
            updated = False

        if updated and rise >= config.RISE_THRESHOLD:
            send_telegram(
                f"\U0001F7E2 RIALZO - {ticker}\n"
                f"Prezzo iniziale: {initial_price}\n"
                f"Prezzo attuale: {final_price}\n"
                f"Minimo ({config.LOOKBACK_HOURS}h): {min_price}\n"
                f"Massimo: {max_price}\n"
                f"Rialzo dal minimo: {rise:.2f}%\n"
                f"Da: {timestamps[0]}\n"
                f"A: {timestamps[-1]}"
            )


def hours(bars):
    return f"{bars * 15 / 60:g}h"


def signal_message(ticker, W, F, bucket, r, price):
    d = r["direction"]
    p_all = r["p_up_all"] if d == 1 else 1 - r["p_up_all"]
    p_oos = r["p_up_oos"] if d == 1 else 1 - r["p_up_oos"]
    icon = "\U0001F4C8" if d == 1 else "\U0001F4C9"
    verb = "salito" if d == 1 else "sceso"
    return (
        f"{icon} SEGNALE STATISTICO - {ticker}\n"
        f"Prezzo: {price:.4g}\n"
        f"Situazione ({hours(W)}): {model.bucket_label(bucket)}\n"
        f"In passato, su {ticker}, in situazioni simili il prezzo e' {verb} "
        f"nelle {hours(F)} successive nel {p_all * 100:.0f}% dei casi "
        f"(n={r['n_all']}; su dati mai visti: {p_oos * 100:.0f}%, n={r['n_oos']}).\n"
        f"Non e' una previsione certa: e' una tendenza statistica."
    )


def check_signals(bars, prices):
    stats = supabase.table("signal_stats").select("*").eq("validated", True).execute().data
    if not stats:
        print("Nessun segnale statistico validato al momento (e' normale: succede spesso).")
        return
    by_ticker = {}
    for r in stats:
        by_ticker.setdefault(r["ticker"], []).append(r)

    since = (datetime.now(timezone.utc) - timedelta(hours=config.SIGNAL_COOLDOWN_HOURS)).isoformat()
    recent = (supabase.table("signals_log").select("ticker,window_bars,fwd_bars,bucket")
              .gte("created_at", since).execute().data)
    recent_keys = {(r["ticker"], r["window_bars"], r["fwd_bars"], r["bucket"]) for r in recent}

    sent = 0
    for t, rules in by_ticker.items():
        if t not in prices:                # solo titoli con dati freschi
            continue
        s = bars[t]
        close = s.to_numpy(float)
        dates = np.array(s.index.date)
        current = {}                       # caso attuale per ogni finestra
        for r in rules:
            W, F, bucket = r["window_bars"], r["fwd_bars"], r["bucket"]
            if W not in current:
                current[W] = model.current_bucket(close, dates, W)
            key = (t, W, F, bucket)
            if current[W] != bucket or key in recent_keys:
                continue
            send_telegram(signal_message(t, W, F, bucket, r, prices[t]))
            supabase.table("signals_log").insert({
                "ticker": t, "window_bars": W, "fwd_bars": F, "bucket": bucket,
                "direction": r["direction"], "price0": prices[t], "p_up": r["p_up_all"],
            }).execute()
            recent_keys.add(key)
            sent += 1
    print(f"Segnali statistici inviati: {sent}")


def resolve_signals(prices):
    """Dopo l'orizzonte previsto, registra cosa e' davvero successo (verifica dal vivo)."""
    rows = supabase.table("signals_log").select("*").eq("resolved", False).execute().data
    now = datetime.now(timezone.utc)
    for r in rows:
        created = datetime.fromisoformat(r["created_at"].replace("Z", "+00:00"))
        horizon = timedelta(minutes=15 * r["fwd_bars"])
        if now < created + horizon:
            continue
        upd = {"resolved": True, "resolved_at": now.isoformat()}
        price = prices.get(r["ticker"])
        if price is not None and now <= created + 2 * horizon:    # troppo in ritardo = non confrontabile
            upd["outcome_pct"] = (price - r["price0"]) / r["price0"] * 100
        supabase.table("signals_log").update(upd).eq("id", r["id"]).execute()


def clean_old_records():
    cutoff = datetime.now(tz=timezone.utc) - timedelta(days=config.RETENTION_DAYS)
    supabase.table("stock_prices").delete().lt("timestamp", cutoff.isoformat()).execute()


def safe(label, fn, *args):
    """Parti opzionali: se falliscono (es. tabella non ancora creata) il resto continua."""
    try:
        fn(*args)
    except Exception:
        print(f"[{label}] errore, proseguo con il resto:")
        traceback.print_exc()


def main():
    global supabase
    supabase = get_supabase()

    clean_old_records()
    bars = fetch_bars()
    prices = fresh_prices(bars)
    store_prices(prices)

    since = (datetime.now(tz=timezone.utc) - timedelta(hours=config.LOOKBACK_HOURS)).isoformat()
    by_ticker = {}
    for r in fetch_recent_records(since):
        by_ticker.setdefault(r["ticker"], []).append(r)

    check_dropdowns(prices, by_ticker)
    check_rises(prices, by_ticker)
    safe("segnali", check_signals, bars, prices)
    safe("verifica segnali", resolve_signals, prices)


if __name__ == "__main__":
    main()
