"""
Ricalcolo del modello (gira una volta al giorno su GitHub Actions).

Scarica l'ultimo storico a 15 minuti (pre-market e after-hours inclusi) e, PER OGNI
TITOLO separatamente, cerca per ogni "caso" del polinomio cosa e' successo dopo.
Salva su Supabase:
  - signal_stats : una riga per titolo/caso, con la scritta validated true/false
  - signal_events: data e ora esatte degli eventi dei casi promettenti, per controllarli a mano
"""

from datetime import datetime, timezone

import numpy as np
import yfinance as yf

import config
import model
from common import get_supabase, send_telegram


def load():
    out = {}
    for i in range(0, len(config.STOCKS), 40):
        chunk = config.STOCKS[i:i + 40]
        try:
            df = yf.download(
                chunk, period="59d", interval="15m", group_by="ticker",
                auto_adjust=True, prepost=True, threads=True, progress=False,
            )
        except Exception as e:
            print(f"Errore download blocco {i // 40 + 1}: {e}")
            continue
        for t in chunk:
            try:
                s = df[t]["Close"].dropna().astype(float)
            except (KeyError, TypeError):
                print(f"  nessun dato per {t}, lo salto")
                continue
            if len(s) > 300:
                idx = s.index
                times = idx.tz_convert("UTC") if idx.tz is not None else idx.tz_localize("UTC")
                out[t] = (s.to_numpy(float), np.array(idx.date), times)
    return out


def hours(bars):
    return f"{bars * 15 / 60:g}h"


def scoreboard(sb):
    """Come sono andati davvero i segnali inviati (verifica dal vivo, la piu' onesta)."""
    rows = sb.table("signals_log").select("direction,outcome_pct").eq("resolved", True).execute().data
    rows = [r for r in rows if r.get("outcome_pct") is not None]
    if not rows:
        return "Nessun segnale ancora verificato dal vivo."
    res = [r["direction"] * r["outcome_pct"] for r in rows]
    hits = sum(1 for x in res if x > 0)
    return (f"Segnali verificati dal vivo: {len(rows)}; direzione azzeccata nel "
            f"{hits / len(rows) * 100:.0f}%; rendimento medio {np.mean(res):+.2f}% (prima dei costi).")


def key(r):
    return (r["ticker"], r["window_bars"], r["fwd_bars"], r["bucket"])


def main():
    data = load()
    print(f"Titoli con dati sufficienti: {len(data)} su {len(config.STOCKS)}")
    if not data:
        raise SystemExit("Nessun dato scaricato: riprovo al prossimo giro.")

    rows, events = model.learn(data, config.WINDOWS, config.FORWARDS)
    n_tested = len(rows)
    validated = [r for r in rows if r["validated"]]
    # falsi positivi attesi per puro caso: test in-sample E conferma sui dati mai visti (stessa direzione)
    expected = n_tested * config.P_IS_MAX * config.P_OOS_MAX / 2
    print(f"Casi provati: {n_tested}. Validati: {len(validated)}. "
          f"Per puro caso ne aspetteresti circa {expected:.1f}. Eventi salvati: {len(events)}.")
    for r in validated:
        print(f"  {r['ticker']} {hours(r['window_bars'])} -> {hours(r['fwd_bars'])}: {r['shape']} "
              f"-> {'SU' if r['direction'] == 1 else 'GIU'} (n={r['n_all']}, verifica {r['p_up_oos']:.0%} su)")

    sb = get_supabase()
    old = sb.table("signal_stats").select("ticker,window_bars,fwd_bars,bucket,validated").execute().data
    old_valid = {key(r) for r in old if r["validated"]}
    new_valid = {key(r) for r in validated}

    now = datetime.now(timezone.utc).isoformat()
    for r in rows:
        r["updated_at"] = now
    sb.table("signal_stats").delete().gte("bucket", 0).execute()
    sb.table("signal_events").delete().gte("bucket", 0).execute()
    for i in range(0, len(rows), 200):
        sb.table("signal_stats").insert(rows[i:i + 200]).execute()
    for i in range(0, len(events), 500):
        sb.table("signal_events").insert(events[i:i + 500]).execute()

    # Messaggio solo se cambia qualcosa, oppure ogni domenica come riepilogo settimanale
    if new_valid != old_valid or datetime.now(timezone.utc).weekday() == 6:
        lines = [f"Modello ricalcolato: {len(validated)} casi validati su {n_tested} provati "
                 f"(per puro caso ne aspetteresti ~{expected:.1f})."]
        for r in validated[:8]:
            lines.append(f"- {r['ticker']} {hours(r['window_bars'])}->{hours(r['fwd_bars'])}: "
                         f"{r['shape']} -> {'SU' if r['direction'] == 1 else 'GIU'}")
        if len(validated) > 8:
            lines.append(f"... e altri {len(validated) - 8} (vedi tabella signal_stats)")
        if not validated:
            lines.append("Nessun segnale solido al momento: non arriveranno notifiche di tendenza.")
        lines.append(scoreboard(sb))
        send_telegram("\n".join(lines))


if __name__ == "__main__":
    main()
