"""
Ricalcolo giornaliero (gira una volta al giorno su GitHub Actions).

PER OGNI TITOLO, separatamente, calcola dallo storico:
  - rule_stats / rule_events : "se e' sceso/salito di X% in N ore o N giorni, quante volte poi
                               e' salito/sceso" (regole principali), con data e ora di ogni evento;
  - signal_stats / signal_events : il vecchio modello a polinomio (solo se ENABLE_CURVE_SIGNALS).
"""

from datetime import datetime, timezone

import numpy as np

import config
import model
import rules
from common import get_supabase, send_telegram_long


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


def insert_chunks(sb, table, rows, size):
    for i in range(0, len(rows), size):
        sb.table(table).insert(rows[i:i + size]).execute()


def rkey(r):
    return (r["ticker"], r["timeframe"], r["window_len"], r["horizon_len"], r["kind"], round(float(r["k"]), 3))


def rule_text(r):
    moved = "scende" if r["kind"] == "drop" else "sale"
    word = "SU" if r["direction"] == 1 else "GIU"
    return (f"- {r['ticker']}: se {moved} >={r['threshold_pct']:.2f}% in {r['window_label']} -> "
            f"dopo {r['horizon_label']} {word} nel {r['prob_dir'] * 100:.0f}% (n={r['n_all']})")


def learn_rules(sb):
    hour = rules.download(config.STOCKS, "59d", "15m", True)
    day = rules.download(config.STOCKS, "10y", "1d", False)
    hour_data = {t: rules.to_arrays(s) for t, s in hour.items() if len(s) > 300}
    day_data = {t: rules.to_arrays(s) for t, s in day.items() if len(s) > 300}
    print(f"Titoli con dati sufficienti: ore {len(hour_data)}, giorni {len(day_data)} su {len(config.STOCKS)}")
    if not hour_data and not day_data:
        raise SystemExit("Nessun dato scaricato: riprovo al prossimo giro.")

    rows, events = rules.learn_all(hour_data, day_data)
    n_tested = len(rows)
    validated = sorted((r for r in rows if r["validated"]), key=lambda r: -r["prob_dir"])
    expected = n_tested * config.RULE_P_IS_MAX * config.RULE_P_OOS_MAX / 2
    print(f"Regole provate: {n_tested}. Validate: {len(validated)}. Per puro caso ne aspetteresti circa "
          f"{expected:.1f}. Eventi salvati: {len(events)}.")
    for r in validated:
        print(rule_text(r))

    old = sb.table("rule_stats").select("ticker,timeframe,window_len,horizon_len,kind,k,validated").execute().data
    old_valid = {rkey(r) for r in old if r["validated"]}
    new_valid = {rkey(r) for r in validated}

    now = datetime.now(timezone.utc).isoformat()
    for r in rows:
        r["updated_at"] = now
    sb.table("rule_stats").delete().neq("ticker", "").execute()
    sb.table("rule_events").delete().gt("id", 0).execute()
    insert_chunks(sb, "rule_stats", rows, 200)
    insert_chunks(sb, "rule_events", events, 500)

    notif = [r for r in validated if r["prob_dir"] >= config.MIN_NOTIFY_PROB and r["n_all"] >= config.MIN_NOTIFY_N]
    return validated, notif, n_tested, expected, new_valid != old_valid


def learn_curve(sb):
    data = {}
    raw = rules.download(config.STOCKS, "59d", "15m", True)
    for t, s in raw.items():
        if len(s) > 300:
            data[t] = rules.to_arrays(s)
    crows, cevents = model.learn(data, config.WINDOWS, config.FORWARDS)
    now = datetime.now(timezone.utc).isoformat()
    for r in crows:
        r["updated_at"] = now
    sb.table("signal_stats").delete().gte("bucket", 0).execute()
    sb.table("signal_events").delete().gte("bucket", 0).execute()
    insert_chunks(sb, "signal_stats", crows, 200)
    insert_chunks(sb, "signal_events", cevents, 500)
    print(f"Modello a curva: {len(crows)} casi, {sum(r['validated'] for r in crows)} validati.")


def main():
    sb = get_supabase()
    validated, notif, n_tested, expected, changed = [], [], 0, 0.0, False
    if config.ENABLE_RULES:
        validated, notif, n_tested, expected, changed = learn_rules(sb)
    if config.ENABLE_CURVE_SIGNALS:
        learn_curve(sb)

    # Messaggio solo se cambia qualcosa, oppure ogni domenica come riepilogo settimanale
    if config.ENABLE_RULES and (changed or datetime.now(timezone.utc).weekday() == 6):
        lines = [f"Regole ricalcolate: {len(validated)} validate su {n_tested} provate "
                 f"(per puro caso ne aspetteresti ~{expected:.1f}).",
                 f"Quelle con probabilita' >= {config.MIN_NOTIFY_PROB:.0%} (che daranno notifiche): {len(notif)}."]
        for r in (notif or validated)[:10]:
            lines.append(rule_text(r))
        if len(validated) > 10:
            lines.append("... altre in tabella rule_stats")
        if not validated:
            lines.append("Nessuna regola solida al momento: non arriveranno notifiche. E' normale con pochi dati.")
        lines.append(scoreboard(sb))
        send_telegram_long("\n".join(lines))


if __name__ == "__main__":
    main()
