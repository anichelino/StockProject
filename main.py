import os
import sys
import time
import requests
import yfinance as yf
from datetime import datetime, timedelta, timezone
from supabase import create_client, Client

# ---------------------- CONFIGURAZIONE --------------------------------

# Le variabili arrivano dai "Secrets" del repository GitHub.
# .strip() elimina spazi e a capo copiati per errore insieme al valore.
SUPABASE_URL = (os.getenv("SUPABASE_URL") or "").strip().rstrip("/")
SUPABASE_KEY = (os.getenv("SUPABASE_KEY") or "").strip()
TELEGRAM_BOT_TOKEN = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
TELEGRAM_CHAT_ID = (os.getenv("TELEGRAM_CHAT_ID") or "").strip()

ALERT_THRESHOLD = 1   # % di calo dal massimo delle ultime ore per mandare l'avviso
LOOKBACK_HOURS = 3      # finestra su cui si cerca il massimo
RETENTION_DAYS = 30     # quanti giorni di prezzi tenere in stock_prices
CHUNK_SIZE = 50         # ticker scaricati per ogni richiesta a Yahoo

if not SUPABASE_URL.startswith("https://"):
    sys.exit(
        "SUPABASE_URL mancante o non valido. Deve essere del tipo "
        "https://xxxxxxxx.supabase.co (Supabase -> Project Settings -> API -> Project URL). "
        "Controlla il secret SUPABASE_URL nel repository GitHub."
    )
if not SUPABASE_KEY:
    sys.exit("SUPABASE_KEY mancante. Controlla il secret SUPABASE_KEY nel repository GitHub.")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Ticker nel formato di Yahoo Finance. dict.fromkeys elimina i duplicati.
STOCKS = list(dict.fromkeys([
    "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "NVDA", "META", "BRK-B", "JNJ", "V",
    "UNH", "WMT", "PG", "JPM", "MA", "XOM", "LLY", "HD", "CVX", "ABBV",
    "KO", "PEP", "MRK", "BAC", "PFE", "COST", "TMO", "AVGO", "DIS", "CSCO",
    "MCD", "ADBE", "CRM", "NFLX", "ACN", "DHR", "TXN", "LIN", "NEE", "PM",
    "NKE", "WFC", "BMY", "AMD", "HON", "UNP", "AMGN", "INTC", "LOW", "RTX",
    "MS", "ELV", "SCHW", "SPGI", "GS", "PLD", "IBM", "BLK", "T", "MDT",
    "CAT", "CVS", "DE", "AMT", "C", "NOW", "LMT", "INTU", "SYK", "MO",
    "BKNG", "ISRG", "ADI", "ZTS", "GE", "EQIX", "REGN", "ADP", "MDLZ", "MU",
    "GILD", "AXP", "TGT", "BSX", "CI", "CB", "MMC", "EW", "CSX", "DUK",
    "SO", "PNC", "BDX", "ITW", "SHW", "APD", "ICE", "HUM", "NSC", "PGR",
    "RY", "BHP", "RIO", "TM", "SHEL", "BP", "UL", "VZ", "FDX", "UPS",
    "NEM", "ORCL", "PAYX",
    # ETF materie prime
    "GLD", "SLV", "USO", "UNG", "DBC",
    # Futures materie prime aggiuntivi
    "NG=F",   # Gas naturale (futures) — in alternativa/aggiunta a UNG
    "ZW=F",   # Grano (Wheat)
    "CC=F",   # Cacao (Cocoa)
    "ALI=F",  # Alluminio
    # Europa (simboli con suffisso di borsa)
    "NOVO-B.CO",  # Novo Nordisk
    "ASML.AS",    # ASML
    "SAP",        # SAP
    "MC.PA",      # LVMH
    "HSBC",       # HSBC
    "SIE.DE",     # Siemens
    "BMW.DE",     # BMW
    "STLAM.MI",   # Stellantis
    "TTE.PA",     # TotalEnergies
    "BAS.DE",     # BASF
    "BN.PA",      # Danone
    "AIR.PA",     # Airbus
    "ROG.SW",     # Roche
    "NOVN.SW",    # Novartis
    "VOD.L",      # Vodafone
    "DBK.DE",     # Deutsche Bank
    "KER.PA",     # Kering
    "ALV.DE",     # Allianz
    "RNO.PA",     # Renault
    "RHM.DE",      # Rheinmetall
    # Crypto
    "BTC-USD", "ETH-USD", "BNB-USD", "XRP-USD", "ADA-USD", "SOL-USD",
    "DOT-USD", "LTC-USD", "AVAX-USD", "ATOM-USD", "LINK-USD", "XMR-USD",
    "UNI-USD", "AAVE-USD", "ALGO-USD", "NEAR-USD", "EGLD-USD", "VET-USD",
]))

# ------------------------------------------------------------------------


def fetch_prices():
    """Scarica l'ultimo prezzo di tutti i ticker, a blocchi (molto piu' veloce)."""
    prices = {}
    for i in range(0, len(STOCKS), CHUNK_SIZE):
        chunk = STOCKS[i:i + CHUNK_SIZE]
        try:
            df = yf.download(
                chunk, period="1d", interval="1m", group_by="ticker",
                threads=True, progress=False, auto_adjust=False,
            )
        except Exception as e:
            print(f"Errore download blocco {i // CHUNK_SIZE + 1}: {e}")
            continue

        for ticker in chunk:
            try:
                close = df[ticker]["Close"].dropna()
            except (KeyError, TypeError):
                print(f"Nessun dato per {ticker} (ticker non valido o mercato senza dati).")
                continue
            if not close.empty:
                prices[ticker] = float(close.iloc[-1])
        time.sleep(2)

    print(f"Prezzi ottenuti per {len(prices)} ticker su {len(STOCKS)}.")
    return prices


def store_prices(prices):
    """Salva tutti i prezzi con un'unica scrittura."""
    if not prices:
        return
    now = datetime.now(tz=timezone.utc).isoformat()
    rows = [{"ticker": t, "price": p, "timestamp": now} for t, p in prices.items()]
    supabase.table("stock_prices").insert(rows).execute()


def fetch_recent_records(since_iso):
    """Legge tutti i prezzi recenti, a pagine da 1000 (limite di Supabase)."""
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


def send_telegram(text):
    if not (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID):
        print("Token o chat ID Telegram non impostati: messaggio non inviato.")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    r = requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text}, timeout=15)
    if r.status_code != 200:
        print(f"Errore invio Telegram: {r.text}")


def check_dropdowns(current_prices):
    since = (datetime.now(tz=timezone.utc) - timedelta(hours=LOOKBACK_HOURS)).isoformat()
    records = fetch_recent_records(since)

    by_ticker = {}
    for r in records:
        by_ticker.setdefault(r["ticker"], []).append(r)

    # Record gia' presenti nella tabella dropdowns (uno per ticker)
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
        elif dropdown > old["dropdown"] or dropdown >= ALERT_THRESHOLD:
            supabase.table("dropdowns").update(payload).eq("id", old["id"]).execute()
            print(f"{ticker}: record dropdown aggiornato a {dropdown:.2f}%")
            updated = True
        else:
            updated = False

        if updated and dropdown >= ALERT_THRESHOLD:
            send_telegram(
                f"Ticker: {ticker}\n"
                f"Prezzo iniziale: {initial_price}\n"
                f"Prezzo attuale: {final_price}\n"
                f"Massimo ({LOOKBACK_HOURS}h): {max_price}\n"
                f"Minimo: {min_price}\n"
                f"Calo dal massimo: {dropdown:.2f}%\n"
                f"Da: {timestamps[0]}\n"
                f"A: {timestamps[-1]}"
            )


def clean_old_records():
    cutoff = datetime.now(tz=timezone.utc) - timedelta(days=RETENTION_DAYS)
    supabase.table("stock_prices").delete().lt("timestamp", cutoff.isoformat()).execute()
    print(f"Eliminati i prezzi piu' vecchi di {RETENTION_DAYS} giorni.")


def main():
    clean_old_records()
    prices = fetch_prices()
    store_prices(prices)
    check_dropdowns(prices)


if __name__ == "__main__":
    main()
