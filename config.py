"""Impostazioni condivise da main.py e learn.py. Modifica qui, non negli altri file."""

# Ticker nel formato di Yahoo Finance. dict.fromkeys elimina i duplicati.
STOCKS = list(dict.fromkeys([
    # USA - un titolo per categoria
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "TSLA", "JPM", "V", "BRK-B", "LLY",
    "UNH", "XOM", "WMT", "PG", "KO", "NFLX", "CAT", "LMT", "UPS", "VZ",
    "NEE", "UBER",
    # Alta volatilita'
    "PLTR", "COIN", "MRNA",
    # Future sugli indici USA: scambiano quasi 24 ore su 24, sono il tuo
    # "termometro" di notte, quando le azioni USA non hanno prezzi su Yahoo
    "ES=F", "NQ=F",
    # ETF e materie prime
    "GLD", "SLV", "USO", "UNG", "DBC", "NG=F", "ZW=F", "CC=F", "ALI=F",
    # Metalli preziosi (future, quasi 24h): oro, argento, platino, palladio
    "GC=F", "SI=F", "PL=F", "PA=F",
    # Europa
    "NOVO-B.CO", "ASML.AS", "SAP", "MC.PA", "HSBC", "SIE.DE", "BMW.DE",
    "STLAM.MI", "TTE.PA", "BAS.DE", "BN.PA", "AIR.PA", "ROG.SW", "NOVN.SW",
    "VOD.L", "DBK.DE", "KER.PA", "ALV.DE", "RNO.PA", "RHM.DE",
    # Crypto (24 ore su 24)
    "BTC-USD", "ETH-USD", "BNB-USD", "XRP-USD", "ADA-USD",
]))
assert all(len(t) <= 12 for t in STOCKS), "Ticker sospetto: manca una virgola nella lista?"

# ---- Avvisi su cali / rialzi (come prima) ----
ALERT_THRESHOLD = 5     # % di calo dal massimo della finestra
RISE_THRESHOLD = 5      # % di rialzo dal minimo della finestra
LOOKBACK_HOURS = 3      # finestra su cui si cercano massimo e minimo
RETENTION_DAYS = 30     # giorni di prezzi tenuti in stock_prices
CHUNK_SIZE = 50         # ticker scaricati per ogni richiesta a Yahoo
STALE_MINUTES = 45      # un titolo senza barre piu' recenti di cosi' e' "fermo" (mercato chiuso)

# ---- Modello statistico (polinomio sulle ultime barre) ----
# Ogni barra dura 15 minuti.
WINDOWS = [8, 12, 24]   # finestra del polinomio: 2h, 3h, 6h
FORWARDS = [4, 12]      # orizzonte successivo valutato: 1h, 3h
TRAIN_FRAC = 0.70       # quota di storico usata per "scoprire" le regole (il resto verifica)
COST_PCT = 0.10         # costo andata+ritorno in % (commissioni + spread)
MIN_EVENTS_IS = 60      # eventi minimi (per singolo titolo) nel periodo di scoperta
MIN_EVENTS_OOS = 25     # eventi minimi nel periodo di verifica
MIN_ROW_IS = 20         # sotto questi numeri un caso non viene nemmeno salvato in signal_stats
MIN_ROW_OOS = 10
CANDIDATE_P = 0.05      # i casi con p-value sotto questa soglia (e almeno CANDIDATE_MIN_N eventi)
CANDIDATE_MIN_N = 30    # hanno l'elenco degli eventi salvato in signal_events, per controllarli a mano
P_IS_MAX = 0.005        # significativita' richiesta nel periodo di scoperta
P_OOS_MAX = 0.05        # significativita' richiesta nel periodo di verifica
SIGNAL_COOLDOWN_HOURS = 3   # non ripete lo stesso segnale sullo stesso titolo prima di cosi'
