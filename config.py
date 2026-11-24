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
    "STLAM.MI", "TTE.PA", "BAS.DE", "BN.PA", "AIR.PA", "RHHBY", "NOVN.SW",
    "VOD.L", "DBK.DE", "KER.PA", "ALV.DE", "RNO.PA", "RHM.DE",
    # Crypto (24 ore su 24)
    "BTC-USD", "ETH-USD", "BNB-USD", "XRP-USD", "ADA-USD",
]))
assert all(len(t) <= 12 for t in STOCKS), "Ticker sospetto: manca una virgola nella lista?"

# ---- Avvisi su cali / rialzi (come prima) ----
ALERT_THRESHOLD = 5     # % di calo dal massimo della finestra
RISE_THRESHOLD = 6      # % di rialzo dal minimo della finestra
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

# ---- Regole "calo/rialzo entro un intervallo -> quanto spesso poi sale/scende" ----
ENABLE_RULES = True
# Ogni regola = (finestra, orizzonte). Esempio (8, 4) = "variazione nelle ultime 2h, esito dopo 1h".
# ORE: valori in barre da 15 minuti.  GIORNI: valori in giorni di borsa (barre giornaliere, 10 anni).
HOUR_RULES = [(4, 4), (4, 8), (8, 4), (8, 8), (12, 4)]     # 1h->1h, 1h->2h, 2h->1h, 2h->2h, 3h->1h
DAY_RULES = [(1, 1), (1, 3), (3, 3), (5, 5), (5, 10)]      # 1g->1g, 1g->3g, 3g->3g, 5g->5g, 5g->10g
# Soglia di calo/rialzo = K x la variazione "normale" di QUEL titolo su quella finestra
# (cosi' per Coca-Cola e per Bitcoin la soglia e' diversa). La % reale viene scritta nel messaggio.
K_LIST = [1.0, 1.5, 2.0]
RULE_MIN_ROW_IS = 15        # sotto questi numeri di eventi la regola non viene nemmeno salvata
RULE_MIN_ROW_OOS = 6
RULE_MIN_EVENTS_IS = 30     # eventi minimi per validare (periodo di scoperta / periodo di verifica)
RULE_MIN_EVENTS_OOS = 12
RULE_P_IS_MAX = 0.01        # significativita' richiesta nel periodo di scoperta
RULE_P_OOS_MAX = 0.10       # ... e nel periodo di verifica (dati mai visti), stessa direzione
# Notifica solo se la probabilita' storica (nella direzione indicata) e' almeno questa:
MIN_NOTIFY_PROB = 0.60
MIN_NOTIFY_N = 25           # ... calcolata su almeno questi eventi
NOTIFY_ONLY_VALIDATED = True    # False = avvisa anche per regole non confermate sui dati mai visti (piu' avvisi, piu' falsi)
MAX_RULES_PER_TICKER = 3    # righe massime per titolo in uno stesso messaggio
EVENTS_MAX_PER_RULE = 300   # in rule_events si tengono gli ultimi N eventi di ogni regola (i piu' recenti)

# Il vecchio modello a polinomio (curva) resta disponibile ma e' spento di default.
ENABLE_CURVE_SIGNALS = False
