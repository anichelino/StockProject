# StockProject

Monitor di titoli (azioni, crypto, materie prime, metalli) con avvisi Telegram, dati su Supabase
e statistiche calcolate **separatamente per ogni singolo titolo**.

## La regola principale

> "Se il titolo e' **sceso** (o salito) di almeno X% entro un certo intervallo, in quante volte
> su 100, nello storico di **quel** titolo, **poi e' salito** (o sceso)?"

Intervalli usati (si cambiano in `config.py`):

| Tipo | Finestra (quanto e' sceso/salito) -> Orizzonte (cosa succede dopo) | Storico |
|---|---|---|
| Ore | 1h->1h, 1h->2h, 2h->1h, 2h->2h, 3h->1h | barre a 15 minuti (pre/after-market inclusi): oggi 59 giorni, poi cresce ogni giorno |
| Giorni | 1g->1g, 1g->3g, 3g->3g, 5g->5g, 5g->10g (giorni di borsa) | tutto lo storico giornaliero di Yahoo |

La soglia X non e' uguale per tutti: e' 1, 1,5 o 2 volte la variazione "normale" di quel titolo su
quella finestra (un -2% e' tanto per Coca-Cola e poco per Bitcoin). Nel messaggio c'e' la % reale.

**Notifica**: arriva quando la regola scatta adesso e la probabilita' storica nella direzione
indicata e' almeno il 60% (`MIN_NOTIFY_PROB`) su almeno 25 eventi, e la regola e' **validata**:
confermata anche sull'ultimo 30% dello storico, che non e' stato usato per sceglierla, e con
guadagno medio superiore ai costi. Tutte le regole che scattano nello stesso giro arrivano in
**un solo messaggio**, massimo 3 righe per titolo. La stessa regola non si ripete prima del suo orizzonte.

Esempio di messaggio:

```
NVDA - prezzo 181.2
- Calo -2.41% in 2h (soglia 2.10%). In passato, dopo 1h, e' salito nel 63% dei casi
  (n=41, variazione media +0.38%). Dati mai visti: 61% (n=13).
```

## Lo storico cresce negli anni

Le tabelle `rule_stats` e `rule_events` vengono riscritte ogni giorno, ma calcolate su uno storico
sempre piu' lungo:

- **Ore**: Yahoo da' solo gli ultimi 59 giorni a 15 minuti. Per questo ogni giorno `learn.py` salva
  le giornate concluse nella cartella `data/bars_15m/` del repository (un file compresso per giorno,
  ~50-80 KB, ~20-30 MB l'anno per tutti i titoli). Quei file non vengono mai modificati ne' cancellati.
  Oggi le statistiche in ore usano 59 giorni; tra due anni useranno 59 giorni + 2 anni; tra tre anni
  59 giorni + 3 anni, e cosi' via.
- **Giorni**: si usa tutto lo storico giornaliero che Yahoo ha (per molte azioni decenni), che
  ovviamente si allunga di un giorno ogni giorno.

Il riepilogo Telegram scrive da quando parte lo storico usato. Attenzione: se il workflow giornaliero
non gira per piu' di 59 giorni di fila, i giorni mancanti non si possono piu' recuperare.
Non cancellare la cartella `data/`.

## Altro che fa

- **Ogni 15 minuti** (`main.py`): prezzi su `stock_prices`, avvisi di calo/rialzo dal massimo/minimo
  delle ultime 3 ore (come prima), controllo delle regole, verifica dei segnali passati.
- **Ogni giorno** (`learn.py`): ricalcola `rule_stats` e `rule_events`. Manda un riepilogo
  se l'elenco delle regole validate cambia, e comunque ogni domenica.
- Ogni segnale inviato finisce in `signals_log`; dopo l'orizzonte viene registrato com'e' andato.
- Il vecchio modello "a curva" (polinomio) c'e' ancora ma e' spento: `ENABLE_CURVE_SIGNALS = False`.

## Installazione / aggiornamento

1. **Supabase -> SQL Editor -> New query**: incolla tutto `supabase_schema.sql` ed esegui
   (crea `rule_stats` e `rule_events` e aggiunge colonne a `signals_log`; le altre tabelle restano).
2. **GitHub**: carica tutti i file, inclusa la cartella nascosta `.github/workflows/`
   (nel Finder: Cmd+Maiusc+.).
3. **Secrets**: `SUPABASE_URL`, `SUPABASE_KEY`, `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`.
4. **Actions -> Ricalcolo modello -> Run workflow** (qualche minuto: scarica tutto lo storico giornaliero).
   Il workflow ha il permesso di scrivere nel repository (`permissions: contents: write`) per
   salvare l'archivio; se fallisce al passo "Salva l'archivio", vai su Settings -> Actions -> General ->
   Workflow permissions e scegli "Read and write permissions".
5. **Actions -> Run Stock App -> Run workflow**.

Per provare solo sul branch `Test_statistic`, in entrambi i file `.github/workflows/*.yml` sostituisci
il blocco `on:` con:

```yaml
on:
  push:
    branches: [Test_statistic]
  workflow_dispatch:
```

e lancia i workflow scegliendo quel branch nel menu "Use workflow from". La pianificazione
automatica (`schedule`) di GitHub funziona solo sul branch principale.
Quindi l'archivio a 15 minuti cresce da solo ogni giorno solo quando i file sono sul branch
principale: sul branch di prova va lanciato a mano (almeno una volta ogni 59 giorni per non perdere giorni).

## Come leggere i risultati (Supabase -> SQL Editor)

Tutte le regole di un titolo, le piu' probabili in cima:

```sql
select timeframe, window_label as finestra, horizon_label as dopo, kind,
       round(threshold_pct, 2) as soglia_pct,
       case when direction = 1 then 'SALE' else 'SCENDE' end as poi,
       round(prob_dir * 100) as prob_pct, n_all, round(mean_after_pct, 2) as media_dopo_pct,
       round((case when direction = 1 then p_up_oos else 1 - p_up_oos end) * 100) as prob_verifica_pct,
       n_oos, validated
from rule_stats
where ticker = 'NVDA'
order by validated desc, prob_dir desc;
```

Le regole che danno notifiche, per tutti i titoli:

```sql
select ticker, timeframe, window_label, horizon_label, kind, round(threshold_pct, 2) as soglia_pct,
       direction, round(prob_dir * 100) as prob_pct, n_all
from rule_stats
where validated and prob_dir >= 0.60 and n_all >= 25
order by prob_dir desc;
```

Gli eventi con data e ora esatte (ora italiana), per controllarli su un grafico:

```sql
select bar_time at time zone 'Europe/Rome' as ora_italiana,
       price_start, price0, round(change_pct, 2) as variazione_finestra_pct,
       price_after, round(outcome_pct, 2) as variazione_dopo_pct, went_up as e_salito, segment
from rule_events
where ticker = 'NVDA' and timeframe = 'h' and window_len = 8 and horizon_len = 4
  and kind = 'drop' and k = 1.5
order by bar_time;
```

Significato dei campi di `rule_stats`:

| Campo | Significato |
|---|---|
| `timeframe` | `h` = ore (barre da 15 min), `d` = giorni di borsa |
| `window_len` / `window_label` | finestra in cui si misura il calo/rialzo (es. 8 barre = 2h) |
| `horizon_len` / `horizon_label` | quanto dopo si guarda l'esito |
| `kind` | `drop` = e' sceso almeno della soglia, `rise` = e' salito almeno della soglia |
| `k`, `threshold_pct` | soglia in multipli della variazione normale del titolo, e la stessa in % |
| `direction` | +1 = dopo di solito SALE, -1 = dopo di solito SCENDE (scelta solo sul periodo di scoperta) |
| `prob_dir` | % di volte (0-1) in cui e' andato nella direzione indicata, su tutto lo storico |
| `n_all` | numero di eventi (senza sovrapposizioni) |
| `p_up_is`, `n_is` | % di salite ed eventi nel periodo di scoperta (primo 70%) |
| `p_up_oos`, `n_oos` | idem nel periodo di verifica (ultimo 30%, mai usato per scegliere) |
| `base_is`, `base_oos` | % di salite in un momento qualunque (per confronto) |
| `pval_is`, `pval_oos` | probabilita' che la differenza dal "momento qualunque" sia solo caso (piccolo = meglio) |
| `mean_after_pct` | variazione media dopo l'orizzonte |
| `net_oos_pct` | vantaggio medio sui dati mai visti meno i costi (0,10%) |
| `validated` | true solo se supera tutti i controlli |

In `rule_events`: `bar_time` e' l'inizio della candela in cui la soglia e' stata superata (per le
regole in giorni, il giorno). `price_start` = prezzo all'inizio della finestra, `price0` = prezzo al
segnale, `price_after` = prezzo dopo l'orizzonte.

## Cose da sapere

- **Nessuna certezza**: nei mercati le regole che reggono alla verifica hanno di solito probabilita'
  del 55-60%. Con la soglia al 60% e la validazione e' possibile che arrivino pochissime notifiche,
  o nessuna. Per averne di piu': abbassa `MIN_NOTIFY_PROB` (es. 0.57) oppure metti
  `NOTIFY_ONLY_VALIDATED = False` (piu' avvisi, ma molti saranno fortuna dello storico).
- **Ore**: all'inizio solo 59 giorni di dati a 15 minuti per titolo -> pochi eventi (migliora con
  l'archivio che cresce). Le regole in giorni
  hanno tutto lo storico e sono piu' affidabili.
- **Regole in giorni dal vivo**: durante la seduta si usa il prezzo attuale come chiusura di oggi
  (approssimazione); l'esito viene verificato dopo circa 7/5 dei giorni di borsa in giorni di calendario.
- **Mercati chiusi**: tra le 02:00 e le 10:00 italiane non ci sono prezzi delle azioni USA su Yahoo.
  Crypto, metalli, materie prime e i future `ES=F` / `NQ=F` restano attivi.
- **Minuti GitHub**: nei repository privati sono 2000/mese, pochi per un giro ogni 15 minuti;
  nei pubblici sono illimitati.
- Strumento informativo, non e' un consiglio di investimento.

## File

| File | A cosa serve |
|---|---|
| `config.py` | ticker, soglie, intervalli delle regole, probabilita' minima per le notifiche |
| `archive.py` | archivio permanente delle barre a 15 minuti (cartella `data/`) |
| `rules.py` | regole "calo/rialzo -> esito": calcolo, validazione, controllo dal vivo |
| `main.py` | ogni 15 minuti: prezzi, avvisi, regole, verifica dei segnali |
| `learn.py` | ogni giorno: ricalcolo delle regole per titolo |
| `model.py` | vecchio modello a polinomio (spento di default) |
| `common.py` | Supabase e Telegram |
| `supabase_schema.sql` | tabelle del database |
| `.github/workflows/` | `main.yml` (ogni 15 minuti) e `learn.yml` (ogni giorno) |
