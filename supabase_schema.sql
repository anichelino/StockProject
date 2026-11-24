-- Esegui tutto in Supabase -> SQL Editor -> New query -> Run.
-- Le tabelle che esistono gia' non vengono toccate.

create table if not exists stock_prices (
    id bigserial primary key,
    ticker text not null,
    price numeric not null,
    "timestamp" timestamptz not null default now(),
    dropdown numeric
);
create index if not exists stock_prices_ticker_ts_idx on stock_prices (ticker, "timestamp");

create table if not exists dropdowns (
    id bigserial primary key,
    ticker text not null,
    initial_price numeric,
    final_price numeric,
    max_price numeric,
    min_price numeric,
    dropdown numeric,
    start_timestamp timestamptz,
    end_timestamp timestamptz,
    calculated_at timestamptz default now()
);

create table if not exists risings (
    id bigserial primary key,
    ticker text not null,
    initial_price numeric,
    final_price numeric,
    max_price numeric,
    min_price numeric,
    rise numeric,
    start_timestamp timestamptz,
    end_timestamp timestamptz,
    calculated_at timestamptz default now()
);

-- Risultati dell'apprendimento: UNA RIGA PER OGNI TITOLO e caso (riscritta ogni giorno da learn.py).
-- Se avevi gia' la vecchia versione (senza colonna ticker) viene sostituita: si riempie di nuovo
-- al prossimo ricalcolo.
drop table if exists signal_stats;
create table signal_stats (
    ticker text not null,
    window_bars int not null,
    fwd_bars int not null,
    bucket int not null,
    shape text,
    direction int,
    n_is int, p_up_is numeric, base_is numeric, pval_is numeric,
    n_oos int, p_up_oos numeric, base_oos numeric, pval_oos numeric,
    p_up_all numeric, n_all int,
    edge_oos_pct numeric, net_oos_pct numeric,
    validated boolean default false,
    updated_at timestamptz default now(),
    primary key (ticker, window_bars, fwd_bars, bucket)
);

-- Elenco degli eventi (data e ora esatte) dei casi piu' promettenti, per controllarli a mano
create table if not exists signal_events (
    id bigserial primary key,
    ticker text not null,
    window_bars int not null,
    fwd_bars int not null,
    bucket int not null,
    shape text,
    bar_time timestamptz not null,
    price0 numeric,
    price_after numeric,
    outcome_pct numeric,
    went_up boolean,
    segment text
);
create index if not exists signal_events_key_idx on signal_events (ticker, window_bars, fwd_bars, bucket);

-- Registro dei segnali inviati e di come sono andati davvero
create table if not exists signals_log (
    id bigserial primary key,
    created_at timestamptz not null default now(),
    ticker text not null,
    window_bars int not null,
    fwd_bars int not null,
    bucket int not null,
    direction int not null,
    price0 numeric not null,
    p_up numeric,
    resolved boolean not null default false,
    outcome_pct numeric,
    resolved_at timestamptz
);
create index if not exists signals_log_created_idx on signals_log (created_at);
-- colonne aggiunte per le regole "calo/rialzo -> esito" (sicuro rieseguirle)
alter table signals_log add column if not exists kind text not null default 'curve';
alter table signals_log add column if not exists rule_key text;
alter table signals_log add column if not exists horizon_minutes int;
alter table signals_log alter column window_bars drop not null;
alter table signals_log alter column fwd_bars drop not null;
alter table signals_log alter column bucket drop not null;

-- ===== REGOLE "se scende/sale di X% entro un intervallo, poi quante volte risale/riscende" =====
-- Una riga per titolo / ore-o-giorni / finestra / orizzonte / calo-o-rialzo / soglia. Riscritta ogni giorno.
drop table if exists rule_stats;
create table rule_stats (
    ticker text not null,
    timeframe text not null,          -- 'h' = ore (barre da 15 min), 'd' = giorni di borsa
    window_len int not null,          -- lunghezza finestra: barre da 15 min (h) o giorni (d)
    horizon_len int not null,         -- quanto dopo si guarda l'esito, stessa unita'
    kind text not null,               -- 'drop' = e' sceso, 'rise' = e' salito
    k numeric not null,               -- soglia in multipli della variazione normale del titolo
    threshold_pct numeric,            -- soglia reale in % (positiva)
    window_label text,
    horizon_label text,
    direction int,                    -- +1 = di solito poi SALE, -1 = di solito poi SCENDE
    prob_dir numeric,                 -- probabilita' storica nella direzione indicata (0-1)
    n_is int, p_up_is numeric, base_is numeric, pval_is numeric,
    n_oos int, p_up_oos numeric, base_oos numeric, pval_oos numeric,
    n_all int, p_up_all numeric,
    mean_after_pct numeric,           -- variazione media dopo l'orizzonte
    edge_oos_pct numeric, net_oos_pct numeric,
    validated boolean default false,
    updated_at timestamptz default now(),
    primary key (ticker, timeframe, window_len, horizon_len, kind, k)
);

-- Data e ora esatte di ogni evento, per controllarli a mano su un grafico
create table if not exists rule_events (
    id bigserial primary key,
    ticker text not null,
    timeframe text not null,
    window_len int not null,
    horizon_len int not null,
    kind text not null,
    k numeric not null,
    bar_time timestamptz not null,    -- momento in cui la soglia e' stata superata (chiusura della barra)
    price_start numeric,              -- prezzo all'inizio della finestra
    price0 numeric,                   -- prezzo al momento del segnale
    price_after numeric,              -- prezzo dopo l'orizzonte
    change_pct numeric,               -- variazione nella finestra (negativa = calo)
    outcome_pct numeric,              -- variazione dopo l'orizzonte
    went_up boolean,
    segment text                      -- 'is' scoperta, 'oos' verifica
);
create index if not exists rule_events_key_idx on rule_events (ticker, timeframe, window_len, horizon_len, kind, k);

-- ===== Svuotamento veloce delle tabelle di statistica (usato ogni giorno da learn.py) =====
-- TRUNCATE e' istantaneo anche con centinaia di migliaia di righe (DELETE va in timeout).
create or replace function public.reset_rule_tables() returns void
language plpgsql security definer set search_path = public as $$
begin
  truncate table public.rule_events, public.rule_stats restart identity;
end;
$$;

create or replace function public.reset_signal_tables() returns void
language plpgsql security definer set search_path = public as $$
begin
  truncate table public.signal_events, public.signal_stats restart identity;
end;
$$;
