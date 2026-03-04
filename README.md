# Statstonk

Dashboard di analisi statistica pre-partita per le scommesse calcistiche.
Fonti dati: **SofaScore** (fixture, quote, formazioni, statistiche live) · **Pinnacle** via The Odds API (value bet).

---

## Avvio rapido

```bash
# Avvia la dashboard (kill sessioni precedenti + apri Brave)
pkill -f streamlit; sleep 1 && .venv/bin/streamlit run dashboard/app.py --server.headless true &
sleep 3 && open -na "Brave Browser" --args http://localhost:8501
```

Poi vai su `http://localhost:8501`.

---

## Prerequisiti (prima esecuzione)

```bash
# Installa il browser Playwright (necessario per SofaScore)
.venv/bin/playwright install chromium

# Carica i dati storici da SofaScore (operazione una-tantum, ~35 min)
.venv/bin/python scheduler/runner.py --historical
```

---

## Configurazione

```bash
cp .env.example .env   # poi edita .env
```

| Variabile | Descrizione | Richiesta per |
|---|---|---|
| `ODDS_API_KEY` | [the-odds-api.com](https://the-odds-api.com) — 500 req/mese gratis | Quote Pinnacle (value bet) |
| `TELEGRAM_BOT_TOKEN` + `TELEGRAM_CHAT_ID` | Bot Telegram | Alert live |

> `FOOTBALL_DATA_ORG_KEY` e `API_FOOTBALL_KEY` non sono più necessarie: tutto passa da SofaScore.

---

## Scheduler (aggiornamento automatico)

```bash
# Avvia lo scheduler in background (sync 08:00 e 23:30, picks 09:00 e 17:00, lineups ogni 30min)
.venv/bin/python scheduler/runner.py &

# Avvia con sync immediato, poi resta attivo
.venv/bin/python scheduler/runner.py --now &

# Sync + picks una sola volta ed esci
.venv/bin/python scheduler/runner.py --once

# Carica tutti i dati storici (10 stagioni, tutte le leghe) ed esci
.venv/bin/python scheduler/runner.py --historical

# Verifica se lo scheduler è in esecuzione
pgrep -fl "scheduler/runner"
```

---

## Comandi utili per il debug

```bash
# Aggiorna risultati stagione corrente per una lega
python -c "from scheduler.jobs import sync_current_season; sync_current_season('serie_a')"

# Genera picks manualmente
python -c "from scheduler.jobs import generate_picks_all_leagues; generate_picks_all_leagues()"

# Aggiorna esiti pick
python -c "from scheduler.jobs import update_pick_outcomes; update_pick_outcomes()"

# Aggiorna formazioni
python -c "from scheduler.jobs import update_lineups; update_lineups()"

# Test SofaScore diretto
python -c "
from scrapers.sofascore import SofaScoreScraper
with SofaScoreScraper() as ss:
    print(ss.get_upcoming_fixtures('serie_a'))
"
```

---

## Struttura

```
statstonk/
├── config.py               — env vars, LEAGUES (13 leghe), RATE_LIMITS
├── db.py                   — SQLAlchemy engine + SessionLocal
├── models/                 — Match, Team, Odd, Lineup, Pick, Player...
├── scrapers/
│   ├── base.py             — BaseScraper + RateLimiter
│   ├── sofascore.py        — Tutto: fixture, quote, formazioni, statistiche, storico
│   └── pinnacle.py         — Quote Pinnacle via The Odds API
├── analytics/
│   ├── prematch.py         — Analisi pre-partita (pandas)
│   └── signals.py          — Generazione segnali + picks
├── scheduler/
│   ├── jobs.py             — Sync, picks, lineups, esiti
│   └── runner.py           — APScheduler (blocking + background)
├── dashboard/
│   ├── app.py              — Entry point Streamlit
│   └── pages/
│       ├── 0_prossima_giornata.py  — Fixture + analisi + picks
│       ├── 2_live.py               — Live monitoring (SofaScore)
│       ├── 3_schedina.py           — Picks pendenti
│       └── 4_storico.py            — Storico picks
└── data/                   — SQLite + cache (gitignored)
```

## Campionati supportati

**Prossima Giornata + Live**: Serie A, Premier League, La Liga, Bundesliga, Ligue 1, Eredivisie, Primeira Liga, Championship, Champions League, Europa League, Conference League, Brasileirão, Copa Libertadores.
