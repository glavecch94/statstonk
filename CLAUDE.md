# CLAUDE.md

Guida completa al codebase Statstonk per Claude Code. Aggiornata Feb 2026.

---

## Cosa fa il progetto

**Statstonk** è una dashboard di analisi calcistica pre-partita e live.
Fonte dati principale: **SofaScore** (via Playwright). Nessuna altra API esterna è richiesta per il funzionamento base.

---

## Comandi rapidi

```bash
# Avvia dashboard
pkill -f streamlit; sleep 1 && .venv/bin/streamlit run dashboard/app.py --server.headless true &
sleep 3 && open -na "Brave Browser" --args --incognito http://localhost:8501

# Scheduler (sync 08:00/23:30 + picks 09:00/17:00 + lineups ogni 30min)
.venv/bin/python scheduler/runner.py &
.venv/bin/python scheduler/runner.py --now    # sync immediato poi resta attivo
.venv/bin/python scheduler/runner.py --once   # sync + picks una volta ed esci
.venv/bin/python scheduler/runner.py --historical  # carica tutto lo storico ed esci

# Debug manuale
python -c "from scheduler.jobs import update_lineups; update_lineups()"
python -c "from scheduler.jobs import update_pick_outcomes; update_pick_outcomes()"
python -c "from scheduler.jobs import generate_picks_all_leagues; generate_picks_all_leagues()"
python -c "from analytics.calibration import save_calibration; save_calibration()"

# Lint
.venv/bin/ruff check . && .venv/bin/ruff format .
```

---

## Architettura completa

```
statstonk/
├── config.py                   — LEAGUES (13 leghe), RATE_LIMITS, CURRENT_SEASON
├── db.py                       — SQLAlchemy engine, SessionLocal, get_session()
├── models/
│   ├── __init__.py             — importa tutti i modelli (necessario per alembic/create_all)
│   ├── base.py                 — Base declarativa
│   ├── matches.py              — Match, Odd, MatchStatus
│   ├── teams.py                — Team
│   ├── lineups.py              — Lineup, LineupPlayer
│   ├── picks.py                — Pick
│   └── players.py              — Player (usato da SofaScore sync)
├── scrapers/
│   ├── base.py                 — BaseScraper, RateLimiter (singleton globale)
│   ├── sofascore.py            — SofaScoreScraper (Playwright, fonte principale)
│   └── pinnacle.py             — Quote Pinnacle via The Odds API (opzionale)
├── analytics/
│   ├── prematch.py             — Funzioni analitiche pandas (form, H2H, ritardi, standings)
│   ├── signals.py              — Generazione 44 segnali + save_picks() con calibrazione
│   ├── picks.py                — evaluate_pick_outcome() + OUTCOME_MAP
│   └── calibration.py          — Difficoltà base mercati + calibrazione dinamica da storico
├── scheduler/
│   ├── jobs.py                 — Tutti i job: sync, picks, lineups, esiti, calibrazione
│   └── runner.py               — APScheduler entry point (--now / --once / --historical)
├── dashboard/
│   ├── app.py                  — Entry point Streamlit
│   ├── components.py           — half_pitch_svg(), render_lineup() (SVG maglia condiviso)
│   └── pages/
│       ├── 0_prossima_giornata.py  — Fixture + analisi + picks auto-save
│       ├── 2_live.py               — Live scores + stats + formazioni
│       ├── 3_schedina.py           — Pick pendenti + puntata suggerita
│       └── 4_storico.py            — Storico pick + bankroll sim + calibrazione
└── data/                       — SQLite (statstonk.db) + calibration.json (gitignored)
```

---

## SofaScore scraper (`scrapers/sofascore.py`)

Unica fonte per fixture, quote, formazioni, statistiche, storico.
Usa **Playwright** (Chromium headless) — prerequisito: `.venv/bin/playwright install chromium`.

```python
with SofaScoreScraper() as ss:
    ss.get_scheduled_events(date_str)          # lista eventi per data "YYYY-MM-DD"
    ss.get_live_events()                       # eventi live ora
    ss.get_odds(event_id)                      # 15+ mercati
    ss.get_lineups(event_id)                   # {confirmed, home/away: {formation, players}}
    ss.get_statistics(event_id)                # {ALL/1ST/2ND: {stat_name: {home, away}}}
    ss.get_upcoming_fixtures(league_key, days) # fixture prossimi N giorni
    ss.get_all_upcoming_matches(days_ahead)    # tutte le leghe
    ss.get_all_today_matches()                 # partite odierne (per live page)
    ss.get_all_for_league(league_key, dates)   # bulk odds+lineups
    ss.get_seasons(league_key)                 # stagioni disponibili
    ss.get_season_results(league_key, season_id)  # risultati stagione (paginati)
    ss.get_match_stats(event_id)               # stats piatte per il DB
```

Costanti chiave: `SS_TOURNAMENT_IDS` (league_key → tournament_id), `SS_TEAM_NAME_MAP`.
Helper modulo: `_season_to_ss_year("2025-26") → "25/26"`.

Il campo `grid` nei player è `"riga:colonna"` (es. `"2:3"`), usato per posizionamento SVG.

---

## Modelli DB

### `Match` (`models/matches.py`)
Campi principali: `home_score`, `away_score`, `home_corners`, `away_corners`,
`home_yellow_cards`, `away_yellow_cards`, `status` (MatchStatus enum), `external_id` (`ss_{event_id}`).

### `Lineup` / `LineupPlayer` (`models/lineups.py`)
- Unique constraint: `(tm_match_id, side)` — `tm_match_id` = `ss_{event_id}` per SofaScore
- `is_official=True` non viene sovrascritto (lineup confermata non cambia)
- Player fields: `name`, `shirt_number`, `position`, `grid`, `is_starting`, `is_captain`

### `Pick` (`models/picks.py`)
- Unique constraint: `(home_team, away_team, match_date, mercato)`
- `esito`: `True`=vinto, `False`=perso, `None`=pendente
- `segnali`: numero di segnali che supportano il pick
- `quota`: quota al momento del salvataggio

---

## Pipeline picks

```
generate_picks_all_leagues()  [scheduler/jobs.py]
  └── per lega: get_upcoming_fixtures() → prematch.py (form, H2H, delays)
      └── signals.generate_signals()  → 44 segnali → counts per mercato
          └── signals.filter_conflicts()  → rimuove mercati mutuamente esclusivi
              └── signals.save_picks()
                    ├── calibration.load_calibration()
                    ├── calibration.effective_min_signals(mercato, calib)
                    └── upsert Pick in DB se count >= soglia AND quota >= 1.2

update_pick_outcomes()  [scheduler/jobs.py]
  ├── cerca Match FINISHED entro ±4h dal kickoff
  ├── picks.evaluate_pick_outcome(mercato, hg, ag, hc, ac, hy, ay)
  └── se updated > 0: calibration.save_calibration()
```

---

## Sistema calibrazione (`analytics/calibration.py`)

Due livelli cumulativi: `effective_min_signals(mercato, calib) = max(base, dynamic)`

**`MARKET_BASE_MIN_SIGNALS`** — difficoltà statica (sempre applicata):
- `2`: Over/Under 2.5, BTTS, Over 1.5, Under 3.5, 12 DC
- `3`: 1X2, Over 3.5, Under 1.5, No Goal, Corner, Handicap, Doppia Chance
- `4`: Pareggio (X), tutti i Cartellini Gialli, Corner Over 11.5 / Under 8.5

**Calibrazione dinamica** (da storico, aggiornata dopo ogni `update_pick_outcomes`):
- ≥15 pick + HR < 40% → base + 1 o base + 2
- ≥25 pick + (HR < 27% o ROI < −22%) → mercato bloccato

File output: `data/calibration.json` — `{"markets": {...}, "market_min_segnali": {...}, "blocked_markets": [...]}`

---

## Segnali (`analytics/signals.py`)

`generate_signals(df, home, away, ...)` → lista di 44 dict `{icon, text, mercato}`.
`filter_conflicts(counts)` → rimuove mercati mutuamente esclusivi (Over/Under stesso livello, ecc.).
`save_picks(home, away, match_date, matchday, league_key, counts, match_odds)` → upsert + cleanup pendenti.

Soglie interne principali (tipiche): `>= 0.55–0.65` per form offensiva/difensiva, `>= 0.50` per H2H, delay ≥ 4 partite, ecc.

---

## Dashboard pages

### `0_prossima_giornata.py`
- `load_sofascore_all(date_strs)` TTL=3600s — carica odds+lineups in bulk da SS
- `load_historical(league_key)` TTL=600s — dati storici dal DB
- Layout per fixture: pronostici supportati → previsione statistica (expander) → formazioni (expander) → H2H (expander)
- Formazioni: usa `dashboard.components.render_lineup` per SVG a mezza-campo con icone maglia
- Quote: solo SofaScore (niente Pinnacle esterno)

### `2_live.py`
- `load_all_today_matches()` TTL=90s, `load_sofascore_stats(event_id)` TTL=300s
- `load_sofascore_lineups(event_id)` TTL=120s
- `_ss_stats_to_live_format(ss_stats)` — converte {ALL/1ST/2ND} → {home/away + first_half/second_half}
- `_score_card_html()` — header dark con score
- `_stat_bar_html()` — barra CSS proporzionale home/away
- `_render_momentum_chart()` — confronto 1°/2° tempo con shift indicator
- xG highlight: `xG > gol + 1` → sfondo ambra (#f59e0b)
- Formazioni in expander "👥 Formazioni" tramite `components.render_lineup`

### `3_schedina.py`
- Pick pendenti raggruppati per bucket segnali (≥4 verde, 3 giallo, 2 blu)
- Sidebar: bankroll + stake % per segnali (2→1.5%, 3→2.5%, 4→5%, 5+→7.5% default)
- Colonna "Puntata (€)" = `bankroll × stake_pct_for(segnali) / 100`

### `4_storico.py`
- Filtri: campionato, stagione, esito, segnali minimi
- Sidebar bankroll: input € + 4 stake % per segnali (stessi default di schedina)
- `_stake_pct(segnali)` → % · `_stake_eur(segnali, bankroll)` → €
- `_bankroll_sim(df, bankroll)` → (DataFrame curva, totale_investito)
- Tabella per giorno: Campionato · Partita · Mercato · Segnali · Quota · **Puntata** · **Esito** · **Netto (€)**
- Expander aperti default: primi 2 giorni `>= oggi` (partite da giocare o di oggi)
- Sezione "🔧 Calibrazione modello": tabella base/effettiva/HR/ROI per ogni mercato

### `dashboard/components.py`
- `half_pitch_svg(formation, players, is_home) → str` — SVG mezza-campo con icone maglia
  - Maglia: `<path>` con maniche + colletto V, colore blu (home) / rosso (away)
  - Posizionamento via campo `grid` ("riga:colonna") di SofaScore
  - Mirroring: home inverte l'indice colonna (`col_idx = n-1-j`), away usa ordine originale
- `render_lineup(formation, players, is_home, team_label, badge)` → Streamlit (caption + img base64 + panchina)

---

## Scheduler jobs (`scheduler/jobs.py`)

| Job | Orario | Funzione |
|---|---|---|
| sync stagione corrente | 08:00, 23:30 | `sync_all_leagues()` |
| genera picks | 09:00, 17:00 | `generate_picks_all_leagues()` |
| aggiorna lineups | ogni 30min | `update_lineups()` |
| aggiorna esiti | 08:15, 23:45 | `update_pick_outcomes()` → auto-run `save_calibration()` |

`_upsert_sofascore_match()`: deduplicazione per `external_id` (`ss_{event_id}`), fallback su (home_id, away_id, lega, stagione, data ±2gg).

---

## Convenzioni

**Rate limiting**: solo `self.get()` / `self.get_json()` negli scraper, mai `requests.get()` diretto.

**Session DB**: usare `get_session()` come context manager (commit automatico). `SessionLocal()` solo per query read-only con `try/finally session.close()`.

**Streamlit**:
- `width="stretch"` (non `use_container_width=True`, deprecato da 2025-12-31)
- `.style.map()` (non `.style.applymap()`, deprecato)
- `@st.fragment` per auto-refresh parziale (live page)

**Team name maps**: ogni scraper ha `*_TEAM_NAME_MAP` che normalizza → nomi interni (football-data.co.uk come riferimento). Se mancante: passa raw + log warning.

**Linee lunghe**: aggiungere `# noqa: E501` se la riga non è spezzabile senza perdere leggibilità.

**Nuovi modelli**: aggiungere import in `models/__init__.py`.

**`matchday`**: può essere `None` per coppe a eliminazione diretta.

---

## Campionati supportati

Definiti in `config.LEAGUES` (13 totali):
- **Prossima Giornata** (8 leghe con dati storici): Serie A, Premier League, La Liga, Bundesliga, Ligue 1, Eredivisie, Primeira Liga, Championship
- **Live** (tutte 13): + Champions League, Europa League, Conference League, Brasileirão, Copa Libertadores

---

## File di dati (gitignored)

- `data/statstonk.db` — SQLite principale
- `data/calibration.json` — soglie calibrazione dinamica per mercato
- `data/*.csv` — cache CSV storici football-data.co.uk
