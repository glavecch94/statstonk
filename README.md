# Statstonk

Dashboard di analisi statistica calcistica pre-partita e live, con alert Telegram e pronostici automatici.
Fonte dati: **SofaScore** (fixture, quote, formazioni, statistiche live).

---

## Stack tecnologico

| Componente | Tecnologia |
|---|---|
| Dashboard | Streamlit |
| Scraping | Playwright (Chromium headless) |
| Database | SQLite via SQLAlchemy |
| Scheduler | APScheduler |
| Alert | Telegram Bot API |
| Analisi | Pandas |
| Deploy | Hetzner Cloud CAX11 (ARM, Ubuntu 24.04) |
| Reverse proxy | Nginx |

---

## Avvio locale

```bash
# Avvia la dashboard
pkill -f streamlit; sleep 1 && .venv/bin/streamlit run dashboard/app.py --server.headless true &
sleep 3 && open -na "Brave Browser" --args --incognito http://localhost:8501

# Avvia lo scheduler (sync 08:00/23:30 · picks 09:00/17:00 · lineups ogni 30min · alert ogni 3min)
.venv/bin/python scheduler/runner.py --now
```

---

## Installazione (prima esecuzione)

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/playwright install chromium
cp .env.example .env   # edita con le tue variabili
.venv/bin/python -c "from db import engine; from models import *; from models.base import Base; Base.metadata.create_all(engine)"
```

---

## Configurazione `.env`

| Variabile | Descrizione |
|---|---|
| `TELEGRAM_BOT_TOKEN` | Token del bot Telegram |
| `TELEGRAM_CHAT_ID` | Chat ID per gli alert |

---

## Scheduler

```bash
.venv/bin/python scheduler/runner.py          # avvia e resta in ascolto
.venv/bin/python scheduler/runner.py --now    # sync immediato poi resta attivo
.venv/bin/python scheduler/runner.py --once   # sync + picks una volta ed esci
.venv/bin/python scheduler/runner.py --historical  # carica tutto lo storico ed esci
```

| Job | Orario |
|---|---|
| Sync stagione corrente | 08:00, 23:30 |
| Aggiorna esiti pick | 08:15, 23:45 |
| Risultati pick Telegram | 08:30 |
| Genera picks | 09:00, 17:00 |
| Pick giornalieri Telegram | 09:30 |
| Aggiorna formazioni | ogni 30 min |
| Alert live Telegram | ogni 3 min |

---

## Debug manuale

```bash
.venv/bin/python -c "from scheduler.jobs import sync_all_leagues; sync_all_leagues()"
.venv/bin/python -c "from scheduler.jobs import generate_picks_all_leagues; generate_picks_all_leagues()"
.venv/bin/python -c "from scheduler.jobs import update_pick_outcomes; update_pick_outcomes()"
.venv/bin/python -c "from scheduler.jobs import update_lineups; update_lineups()"
.venv/bin/python -c "from analytics.calibration import save_calibration; save_calibration()"
```

---

## Deploy su Hetzner Cloud

### Requisiti
- Hetzner CAX11 (ARM, 4GB RAM) con Ubuntu 24.04
- Dominio DuckDNS puntato all'IP del server

### Setup iniziale

```bash
apt update && apt upgrade -y && apt install -y git python3 python3-pip python3-venv screen nginx certbot python3-certbot-nginx
git clone https://github.com/glavecch94/statstonk.git && cd statstonk
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/playwright install-deps chromium && .venv/bin/playwright install chromium
cp .env.example .env  # edita con le variabili reali
.venv/bin/python -c "from db import engine; from models import *; from models.base import Base; Base.metadata.create_all(engine)"
```

### Avvio con screen

```bash
screen -S dashboard -dm bash -c "cd /root/statstonk && PYTHONPATH=/root/statstonk .venv/bin/streamlit run dashboard/app.py --server.headless true --server.port 8501"
screen -S scheduler -dm bash -c "cd /root/statstonk && PYTHONPATH=/root/statstonk .venv/bin/python scheduler/runner.py --now"
```

### Firewall

```bash
ufw allow 22 && ufw allow 80 && ufw allow 443 && ufw allow 8501 && ufw enable
```

### Nginx + HTTPS

```bash
# Crea /etc/nginx/sites-available/statstonk con:
server {
    listen 80;
    server_name statstonk.duckdns.org;
    location / {
        proxy_pass http://localhost:8501;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
        proxy_cache_bypass $http_upgrade;
    }
}

ln -s /etc/nginx/sites-available/statstonk /etc/nginx/sites-enabled/
nginx -t && systemctl reload nginx
certbot --nginx -d statstonk.duckdns.org
```

### Update

```bash
cd /root/statstonk && git pull
screen -S dashboard -X quit && screen -S scheduler -X quit
screen -S dashboard -dm bash -c "cd /root/statstonk && PYTHONPATH=/root/statstonk .venv/bin/streamlit run dashboard/app.py --server.headless true --server.port 8501"
screen -S scheduler -dm bash -c "cd /root/statstonk && PYTHONPATH=/root/statstonk .venv/bin/python scheduler/runner.py"
```

---

## Struttura

```
statstonk/
├── config.py               — LEAGUES (13 leghe), RATE_LIMITS, CURRENT_SEASON
├── db.py                   — SQLAlchemy engine, SessionLocal, get_session()
├── models/                 — Match, Team, Odd, Lineup, LineupPlayer, Pick, Player
├── scrapers/
│   ├── base.py             — BaseScraper + RateLimiter singleton
│   └── sofascore.py        — Fixture, quote, formazioni, statistiche, storico
├── analytics/
│   ├── prematch.py         — Form, H2H, standings (pandas)
│   ├── signals.py          — 44 segnali + save_picks()
│   ├── picks.py            — evaluate_pick_outcome()
│   └── calibration.py      — Soglie dinamiche per mercato
├── scheduler/
│   ├── jobs.py             — Tutti i job schedulati
│   ├── runner.py           — APScheduler entry point
│   └── alerts.py           — Alert Telegram live + messaggi giornalieri
├── dashboard/
│   ├── app.py              — Entry point Streamlit
│   ├── components.py       — SVG formazioni condiviso
│   └── pages/
│       ├── 0_live.py           — Live scores + stats + formazioni
│       ├── 1_prossime_partite.py — Fixture + analisi + picks
│       └── 2_pronostici.py     — Storico picks + bankroll sim + calibrazione
└── data/                   — SQLite + calibration.json + interventions.json (gitignored)
```

---

## Campionati supportati

Serie A · Premier League · La Liga · Bundesliga · Ligue 1 · Eredivisie · Primeira Liga · Championship · Champions League · Europa League · Conference League · Brasileirão · Copa Libertadores
