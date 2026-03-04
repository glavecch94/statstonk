from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

# ── Database ──────────────────────────────────────────────────────────────────
DATABASE_URL: str = os.getenv("DATABASE_URL", f"sqlite:///{DATA_DIR}/statstonk.db")

# ── Telegram ──────────────────────────────────────────────────────────────────
TELEGRAM_BOT_TOKEN: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID: str = os.getenv("TELEGRAM_CHAT_ID", "")

# ── Rate limiting (secondi minimi tra richieste per dominio) ──────────────────
# Valori conservativi per evitare ban/throttling
RATE_LIMITS: dict[str, float] = {
    "sofascore.com": 2.0,
    "api.sofascore.com": 2.0,
    "default": 2.0,
}

# ── Campionati supportati ─────────────────────────────────────────────────────
# football_data_code → codice per football-data.co.uk (dati storici CSV)
# Competizioni senza football_data_code non hanno dati storici scaricabili
LEAGUES: dict[str, dict] = {
    # ── Top 5 europei (dati storici + live) ───────────────────────────────────
    "serie_a": {
        "name":               "Serie A",
        "country":            "Italy",
        "football_data_code": "I1",
    },
    "premier_league": {
        "name":               "Premier League",
        "country":            "England",
        "football_data_code": "E0",
    },
    "la_liga": {
        "name":               "La Liga",
        "country":            "Spain",
        "football_data_code": "SP1",
    },
    "bundesliga": {
        "name":               "Bundesliga",
        "country":            "Germany",
        "football_data_code": "D1",
    },
    "ligue_1": {
        "name":               "Ligue 1",
        "country":            "France",
        "football_data_code": "F1",
    },
    # ── Altre leghe europee (live only) ───────────────────────────────────────
    "eredivisie": {
        "name":               "Eredivisie",
        "country":            "Netherlands",
        "football_data_code": "N1",
    },
    "primeira_liga": {
        "name":               "Primeira Liga",
        "country":            "Portugal",
        "football_data_code": "P1",
    },
    "championship": {
        "name":               "Championship",
        "country":            "England",
        "football_data_code": "E1",
    },
    # ── Competizioni europee (live only, no dati storici CSV) ─────────────────
    "champions_league": {
        "name":    "Champions League",
        "country": "Europe",
    },
    "europa_league": {
        "name":    "Europa League",
        "country": "Europe",
    },
    "conference_league": {
        "name":    "Conference League",
        "country": "Europe",
    },
    # ── Sud America (live only) ───────────────────────────────────────────────
    "brasileirao": {
        "name":    "Brasileirão Série A",
        "country": "Brazil",
    },
    "copa_libertadores": {
        "name":    "Copa Libertadores",
        "country": "South America",
    },
}

CURRENT_SEASON = "2025-26"

# Stagioni storiche da caricare (dalla corrente indietro di 10 anni)
HISTORICAL_SEASONS: list[str] = [
    "2025-26", "2024-25", "2023-24", "2022-23", "2021-22",
    "2020-21", "2019-20", "2018-19", "2017-18", "2016-17",
]
