"""
Scraper per football-data.co.uk

Scarica CSV con risultati, statistiche e quote di chiusura.
URL pattern: https://www.football-data.co.uk/mmz4281/{season_code}/{league_code}.csv

Uso diretto:
    python scrapers/football_data_co.py                           # Serie A stagione corrente
    python scrapers/football_data_co.py serie_a 2024-25 2023-24  # più stagioni
"""

from __future__ import annotations

import io
import logging
import sys
from datetime import datetime, timezone
from typing import Any

import pandas as pd
from sqlalchemy import func
from sqlalchemy.orm import Session

from config import DATA_DIR, LEAGUES
from db import get_session, init_db
from models.matches import Match, MatchStatus, Odd
from models.teams import Team
from scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

CACHE_DIR = DATA_DIR / "football_data"
CACHE_DIR.mkdir(exist_ok=True)

# ── Mapping colonne CSV → campo Match ─────────────────────────────────────────

STAT_MAP: dict[str, str] = {
    "HS":  "home_shots",
    "AS":  "away_shots",
    "HST": "home_shots_on_target",
    "AST": "away_shots_on_target",
    "HC":  "home_corners",
    "AC":  "away_corners",
    "HF":  "home_fouls",
    "AF":  "away_fouls",
    "HY":  "home_yellow_cards",
    "AY":  "away_yellow_cards",
    "HR":  "home_red_cards",
    "AR":  "away_red_cards",
}

# (colonna_csv, bookmaker, market, selection)
# Incluse solo le colonne presenti nei file Serie A attuali
ODD_MAP: list[tuple[str, str, str, str]] = [
    # ── 1X2 ──────────────────────────────────────────────────────────────────
    ("B365H",  "Bet365",          "1x2",          "home"),
    ("B365D",  "Bet365",          "1x2",          "draw"),
    ("B365A",  "Bet365",          "1x2",          "away"),
    ("PSH",    "Pinnacle",        "1x2",          "home"),
    ("PSD",    "Pinnacle",        "1x2",          "draw"),
    ("PSA",    "Pinnacle",        "1x2",          "away"),
    ("WHH",    "WilliamHill",     "1x2",          "home"),
    ("WHD",    "WilliamHill",     "1x2",          "draw"),
    ("WHA",    "WilliamHill",     "1x2",          "away"),
    ("BWH",    "Betway",          "1x2",          "home"),
    ("BWD",    "Betway",          "1x2",          "draw"),
    ("BWA",    "Betway",          "1x2",          "away"),
    ("BFH",    "Betfair",         "1x2",          "home"),
    ("BFD",    "Betfair",         "1x2",          "draw"),
    ("BFA",    "Betfair",         "1x2",          "away"),
    ("1XBH",   "1xBet",           "1x2",          "home"),
    ("1XBD",   "1xBet",           "1x2",          "draw"),
    ("1XBA",   "1xBet",           "1x2",          "away"),
    # Max e Avg di mercato — utili per calcolare il fair value
    ("MaxH",   "Market",          "1x2_max",      "home"),
    ("MaxD",   "Market",          "1x2_max",      "draw"),
    ("MaxA",   "Market",          "1x2_max",      "away"),
    ("AvgH",   "Market",          "1x2_avg",      "home"),
    ("AvgD",   "Market",          "1x2_avg",      "draw"),
    ("AvgA",   "Market",          "1x2_avg",      "away"),
    # Betfair Exchange (prezzi di scambio)
    ("BFEH",   "BetfairExch",     "1x2",          "home"),
    ("BFED",   "BetfairExch",     "1x2",          "draw"),
    ("BFEA",   "BetfairExch",     "1x2",          "away"),
    # ── Over/Under 2.5 ───────────────────────────────────────────────────────
    ("B365>2.5", "Bet365",        "over_2.5",     "over"),
    ("B365<2.5", "Bet365",        "over_2.5",     "under"),
    ("P>2.5",    "Pinnacle",      "over_2.5",     "over"),
    ("P<2.5",    "Pinnacle",      "over_2.5",     "under"),
    ("Max>2.5",  "Market",        "over_2.5_max", "over"),
    ("Max<2.5",  "Market",        "over_2.5_max", "under"),
    ("Avg>2.5",  "Market",        "over_2.5_avg", "over"),
    ("Avg<2.5",  "Market",        "over_2.5_avg", "under"),
    ("BFE>2.5",  "BetfairExch",   "over_2.5",     "over"),
    ("BFE<2.5",  "BetfairExch",   "over_2.5",     "under"),
    # ── Asian Handicap ────────────────────────────────────────────────────────
    ("B365AHH", "Bet365",         "asian_handicap", "home"),
    ("B365AHA", "Bet365",         "asian_handicap", "away"),
]


def _season_to_code(season: str) -> str:
    """Converte il formato stagione nell'encoding del sito.
    Es: '2024-25' → '2425'
    """
    parts = season.split("-")
    return parts[0][2:] + parts[1]


def _safe_int(val: Any) -> int | None:
    try:
        f = float(val)
        return None if pd.isna(f) else int(f)
    except (TypeError, ValueError):
        return None


def _safe_float(val: Any) -> float | None:
    try:
        f = float(val)
        return None if pd.isna(f) else f
    except (TypeError, ValueError):
        return None


def _parse_datetime(date_str: str, time_str: Any) -> datetime:
    """Combina data e ora dal CSV in un oggetto datetime."""
    dt = pd.to_datetime(date_str, dayfirst=True).to_pydatetime()
    if time_str and not pd.isna(time_str):
        try:
            h, m = str(time_str).strip().split(":")
            dt = dt.replace(hour=int(h), minute=int(m), second=0, microsecond=0)
        except ValueError:
            dt = dt.replace(hour=12, minute=0, second=0, microsecond=0)
    else:
        dt = dt.replace(hour=12, minute=0, second=0, microsecond=0)
    return dt


class FootballDataScraper(BaseScraper):
    BASE_URL = "https://www.football-data.co.uk"
    SOURCE_NAME = "football_data_co"

    def download_season(
        self, league_key: str, season: str, force: bool = False
    ) -> pd.DataFrame:
        """
        Scarica (o legge dalla cache locale) il CSV di una stagione.

        Args:
            league_key: chiave campionato definita in config.LEAGUES (es. "serie_a")
            season:     es. "2024-25"
            force:      se True ri-scarica anche se il file è già in cache

        Returns:
            DataFrame pulito con tutti i dati della stagione
        """
        league = LEAGUES[league_key]
        code = league["football_data_code"]
        season_code = _season_to_code(season)
        url = f"https://www.football-data.co.uk/mmz4281/{season_code}/{code}.csv"
        cache_file = CACHE_DIR / f"{league_key}_{season_code}.csv"

        if cache_file.exists() and not force:
            logger.info(f"Cache locale: {cache_file.name}")
            raw = cache_file.read_text(encoding="latin-1")
        else:
            logger.info(f"Download: {url}")
            response = self.get(url)
            raw = response.content.decode("latin-1")
            cache_file.write_text(raw, encoding="latin-1")

        df = pd.read_csv(io.StringIO(raw), encoding="latin-1")
        # Normalizza nomi colonne (rimuove spazi residui)
        df.columns = df.columns.str.strip()
        # Scarta righe completamente vuote (frequenti a fine file stagioni in corso)
        df = df.dropna(how="all")
        df = df[df["HomeTeam"].notna() & df["AwayTeam"].notna()]

        logger.info(f"{league_key} {season}: {len(df)} righe caricate")
        return df

    def sync_season(
        self,
        league_key: str,
        season: str,
        force_download: bool = False,
    ) -> dict[str, int]:
        """
        Sincronizza una stagione nel DB: teams, matches, statistiche e quote.

        Returns:
            dict con contatori: matches_created, matches_updated, odds_upserted
        """
        df = self.download_season(league_key, season, force=force_download)
        counters = {"matches_created": 0, "matches_updated": 0, "odds_upserted": 0}

        with get_session() as session:
            for _, row in df.iterrows():
                home_team = self._get_or_create_team(
                    session, str(row["HomeTeam"]), league_key, season
                )
                away_team = self._get_or_create_team(
                    session, str(row["AwayTeam"]), league_key, season
                )
                # Flush per ottenere gli ID prima di creare il Match
                session.flush()

                match, created = self._upsert_match(
                    session, row, home_team, away_team, league_key, season
                )
                session.flush()

                odds_count = self._upsert_odds(session, row, match)

                if created:
                    counters["matches_created"] += 1
                else:
                    counters["matches_updated"] += 1
                counters["odds_upserted"] += odds_count

        logger.info(
            f"✓ {league_key} {season} — "
            f"{counters['matches_created']} nuove, "
            f"{counters['matches_updated']} aggiornate, "
            f"{counters['odds_upserted']} quote"
        )
        return counters

    def sync_multiple_seasons(
        self,
        league_key: str,
        seasons: list[str],
        force_download: bool = False,
    ) -> None:
        """Sincronizza più stagioni in sequenza."""
        for season in seasons:
            logger.info(f"─── Sync {league_key} {season} ───")
            self.sync_season(league_key, season, force_download=force_download)

    # ── Private helpers ───────────────────────────────────────────────────────

    def _get_or_create_team(
        self, session: Session, name: str, league_key: str, season: str
    ) -> Team:
        team = (
            session.query(Team)
            .filter_by(name=name, league=league_key, season=season)
            .first()
        )
        if not team:
            team = Team(name=name, league=league_key, season=season)
            session.add(team)
            logger.debug(f"Nuova squadra: {name}")
        return team

    def _upsert_match(
        self,
        session: Session,
        row: pd.Series,
        home_team: Team,
        away_team: Team,
        league_key: str,
        season: str,
    ) -> tuple[Match, bool]:
        """Crea o aggiorna una partita. Returns (match, created)."""
        match_dt = _parse_datetime(row["Date"], row.get("Time"))
        date_only = match_dt.date()

        match = (
            session.query(Match)
            .filter_by(
                home_team_id=home_team.id,
                away_team_id=away_team.id,
                league=league_key,
                season=season,
            )
            .filter(func.date(Match.date) == date_only)
            .first()
        )
        created = match is None

        if not match:
            match = Match(
                league=league_key,
                season=season,
                date=match_dt,
                home_team_id=home_team.id,
                away_team_id=away_team.id,
                source=self.SOURCE_NAME,
            )
            session.add(match)
        else:
            # Aggiorna data/ora se più precisa (file aggiornati possono avere l'orario)
            if match_dt.hour != 12:
                match.date = match_dt

        # Risultato finale
        fthg = _safe_int(row.get("FTHG"))
        ftag = _safe_int(row.get("FTAG"))
        if fthg is not None and ftag is not None:
            match.home_score = fthg
            match.away_score = ftag
            match.home_ht_score = _safe_int(row.get("HTHG"))
            match.away_ht_score = _safe_int(row.get("HTAG"))
            match.status = MatchStatus.FINISHED
        else:
            match.status = MatchStatus.SCHEDULED

        # Statistiche aggregate
        for csv_col, field in STAT_MAP.items():
            val = _safe_int(row.get(csv_col))
            if val is not None:
                setattr(match, field, val)

        match.updated_at = datetime.now(timezone.utc)
        return match, created

    def _upsert_odds(self, session: Session, row: pd.Series, match: Match) -> int:
        """Inserisce o aggiorna le quote disponibili nella riga CSV.

        Returns:
            Numero di quote inserite/aggiornate.
        """
        # Carica tutte le quote esistenti per questo match in un dict
        existing: dict[tuple[str, str, str], Odd] = {
            (o.bookmaker, o.market, o.selection): o
            for o in session.query(Odd).filter_by(match_id=match.id).all()
        }

        count = 0
        for csv_col, bookmaker, market, selection in ODD_MAP:
            val = _safe_float(row.get(csv_col))
            if val is None or val <= 1.0:
                continue

            key = (bookmaker, market, selection)
            if key in existing:
                existing[key].odd = val
            else:
                odd = Odd(
                    match_id=match.id,
                    bookmaker=bookmaker,
                    market=market,
                    selection=selection,
                    odd=val,
                )
                session.add(odd)
            count += 1

        return count


# ── Entry point CLI ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    args = sys.argv[1:]
    league_key = args[0] if args else "serie_a"
    seasons = args[1:] if len(args) > 1 else ["2024-25"]

    if league_key not in LEAGUES:
        print(f"Campionato non riconosciuto: {league_key}")
        print(f"Disponibili: {list(LEAGUES.keys())}")
        sys.exit(1)

    init_db()
    with FootballDataScraper() as scraper:
        scraper.sync_multiple_seasons(league_key, seasons)
