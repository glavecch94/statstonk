"""
Scraper per api.sofascore.com usando Playwright (Chromium headless).

Bypassa la protezione Cloudflare usando un browser reale.
Fornisce:
  - Quote pre-partita (15+ mercati: 1X2, BTTS, O/U, Corners, 1T, DNB, AH...)
  - Formazioni (probabili + ufficiali con confirmed flag)
  - Statistiche partita (xG, possesso, tiri, corner...)
  - Lista eventi live

Prerequisiti:
    playwright install chromium   (eseguire una volta)

Usage:
    with SofaScoreScraper() as ss:
        events = ss.get_scheduled_events("2026-03-01")
        eid = ss.get_event_id("Como", "Lecce", "2026-03-01", "serie_a")
        odds = ss.get_odds(eid)
        lineups = ss.get_lineups(eid)
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from config import LEAGUES as _LEAGUES_CFG

logger = logging.getLogger(__name__)

# ── SofaScore unique tournament IDs ────────────────────────────────────────────
# Verificati empiricamente (febbraio 2026).
SS_TOURNAMENT_IDS: dict[str, int] = {
    "serie_a": 23,
    "premier_league": 17,
    "la_liga": 8,
    "bundesliga": 35,
    "ligue_1": 34,
    "eredivisie": 37,
    "primeira_liga": 238,
    "championship": 18,
    "champions_league": 7,
    "europa_league": 679,
    "conference_league": 17015,
    "brasileirao": 325,
    "copa_libertadores": 384,
}

# ── Normalizzazione nomi: SofaScore → nomi interni (da football-data.co.uk) ────
SS_TEAM_NAME_MAP: dict[str, str] = {
    # ── Serie A ─────────────────────────────────────────────────────────────────
    "Inter": "Inter",
    "Internazionale": "Inter",
    "Inter Milan": "Inter",
    "Milan": "AC Milan",
    "AC Milan": "AC Milan",
    "Juventus": "Juventus",
    "Juventus FC": "Juventus",
    "Napoli": "Napoli",
    "SSC Napoli": "Napoli",
    "Roma": "Roma",
    "AS Roma": "Roma",
    "Lazio": "Lazio",
    "SS Lazio": "Lazio",
    "Atalanta": "Atalanta",
    "Atalanta BC": "Atalanta",
    "Fiorentina": "Fiorentina",
    "ACF Fiorentina": "Fiorentina",
    "Torino": "Torino",
    "Torino FC": "Torino",
    "FC Turin": "Torino",
    "Bologna": "Bologna",
    "Bologna FC 1909": "Bologna",
    "Genoa": "Genoa",
    "Genoa CFC": "Genoa",
    "CFC Genua": "Genoa",
    "Monza": "Monza",
    "AC Monza": "Monza",
    "Lecce": "Lecce",
    "US Lecce": "Lecce",
    "Cagliari": "Cagliari",
    "Cagliari Calcio": "Cagliari",
    "Parma": "Parma",
    "Parma Calcio 1913": "Parma",
    "Como": "Como",
    "Como 1907": "Como",
    "Venezia": "Venezia",
    "Venezia FC": "Venezia",
    "Empoli": "Empoli",
    "Empoli FC": "Empoli",
    "Udinese": "Udinese",
    "Udinese Calcio": "Udinese",
    "Hellas Verona": "Verona",
    "Verona": "Verona",
    "Hellas Verona FC": "Verona",
    "Salernitana": "Salernitana",
    "Frosinone": "Frosinone",
    "Sassuolo": "Sassuolo",
    # ── Premier League ──────────────────────────────────────────────────────────
    "Manchester City": "Man City",
    "Manchester United": "Man United",
    "Liverpool": "Liverpool",
    "Arsenal": "Arsenal",
    "Chelsea": "Chelsea",
    "Tottenham": "Tottenham",
    "Tottenham Hotspur": "Tottenham",
    "Newcastle": "Newcastle",
    "Newcastle United": "Newcastle",
    "Aston Villa": "Aston Villa",
    "West Ham": "West Ham",
    "West Ham United": "West Ham",
    "Brighton": "Brighton",
    "Brighton & Hove Albion": "Brighton",
    "Fulham": "Fulham",
    "Wolves": "Wolves",
    "Wolverhampton": "Wolves",
    "Wolverhampton Wanderers": "Wolves",
    "Brentford": "Brentford",
    "Crystal Palace": "Crystal Palace",
    "Everton": "Everton",
    "Nottingham Forest": "Nott'm Forest",
    "Nottingham": "Nott'm Forest",
    "Bournemouth": "Bournemouth",
    "Southampton": "Southampton",
    "Ipswich": "Ipswich",
    "Ipswich Town": "Ipswich",
    "Leicester": "Leicester",
    "Leicester City": "Leicester",
    # ── La Liga ─────────────────────────────────────────────────────────────────
    "Real Madrid": "Real Madrid",
    "Barcelona": "Barcelona",
    "Atletico Madrid": "Ath Madrid",
    "Atlético Madrid": "Ath Madrid",
    "Athletic Club": "Ath Bilbao",
    "Athletic Bilbao": "Ath Bilbao",
    "Real Sociedad": "Real Sociedad",
    "Villarreal": "Villarreal",
    "Real Betis": "Betis",
    "Betis": "Betis",
    "Sevilla": "Sevilla",
    "Valencia": "Valencia",
    "Osasuna": "Osasuna",
    "Celta Vigo": "Celta",
    "Celta de Vigo": "Celta",
    "Getafe": "Getafe",
    "Rayo Vallecano": "Rayo Vallecano",
    "Girona": "Girona",
    "Alaves": "Alaves",
    "Alavés": "Alaves",
    "Deportivo Alaves": "Alaves",
    "Mallorca": "Mallorca",
    "Las Palmas": "Las Palmas",
    "Espanyol": "Espanyol",
    "Leganes": "Leganes",
    "Leganés": "Leganes",
    "Valladolid": "Valladolid",
    # ── Bundesliga ──────────────────────────────────────────────────────────────
    "Bayern Munich": "Bayern Munich",
    "Bayer Leverkusen": "Leverkusen",
    "Borussia Dortmund": "Dortmund",
    "RB Leipzig": "RB Leipzig",
    "Eintracht Frankfurt": "Ein Frankfurt",
    "SC Freiburg": "Freiburg",
    "Freiburg": "Freiburg",
    "Wolfsburg": "Wolfsburg",
    "VfL Wolfsburg": "Wolfsburg",
    "Borussia Mönchengladbach": "Monchengladbach",
    "Gladbach": "Monchengladbach",
    "Union Berlin": "Union Berlin",
    "1. FC Union Berlin": "Union Berlin",
    "Mainz": "Mainz",
    "Mainz 05": "Mainz",
    "1. FSV Mainz 05": "Mainz",
    "Hoffenheim": "Hoffenheim",
    "TSG Hoffenheim": "Hoffenheim",
    "Augsburg": "Augsburg",
    "FC Augsburg": "Augsburg",
    "VfB Stuttgart": "Stuttgart",
    "Stuttgart": "Stuttgart",
    "Werder Bremen": "Werder Bremen",
    "SV Werder Bremen": "Werder Bremen",
    "St. Pauli": "St Pauli",
    "FC St. Pauli": "St Pauli",
    "Holstein Kiel": "Holstein Kiel",
    "Heidenheim": "Heidenheim",
    "1. FC Heidenheim": "Heidenheim",
    # ── Ligue 1 ─────────────────────────────────────────────────────────────────
    "Paris Saint-Germain": "PSG",
    "PSG": "PSG",
    "Marseille": "Marseille",
    "Olympique de Marseille": "Marseille",
    "Lyon": "Lyon",
    "Olympique Lyon": "Lyon",
    "Olympique Lyonnais": "Lyon",
    "Monaco": "Monaco",
    "AS Monaco": "Monaco",
    "Lille": "Lille",
    "LOSC Lille": "Lille",
    "Nice": "Nice",
    "OGC Nice": "Nice",
    "Lens": "Lens",
    "RC Lens": "Lens",
    "Rennes": "Rennes",
    "Stade Rennais": "Rennes",
    "Strasbourg": "Strasbourg",
    "RC Strasbourg": "Strasbourg",
    "Nantes": "Nantes",
    "FC Nantes": "Nantes",
    "Montpellier": "Montpellier",
    "Montpellier HSC": "Montpellier",
    "Brest": "Brest",
    "Stade Brestois 29": "Brest",
    "Reims": "Reims",
    "Stade de Reims": "Reims",
    "Toulouse": "Toulouse",
    "Toulouse FC": "Toulouse",
    "Auxerre": "Auxerre",
    "AJ Auxerre": "Auxerre",
    "Angers": "Angers",
    "Angers SCO": "Angers",
    "Saint-Etienne": "St Etienne",
    "AS Saint-Etienne": "St Etienne",
    "Le Havre": "Le Havre",
    "HAC Le Havre": "Le Havre",
    # ── Eredivisie ──────────────────────────────────────────────────────────────
    "Ajax": "Ajax",
    "PSV": "PSV Eindhoven",
    "PSV Eindhoven": "PSV Eindhoven",
    "Feyenoord": "Feyenoord",
    "AZ": "AZ Alkmaar",
    "AZ Alkmaar": "AZ Alkmaar",
    "FC Utrecht": "Utrecht",
    "Utrecht": "Utrecht",
    "NEC": "NEC Nijmegen",
    "NEC Nijmegen": "NEC Nijmegen",
    "Twente": "Twente",
    "FC Twente": "Twente",
    "Vitesse": "Vitesse",
    "NAC Breda": "NAC Breda",
    "Go Ahead Eagles": "Go Ahead Eagles",
    "Sparta Rotterdam": "Sparta Rotterdam",
    "RKC Waalwijk": "RKC Waalwijk",
    "Almere City": "Almere City",
    "PEC Zwolle": "PEC Zwolle",
    "Heracles": "Heracles",
    "Heracles Almelo": "Heracles",
    # ── Primeira Liga ───────────────────────────────────────────────────────────
    "Sporting CP": "Sporting CP",
    "Sporting": "Sporting CP",
    "Porto": "Porto",
    "FC Porto": "Porto",
    "Benfica": "Benfica",
    "SL Benfica": "Benfica",
    "Braga": "Sp Braga",
    "SC Braga": "Sp Braga",
    "Sporting Braga": "Sp Braga",
    "Sp. Braga": "Sp Braga",
    "Vitória SC": "Guimaraes",
    "Guimaraes": "Guimaraes",
    "Vitória de Guimarães": "Guimaraes",
    "Nacional": "Nacional",
    "CD Nacional": "Nacional",
    "Casa Pia": "Casa Pia",
    "Casa Pia AC": "Casa Pia",
    "Moreirense": "Moreirense",
    "Moreirense FC": "Moreirense",
    "Estoril": "Estoril",
    "Estoril Praia": "Estoril",
    "Farense": "Farense",
    "SC Farense": "Farense",
    "Rio Ave": "Rio Ave",
    "Rio Ave FC": "Rio Ave",
    "Chaves": "Chaves",
    "GD Chaves": "Chaves",
    "Famalicão": "Famalicao",
    "FC Famalicão": "Famalicao",
    "Arouca": "Arouca",
    "FC Arouca": "Arouca",
    "Boavista": "Boavista",
    "Boavista FC": "Boavista",
    "Gil Vicente": "Gil Vicente",
    "Gil Vicente FC": "Gil Vicente",
    "Pacos Ferreira": "Pacos de Ferreira",
    "Paços de Ferreira": "Pacos de Ferreira",
    "FC Paços de Ferreira": "Pacos de Ferreira",
    "Santa Clara": "Santa Clara",
    "CD Santa Clara": "Santa Clara",
    "Tondela": "Tondela",
    "CD Tondela": "Tondela",
    "AVS": "AVS",
    "AVS FC": "AVS",
    "Estrela Amadora": "Estrela Amadora",
    "CF Estrela Amadora": "Estrela Amadora",
    "Alverca": "Alverca",
    "FC Alverca": "Alverca",
    # ── Championship ────────────────────────────────────────────────────────────
    "Leeds United": "Leeds",
    "Leeds": "Leeds",
    "Sheffield United": "Sheffield United",
    "Sheffield Wednesday": "Sheff Weds",
    "Middlesbrough": "Middlesbrough",
    "Norwich City": "Norwich",
    "Norwich": "Norwich",
    "Bristol City": "Bristol City",
    "Watford": "Watford",
    "West Brom": "West Brom",
    "West Bromwich Albion": "West Brom",
    "Millwall": "Millwall",
    "Cardiff City": "Cardiff",
    "Cardiff": "Cardiff",
    "Queens Park Rangers": "QPR",
    "QPR": "QPR",
    "Hull City": "Hull",
    "Hull": "Hull",
    "Swansea City": "Swansea",
    "Swansea": "Swansea",
    "Burnley": "Burnley",
    "Stoke City": "Stoke",
    "Stoke": "Stoke",
    "Plymouth Argyle": "Plymouth",
    "Coventry City": "Coventry",
    "Coventry": "Coventry",
    "Preston North End": "Preston",
    "Preston": "Preston",
    "Blackburn": "Blackburn",
    "Blackburn Rovers": "Blackburn",
    "Oxford United": "Oxford",
    "Derby County": "Derby",
    "Derby": "Derby",
    "Portsmouth": "Portsmouth",
    "Sunderland": "Sunderland",
}


class SofaScoreScraper:
    """
    Scraper per api.sofascore.com tramite Playwright (Chromium headless).

    Bypassa Cloudflare usando un browser reale. Si usa come context manager:

        with SofaScoreScraper() as ss:
            events = ss.get_scheduled_events("2026-03-01")
            odds = ss.get_odds(eid)
    """

    _API_BASE = "https://api.sofascore.com/api/v1"
    _HOME_URL = "https://www.sofascore.com/it/"
    _UA = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    )
    _HEADERS = {
        "Referer": "https://www.sofascore.com/",
        "Accept": "application/json",
    }

    def __init__(self) -> None:
        from playwright.sync_api import sync_playwright

        self._pw = sync_playwright().start()
        self._browser = self._pw.chromium.launch(headless=True)
        ctx = self._browser.new_context(user_agent=self._UA, locale="it-IT")
        self._page = ctx.new_page()
        # Cookie warmup: visita homepage per ottenere i cookie SofaScore/Cloudflare
        self._page.goto(self._HOME_URL, wait_until="domcontentloaded", timeout=30000)

    def __enter__(self) -> SofaScoreScraper:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def close(self) -> None:
        try:
            self._browser.close()
        except Exception:
            pass
        try:
            self._pw.stop()
        except Exception:
            pass

    # ── Internal ──────────────────────────────────────────────────────────────

    def _get(self, path: str) -> dict | None:
        url = f"{self._API_BASE}/{path.lstrip('/')}"
        try:
            resp = self._page.request.get(url, headers=self._HEADERS)
            if resp.status == 200:
                return resp.json()
            logger.debug("SofaScore %s → HTTP %d", path, resp.status)
        except Exception as exc:
            logger.warning("SofaScore request error %s: %s", path, exc)
        return None

    # ── Public API ─────────────────────────────────────────────────────────────

    def get_scheduled_events(self, date_str: str) -> list[dict]:
        """Tutti gli eventi football programmati per una data (YYYY-MM-DD)."""
        data = self._get(f"sport/football/scheduled-events/{date_str}")
        return data.get("events", []) if data else []

    def get_live_events(self) -> list[dict]:
        """Tutti gli eventi football live in questo momento."""
        data = self._get("sport/football/events/live")
        return data.get("events", []) if data else []

    def get_event_id(
        self,
        home_team: str,
        away_team: str,
        match_date: str,
        league_key: str | None = None,
    ) -> int | None:
        """
        Mappa (home, away, date) → SofaScore event ID cercando per data.

        home_team/away_team devono essere nel formato interno (football-data.co.uk).
        """
        events = self.get_scheduled_events(match_date)
        tournament_id = SS_TOURNAMENT_IDS.get(league_key) if league_key else None
        home_norm = _norm_internal(home_team)
        away_norm = _norm_internal(away_team)

        for e in events:
            if tournament_id is not None:
                e_tid = e.get("tournament", {}).get("uniqueTournament", {}).get("id")
                if e_tid != tournament_id:
                    continue
            e_home = _norm_ss(e.get("homeTeam", {}).get("name", ""))
            e_away = _norm_ss(e.get("awayTeam", {}).get("name", ""))
            if e_home == home_norm and e_away == away_norm:
                return e["id"]

        logger.debug(
            "SofaScore: nessun evento per '%s' vs '%s' il %s",
            home_team,
            away_team,
            match_date,
        )
        return None

    def get_odds(self, event_id: int) -> dict[str, Any]:
        """
        Quote pre-partita organizzate per mercato.

        Returns:
            {
                "full_time":      {"1": 1.40, "x": 4.50, "2": 9.00},
                "double_chance":  {"1x": ..., "x2": ..., "12": ...},
                "first_half":     {"1": ..., "x": ..., "2": ...},
                "draw_no_bet":    {"1": ..., "2": ...},
                "btts":           {"yes": 2.25, "no": 1.57},
                "over_under":     {"2.5": {"over": 2.00, "under": 1.80}, ...},
                "corners":        {"over": 2.10, "under": 1.67, "line": 9.5},
                "asian_handicap": {"<home>": ..., "<away>": ..., "line": str},
                "cards":          {"over": 1.85, "under": 1.95, "line": 4.5},
                "first_scorer":   {<team_name>: ...},
            }
        """
        data = self._get(f"event/{event_id}/odds/1/all")
        if not data:
            return {}

        result: dict[str, Any] = {}
        for market in data.get("markets", []):
            name = market.get("marketName", "")
            cg = market.get("choiceGroup", "")
            choices = {
                c["name"].lower(): _frac_to_dec(c["fractionalValue"])
                for c in market.get("choices", [])
                if c.get("fractionalValue")
            }
            if name == "Full time":
                result["full_time"] = choices
            elif name == "Both teams to score":
                result["btts"] = choices
            elif name == "Match goals" and cg:
                result.setdefault("over_under", {})[cg] = choices
            elif name == "Corners 2-Way" and cg:
                result["corners"] = {**choices, "line": float(cg)}
            elif name == "1st half":
                result["first_half"] = choices
            elif name == "Double chance":
                result["double_chance"] = choices
            elif name == "Draw no bet":
                result["draw_no_bet"] = choices
            elif name == "Asian handicap":
                # La linea è nel nome della scelta, es. "(-1) Arsenal" / "(1) Chelsea"
                import re as _re
                home_q = away_q = ah_line = None
                for c in market.get("choices", []):
                    m = _re.match(r"\(([+-]?\d+\.?\d*)\)", c.get("name", ""))
                    if m:
                        val = float(m.group(1))
                        q = _frac_to_dec(c.get("fractionalValue"))
                        if val < 0:
                            home_q, ah_line = q, val
                        elif val > 0:
                            away_q = q
                if home_q is not None or away_q is not None:
                    result["asian_handicap"] = {"home": home_q, "away": away_q, "line": ah_line}
            elif name == "Cards in match" and cg:
                result["cards"] = {**choices, "line": float(cg)}
            elif name == "First team to score":
                result["first_scorer"] = choices
        return result

    def get_lineups(self, event_id: int) -> dict | None:
        """
        Formazioni per un evento.

        Returns:
            {
                "confirmed": bool,
                "home": {
                    "formation": "4-2-3-1",
                    "players": [
                        {"name": str, "position": str, "shirt_number": int|None,
                         "is_starting": bool, "is_captain": bool},
                        ...
                    ],
                },
                "away": {...},
            }
            oppure None se non ancora disponibili.
        """
        data = self._get(f"event/{event_id}/lineups")
        if not data or ("home" not in data and "away" not in data):
            return None

        now_ts = datetime.now(UTC).timestamp()

        def _age_from_ts(dob_ts: int | None) -> float | None:
            if not dob_ts:
                return None
            return (now_ts - dob_ts) / (365.25 * 86400)

        def _parse_side(sd: dict) -> dict:
            players = [
                {
                    "name": p.get("player", {}).get("name", ""),
                    "position": p.get("position", ""),
                    "shirt_number": p.get("player", {}).get("shirtNumber"),
                    "is_starting": not p.get("substitute", True),
                    "is_captain": p.get("captain", False),
                    "grid": p.get("grid"),  # "row:col" es. "2:3"
                    "date_of_birth_ts": p.get("player", {}).get("dateOfBirthTimestamp"),
                }
                for p in sd.get("players", [])
            ]
            starter_ages = [
                _age_from_ts(p["date_of_birth_ts"])
                for p in players
                if p.get("is_starting") and p.get("date_of_birth_ts")
            ]
            avg_age = round(sum(starter_ages) / len(starter_ages), 1) if starter_ages else None
            return {
                "formation": sd.get("formation"),
                "players": players,
                "avg_age": avg_age,
            }

        return {
            "confirmed": data.get("confirmed", False),
            "home": _parse_side(data.get("home", {})),
            "away": _parse_side(data.get("away", {})),
        }

    def get_statistics(self, event_id: int) -> dict[str, dict]:
        """
        Statistiche partita (live o conclusa): xG, possesso, tiri, corner...

        Returns:
            {
                "ALL": {"Ball possession": {"home": "44%", "away": "56%"}, ...},
                "1ST": {...},
                "2ND": {...},
            }
        """
        data = self._get(f"event/{event_id}/statistics")
        if not data:
            return {}
        result: dict[str, dict] = {}
        for grp in data.get("statistics", []):
            period = grp.get("period", "ALL")
            result[period] = {}
            for subgrp in grp.get("groups", []):
                for item in subgrp.get("statisticsItems", []):
                    result[period][item["name"]] = {
                        "home": item.get("home"),
                        "away": item.get("away"),
                    }
        return result

    def get_all_for_league(
        self,
        league_key: str,
        date_strs: list[str],
    ) -> dict[tuple[str, str], dict]:
        """
        Recupera odds + lineups per tutte le partite di una lega nelle date indicate.
        Usa una singola sessione browser (efficiente: 1 sola apertura Playwright).

        Returns:
            {
                (home_norm, away_norm): {
                    "event_id": int,
                    "odds":     dict,
                    "lineups":  dict | None,
                }
            }
        dove home_norm/away_norm sono i nomi interni (title case, da SS_TEAM_NAME_MAP).
        """
        tournament_id = SS_TOURNAMENT_IDS.get(league_key)
        result: dict[tuple[str, str], dict] = {}
        seen_ids: set[int] = set()

        for date_str in dict.fromkeys(date_strs):  # dedup mantenendo l'ordine
            for e in self.get_scheduled_events(date_str):
                if tournament_id is not None:
                    e_tid = e.get("tournament", {}).get("uniqueTournament", {}).get("id")
                    if e_tid != tournament_id:
                        continue
                eid = e["id"]
                if eid in seen_ids:
                    continue
                seen_ids.add(eid)

                ss_home = e.get("homeTeam", {}).get("name", "")
                ss_away = e.get("awayTeam", {}).get("name", "")
                key = (_norm_ss(ss_home), _norm_ss(ss_away))

                odds = self.get_odds(eid)
                lineups = self.get_lineups(eid)
                result[key] = {"event_id": eid, "odds": odds, "lineups": lineups}
                logger.debug(
                    "SofaScore: %s vs %s → %d mercati, lineups=%s",
                    ss_home,
                    ss_away,
                    len(odds),
                    lineups is not None,
                )

        logger.info(
            "SofaScore get_all_for_league('%s'): %d partite trovate.",
            league_key,
            len(result),
        )
        return result

    # ── Historical data ────────────────────────────────────────────────────────

    def get_seasons(self, league_key: str) -> list[dict]:
        """Stagioni disponibili per un campionato: [{id, year, name}, ...]."""
        tid = SS_TOURNAMENT_IDS.get(league_key)
        if tid is None:
            return []
        data = self._get(f"unique-tournament/{tid}/seasons")
        return data.get("seasons", []) if data else []

    def get_season_results(self, league_key: str, season_id: int) -> list[dict]:
        """
        Tutti i risultati finali di una stagione (paginati, 30/pagina).
        Itera dalla pagina 0 finché hasNextPage=False.

        Returns lista di dict da _parse_result_event.
        """
        tid = SS_TOURNAMENT_IDS.get(league_key)
        if tid is None:
            return []
        results: list[dict] = []
        page = 0
        while True:
            data = self._get(f"unique-tournament/{tid}/season/{season_id}/events/last/{page}")
            if not data:
                break
            for e in data.get("events", []):
                r = self._parse_result_event(e)
                if r:
                    results.append(r)
            if not data.get("hasNextPage", False):
                break
            page += 1
        logger.info(
            "SofaScore season results '%s' season_id=%d: %d partite",
            league_key,
            season_id,
            len(results),
        )
        return results

    def get_match_stats(self, event_id: int) -> dict:
        """
        Statistiche piatte per una partita terminata: xG, tiri, corner, ecc.
        Compatibile con i campi del modello Match.
        """
        stats = self.get_statistics(event_id)
        alltime = stats.get("ALL", {})

        def _num(key: str, side: str) -> float | None:
            v = alltime.get(key, {}).get(side)
            if v is None:
                return None
            try:
                return float(str(v).rstrip("%"))
            except (ValueError, TypeError):
                return None

        return {
            "home_xg": _num("Expected goals", "home"),
            "away_xg": _num("Expected goals", "away"),
            "home_shots": _num("Total shots", "home"),
            "away_shots": _num("Total shots", "away"),
            "home_shots_on_target": _num("Shots on target", "home"),
            "away_shots_on_target": _num("Shots on target", "away"),
            "home_corners": _num("Corner kicks", "home"),
            "away_corners": _num("Corner kicks", "away"),
            "home_fouls": _num("Fouls", "home"),
            "away_fouls": _num("Fouls", "away"),
            "home_yellow_cards": _num("Yellow cards", "home"),
            "away_yellow_cards": _num("Yellow cards", "away"),
            "home_red_cards": _num("Red cards", "home"),
            "away_red_cards": _num("Red cards", "away"),
            "home_offsides": _num("Offsides", "home"),
            "away_offsides": _num("Offsides", "away"),
            "home_possession": _num("Ball possession", "home"),
            "away_possession": _num("Ball possession", "away"),
        }

    # ── Fixture / schedule helpers ─────────────────────────────────────────────

    def get_upcoming_fixtures(
        self,
        league_key: str,
        days_ahead: int = 7,
    ) -> list[dict]:
        """
        Fixture non ancora giocate nei prossimi `days_ahead` giorni per una lega.

        Returns:
            [{home_team, away_team, date, matchday, event_id,
              league_key, league_name, status}, ...]
        """
        tid = SS_TOURNAMENT_IDS.get(league_key)
        if tid is None:
            return []
        today = datetime.now(UTC).date()
        seen_ids: set[int] = set()
        fixtures: list[dict] = []

        for i in range(days_ahead + 1):
            d = (today + timedelta(days=i)).strftime("%Y-%m-%d")
            for e in self.get_scheduled_events(d):
                e_tid = e.get("tournament", {}).get("uniqueTournament", {}).get("id")
                if e_tid != tid:
                    continue
                eid = e["id"]
                if eid in seen_ids:
                    continue
                seen_ids.add(eid)
                if e.get("status", {}).get("type") == "finished":
                    continue
                start_ts = e.get("startTimestamp")
                match_date = (
                    datetime.fromtimestamp(start_ts, tz=UTC).replace(tzinfo=None)
                    if start_ts
                    else None
                )
                fixtures.append(
                    {
                        "home_team": _norm_ss(e.get("homeTeam", {}).get("name", "")),
                        "away_team": _norm_ss(e.get("awayTeam", {}).get("name", "")),
                        "date": match_date,
                        "matchday": e.get("roundInfo", {}).get("round"),
                        "event_id": eid,
                        "league_key": league_key,
                        "league_name": _LEAGUES_CFG.get(league_key, {}).get("name", league_key),
                        "status": "SCHEDULED",
                    }
                )

        logger.info(
            "SofaScore get_upcoming_fixtures('%s'): %d partite",
            league_key,
            len(fixtures),
        )
        return fixtures

    def get_all_upcoming_matches(self, days_ahead: int = 7) -> list[dict]:
        """
        Tutti i match programmati nei prossimi N giorni in tutte le leghe SS.
        Usato per il segnale S44 (turnover/rotazione).

        Returns:
            [{home_team, away_team, date, league_key, league_name}, ...]
        """
        today = datetime.now(UTC).date()
        tid_to_league = {v: k for k, v in SS_TOURNAMENT_IDS.items()}
        seen_ids: set[int] = set()
        result: list[dict] = []

        for i in range(1, days_ahead + 2):
            d = (today + timedelta(days=i)).strftime("%Y-%m-%d")
            for e in self.get_scheduled_events(d):
                tid = e.get("tournament", {}).get("uniqueTournament", {}).get("id")
                league_key = tid_to_league.get(tid)
                if not league_key:
                    continue
                eid = e["id"]
                if eid in seen_ids:
                    continue
                seen_ids.add(eid)
                if e.get("status", {}).get("type") not in ("notstarted",):
                    continue
                start_ts = e.get("startTimestamp")
                match_date = (
                    datetime.fromtimestamp(start_ts, tz=UTC).replace(tzinfo=None)
                    if start_ts
                    else None
                )
                result.append(
                    {
                        "home_team": _norm_ss(e.get("homeTeam", {}).get("name", "")),
                        "away_team": _norm_ss(e.get("awayTeam", {}).get("name", "")),
                        "date": match_date,
                        "league_key": league_key,
                        "league_name": _LEAGUES_CFG.get(league_key, {}).get("name", league_key),
                    }
                )

        logger.info(
            "SofaScore get_all_upcoming_matches: %d partite nei prossimi %d giorni",
            len(result),
            days_ahead,
        )
        return result

    def get_all_live_matches(self) -> list[dict]:
        """
        Tutte le partite di calcio live con xG tracking abilitato.

        Non filtra per campionato configurato: include qualsiasi competizione worldwide
        purché SofaScore tracki gli xG (campo hasXg=True sull'evento).

        Returns:
            Lista di dict nel formato di _event_to_match_dict, ordinata per ora di inizio.
            league_key è None per competizioni non presenti in SS_TOURNAMENT_IDS.
            league_name è ricavato dal nome torneo/categoria SofaScore.
        """
        events = self.get_live_events()
        tid_to_league = {v: k for k, v in SS_TOURNAMENT_IDS.items()}
        result: list[dict] = []

        for e in events:
            if not e.get("hasXg", False):
                continue
            tid = e.get("tournament", {}).get("uniqueTournament", {}).get("id")
            league_key = tid_to_league.get(tid)  # None se non configurato

            # Nome competizione dall'evento stesso
            tournament = e.get("tournament", {})
            comp_name = tournament.get("name", "")
            category = tournament.get("category", {}).get("name", "")
            if league_key:
                league_name = _LEAGUES_CFG.get(league_key, {}).get("name", comp_name)
            else:
                league_name = (
                    f"{category} — {comp_name}" if category and comp_name
                    else comp_name or category or "Altro"
                )

            m = self._event_to_match_dict(e, league_key or "")
            m["league_key"] = league_key
            m["league_name"] = league_name
            result.append(m)

        result.sort(key=lambda x: x["local_date"] or datetime.min)
        logger.info("SofaScore get_all_live_matches: %d partite live con xG", len(result))
        return result

    def get_all_today_matches(self) -> list[dict]:
        """
        Tutte le partite di oggi in tutti i campionati SS configurati.
        Singola chiamata all'endpoint scheduled-events.

        Returns:
            Lista di dict normalizzati per la dashboard live (include event_id,
            league_key, league_name, local_date, status, minute, ecc.)
        """
        today = datetime.now(UTC).date().strftime("%Y-%m-%d")
        events = self.get_scheduled_events(today)
        tid_to_league = {v: k for k, v in SS_TOURNAMENT_IDS.items()}

        result: list[dict] = []
        for e in events:
            tid = e.get("tournament", {}).get("uniqueTournament", {}).get("id")
            league_key = tid_to_league.get(tid)
            if not league_key:
                continue
            m = self._event_to_match_dict(e, league_key)
            if m:
                result.append(m)

        result.sort(key=lambda x: x["local_date"] or datetime.min)
        logger.info("SofaScore get_all_today_matches: %d partite oggi", len(result))
        return result

    # ── Internal helpers ────────────────────────────────────────────────────────

    def _parse_result_event(self, e: dict) -> dict | None:
        """Converte un evento SofaScore in un dict pronto per il DB."""
        if e.get("status", {}).get("type") != "finished":
            return None
        start_ts = e.get("startTimestamp")
        match_date = (
            datetime.fromtimestamp(start_ts, tz=UTC).replace(tzinfo=None) if start_ts else None
        )
        home_score_d = e.get("homeScore", {})
        away_score_d = e.get("awayScore", {})
        return {
            "event_id": e["id"],
            "home": _norm_ss(e.get("homeTeam", {}).get("name", "")),
            "away": _norm_ss(e.get("awayTeam", {}).get("name", "")),
            "home_score": home_score_d.get("current"),
            "away_score": away_score_d.get("current"),
            "home_ht_score": home_score_d.get("period1"),
            "away_ht_score": away_score_d.get("period1"),
            "date": match_date,
            "matchday": e.get("roundInfo", {}).get("round"),
            "has_xg": e.get("hasXg", False),
        }

    def _event_to_match_dict(self, e: dict, league_key: str) -> dict:
        """Converte un evento SofaScore nel formato atteso dalla pagina live."""
        _STATUS_MAP = {
            "notstarted": "SCHEDULED",
            "inprogress": "IN_PLAY",
            "finished": "FINISHED",
            "postponed": "POSTPONED",
            "cancelled": "CANCELLED",
            "halftime": "PAUSED",
            "interrupted": "PAUSED",
        }
        status_type = e.get("status", {}).get("type", "notstarted")
        status_desc = e.get("status", {}).get("description", "")
        status = _STATUS_MAP.get(status_type, "SCHEDULED")

        # Calcola minuto per partite live
        minute: int | None = None
        if status_type == "inprogress":
            time_data = e.get("time", {})
            period_start = time_data.get("currentPeriodStartTimestamp")
            now_ts = int(datetime.now(UTC).timestamp())
            desc_l = status_desc.lower()
            if period_start and now_ts > period_start:
                elapsed = (now_ts - period_start) // 60
                if "2nd" in desc_l or "second" in desc_l:
                    minute = min(90, 45 + elapsed)
                elif "extra" in desc_l or "overtime" in desc_l:
                    minute = min(120, 90 + elapsed)
                else:
                    minute = min(45, elapsed)
        elif status_type == "halftime":
            minute = 45

        start_ts = e.get("startTimestamp")
        local_date = (
            datetime.fromtimestamp(start_ts, tz=UTC).astimezone(ZoneInfo("Europe/Rome"))
            if start_ts
            else None
        )

        home_score_d = e.get("homeScore", {})
        away_score_d = e.get("awayScore", {})
        return {
            "event_id": e["id"],
            "home_team": _norm_ss(e.get("homeTeam", {}).get("name", "")),
            "away_team": _norm_ss(e.get("awayTeam", {}).get("name", "")),
            "home_score": home_score_d.get("current"),
            "away_score": away_score_d.get("current"),
            "home_ht_score": home_score_d.get("period1"),
            "away_ht_score": away_score_d.get("period1"),
            "minute": minute,
            "status": status,
            "status_label": status_desc,
            "goals": [],
            "league_key": league_key,
            "league_name": _LEAGUES_CFG.get(league_key, {}).get("name", league_key),
            "local_date": local_date,
        }


# ── Helpers ────────────────────────────────────────────────────────────────────


def _norm_ss(ss_name: str) -> str:
    """Mappa nome SofaScore → nome interno (title case, come da SS_TEAM_NAME_MAP)."""
    return SS_TEAM_NAME_MAP.get(ss_name, ss_name).strip()


def _norm_internal(name: str) -> str:
    """Normalizza nome interno (football-data.co.uk): lowercase strip."""
    return name.strip().lower()


def _frac_to_dec(frac: str) -> float:
    """Converte quota frazionaria ('7/2') in decimale (4.50)."""
    try:
        n, d = frac.split("/")
        return round(int(n) / int(d) + 1, 3)
    except Exception:
        return 0.0


def _season_to_ss_year(season: str) -> str:
    """Converte formato interno in formato SofaScore: '2025-26' → '25/26'."""
    parts = season.split("-")
    if len(parts) == 2:
        return f"{parts[0][-2:]}/{parts[1][-2:]}"
    return season


def ss_quota_for_mercato(mercato: str, ss_odds: dict) -> float | None:
    """
    Mappa nome mercato interno → quota SofaScore.

    Args:
        mercato:  nome mercato interno (es. "Over 2.5 Gol", "1 — Vittoria Casa")
        ss_odds:  dict restituito da SofaScoreScraper.get_odds()

    Returns:
        quota decimale, o None se il mercato non è disponibile nelle odds.
    """
    ft = ss_odds.get("full_time", {})
    btts = ss_odds.get("btts", {})
    ou = ss_odds.get("over_under", {})
    dc = ss_odds.get("double_chance", {})
    corn = ss_odds.get("corners", {})
    cards = ss_odds.get("cards", {})
    ah = ss_odds.get("asian_handicap", {})
    _static: dict[str, float | None] = {
        "1 — Vittoria Casa": ft.get("1"),
        "X — Pareggio": ft.get("x"),
        "2 — Vittoria Trasferta": ft.get("2"),
        "Goal — Entrambe Segnano": btts.get("yes"),
        "No Goal — Solo una Segna": btts.get("no"),
        "Over 2.5 Gol": ou.get("2.5", {}).get("over"),
        "Under 2.5 Gol": ou.get("2.5", {}).get("under"),
        "Over 1.5 Gol": ou.get("1.5", {}).get("over"),
        "Under 1.5 Gol": ou.get("1.5", {}).get("under"),
        "Over 3.5 Gol": ou.get("3.5", {}).get("over"),
        "Under 3.5 Gol": ou.get("3.5", {}).get("under"),
        "1X — Doppia Chance Casa": dc.get("1x"),
        "X2 — Doppia Chance Trasferta": dc.get("x2"),
        "12 — Escludi Pareggio": dc.get("12"),
    }
    _MAX_QUOTA = 15.0  # soglia oltre la quale la quota è da considerare un errore dati

    v = _static.get(mercato)
    if v is not None:
        return v if v <= _MAX_QUOTA else None

    def _capped(val: float | None) -> float | None:
        return val if val and val <= _MAX_QUOTA else None

    # Corner lines (dinamici: verifica se la linea corrisponde)
    corn_line = corn.get("line")
    if corn_line is not None:
        if "Corner O/U Over" in mercato:
            try:
                if abs(float(mercato.split()[-1]) - corn_line) < 0.6:
                    return _capped(corn.get("over"))
            except ValueError:
                pass
        elif "Corner O/U Under" in mercato:
            try:
                if abs(float(mercato.split()[-1]) - corn_line) < 0.6:
                    return _capped(corn.get("under"))
            except ValueError:
                pass

    # Cartellini (dinamici: linea SofaScore variabile, tolleranza ±0.6)
    cards_line = cards.get("line")
    if cards_line is not None:
        if "Cartellini Gialli Over" in mercato:
            try:
                if abs(float(mercato.split()[-1]) - cards_line) < 0.6:
                    return _capped(cards.get("over"))
            except ValueError:
                pass
        elif "Cartellini Gialli Under" in mercato:
            try:
                if abs(float(mercato.split()[-1]) - cards_line) < 0.6:
                    return _capped(cards.get("under"))
            except ValueError:
                pass

    # Handicap asiatico (linea −1: casa deve vincere con ≥2 gol)
    ah_line = ah.get("line")
    if ah_line is not None and abs(ah_line - (-1)) < 0.1:
        if mercato == "Handicap −1 Casa":
            v = ah.get("home")
            return v if v and v <= _MAX_QUOTA else None
        if mercato == "Handicap +1 Trasferta":
            v = ah.get("away")
            return v if v and v <= _MAX_QUOTA else None

    return None
