"""
Job functions eseguite dallo scheduler.
Ogni job è indipendente e gestisce internamente gli errori
per non bloccare gli altri job in caso di fallimento.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta

from config import CURRENT_SEASON, DATA_DIR, HISTORICAL_SEASONS, LEAGUES
from db import get_session
from scrapers.sofascore import SofaScoreScraper, _season_to_ss_year

logger = logging.getLogger(__name__)


# ── DB helpers ────────────────────────────────────────────────────────────────


def _get_or_create_team(session, name: str, league: str = "", season: str = ""):
    """Restituisce il Team con quel nome (crea se non esiste). Ricerca case-insensitive."""
    from sqlalchemy import func

    from models.teams import Team

    team = session.query(Team).filter(func.lower(Team.name) == name.lower()).first()
    if not team:
        team = Team(name=name, league=league, season=season)
        session.add(team)
        session.flush()
    return team


def _upsert_sofascore_match(session, league_key: str, season: str, r: dict):
    """
    Crea o aggiorna una partita da SofaScore nel DB.

    Strategia di matching:
    1. external_id = "ss_{event_id}" (priorità)
    2. (home_id, away_id, league, season) con finestra date ±2 giorni
       per riconoscere record già presenti (es. da CSV precedente)

    Returns:
        (match, created: bool)
    """
    from models.matches import Match, MatchStatus

    home_name = r["home"]
    away_name = r["away"]

    home_team = _get_or_create_team(session, home_name, league_key, season)
    away_team = _get_or_create_team(session, away_name, league_key, season)

    external_id = f"ss_{r['event_id']}"

    # 1. Cerca per external_id
    match = session.query(Match).filter_by(external_id=external_id).first()

    # 2. Fallback: cerca per squadre + stagione + date (finestra ±2gg)
    if match is None and r.get("date"):
        window = timedelta(days=2)
        match = (
            session.query(Match)
            .filter_by(
                home_team_id=home_team.id,
                away_team_id=away_team.id,
                league=league_key,
                season=season,
            )
            .filter(
                Match.date >= r["date"] - window,
                Match.date <= r["date"] + window,
            )
            .first()
        )

    created = match is None
    if created:
        match = Match(
            league=league_key,
            season=season,
            home_team_id=home_team.id,
            away_team_id=away_team.id,
            source="sofascore",
        )
        session.add(match)

    # Aggiorna i campi
    match.external_id = external_id
    match.source = "sofascore"
    if r.get("date"):
        match.date = r["date"]
    if r.get("matchday") is not None:
        match.matchday = r["matchday"]
    if r.get("home_score") is not None:
        match.home_score = r["home_score"]
        match.away_score = r["away_score"]
        match.status = MatchStatus.FINISHED
    if r.get("home_ht_score") is not None:
        match.home_ht_score = r["home_ht_score"]
        match.away_ht_score = r["away_ht_score"]

    # Statistiche opzionali
    _STAT_FIELDS = [
        "home_xg",
        "away_xg",
        "home_shots",
        "away_shots",
        "home_shots_on_target",
        "away_shots_on_target",
        "home_corners",
        "away_corners",
        "home_fouls",
        "away_fouls",
        "home_yellow_cards",
        "away_yellow_cards",
        "home_red_cards",
        "away_red_cards",
        "home_offsides",
        "away_offsides",
        "home_possession",
        "away_possession",
    ]
    for field in _STAT_FIELDS:
        val = r.get(field)
        if val is not None:
            setattr(match, field, val)

    return match, created


# ── Sync jobs ─────────────────────────────────────────────────────────────────


def sync_sofascore_season(
    league_key: str,
    season_str: str,
    include_stats: bool = False,
) -> dict:
    """
    Scarica i risultati di una stagione da SofaScore e li upserta nel DB.

    Args:
        league_key:    chiave campionato da config.LEAGUES
        season_str:    es. "2025-26"
        include_stats: se True, scarica anche xG/tiri/corner per ogni partita
                       (N richieste aggiuntive — usare solo per stagioni recenti)

    Returns:
        {"created": int, "updated": int}
    """
    league_name = LEAGUES.get(league_key, {}).get("name", league_key)
    ss_year = _season_to_ss_year(season_str)  # "2025-26" → "25/26"

    logger.info("[job] sync_sofascore: %s %s (stats=%s)", league_name, season_str, include_stats)

    try:
        with SofaScoreScraper() as ss:
            # Trova il season_id corrispondente all'anno richiesto
            seasons = ss.get_seasons(league_key)
            season_id = next(
                (s["id"] for s in seasons if s.get("year") == ss_year),
                None,
            )
            if season_id is None:
                logger.warning(
                    "[job] sync_sofascore: stagione '%s' (%s) non trovata per %s",
                    season_str,
                    ss_year,
                    league_name,
                )
                return {"created": 0, "updated": 0}

            results = ss.get_season_results(league_key, season_id)

            created = updated = 0
            for r in results:
                if include_stats and r.get("has_xg"):
                    try:
                        stats = ss.get_match_stats(r["event_id"])
                        r.update(stats)
                    except Exception:
                        logger.debug(
                            "[job] sync_sofascore: stats non disponibili per event %d",
                            r["event_id"],
                        )

                try:
                    with get_session() as session:
                        _, was_created = _upsert_sofascore_match(session, league_key, season_str, r)
                    if was_created:
                        created += 1
                    else:
                        updated += 1
                except Exception:
                    logger.exception("[job] sync_sofascore: DB error per event %d", r["event_id"])

    except Exception:
        logger.exception("[job] sync_sofascore: errore per %s %s", league_name, season_str)
        return {"created": 0, "updated": 0}

    logger.info(
        "[job] sync_sofascore %s %s completato — %d create, %d aggiornate",
        league_name,
        season_str,
        created,
        updated,
    )
    return {"created": created, "updated": updated}


def sync_current_season(league_key: str = "serie_a") -> None:
    """Scarica e aggiorna nel DB i risultati della stagione corrente."""
    sync_sofascore_season(league_key, CURRENT_SEASON, include_stats=True)


def sync_all_leagues() -> None:
    """Sincronizza la stagione corrente per tutti i campionati supportati da SS."""
    from scrapers.sofascore import SS_TOURNAMENT_IDS

    supported = list(SS_TOURNAMENT_IDS.keys())
    logger.info("[job] sync_all_leagues: %d leghe", len(supported))
    for league_key in supported:
        sync_current_season(league_key)


def sync_historical_all(include_stats_seasons: int = 2) -> None:
    """
    Carica tutte le stagioni storiche per tutti i campionati SS.

    Operazione lunga (eseguire una tantum, non schedulata).
    include_stats_seasons: quante stagioni recenti includono anche xG/tiri/corner.
    """
    from scrapers.sofascore import SS_TOURNAMENT_IDS

    supported = list(SS_TOURNAMENT_IDS.keys())
    logger.info(
        "[job] sync_historical_all: %d leghe, %d stagioni",
        len(supported),
        len(HISTORICAL_SEASONS),
    )
    for league_key in supported:
        for i, season_str in enumerate(HISTORICAL_SEASONS):
            include_stats = i < include_stats_seasons
            sync_sofascore_season(league_key, season_str, include_stats=include_stats)


# ── Picks generation ──────────────────────────────────────────────────────────


def generate_picks_for_league(league_key: str, all_upcoming: list | None = None) -> None:
    """
    Genera e salva nel DB i pick qualificati per la prossima giornata.
    Usa SofaScore per fixture e quote.
    """
    from collections import Counter

    import pandas as pd

    from analytics.prematch import (
        compute_delays,
        get_matches_df,
        h2h_summary,
        head_to_head,
        team_last_match_date,
        team_next_match,
    )
    from analytics.signals import filter_conflicts, generate_signals, save_picks, season_form_stats
    from db import SessionLocal
    from scrapers.sofascore import SS_TOURNAMENT_IDS, ss_quota_for_mercato

    league_name = LEAGUES.get(league_key, {}).get("name", league_key)
    if league_key not in SS_TOURNAMENT_IDS:
        logger.debug(f"[job] generate_picks: {league_name} non supportato da SS, skip")
        return

    logger.info(f"[job] generate_picks: avvio per {league_name}")

    # Fixture della prossima giornata + odds (unica sessione Playwright)
    try:
        with SofaScoreScraper() as sc:
            fixtures = sc.get_upcoming_fixtures(league_key)
            ss_odds_by_id = {f["event_id"]: sc.get_odds(f["event_id"]) for f in fixtures}
    except Exception:
        logger.warning(f"[job] generate_picks: fixture non disponibili per {league_name}")
        return

    if not fixtures:
        logger.info(f"[job] generate_picks: nessuna fixture per {league_name}")
        return

    # Dati storici dal DB
    session = SessionLocal()
    try:
        seasons = HISTORICAL_SEASONS
        df = get_matches_df(session, league_key, seasons)
    finally:
        session.close()

    if df.empty:
        logger.warning(f"[job] generate_picks: nessun dato storico per {league_name}")
        return

    df_current = df[df["season"] == CURRENT_SEASON]
    delays = compute_delays(df_current)

    saved_count = 0
    for fixture in fixtures:
        home = fixture["home_team"]
        away = fixture["away_team"]
        fixture_dt = pd.Timestamp(fixture["date"])
        matchday = fixture.get("matchday")

        fh = season_form_stats(df_current, home, "home")
        fa = season_form_stats(df_current, away, "away")

        h2h_df = head_to_head(df, home, away)
        h2h_sum = h2h_summary(h2h_df, home) if not h2h_df.empty else {}

        _hd_s = delays[delays["Squadra"] == home] if not delays.empty else pd.DataFrame()
        _ad_s = delays[delays["Squadra"] == away] if not delays.empty else pd.DataFrame()
        hd = _hd_s.iloc[0] if not _hd_s.empty else None
        ad = _ad_s.iloc[0] if not _ad_s.empty else None

        last_home = team_last_match_date(df, home, fixture_dt)
        last_away = team_last_match_date(df, away, fixture_dt)
        days_home = int((fixture_dt - last_home).days) if last_home is not None else None
        days_away = int((fixture_dt - last_away).days) if last_away is not None else None

        def _next_match_info(team: str) -> dict | None:
            if not all_upcoming:
                return None
            nm = team_next_match(all_upcoming, team, fixture_dt)
            if nm is None:
                return None
            days_gap = int((pd.Timestamp(nm["date"]) - fixture_dt).days)
            return {
                "days": days_gap,
                "league_key": nm["league_key"],
                "league_name": nm["league_name"],
            }

        signals = generate_signals(
            home,
            away,
            fh,
            fa,
            h2h_sum,
            hd,
            ad,
            days_home,
            days_away,
            injuries={"home": [], "away": []},
            next_match_home=_next_match_info(home),
            next_match_away=_next_match_info(away),
        )

        pos_sigs = [s for s in signals if s["verdict"] == "✅" and s["mercato"]]
        if not pos_sigs:
            continue

        counts = filter_conflicts(Counter(s["mercato"] for s in pos_sigs))
        _raw = ss_odds_by_id.get(fixture["event_id"], {})
        match_odds = {m: q for m in counts if (q := ss_quota_for_mercato(m, _raw)) is not None}

        save_picks(
            home=home,
            away=away,
            match_date=fixture_dt,
            matchday=matchday,
            league_key=league_key,
            counts=counts,
            match_odds=match_odds,
        )
        saved_count += 1

    logger.info(
        f"[job] generate_picks: {league_name} completato — "
        f"{saved_count}/{len(fixtures)} fixture con picks qualificati"
    )


def generate_picks_all_leagues() -> None:
    """Genera picks per tutte le leghe supportate da SofaScore."""
    from scrapers.sofascore import SS_TOURNAMENT_IDS

    fixture_leagues = list(SS_TOURNAMENT_IDS.keys())
    logger.info(f"[job] generate_picks_all_leagues: {len(fixture_leagues)} leghe")

    # Carica all_upcoming una sola volta per il segnale S44
    try:
        with SofaScoreScraper() as sc:
            all_upcoming = sc.get_all_upcoming_matches(days_ahead=7)
        logger.info(f"[job] upcoming matches caricati: {len(all_upcoming)}")
    except Exception:
        logger.warning("[job] get_all_upcoming_matches fallita — segnale S44 disabilitato")
        all_upcoming = []

    for league_key in fixture_leagues:
        generate_picks_for_league(league_key, all_upcoming=all_upcoming)


# ── Lineups ───────────────────────────────────────────────────────────────────


def _upsert_lineup(
    tm_match_id: str,
    home_team: str,
    away_team: str,
    match_date: object,
    league: str,
    lineup_data: dict,
) -> None:
    """
    Salva o aggiorna nel DB le formazioni per entrambi i lati di un match.

    Idempotente su (tm_match_id, side). Se is_official è già True in DB,
    non sovrascrive (la lineup ufficiale non cambia dopo la conferma).
    """
    from models.lineups import Lineup, LineupPlayer

    now = datetime.utcnow()
    is_official = lineup_data.get("confirmed", lineup_data.get("is_official", False))

    for side in ("home", "away"):
        side_data = lineup_data.get(side, {})
        players = side_data.get("players", [])
        if not players:
            continue

        try:
            with get_session() as session:
                existing = (
                    session.query(Lineup).filter_by(tm_match_id=tm_match_id, side=side).first()
                )

                if existing:
                    if existing.is_official:
                        continue
                    existing.formation = side_data.get("formation")
                    existing.scraped_at = now
                    if is_official:
                        existing.is_official = True
                        existing.confirmed_at = now
                    for p in list(existing.players):
                        session.delete(p)
                    session.flush()
                    lineup_id = existing.id
                else:
                    lineup = Lineup(
                        tm_match_id=tm_match_id,
                        side=side,
                        home_team=home_team,
                        away_team=away_team,
                        match_date=match_date,
                        league=league,
                        is_official=is_official,
                        confirmed_at=now if is_official else None,
                        formation=side_data.get("formation"),
                        scraped_at=now,
                    )
                    session.add(lineup)
                    session.flush()
                    lineup_id = lineup.id

                for p in players:
                    session.add(
                        LineupPlayer(
                            lineup_id=lineup_id,
                            name=p["name"],
                            shirt_number=p.get("shirt_number"),
                            position=p.get("position"),
                            grid=p.get("grid"),
                            is_starting=p.get("is_starting", True),
                            is_captain=p.get("is_captain", False),
                        )
                    )

            n_start = sum(1 for p in players if p.get("is_starting"))
            n_bench = len(players) - n_start
            logger.info(
                f"[job] lineup upserted: {home_team} vs {away_team} [{side}] "
                f"{'✅ ufficiale' if is_official else '🟡 probabile'} — "
                f"{n_start} titolari, {n_bench} in panchina"
            )
        except Exception:
            logger.exception(f"[job] _upsert_lineup error: {home_team} vs {away_team} [{side}]")


_AGE_ADJUSTED_FILE = DATA_DIR / "age_adjusted_matches.json"

# Mercati direzionali: home-side e away-side
_AGE_HOME_MARKETS = {"1 — Vittoria Casa", "1X — Doppia Chance Casa", "Handicap −1 Casa"}
_AGE_AWAY_MARKETS = {"2 — Vittoria Trasferta", "X2 — Doppia Chance Trasferta", "Handicap +1 Trasferta"}  # noqa: E501
_AGE_GAP_THRESHOLD = 4.0


def _load_age_adjusted() -> dict:
    try:
        if _AGE_ADJUSTED_FILE.exists():
            data = json.loads(_AGE_ADJUSTED_FILE.read_text())
            # Migrazione: vecchio formato lista → nuovo formato dict
            if isinstance(data, list):
                return {k: {} for k in data}
            return data
    except Exception:
        pass
    return {}


def _save_age_adjusted(data: dict) -> None:
    try:
        _AGE_ADJUSTED_FILE.write_text(json.dumps(data, indent=2))
    except Exception as exc:
        logger.warning("[job] age_signal: salvataggio tracking fallito: %s", exc)


def _apply_age_signal(
    home: str,
    away: str,
    match_date: object,
    avg_age_home: float,
    avg_age_away: float,
) -> None:
    """
    Aggiusta i segnali dei pick pendenti in base al gap di età media XI.
    +1 ai pick della squadra senior, -1 ai pick della squadra giovane.
    Eseguito una sola volta per partita (tracking in age_adjusted_matches.json).
    """
    gap = abs(avg_age_home - avg_age_away)
    if gap < _AGE_GAP_THRESHOLD:
        return

    date_str = match_date.strftime("%Y-%m-%d") if hasattr(match_date, "strftime") else str(match_date)[:10]  # noqa: E501
    key = f"{home}|{away}|{date_str}"
    adjusted = _load_age_adjusted()
    if key in adjusted:
        return

    home_is_senior = avg_age_home > avg_age_away
    senior = home if home_is_senior else away
    young = away if home_is_senior else home
    from models.picks import Pick

    try:
        with get_session() as session:
            picks = (
                session.query(Pick)
                .filter(Pick.home_team == home, Pick.away_team == away, Pick.esito.is_(None))
                .all()
            )
            updated = 0
            for p in picks:
                if p.mercato in _AGE_HOME_MARKETS:
                    p.segnali += 1 if home_is_senior else -1
                    p.segnali = max(1, p.segnali)
                    updated += 1
                elif p.mercato in _AGE_AWAY_MARKETS:
                    p.segnali += 1 if not home_is_senior else -1
                    p.segnali = max(1, p.segnali)
                    updated += 1

        logger.info(
            "[job] age_signal: %s vs %s — senior=%s (%.1f) young=%s (%.1f) Δ=%.1f — %d pick aggiornati",  # noqa: E501
            home, away, senior, max(avg_age_home, avg_age_away),
            young, min(avg_age_home, avg_age_away), gap, updated,
        )
    except Exception:
        logger.exception("[job] age_signal: errore per %s vs %s", home, away)
        return

    adjusted[key] = {
        "home": home,
        "away": away,
        "date": date_str,
        "senior": senior,
        "young": young,
        "avg_age_senior": round(max(avg_age_home, avg_age_away), 1),
        "avg_age_young": round(min(avg_age_home, avg_age_away), 1),
        "gap": round(gap, 1),
    }
    _save_age_adjusted(adjusted)


def update_lineups() -> None:
    """
    Scarica e aggiorna nel DB le formazioni pre-partita da SofaScore.

    Per ogni lega supportata:
    1. Carica fixture dei prossimi 7 giorni
    2. Per ogni fixture: scarica la lineup se non già ufficiale in DB

    Cadenza raccomandata: ogni 30 minuti.
    """
    from scrapers.sofascore import SS_TOURNAMENT_IDS

    ss_leagues = list(SS_TOURNAMENT_IDS.keys())
    logger.info(f"[job] update_lineups: avvio per {len(ss_leagues)} leghe")

    try:
        with SofaScoreScraper() as ss:
            for league_key in ss_leagues:
                try:
                    fixtures = ss.get_upcoming_fixtures(league_key, days_ahead=7)
                except Exception:
                    logger.warning(
                        f"[job] update_lineups: fixture non disponibili per {league_key}"
                    )
                    continue

                for fixture in fixtures:
                    home = fixture["home_team"]
                    away = fixture["away_team"]
                    event_id = fixture.get("event_id")
                    if not event_id:
                        continue

                    ss_match_id = f"ss_{event_id}"

                    # Skip se entrambi i lati già ufficiali in DB
                    try:
                        from models.lineups import Lineup

                        with get_session() as session:
                            official_count = (
                                session.query(Lineup)
                                .filter_by(tm_match_id=ss_match_id, is_official=True)
                                .count()
                            )
                        if official_count >= 2:
                            logger.debug(
                                f"[job] update_lineups: {home} vs {away} già ufficiale, skip"
                            )
                            continue
                    except Exception:
                        logger.exception(f"[job] update_lineups: DB check error per {ss_match_id}")
                        continue

                    try:
                        lineup_data = ss.get_lineups(event_id)
                    except Exception:
                        logger.exception(f"[job] update_lineups: errore scrape lineup {event_id}")
                        continue

                    if lineup_data is None:
                        continue

                    _upsert_lineup(
                        tm_match_id=ss_match_id,
                        home_team=home,
                        away_team=away,
                        match_date=fixture.get("date"),
                        league=league_key,
                        lineup_data=lineup_data,
                    )

                    # Aggiusta segnali pick in base al gap di età
                    age_h = lineup_data.get("home", {}).get("avg_age")
                    age_a = lineup_data.get("away", {}).get("avg_age")
                    if age_h is not None and age_a is not None:
                        _apply_age_signal(home, away, fixture.get("date"), age_h, age_a)

    except Exception:
        logger.exception("[job] update_lineups: errore generale")


# ── Esiti pick ────────────────────────────────────────────────────────────────


def update_pick_outcomes() -> None:
    """
    Aggiorna l'esito dei pick pendenti dopo che le partite sono terminate.

    Logica:
    1. Cerca pick con esito=None e match_date < ora (UTC)
    2. Per ogni pick cerca il Match corrispondente (±4h dal kickoff)
    3. Se il match è FINISHED, calcola l'esito via evaluate_pick_outcome
    4. Aggiorna pick.esito e pick.updated_at
    """
    from sqlalchemy import func

    from analytics.picks import evaluate_pick_outcome
    from models.matches import Match, MatchStatus
    from models.picks import Pick
    from models.teams import Team

    now = datetime.utcnow()
    updated = 0
    skipped = 0

    try:
        with get_session() as session:
            pending_picks = (
                session.query(Pick).filter(Pick.esito.is_(None), Pick.match_date < now).all()
            )

            logger.info(f"[job] update_pick_outcomes: {len(pending_picks)} pick pendenti")

            for pick in pending_picks:
                window_start = pick.match_date - timedelta(hours=4)
                window_end = pick.match_date + timedelta(hours=4)

                match = (
                    session.query(Match)
                    .filter(
                        Match.home_team.has(func.lower(Team.name) == pick.home_team.lower()),
                        Match.away_team.has(func.lower(Team.name) == pick.away_team.lower()),
                        Match.date >= window_start,
                        Match.date <= window_end,
                        Match.status == MatchStatus.FINISHED,
                    )
                    .first()
                )

                if match is None:
                    skipped += 1
                    continue

                outcome = evaluate_pick_outcome(
                    mercato=pick.mercato,
                    hg=match.home_score,
                    ag=match.away_score,
                    hc=match.home_corners,
                    ac=match.away_corners,
                    hy=match.home_yellow_cards,
                    ay=match.away_yellow_cards,
                )

                if outcome is None:
                    skipped += 1
                    continue

                pick.esito = outcome
                pick.updated_at = now
                updated += 1

        logger.info(
            f"[job] update_pick_outcomes completato — {updated} aggiornati, {skipped} saltati"
        )

    except Exception:
        logger.exception("[job] Errore durante update_pick_outcomes")
        return

    # Ricalcola calibrazione con i nuovi esiti
    if updated > 0:
        try:
            from analytics.calibration import save_calibration
            save_calibration()
        except Exception:
            logger.warning("[job] Calibrazione fallita (non bloccante)")
