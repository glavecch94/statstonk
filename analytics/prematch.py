"""
Analytics pre-partita.

Tutte le funzioni accettano un SQLAlchemy Session e restituiscono DataFrame pandas.
"""

from __future__ import annotations

import pandas as pd
from sqlalchemy import select
from sqlalchemy.orm import Session, joinedload

from models.matches import Match, MatchStatus, Odd
from models.teams import Team


# ── Query base ────────────────────────────────────────────────────────────────

def get_matches_df(session: Session, league: str, seasons: list[str]) -> pd.DataFrame:
    """
    Carica tutte le partite finite di un campionato/stagioni come DataFrame.
    Colonne: match_id, date, season, matchday, home, away,
             hg, ag, hthg, htag, hs, as_, hst, ast, hc, ac,
             hf, af, hy, ay, hr, ar
    """
    rows = (
        session.query(Match)
        .options(joinedload(Match.home_team), joinedload(Match.away_team))
        .filter(
            Match.league == league,
            Match.season.in_(seasons),
            Match.status == MatchStatus.FINISHED,
        )
        .order_by(Match.date)
        .all()
    )
    records = []
    for m in rows:
        records.append({
            "match_id":   m.id,
            "date":       pd.Timestamp(m.date),
            "season":     m.season,
            "home":       m.home_team.name,
            "away":       m.away_team.name,
            "hg":         m.home_score,
            "ag":         m.away_score,
            "hthg":       m.home_ht_score,
            "htag":       m.away_ht_score,
            "hs":         m.home_shots,
            "as_":        m.away_shots,
            "hst":        m.home_shots_on_target,
            "ast":        m.away_shots_on_target,
            "hc":         m.home_corners,
            "ac":         m.away_corners,
            "hf":         m.home_fouls,
            "af":         m.away_fouls,
            "hy":         m.home_yellow_cards,
            "ay":         m.away_yellow_cards,
            "hr":         m.home_red_cards,
            "ar":         m.away_red_cards,
        })
    _MATCH_COLUMNS = [
        "match_id", "date", "season", "home", "away",
        "hg", "ag", "hthg", "htag", "hs", "as_", "hst", "ast",
        "hc", "ac", "hf", "af", "hy", "ay", "hr", "ar",
        "total_goals", "total_corners", "total_yellow",
        "over_0_5", "over_1_5", "over_2_5", "over_3_5", "btts", "result",
    ]
    df = pd.DataFrame(records)
    if df.empty:
        return pd.DataFrame(columns=_MATCH_COLUMNS)

    # Colonne derivate
    df["total_goals"]   = df["hg"] + df["ag"]
    df["total_corners"] = df["hc"] + df["ac"]
    df["total_yellow"]  = df["hy"] + df["ay"]
    df["over_0_5"]  = df["total_goals"] >= 1
    df["over_1_5"]  = df["total_goals"] >= 2
    df["over_2_5"]  = df["total_goals"] >= 3
    df["over_3_5"]  = df["total_goals"] >= 4
    df["btts"]      = (df["hg"] > 0) & (df["ag"] > 0)
    df["result"]    = df.apply(
        lambda r: "H" if r.hg > r.ag else ("A" if r.ag > r.hg else "D"), axis=1
    )
    return df


def get_odds_df(session: Session, league: str, seasons: list[str]) -> pd.DataFrame:
    """
    Carica tutte le quote delle partite finite (Avg e Max di mercato + Bet365).
    Ritorna un DataFrame pivot con colonne: match_id, avg_h, avg_d, avg_a,
    max_h, max_d, max_a, b365_h, b365_d, b365_a, avg_over_2_5, avg_under_2_5.
    """
    match_ids_subq = (
        select(Match.id)
        .where(
            Match.league == league,
            Match.season.in_(seasons),
            Match.status == MatchStatus.FINISHED,
        )
        .scalar_subquery()
    )
    odds = (
        session.query(Odd)
        .filter(Odd.match_id.in_(match_ids_subq))
        .all()
    )
    if not odds:
        return pd.DataFrame()

    raw = pd.DataFrame([{
        "match_id":  o.match_id,
        "bookmaker": o.bookmaker,
        "market":    o.market,
        "selection": o.selection,
        "odd":       o.odd,
    } for o in odds])

    def pivot_market(market: str, book: str, sel_map: dict[str, str]) -> pd.DataFrame:
        sub = raw[(raw.market == market) & (raw.bookmaker == book)]
        if sub.empty:
            return pd.DataFrame()
        p = sub.pivot_table(index="match_id", columns="selection", values="odd", aggfunc="last")
        p = p.rename(columns=sel_map)
        return p

    frames = []
    frames.append(pivot_market("1x2_avg", "Market",
                               {"home": "avg_h", "draw": "avg_d", "away": "avg_a"}))
    frames.append(pivot_market("1x2_max", "Market",
                               {"home": "max_h", "draw": "max_d", "away": "max_a"}))
    frames.append(pivot_market("1x2", "Bet365",
                               {"home": "b365_h", "draw": "b365_d", "away": "b365_a"}))
    frames.append(pivot_market("over_2.5_avg", "Market",
                               {"over": "avg_over_2_5", "under": "avg_under_2_5"}))
    frames.append(pivot_market("over_2.5", "Bet365",
                               {"over": "b365_over_2_5", "under": "b365_under_2_5"}))

    result = pd.concat([f for f in frames if not f.empty], axis=1)
    result = result.reset_index()
    return result


# ── Statistiche di campionato ─────────────────────────────────────────────────

def league_overview(df: pd.DataFrame) -> pd.DataFrame:
    """
    Statistiche aggregate per stagione.
    """
    if df.empty:
        return pd.DataFrame()
    g = df.groupby("season")
    overview = pd.DataFrame({
        "Partite":           g["match_id"].count(),
        "Media gol":         g["total_goals"].mean().round(2),
        "Over 2.5 %":        (g["over_2_5"].mean() * 100).round(1),
        "Goal/No Goal %":    (g["btts"].mean() * 100).round(1),
        "Media corner":      g["total_corners"].mean().round(1),
        "Media gialli":      g["total_yellow"].mean().round(1),
        "Vittorie casa %":   (g["result"].apply(lambda s: (s == "H").mean()) * 100).round(1),
        "Pareggi %":         (g["result"].apply(lambda s: (s == "D").mean()) * 100).round(1),
        "Vittorie trasferta %": (g["result"].apply(lambda s: (s == "A").mean()) * 100).round(1),
    }).reset_index().rename(columns={"season": "Stagione"})
    return overview


# ── Statistiche per squadra ───────────────────────────────────────────────────

def team_stats(df: pd.DataFrame, season: str | None = None) -> pd.DataFrame:
    """
    Rendimento di ogni squadra (casa + trasferta combinati).
    """
    if df.empty:
        return pd.DataFrame()
    d = df[df.season == season] if season else df.copy()

    home = d.rename(columns={
        "home": "team", "hg": "gf", "ag": "gs",
        "hc": "corners_f", "ac": "corners_a",
        "hy": "yellow", "hr": "red",
    })[["team", "season", "gf", "gs", "corners_f", "corners_a",
        "yellow", "red", "over_2_5", "btts", "result"]].copy()
    home["venue"] = "home"
    home["won"]  = home["result"] == "H"
    home["draw"] = home["result"] == "D"
    home["lost"] = home["result"] == "A"

    away = d.rename(columns={
        "away": "team", "ag": "gf", "hg": "gs",
        "ac": "corners_f", "hc": "corners_a",
        "ay": "yellow", "ar": "red",
    })[["team", "season", "gf", "gs", "corners_f", "corners_a",
        "yellow", "red", "over_2_5", "btts", "result"]].copy()
    away["venue"] = "away"
    away["won"]  = away["result"] == "A"
    away["draw"] = away["result"] == "D"
    away["lost"] = away["result"] == "H"

    combined = pd.concat([home, away], ignore_index=True)
    g = combined.groupby("team")

    stats = pd.DataFrame({
        "PG":               g["gf"].count(),
        "V":                g["won"].sum().astype(int),
        "P":                g["draw"].sum().astype(int),
        "S":                g["lost"].sum().astype(int),
        "Gol fatti":        g["gf"].sum().astype(int),
        "Gol subiti":       g["gs"].sum().astype(int),
        "Media gol F":      g["gf"].mean().round(2),
        "Media gol S":      g["gs"].mean().round(2),
        "Over 2.5 %":       (g["over_2_5"].mean() * 100).round(1),
        "Goal/No Goal %":   (g["btts"].mean() * 100).round(1),
        "Corner medi":      g["corners_f"].mean().round(1),
        "Gialli medi":      g["yellow"].mean().round(2),
    }).reset_index().rename(columns={"team": "Squadra"})
    stats = stats.sort_values("V", ascending=False).reset_index(drop=True)
    return stats


def team_form(df: pd.DataFrame, team: str, last_n: int = 10) -> pd.DataFrame:
    """
    Ultime N partite di una squadra (casa + trasferta).
    """
    if df.empty:
        return pd.DataFrame()
    tl = team.lower()
    mask = (df["home"].str.lower() == tl) | (df["away"].str.lower() == tl)
    d = df[mask].sort_values("date", ascending=False).head(last_n).copy()

    def row_result(r: pd.Series) -> str:
        if r["home"].lower() == tl:
            return "V" if r["result"] == "H" else ("P" if r["result"] == "D" else "S")
        else:
            return "V" if r["result"] == "A" else ("P" if r["result"] == "D" else "S")

    d["esito"]     = d.apply(row_result, axis=1)
    d["avversario"] = d.apply(
        lambda r: r["away"] if r["home"].lower() == tl else r["home"], axis=1
    )
    d["sede"]       = d.apply(lambda r: "Casa" if r["home"].lower() == tl else "Trasf.", axis=1)
    d["risultato"]  = d["hg"].astype(str) + "-" + d["ag"].astype(str)

    return d[["date", "sede", "avversario", "risultato", "esito",
              "total_goals", "total_corners", "total_yellow",
              "over_2_5", "btts"]].reset_index(drop=True)


# ── Ritardi (streak senza che si verifichi un evento) ────────────────────────

def compute_delays(df: pd.DataFrame) -> pd.DataFrame:
    """
    Per ogni squadra calcola il ritardo attuale (partite consecutive senza l'evento).
    Utile per trovare squadre 'in ritardo' su Over/Under/BTTS/Corner.

    Ritorna un DataFrame con una riga per squadra e colonne:
        team, delay_over_2_5, delay_under_2_5, delay_btts, delay_no_btts,
        delay_over_corners_9 (totale angoli > 9)
    """
    if df.empty:
        return pd.DataFrame()

    teams = sorted(set(df["home"].tolist() + df["away"].tolist()))
    records = []

    for team in teams:
        mask = (df["home"] == team) | (df["away"] == team)
        matches = df[mask].sort_values("date", ascending=False)

        def streak(series: pd.Series) -> int:
            """Numero di partite consecutive in cui la condizione è False."""
            count = 0
            for val in series:
                if not val:
                    count += 1
                else:
                    break
            return count

        records.append({
            "Squadra":             team,
            "Ritardo Over 2.5":    streak(matches["over_2_5"]),
            "Ritardo Under 2.5":   streak(~matches["over_2_5"]),
            "Ritardo Goal":        streak(matches["btts"]),
            "Ritardo No Goal":     streak(~matches["btts"]),
            "Ritardo Corner>9":    streak(matches["total_corners"] > 9),
        })

    return pd.DataFrame(records).sort_values("Ritardo Over 2.5", ascending=False)


# ── Giorni di riposo ──────────────────────────────────────────────────────────

def team_last_match_date(
    df: pd.DataFrame,
    team: str,
    before_date: pd.Timestamp,
) -> pd.Timestamp | None:
    """
    Restituisce la data dell'ultima partita giocata da una squadra
    prima di before_date. Utile per calcolare i giorni di riposo.
    """
    tl = team.lower()
    mask = (
        ((df["home"].str.lower() == tl) | (df["away"].str.lower() == tl)) &
        (df["date"] < before_date)
    )
    past = df[mask]
    if past.empty:
        return None
    return past["date"].max()


def team_next_match(
    upcoming: list[dict],
    team: str,
    after_date,
) -> dict | None:
    """
    Restituisce la prossima partita programmata per una squadra dopo after_date.

    Args:
        upcoming: lista di dict {home_team, away_team, date, league_key, league_name}
                  (output di FootballDataOrgScraper.get_all_upcoming_matches)
        team:     nome interno squadra (es. "Inter", "Man City")
        after_date: data di riferimento; cerca match STRETTAMENTE dopo questa data

    Returns:
        dict {date, league_key, league_name} oppure None se nessuna trovata.
    """
    after = pd.Timestamp(after_date)
    tl = team.lower()
    candidates = [
        m for m in upcoming
        if (m["home_team"].lower() == tl or m["away_team"].lower() == tl)
        and pd.Timestamp(m["date"]) > after
    ]
    if not candidates:
        return None
    return min(candidates, key=lambda m: pd.Timestamp(m["date"]))


# ── Value bet ─────────────────────────────────────────────────────────────────

def value_bets(
    matches_df: pd.DataFrame,
    odds_df: pd.DataFrame,
    min_value: float = 0.05,
) -> pd.DataFrame:
    """
    Identifica value bet sulle partite finite confrontando la probabilità
    implicita nelle quote Avg di mercato con la frequenza storica reale.

    Un value bet esiste quando:
        prob_reale > prob_implicita_avg  (edge >= min_value)

    Args:
        min_value: edge minimo (0.05 = 5%) per considerare una scommessa di valore

    Returns:
        DataFrame con partite e selezioni che hanno mostrato value, ordinato per edge.
    """
    if matches_df.empty or odds_df.empty:
        return pd.DataFrame()

    merged = matches_df.merge(odds_df, on="match_id", how="inner")
    records = []

    for _, r in merged.iterrows():
        for sel, odd_col, actual_col, true_val in [
            ("Casa",    "avg_h",  "result",   "H"),
            ("Pareggio","avg_d",  "result",   "D"),
            ("Trasferta","avg_a", "result",   "A"),
            ("Over 2.5","avg_over_2_5", "over_2_5", True),
            ("Under 2.5","avg_under_2_5","over_2_5", False),
        ]:
            odd = r.get(odd_col)
            actual = r.get(actual_col)
            if pd.isna(odd) or odd <= 1.0:
                continue
            implied_prob = 1 / odd
            outcome = (actual == true_val) if isinstance(true_val, str) else bool(actual) == true_val
            records.append({
                "match_id":      r["match_id"],
                "data":          r["date"].strftime("%d/%m/%Y"),
                "partita":       f"{r['home']} - {r['away']}",
                "selezione":     sel,
                "quota_avg":     round(odd, 2),
                "prob_implicita": round(implied_prob, 3),
                "esito_reale":   int(outcome),
            })

    if not records:
        return pd.DataFrame()

    df_bets = pd.DataFrame(records)
    # Aggrega per selezione: frequenza reale vs prob implicita media
    summary = (
        df_bets.groupby("selezione")
        .agg(
            partite=("match_id", "count"),
            freq_reale=("esito_reale", "mean"),
            prob_implicita_avg=("prob_implicita", "mean"),
            quota_avg=("quota_avg", "mean"),
        )
        .reset_index()
    )
    summary["edge"] = (summary["freq_reale"] - summary["prob_implicita_avg"]).round(3)
    summary["value"] = summary["edge"] >= min_value
    summary = summary.sort_values("edge", ascending=False)
    return summary


# ── Classifica e tier avversari ───────────────────────────────────────────────

def compute_standings(df: pd.DataFrame, season: str) -> pd.DataFrame:
    """
    Calcola la classifica di una stagione dai risultati.
    Aggiunge colonna 'tier': "top" | "mid" | "bottom" (terzi della classifica).
    """
    d = df[df["season"] == season]
    if d.empty:
        return pd.DataFrame()

    teams = sorted(set(d["home"].tolist() + d["away"].tolist()))
    records = []
    for team in teams:
        hm = d[d["home"] == team]
        am = d[d["away"] == team]
        pts = (
            (hm["result"] == "H").sum() * 3 +
            (hm["result"] == "D").sum() +
            (am["result"] == "A").sum() * 3 +
            (am["result"] == "D").sum()
        )
        gf = int(hm["hg"].sum() + am["ag"].sum())
        gs = int(hm["ag"].sum() + am["hg"].sum())
        records.append({
            "Squadra": team,
            "PG": len(hm) + len(am),
            "Punti": int(pts),
            "GF": gf,
            "GS": gs,
            "DR": gf - gs,
        })

    standings = (
        pd.DataFrame(records)
        .sort_values(["Punti", "DR", "GF"], ascending=False)
        .reset_index(drop=True)
    )
    standings["Pos"] = standings.index + 1
    n = len(standings)
    top_cut = n // 3
    bot_cut = n - n // 3
    standings["tier"] = "mid"
    standings.loc[standings["Pos"] <= top_cut, "tier"] = "top"
    standings.loc[standings["Pos"] > bot_cut, "tier"] = "bottom"
    return standings


# ── Statistiche contestuali di una squadra ────────────────────────────────────

def team_context_stats(
    df: pd.DataFrame,
    team: str,
    venue: str = "both",
    opponent_tier: str | None = None,
    standings: pd.DataFrame | None = None,
) -> dict:
    """
    Statistiche di una squadra filtrate per contesto.

    Args:
        venue:         "home" | "away" | "both"
        opponent_tier: "top" | "mid" | "bottom" | None (tutti)
        standings:     DataFrame da compute_standings(), necessario per opponent_tier
    """
    tl = team.lower()
    home_m = df[df["home"].str.lower() == tl].copy()
    away_m = df[df["away"].str.lower() == tl].copy()

    if venue == "home":
        games = home_m.copy()
        games["gf"] = games["hg"]; games["gs"] = games["ag"]
        games["won"] = games["result"] == "H"
        games["draw"] = games["result"] == "D"
        games["opponent"] = games["away"]
    elif venue == "away":
        games = away_m.copy()
        games["gf"] = games["ag"]; games["gs"] = games["hg"]
        games["won"] = games["result"] == "A"
        games["draw"] = games["result"] == "D"
        games["opponent"] = games["home"]
    else:
        hg = home_m.copy()
        hg["gf"] = hg["hg"]; hg["gs"] = hg["ag"]
        hg["won"] = hg["result"] == "H"; hg["draw"] = hg["result"] == "D"
        hg["opponent"] = hg["away"]
        ag = away_m.copy()
        ag["gf"] = ag["ag"]; ag["gs"] = ag["hg"]
        ag["won"] = ag["result"] == "A"; ag["draw"] = ag["result"] == "D"
        ag["opponent"] = ag["home"]
        games = pd.concat([hg, ag]).sort_values("date")

    if opponent_tier and standings is not None and not standings.empty:
        tier_teams = standings.loc[standings["tier"] == opponent_tier, "Squadra"].tolist()
        games = games[games["opponent"].isin(tier_teams)]

    if games.empty:
        return {}

    return {
        "campione":         len(games),
        "win_pct":          round(games["won"].mean() * 100, 1),
        "draw_pct":         round(games["draw"].mean() * 100, 1),
        "loss_pct":         round((~games["won"] & ~games["draw"]).mean() * 100, 1),
        "media_gf":         round(games["gf"].mean(), 2),
        "media_gs":         round(games["gs"].mean(), 2),
        "scored_pct":       round((games["gf"] > 0).mean() * 100, 1),
        "clean_sheet_pct":  round((games["gs"] == 0).mean() * 100, 1),
        "over_1_5_pct":     round((games["total_goals"] >= 2).mean() * 100, 1),
        "over_2_5_pct":     round(games["over_2_5"].mean() * 100, 1),
        "over_3_5_pct":     round((games["total_goals"] >= 4).mean() * 100, 1),
        "goal_pct":         round(games["btts"].mean() * 100, 1),
        "media_corner":     round(games["total_corners"].mean(), 1),
        "corner_over9_pct": round((games["total_corners"] > 9).mean() * 100, 1),
        "corner_over10_pct":round((games["total_corners"] > 10).mean() * 100, 1),
        "media_gialli":     round(games["total_yellow"].mean(), 1),
        "gialli_over3_pct": round((games["total_yellow"] > 3).mean() * 100, 1),
    }


# ── Head-to-Head ──────────────────────────────────────────────────────────────

def head_to_head(df: pd.DataFrame, team_a: str, team_b: str) -> pd.DataFrame:
    """
    Storico completo degli scontri diretti tra due squadre.
    I risultati (esito, sede) sono sempre dal punto di vista di team_a.
    """
    tal = team_a.lower()
    tbl = team_b.lower()
    mask = (
        ((df["home"].str.lower() == tal) & (df["away"].str.lower() == tbl)) |
        ((df["home"].str.lower() == tbl) & (df["away"].str.lower() == tal))
    )
    h2h = df[mask].copy().sort_values("date", ascending=False)
    if h2h.empty:
        return pd.DataFrame()

    def esito(r: pd.Series) -> str:
        if r["home"].lower() == tal:
            return {"H": "V", "D": "P", "A": "S"}[r["result"]]
        return {"A": "V", "D": "P", "H": "S"}[r["result"]]

    h2h["sede_a"]    = h2h.apply(lambda r: "Casa" if r["home"].lower() == tal else "Trasf.", axis=1)
    h2h["esito_a"]   = h2h.apply(esito, axis=1)
    h2h["risultato"] = h2h["hg"].astype(str) + "-" + h2h["ag"].astype(str)

    return h2h[[
        "date", "season", "sede_a", "home", "away",
        "risultato", "esito_a", "total_goals", "total_corners",
        "over_2_5", "btts",
    ]].reset_index(drop=True)


def h2h_summary(h2h_df: pd.DataFrame, team_a: str) -> dict:
    """Statistiche aggregate H2H."""
    if h2h_df.empty:
        return {}
    n = len(h2h_df)
    return {
        "campione":          n,
        "win_pct":           round((h2h_df["esito_a"] == "V").mean() * 100, 1),
        "draw_pct":          round((h2h_df["esito_a"] == "P").mean() * 100, 1),
        "loss_pct":          round((h2h_df["esito_a"] == "S").mean() * 100, 1),
        "media_gol":         round(h2h_df["total_goals"].mean(), 2),
        "over_1_5_pct":      round((h2h_df["total_goals"] >= 2).mean() * 100, 1),
        "over_2_5_pct":      round(h2h_df["over_2_5"].mean() * 100, 1),
        "over_3_5_pct":      round((h2h_df["total_goals"] >= 4).mean() * 100, 1),
        "goal_pct":          round(h2h_df["btts"].mean() * 100, 1),
        "media_corner":      round(h2h_df["total_corners"].mean(), 1),
        "corner_over9_pct":  round((h2h_df["total_corners"] > 9).mean() * 100, 1),
        "corner_over10_pct": round((h2h_df["total_corners"] > 10).mean() * 100, 1),
    }


def match_value_analysis(
    df: pd.DataFrame,
    odds_df: pd.DataFrame,
    home_team: str,
    away_team: str,
) -> pd.DataFrame:
    """
    Per tutti gli scontri diretti storici tra le due squadre, confronta
    la frequenza reale degli esiti con le quote medie di mercato di quelle partite.

    Utile per capire se il mercato ha storicamente valutato correttamente
    questa specifica sfida.
    """
    hl = home_team.lower()
    al = away_team.lower()
    h2h_mask = (
        ((df["home"].str.lower() == hl) & (df["away"].str.lower() == al)) |
        ((df["home"].str.lower() == al) & (df["away"].str.lower() == hl))
    )
    h2h_df = df[h2h_mask]
    if h2h_df.empty or odds_df.empty:
        return pd.DataFrame()

    merged = h2h_df.merge(odds_df, on="match_id", how="inner")
    if merged.empty:
        return pd.DataFrame()

    records = []
    for sel, odd_col, col, true_val in [
        ("1 (Casa H2H)",    "avg_h",        "result",   "H"),
        ("X (Pareggio H2H)","avg_d",        "result",   "D"),
        ("2 (Trasf. H2H)",  "avg_a",        "result",   "A"),
        ("Over 2.5 H2H",    "avg_over_2_5", "over_2_5", True),
        ("Under 2.5 H2H",   "avg_under_2_5","over_2_5", False),
    ]:
        sub = merged[merged[odd_col].notna() & (merged[odd_col] > 1.0)]
        if sub.empty:
            continue
        freq = (sub[col] == true_val).mean() if isinstance(true_val, str) else (sub[col] == true_val).mean()
        imp  = (1 / sub[odd_col]).mean()
        records.append({
            "Selezione":          sel,
            "Partite H2H":        len(sub),
            "Freq. reale %":      round(freq * 100, 1),
            "Prob. implicita %":  round(imp * 100, 1),
            "Quota avg":          round(sub[odd_col].mean(), 2),
            "Edge %":             round((freq - imp) * 100, 1),
        })

    return pd.DataFrame(records).sort_values("Edge %", ascending=False)


# ── Top scommesse per partita ─────────────────────────────────────────────────

def match_top_bets(
    df: pd.DataFrame,
    home_team: str,
    away_team: str,
    standings: pd.DataFrame | None = None,
    min_odd: float = 1.20,
    max_odd: float = 2.50,
    top_n: int = 10,
) -> pd.DataFrame:
    """
    Calcola le scommesse più supportate storicamente per una partita specifica.

    Per ogni mercato la probabilità è calcolata come media pesata tra:
      - 60% contesto (home team in casa + away team in trasferta, mediati)
      - 40% H2H (scontri diretti)
    Se H2H assente, usa solo il contesto.

    Returns:
        DataFrame ordinato per quota (crescente) nel range [min_odd, max_odd],
        con le prime top_n scommesse.
    """
    ctx_h = team_context_stats(df, home_team, venue="home", standings=standings)
    ctx_a = team_context_stats(df, away_team, venue="away", standings=standings)
    h2h   = h2h_summary(head_to_head(df, home_team, away_team), home_team)

    def b(vh, va, hv, w_ctx=0.6, w_h2h=0.4) -> float | None:
        """
        Blend contestuale + H2H per mercati che dipendono da entrambe le squadre.
        vh: valore home context, va: valore away context, hv: valore H2H.
        """
        ctx_val = (vh + va) / 2 if vh is not None and va is not None else (vh or va)
        if ctx_val is None:
            return None
        if not hv:
            return ctx_val / 100
        return (ctx_val * w_ctx + hv * w_h2h) / 100

    def bh(vh, hv, w_ctx=0.6, w_h2h=0.4) -> float | None:
        """Blend per mercati che dipendono solo dalla squadra di casa."""
        if vh is None:
            return None
        if not hv:
            return vh / 100
        return (vh * w_ctx + hv * w_h2h) / 100

    n_ctx = min(ctx_h.get("campione", 0), ctx_a.get("campione", 0))
    n_h2h = h2h.get("campione", 0)

    # ── Definizione mercati ───────────────────────────────────────────────────
    # (label, probabilità stimata, campione di riferimento)
    entries: list[tuple[str, float | None, int]] = [
        # 1X2
        ("1 — Vittoria Casa",
         bh(ctx_h.get("win_pct"),  h2h.get("win_pct")),   n_ctx),
        ("X — Pareggio",
         bh(ctx_h.get("draw_pct"), h2h.get("draw_pct")),  n_ctx),
        ("2 — Vittoria Trasferta",
         bh(ctx_a.get("win_pct"),  h2h.get("loss_pct")),  n_ctx),
        # Doppia chance
        ("1X — Casa o Pareggio",
         bh(100 - ctx_a.get("win_pct", 50), 100 - h2h.get("loss_pct", 33)), n_ctx),
        ("X2 — Pareggio o Trasferta",
         bh(100 - ctx_h.get("win_pct", 50), 100 - h2h.get("win_pct", 33)),  n_ctx),
        # Gol
        ("Over 1.5 Gol",
         b(ctx_h.get("over_1_5_pct"), ctx_a.get("over_1_5_pct"), h2h.get("over_1_5_pct")), n_ctx),
        ("Over 2.5 Gol",
         b(ctx_h.get("over_2_5_pct"), ctx_a.get("over_2_5_pct"), h2h.get("over_2_5_pct")), n_ctx),
        ("Under 2.5 Gol",
         b(100 - ctx_h.get("over_2_5_pct", 50), 100 - ctx_a.get("over_2_5_pct", 50),
           100 - h2h.get("over_2_5_pct", 50)), n_ctx),
        ("Over 3.5 Gol",
         b(ctx_h.get("over_3_5_pct"), ctx_a.get("over_3_5_pct"), h2h.get("over_3_5_pct")), n_ctx),
        # Goal / No Goal
        ("Goal — Entrambe Segnano",
         b(ctx_h.get("goal_pct"), ctx_a.get("goal_pct"), h2h.get("goal_pct")), n_ctx),
        ("No Goal — Solo una Segna",
         b(100 - ctx_h.get("goal_pct", 50), 100 - ctx_a.get("goal_pct", 50),
           100 - h2h.get("goal_pct", 50)), n_ctx),
        # Casa / Trasferta segnano
        ("Casa Segna (almeno 1 gol)",
         bh(ctx_h.get("scored_pct"), None), ctx_h.get("campione", 0)),
        ("Trasferta Segna (almeno 1 gol)",
         bh(ctx_a.get("scored_pct"), None), ctx_a.get("campione", 0)),
        ("Casa Clean Sheet",
         bh(ctx_h.get("clean_sheet_pct"), None), ctx_h.get("campione", 0)),
        ("Trasferta Clean Sheet",
         bh(ctx_a.get("clean_sheet_pct"), None), ctx_a.get("campione", 0)),
        # Corner
        ("Corner Over 9.5",
         b(ctx_h.get("corner_over9_pct"), ctx_a.get("corner_over9_pct"),
           h2h.get("corner_over9_pct")), n_ctx),
        ("Corner Under 9.5",
         b(100 - ctx_h.get("corner_over9_pct", 50), 100 - ctx_a.get("corner_over9_pct", 50),
           100 - h2h.get("corner_over9_pct", 50)), n_ctx),
        ("Corner Over 10.5",
         b(ctx_h.get("corner_over10_pct"), ctx_a.get("corner_over10_pct"),
           h2h.get("corner_over10_pct")), n_ctx),
        # Cartellini
        ("Cartellini Gialli Over 3.5",
         b(ctx_h.get("gialli_over3_pct"), ctx_a.get("gialli_over3_pct"), None), n_ctx),
    ]

    records = []
    for label, prob, campione in entries:
        if prob is None or prob <= 0.01 or prob >= 0.99:
            continue
        quota = round(1 / prob, 2)
        if min_odd <= quota <= max_odd:
            records.append({
                "Mercato":       label,
                "Prob. %":       round(prob * 100, 1),
                "Quota fair":    quota,
                "Campione":      campione,
                "H2H":           n_h2h,
            })

    if not records:
        return pd.DataFrame()

    return (
        pd.DataFrame(records)
        .sort_values(["Campione", "Prob. %"], ascending=[False, False])
        .head(top_n)
        .reset_index(drop=True)
    )
