"""
Prossime Partite
────────────────
Fixture delle prossime partite + analisi storica automatica per ogni partita.
Struttura card: confronto attacco/difesa (tabella) + segnali incrociati + pronostici.
"""

from __future__ import annotations

from collections import Counter
from datetime import UTC
from zoneinfo import ZoneInfo

_TZ_ROME = ZoneInfo("Europe/Rome")

import pandas as pd
import streamlit as st

from analytics.prematch import (
    compute_delays,
    get_matches_df,
    h2h_summary,
    head_to_head,
    team_form,
    team_last_match_date,
    team_next_match,
)
from analytics.signals import (
    filter_conflicts as _filter_conflicts,
)
from analytics.signals import (
    generate_signals as _generate_signals,
)
from analytics.signals import (
    save_picks as _save_picks_fn,
)
from analytics.signals import (
    season_form_stats as _season_form_stats,
)
from config import (
    CURRENT_SEASON,
    HISTORICAL_SEASONS,
    LEAGUES,
)
from dashboard.components import render_lineup as _render_lineup_shared
from db import SessionLocal
from scrapers.sofascore import ss_quota_for_mercato as _ss_quota_for_mercato

st.set_page_config(page_title="Prossime Partite | Statstonk", layout="wide")
st.title("📅 Prossime Partite")

# ── Sidebar ───────────────────────────────────────────────────────────────────

with st.sidebar:
    st.header("Impostazioni")
    from scrapers.sofascore import SS_TOURNAMENT_IDS
    _fixture_leagues = list(SS_TOURNAMENT_IDS.keys())
    league_key = st.selectbox(
        "Campionato", _fixture_leagues, format_func=lambda k: LEAGUES[k]["name"]
    )

    st.markdown("---")
    top_n = st.slider("Max pronostici per partita", 3, 15, 8)

    if st.button("🔄 Aggiorna fixture"):
        st.cache_data.clear()
        st.rerun()

# ── Caricamento dati ──────────────────────────────────────────────────────────


@st.cache_data(ttl=600)
def load_fixtures(league: str) -> list[dict]:
    from scrapers.sofascore import SofaScoreScraper

    with SofaScoreScraper() as ss:
        return ss.get_upcoming_fixtures(league, days_ahead=14)


@st.cache_data(ttl=3600)
def load_recent_match_dates() -> dict:
    """Data dell'ultima partita giocata per squadra (ultimi 14 gg, da DB)."""
    from datetime import timedelta

    from models.matches import Match, MatchStatus

    session = SessionLocal()
    try:
        cutoff = pd.Timestamp.utcnow().to_pydatetime() - timedelta(days=14)
        matches = (
            session.query(Match)
            .filter(Match.status == MatchStatus.FINISHED, Match.date >= cutoff)
            .all()
        )
        result: dict = {}
        for m in matches:
            for team_obj in (m.home_team, m.away_team):
                name = team_obj.name
                if name not in result or m.date > result[name]:
                    result[name] = m.date
        return result
    finally:
        session.close()


@st.cache_data(ttl=3600)
def load_all_upcoming_matches() -> list[dict]:
    """Match programmati nei prossimi 7 giorni (tutte le competizioni). Cache 1h."""
    try:
        from scrapers.sofascore import SofaScoreScraper

        with SofaScoreScraper() as ss:
            return ss.get_all_upcoming_matches(days_ahead=7)
    except Exception:
        return []


@st.cache_data(ttl=300)
def load_lineups(home: str, away: str, match_date_str: str) -> dict | None:
    """
    Carica le formazioni dal DB per la partita specificata.

    Returns:
        {
            "home": {"is_official": bool, "formation": str|None, "players": list[dict]},
            "away": {...},
        }
        None se non ci sono formazioni disponibili.
    """
    from models.lineups import Lineup, LineupPlayer

    session = SessionLocal()
    try:
        lineups = (
            session.query(Lineup)
            .filter(Lineup.home_team == home, Lineup.away_team == away)
            .all()
        )
        if not lineups:
            return None

        result: dict = {}
        for lineup in lineups:
            players = (
                session.query(LineupPlayer)
                .filter(LineupPlayer.lineup_id == lineup.id)
                .all()
            )
            result[lineup.side] = {
                "is_official": lineup.is_official,
                "formation": lineup.formation,
                "players": [
                    {
                        "name": p.name,
                        "number": p.shirt_number,
                        "position": p.position,
                        "is_starting": p.is_starting,
                        "is_captain": p.is_captain,
                    }
                    for p in players
                ],
            }
        return result if result else None
    finally:
        session.close()


@st.cache_data(ttl=300)
def load_historical(league: str) -> pd.DataFrame:
    session = SessionLocal()
    try:
        return get_matches_df(session, league, HISTORICAL_SEASONS)
    finally:
        session.close()


@st.cache_data(ttl=3600)  # 1h — quote/lineups SofaScore per tutte le partite del turno
def load_sofascore_all(league_key: str, fixture_dates: tuple[str, ...]) -> dict:
    """
    Carica odds (15 mercati) + lineups da SofaScore per tutte le partite del turno.

    Returns:
        {(home_norm, away_norm): {"event_id": int, "odds": dict, "lineups": dict|None}}
    dove le chiavi sono nomi interni title case (da SS_TEAM_NAME_MAP).
    Restituisce {} in caso di errore (fallisce silenziosamente).
    """
    try:
        from scrapers.sofascore import SofaScoreScraper

        with SofaScoreScraper() as ss:
            return ss.get_all_for_league(league_key, list(fixture_dates))
    except Exception as exc:
        import logging
        logging.getLogger(__name__).warning("SofaScore load_all fallito: %s", exc)
        return {}


with st.spinner("Caricamento fixture..."):
    try:
        fixtures = load_fixtures(league_key)
    except ValueError:
        st.warning(
            f"**Fixture non disponibili per {LEAGUES[league_key]['name']}.**\n\n"
            "Campionato non supportato da SofaScore o ID torneo mancante. "
            "Seleziona un altro campionato."
        )
        st.stop()
    except Exception as e:
        st.error(f"Errore nel recupero delle fixture: {e}")
        st.stop()

if not fixtures:
    st.info("Nessuna partita pianificata trovata.")
    st.stop()

df = load_historical(league_key)
st.subheader(LEAGUES[league_key]['name'])
st.caption(f"{len(fixtures)} partite · {len(df):,} partite storiche")

# ── Pre-calcola ritardi (stagione corrente) ───────────────────────────────────

df_current = df[df["season"] == CURRENT_SEASON]
delays = compute_delays(df_current)


# ── Helper: HT/FT e risultati esatti ─────────────────────────────────────────


def _ht_ft_freqs(df: pd.DataFrame, team: str, venue: str) -> list[tuple]:
    """
    Top combinazioni HT/FT per il team nel venue specificato.
    Ritorna lista di (label "1/1", count, pct) ordinata per pct desc.
    """
    tl = team.lower()
    d = df[df["home"].str.lower() == tl] if venue == "home" else df[df["away"].str.lower() == tl]
    if d.empty:
        return []
    d = d.copy()
    d["ht_res"] = d.apply(
        lambda r: "H" if r["hthg"] > r["htag"] else ("A" if r["hthg"] < r["htag"] else "D"),
        axis=1,
    )
    n = len(d)
    combos = d.groupby(["ht_res", "result"]).size().reset_index(name="cnt")
    lbl = {"H": "1", "D": "X", "A": "2"}
    result = []
    for _, row in combos.sort_values("cnt", ascending=False).iterrows():
        result.append(
            (
                f"{lbl[row['ht_res']]}/{lbl[row['result']]}",
                int(row["cnt"]),
                round(row["cnt"] / n * 100, 1),
            )
        )
    return result


def _top_scores(df: pd.DataFrame, team: str, venue: str, top_n: int = 5) -> list[tuple]:
    """
    Top N risultati esatti nelle partite del team nel venue specificato.
    Ritorna lista di ("2-1", count, pct) ordinata per count desc.
    """
    tl = team.lower()
    d = df[df["home"].str.lower() == tl] if venue == "home" else df[df["away"].str.lower() == tl]
    if d.empty:
        return []
    n = len(d)
    scores = d.groupby(["hg", "ag"]).size().reset_index(name="cnt")
    scores = scores.sort_values("cnt", ascending=False).head(top_n)
    return [
        (f"{int(r['hg'])}-{int(r['ag'])}", int(r["cnt"]), round(r["cnt"] / n * 100, 1))
        for _, r in scores.iterrows()
    ]


# ── Tabella previsionale ──────────────────────────────────────────────────────


def _build_forecast_df(fh: dict, fa: dict) -> pd.DataFrame:
    """Tabella previsionale (3 colonne: Categoria, Mercato, Stima)."""

    def avg2(a, b):
        if a is None or b is None:
            return None
        return (a + b) / 2

    def sum2(a, b):
        if a is None or b is None:
            return None
        return a + b

    def pct(v) -> str:
        return f"{v * 100:.0f}%" if v is not None else "—"

    def num(v, d: int = 1) -> str:
        return f"{v:.{d}f}" if v is not None else "—"

    rows = [
        ("Risultato", "1 — Vittoria casa", pct(fh.get("won"))),
        ("Risultato", "X — Pareggio", pct(avg2(fh.get("draw"), fa.get("draw")))),
        ("Risultato", "2 — Vittoria trasferta", pct(fa.get("won"))),
        ("Gol totali", "Gol attesi", num(sum2(fh.get("avg_gf"), fa.get("avg_gf")))),
        ("Gol totali", "Over 1.5", pct(avg2(fh.get("over_1_5"), fa.get("over_1_5")))),
        ("Gol totali", "Over 2.5", pct(avg2(fh.get("over_2_5"), fa.get("over_2_5")))),
        ("Gol totali", "Over 3.5", pct(avg2(fh.get("over_3_5"), fa.get("over_3_5")))),
        ("Gol totali", "BTTS", pct(avg2(fh.get("btts"), fa.get("btts")))),
        ("Gol per squadra", "Casa segna ≥1", pct(fh.get("scored"))),
        ("Gol per squadra", "Casa segna ≥2", pct(fh.get("scored_2plus"))),
        ("Gol per squadra", "Trasferta segna ≥1", pct(fa.get("scored"))),
        ("Gol per squadra", "Trasferta segna ≥2", pct(fa.get("scored_2plus"))),
        ("Corner", "Corner attesi", num(avg2(fh.get("corners"), fa.get("corners")))),
        ("Corner", "Over 9.5", pct(avg2(fh.get("over_9_5_corners"), fa.get("over_9_5_corners")))),
        (
            "Corner",
            "Over 10.5",
            pct(avg2(fh.get("over_10_5_corners"), fa.get("over_10_5_corners"))),
        ),
        (
            "Cartellini",
            "Gialli attesi",
            num(avg2(fh.get("avg_match_yellow"), fa.get("avg_match_yellow"))),
        ),
        (
            "Cartellini",
            "Gialli Over 3.5",
            pct(avg2(fh.get("over_3_5_yellow"), fa.get("over_3_5_yellow"))),
        ),
        ("Falli", "Falli attesi", num(avg2(fh.get("avg_match_fouls"), fa.get("avg_match_fouls")))),
        (
            "Primo tempo",
            "Gol nel 1° tempo",
            pct(avg2(fh.get("ht_over_0_5"), fa.get("ht_over_0_5"))),
        ),
        ("Primo tempo", "BTTS nel 1° tempo", pct(avg2(fh.get("ht_btts"), fa.get("ht_btts")))),
        (
            "Secondo tempo",
            "Gol nel 2° tempo",
            pct(avg2(fh.get("st_over_0_5"), fa.get("st_over_0_5"))),
        ),
        (
            "Secondo tempo",
            "2+ gol nel 2° tempo",
            pct(avg2(fh.get("st_over_1_5"), fa.get("st_over_1_5"))),
        ),
    ]
    return pd.DataFrame(rows, columns=["Categoria", "Mercato", "Stima"])


def _style_forecast(s: pd.DataFrame) -> pd.DataFrame:
    """Colora la colonna Stima: verde ≥65%, rosso ≤35%, neutro altrimenti."""
    styles = pd.DataFrame("", index=s.index, columns=s.columns)
    for idx in s.index:
        val = s.at[idx, "Stima"]
        if not isinstance(val, str) or not val.endswith("%"):
            continue
        try:
            v = float(val.replace("%", ""))
        except ValueError:
            continue
        if v >= 65:
            styles.at[idx, "Stima"] = "background-color:#2dc65333"
        elif v <= 35:
            styles.at[idx, "Stima"] = "background-color:#e6394633"
    return styles


# ── SofaScore: odds (15 mercati) + lineups (1 sessione Playwright per turno) ──

_fixture_dates_tuple: tuple[str, ...] = tuple(
    sorted({f["date"].strftime("%Y-%m-%d") for f in fixtures})
)
with st.spinner("Caricamento quote e formazioni SofaScore..."):
    _ss_all: dict = load_sofascore_all(league_key, _fixture_dates_tuple)



# ── Date ultime partite giocate (posticipi inclusi) ───────────────────────────

_recent_dates: dict[str, object] = load_recent_match_dates()

# ── Prossimi match (tutte le competizioni, 7 giorni) per segnale turnover ─────

_all_upcoming: list[dict] = load_all_upcoming_matches()

# ── Una card per partita ──────────────────────────────────────────────────────

for fixture in fixtures:
    home = fixture["home_team"]
    away = fixture["away_team"]
    date_str = fixture["date"].replace(tzinfo=UTC).astimezone(_TZ_ROME).strftime("%d/%m/%Y %H:%M")
    fixture_matchday = fixture.get("matchday")

    # Statistiche stagione corrente in casa / in trasferta
    fh = _season_form_stats(df_current, home, "home")
    fa = _season_form_stats(df_current, away, "away")

    # H2H
    h2h_df = head_to_head(df, home, away)
    h2h_sum = h2h_summary(h2h_df, home) if not h2h_df.empty else {}

    # Ritardi
    _hd_s = delays[delays["Squadra"].str.lower() == home.lower()] if not delays.empty else pd.DataFrame()  # noqa: E501
    _ad_s = delays[delays["Squadra"].str.lower() == away.lower()] if not delays.empty else pd.DataFrame()  # noqa: E501
    hd = _hd_s.iloc[0] if not _hd_s.empty else None
    ad = _ad_s.iloc[0] if not _ad_s.empty else None

    # Giorni di riposo — usa il max tra DB locale e API recente
    fixture_dt = pd.Timestamp(fixture["date"])

    def _last_played(team: str) -> pd.Timestamp | None:
        db_date = team_last_match_date(df, team, fixture_dt)
        api_date = _recent_dates.get(team)
        candidates = [d for d in (db_date, api_date) if d is not None]
        if not candidates:
            return None
        return max(pd.Timestamp(d) for d in candidates)

    _last_home = _last_played(home)
    _last_away = _last_played(away)
    days_home = int((fixture_dt - _last_home).days) if _last_home is not None else None
    days_away = int((fixture_dt - _last_away).days) if _last_away is not None else None

    # Prossima partita per ciascuna squadra (segnale turnover S44)
    def _next_match_info(team: str) -> dict | None:
        nm = team_next_match(_all_upcoming, team, fixture_dt)
        if nm is None:
            return None
        days_gap = int((pd.Timestamp(nm["date"]) - fixture_dt).days)
        return {"days": days_gap, "league_key": nm["league_key"], "league_name": nm["league_name"]}

    _next_home = _next_match_info(home)
    _next_away = _next_match_info(away)

    # SofaScore: lookup per questa partita
    _ss_key = (home.strip(), away.strip())
    _ss_data = _ss_all.get(_ss_key, {})
    _ss_odds = _ss_data.get("odds", {})
    _ss_lineups = _ss_data.get("lineups")

    # ── Segnali e pick (fuori dall'expander — eseguiti per ogni fixture) ─────────
    signals = _generate_signals(
        home, away, fh, fa, h2h_sum, hd, ad, days_home, days_away, {"home": [], "away": []},
        next_match_home=_next_home, next_match_away=_next_away,
    )
    pos_sigs = [s for s in signals if s["verdict"] == "✅" and s["mercato"]]
    counts = _filter_conflicts(Counter(s["mercato"] for s in pos_sigs)) if pos_sigs else {}
    # Quote da SofaScore (sostituisce Pinnacle)
    match_odds = {
        m: q
        for m in counts
        if (q := _ss_quota_for_mercato(m, _ss_odds)) is not None
    }
    _save_picks_fn(
        home=home,
        away=away,
        match_date=fixture_dt,
        matchday=fixture_matchday,
        league_key=league_key,
        counts=counts,
        match_odds=match_odds,
    )

    _fix_status  = fixture.get("status", "SCHEDULED")
    _fix_hs      = fixture.get("home_score")
    _fix_as      = fixture.get("away_score")
    _LIVE_ST     = {"IN_PLAY", "PAUSED", "EXTRA_TIME", "PENALTY_SHOOTOUT"}
    if _fix_status == "FINISHED" and _fix_hs is not None and _fix_as is not None:
        _status_badge = f"  ·  ✅ **{_fix_hs}–{_fix_as}**"
    elif _fix_status in _LIVE_ST:
        _status_badge = "  ·  ⚡ *In corso*"
    else:
        _status_badge = ""

    with st.expander(f"**{home}** vs **{away}**  ·  {date_str}{_status_badge}", expanded=False):
        # ── Caption riposo + prossima partita ─────────────────────────────────
        _caption_parts = []
        for _team, _days, _next in [
            (home, days_home, _next_home),
            (away, days_away, _next_away),
        ]:
            if _days is not None:
                _icon = "⚠️" if _days < 4 else "✅"
                _rest = f"{_icon} **{_team}**: {_days}gg riposo"
            else:
                _rest = f"**{_team}**"
            if _next is not None:
                _d = _next["days"]
                _comp = _next["league_name"]
                _next_icon = "🔴" if _d <= 3 else ("🟡" if _d <= 5 else "")
                _rest += f"  {_next_icon} _(prossima: {_comp} +{_d}gg)_"
            _caption_parts.append(_rest)
        if _caption_parts:
            st.caption("  ·  ".join(_caption_parts))

        # ── Pronostici supportati ──────────────────────────────────────────────
        st.markdown("**Pronostici supportati**")

        if not pos_sigs:
            st.caption("Nessun mercato con segnali ✅ sufficienti.")
        elif not counts:
            st.caption(
                "Segnali contrastanti su tutti i mercati — nessun pronostico affidabile."
            )
        else:
            top_pronostici = sorted(counts.items(), key=lambda x: x[1], reverse=True)[:top_n]
            _max_dots = 8
            for mercato, count in top_pronostici:
                dots = "●" * count + "○" * max(0, _max_dots - count)
                label = "segnale" if count == 1 else "segnali"
                quota = match_odds.get(mercato)
                if quota is not None:
                    _q = (
                        f"<span style='"
                        f"background:#f59e0b;color:#000;"
                        f"padding:1px 8px;border-radius:4px;"
                        f"font-weight:700;font-size:0.95em"
                        f"'>{quota:.2f}</span>"
                    )
                    st.markdown(
                        f"`{mercato}` &nbsp; {count} {label} &nbsp; {_q} &nbsp; {dots}",
                        unsafe_allow_html=True,
                    )
                else:
                    st.markdown(f"`{mercato}` &nbsp; {count} {label} &nbsp; {dots}")

        # ── Expanders ─────────────────────────────────────────────────────────

        with st.expander(
            f"📈 Previsione statistica  _({fh.get('n', 0)} casa · {fa.get('n', 0)} trasf.)_",
            expanded=False,
        ):
            if fh.get("n", 0) == 0 or fa.get("n", 0) == 0:
                st.info("Dati insufficienti per la previsione.")
            else:
                st.dataframe(
                    _build_forecast_df(fh, fa).style.apply(_style_forecast, axis=None),
                    width="stretch",
                    hide_index=True,
                )

        with st.expander("🎯 Segnali", expanded=False):
            if not signals:
                st.info("Dati insufficienti per generare segnali.")
            else:
                for sig in signals:
                    st.markdown(f"{sig['verdict']} {sig['text']}")

        with st.expander("⚔️ H2H — Scontri diretti", expanded=False):
            if h2h_df.empty:
                st.info("Nessun precedente disponibile.")
            else:
                st.dataframe(
                    h2h_df[["date", "sede_a", "risultato", "esito_a", "over_2_5", "btts"]]
                    .rename(
                        columns={
                            "date": "Data",
                            "sede_a": "Sede",
                            "risultato": "Ris.",
                            "esito_a": "Esito",
                            "over_2_5": "O2.5",
                            "btts": "GNG",
                        }
                    )
                    .head(8)
                    .style.map(
                        lambda v: {
                            "V": "background-color:#2dc65333",
                            "S": "background-color:#e6394633",
                        }.get(v, ""),
                        subset=["Esito"],
                    ),
                    width="stretch",
                    hide_index=True,
                )

        with st.expander("⏳ Ritardi (stagione corrente)", expanded=False):
            if delays.empty:
                st.info("Nessun dato ritardi.")
            else:
                st.caption(
                    "Partite consecutive senza che si verifichi l'evento. "
                    "Valori alti indicano squadre 'in debito' statistico."
                )
                rows_del = delays[delays["Squadra"].str.lower().isin([home.lower(), away.lower()])].reset_index(drop=True)  # noqa: E501
                st.dataframe(rows_del, width="stretch", hide_index=True)

        with st.expander("📊 Analisi avanzata (HT/FT · Risultati esatti)", expanded=False):
            _adv_teams = [(home, "home", f"🏠 {home}"), (away, "away", f"✈️ {away}")]
            col_htft, col_scores = st.columns(2)
            with col_htft:
                for _team, _venue, _label in _adv_teams:
                    st.markdown(f"**{_label} — HT/FT**")
                    _freqs = _ht_ft_freqs(df, _team, _venue)
                    if _freqs:
                        for _lbl, _cnt, _p in _freqs[:5]:
                            st.markdown(f"`{_lbl}` — {_cnt}× ({_p}%)")
                    else:
                        st.caption("Nessun dato.")
            with col_scores:
                for _team, _venue, _label in _adv_teams:
                    st.markdown(f"**{_label} — Risultati esatti**")
                    _scores = _top_scores(df, _team, _venue)
                    if _scores:
                        for _lbl, _cnt, _p in _scores:
                            st.markdown(f"`{_lbl}` — {_cnt}× ({_p}%)")
                    else:
                        st.caption("Nessun dato.")

        with st.expander("👥 Formazioni", expanded=False):
            # Preferisci SofaScore; fallback al DB Transfermarkt
            if _ss_lineups is not None:
                _confirmed = _ss_lineups.get("confirmed", False)
                _badge = "🟢" if _confirmed else "🟡"
                _source = "UFFICIALI" if _confirmed else "PROBABILI"
                st.caption(f"{_badge} **{_source}** (SofaScore)")
                _col_lh, _col_la = st.columns(2)
                with _col_lh:
                    _sd = _ss_lineups.get("home", {})
                    _render_lineup_shared(
                        _sd.get("formation"), _sd.get("players", []),
                        True, f"🏠 {home}", _badge,
                    )
                with _col_la:
                    _sd = _ss_lineups.get("away", {})
                    _render_lineup_shared(
                        _sd.get("formation"), _sd.get("players", []),
                        False, f"✈️ {away}", _badge,
                    )
            else:
                # Fallback: DB Transfermarkt (normalizza number → shirt_number)
                _lineups = load_lineups(home, away, fixture["date"].strftime("%Y-%m-%d"))
                if _lineups is None:
                    st.caption("Formazioni non ancora disponibili.")
                else:
                    _off_h = _lineups.get("home", {}).get("is_official", False)
                    _off_a = _lineups.get("away", {}).get("is_official", False)
                    _badge = "🟢" if (_off_h or _off_a) else "🟡"
                    _source = "UFFICIALI" if (_off_h or _off_a) else "PROBABILI"
                    st.caption(f"{_badge} **{_source}** (Transfermarkt)")
                    _col_lh, _col_la = st.columns(2)
                    def _norm_tm(pl: list[dict]) -> list[dict]:
                        return [{**p, "shirt_number": p.get("number")} for p in pl]

                    with _col_lh:
                        _sd = _lineups.get("home", {})
                        _render_lineup_shared(
                            _sd.get("formation"), _norm_tm(_sd.get("players", [])),
                            True, f"🏠 {home}", _badge,
                        )
                    with _col_la:
                        _sd = _lineups.get("away", {})
                        _render_lineup_shared(
                            _sd.get("formation"), _norm_tm(_sd.get("players", [])),
                            False, f"✈️ {away}", _badge,
                        )

        with st.expander("🔁 Forma recente (ultime 5 partite)", expanded=False):
            col_fh, col_fa = st.columns(2)
            for col_f, team_f, label_f in [
                (col_fh, home, f"🏠 {home}"),
                (col_fa, away, f"✈️ {away}"),
            ]:
                forma = team_form(df, team_f, last_n=5)
                with col_f:
                    st.markdown(f"**{label_f}**")
                    if forma.empty:
                        st.caption("Nessun dato disponibile.")
                    else:
                        st.dataframe(
                            forma[
                                [
                                    "date",
                                    "sede",
                                    "avversario",
                                    "risultato",
                                    "esito",
                                    "total_goals",
                                    "over_2_5",
                                    "btts",
                                ]
                            ]
                            .rename(
                                columns={
                                    "date": "Data",
                                    "sede": "Sede",
                                    "avversario": "Avversario",
                                    "risultato": "Ris.",
                                    "esito": "Esito",
                                    "total_goals": "Gol",
                                    "over_2_5": "O2.5",
                                    "btts": "GNG",
                                }
                            )
                            .style.map(
                                lambda v: {
                                    "V": "background-color:#2dc65333",
                                    "S": "background-color:#e6394633",
                                }.get(v, ""),
                                subset=["Esito"],
                            ),
                            width="stretch",
                            hide_index=True,
                        )

# ── Footer ────────────────────────────────────────────────────────────────────
st.caption(
    "💡 **Nota**: i segnali sono calcolati da frequenze storiche, "
    "non da modelli predittivi. Usali come riferimento, non come certezze."
)
