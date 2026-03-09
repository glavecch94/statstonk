"""
Live Monitoring
───────────────
Tutte le partite in corso in tutti i campionati configurati.
Aggiornamento automatico ogni 60 secondi.

Fonte dati: SofaScore (via Playwright Chromium)
  - Schedule giornaliero + score + statistiche live (xG, tiri, possesso, corner)
"""

from __future__ import annotations

import logging
from datetime import UTC
from zoneinfo import ZoneInfo

_TZ_ROME = ZoneInfo("Europe/Rome")
from itertools import groupby

import pandas as pd
import streamlit as st

from analytics.prematch import (
    get_matches_df,
    h2h_summary,
    head_to_head,
)
from config import (
    HISTORICAL_SEASONS,
    TELEGRAM_BOT_TOKEN,
    TELEGRAM_CHAT_ID,
)
from dashboard.components import render_lineup as _render_lineup
from db import SessionLocal

logger = logging.getLogger(__name__)

# ── Design constants ───────────────────────────────────────────────────────────
_C_HOME = "#3b82f6"       # blue — squadra casa
_C_AWAY = "#ef4444"       # red  — squadra ospite
_C_HIGHLIGHT = "#f59e0b"  # amber — xG alert
_C_XG_BG = "rgba(245,158,11,0.18)"

st.set_page_config(page_title="Live | Statstonk", layout="wide")
st.title("⚡ Live Monitoring")

# ── Sidebar ────────────────────────────────────────────────────────────────────

with st.sidebar:
    st.header("Impostazioni")

    st.markdown("**Alert 0-0**")
    alert_threshold = st.slider(
        "Soglia minuti",
        min_value=60,
        max_value=85,
        value=70,
        step=5,
        help="Mostra badge ⚠️ se il punteggio è ancora 0-0 dopo questo minuto",
    )

    st.markdown("---")
    st.info("Dati live da **SofaScore** (xG, tiri, possesso, corner inclusi)")

    st.markdown("---")
    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        st.success("Telegram attivo")
    else:
        st.warning("Telegram non configurato")
    st.caption("Gli alert Telegram sono gestiti dallo scheduler, non dalla dashboard.")

# ── Cache functions ────────────────────────────────────────────────────────────


@st.cache_data(ttl=600)
def load_historical(league: str) -> pd.DataFrame:
    session = SessionLocal()
    try:
        return get_matches_df(session, league, HISTORICAL_SEASONS)
    finally:
        session.close()


_LIVE_STATUSES = {"IN_PLAY", "PAUSED", "EXTRA_TIME", "PENALTY_SHOOTOUT"}


@st.cache_data(ttl=90)
def load_all_today_matches() -> list[dict]:
    """
    Tutte le partite di oggi nei campionati configurati — singola sessione SS.
    Cache 90s. Usato solo per lo schedule quando non ci sono partite live.
    """
    from scrapers.sofascore import SofaScoreScraper

    with SofaScoreScraper() as ss:
        return ss.get_all_today_matches()


@st.cache_data(ttl=90)
def load_all_live_matches() -> list[dict]:
    """
    Tutte le partite live con xG tracking (qualsiasi campionato mondiale). Cache 90s.
    """
    from scrapers.sofascore import SofaScoreScraper

    with SofaScoreScraper() as ss:
        return ss.get_all_live_matches()


@st.cache_data(ttl=300)
def load_sofascore_stats(event_id: int) -> dict:
    """Statistiche avanzate per partita live. Cache 5 min."""
    from scrapers.sofascore import SofaScoreScraper

    with SofaScoreScraper() as ss:
        return ss.get_statistics(event_id)


@st.cache_data(ttl=120)
def load_sofascore_lineups(event_id: int) -> dict | None:
    """Formazioni live da SofaScore. Cache 2 min."""
    from scrapers.sofascore import SofaScoreScraper

    with SofaScoreScraper() as ss:
        return ss.get_lineups(event_id)


# ── Stats format conversion ────────────────────────────────────────────────────


def _ss_stats_to_live_format(ss_stats: dict) -> dict:
    """
    Converte le statistiche SofaScore nel formato atteso da _render_live_stats.

    Input:  {"ALL": {...}, "1ST": {...}, "2ND": {...}}
    Output: {"home": {...}, "away": {...},
             "first_half": {"home": {...}, "away": {...}},   # se disponibile
             "second_half": {"home": {...}, "away": {...}}}  # se disponibile
    """

    def _extract(period_key: str) -> dict:
        period = ss_stats.get(period_key, {})

        def _num(key: str, side: str) -> float | None:
            v = period.get(key, {}).get(side)
            if v is None:
                return None
            try:
                return float(str(v).rstrip("%"))
            except (ValueError, TypeError):
                return None

        return {
            "home": {
                "shots_total": _num("Total shots", "home"),
                "shots_on_target": _num("Shots on target", "home"),
                "corners": _num("Corner kicks", "home"),
                "fouls": _num("Fouls", "home"),
                "saves": _num("Goalkeeper saves", "home"),
                "yellow_cards": _num("Yellow cards", "home"),
                "red_cards": _num("Red cards", "home"),
                "xg": _num("Expected goals", "home"),
                "offsides": _num("Offsides", "home"),
                "possession": _num("Ball possession", "home"),
            },
            "away": {
                "shots_total": _num("Total shots", "away"),
                "shots_on_target": _num("Shots on target", "away"),
                "corners": _num("Corner kicks", "away"),
                "fouls": _num("Fouls", "away"),
                "saves": _num("Goalkeeper saves", "away"),
                "yellow_cards": _num("Yellow cards", "away"),
                "red_cards": _num("Red cards", "away"),
                "xg": _num("Expected goals", "away"),
                "offsides": _num("Offsides", "away"),
                "possession": _num("Ball possession", "away"),
            },
        }

    result = _extract("ALL")
    fh = _extract("1ST")
    sh = _extract("2ND")
    if any(v is not None for v in fh["home"].values()):
        result["first_half"] = fh
    if any(v is not None for v in sh["home"].values()):
        result["second_half"] = sh
    return result


# ── Helper: barra comparativa HTML ────────────────────────────────────────────


def _stat_bar_html(
    name: str,
    hv: float | None,
    av: float | None,
    dec: int = 0,
    hl_home: bool = False,
    hl_away: bool = False,
) -> str:
    """Riga HTML con barra proporzionale bicolore (home=blu, away=rosso)."""
    h_str = "—" if hv is None else f"{hv:.{dec}f}"
    a_str = "—" if av is None else f"{av:.{dec}f}"
    fhv = float(hv) if hv is not None else 0.0
    fav = float(av) if av is not None else 0.0
    total = fhv + fav
    h_pct = fhv / total * 100 if total > 0 else 50.0
    h_vs = (
        f"color:{_C_HIGHLIGHT};background:{_C_XG_BG};"
        "border-radius:3px;padding:0 4px;font-weight:900"
        if hl_home
        else f"color:{_C_HOME};font-weight:700"
    )
    a_vs = (
        f"color:{_C_HIGHLIGHT};background:{_C_XG_BG};"
        "border-radius:3px;padding:0 4px;font-weight:900"
        if hl_away
        else f"color:{_C_AWAY};font-weight:700"
    )
    bar_bg = (  # noqa: E501
        f"background:linear-gradient(to right,{_C_HOME} {h_pct:.0f}%,{_C_AWAY} {h_pct:.0f}%)"
    )
    return (
        '<div style="display:flex;align-items:center;gap:8px;margin:4px 0;font-size:.9em">'
        f'<div style="width:46px;text-align:right;{h_vs}">{h_str}</div>'
        f'<div style="flex:1;height:5px;border-radius:3px;{bar_bg}"></div>'
        f'<div style="width:46px;{a_vs}">{a_str}</div>'
        f'<div style="min-width:88px;font-size:.8em;color:#94a3b8">{name}</div>'
        "</div>"
    )


# ── Helper: scheda punteggio HTML ──────────────────────────────────────────────


def _score_card_html(
    home: str,
    away: str,
    hs: int,
    as_: int,
    display_status: str,
    ht_h: int | None,
    ht_a: int | None,
    alert: bool,
) -> str:
    """Header partita: squadre ai lati, score al centro, sottoriga con minuto/HT."""
    h_col = "#10b981" if hs > as_ else ("white" if hs == as_ else "#94a3b8")
    a_col = "#10b981" if as_ > hs else ("white" if hs == as_ else "#94a3b8")
    sub_parts = list(filter(None, [
        f"⏱ {display_status}" if display_status else "",
        f"PT: {ht_h}–{ht_a}" if ht_h is not None and ht_a is not None else "",
        "⚠️ 0-0" if alert else "",
        f"⚽ {hs + as_} gol" if hs + as_ > 0 else "",
    ]))
    sub_html = (
        '<div style="text-align:center;color:#94a3b8;font-size:.82em;margin-top:6px">'
        + "  ·  ".join(sub_parts)
        + "</div>"
        if sub_parts
        else ""
    )
    return (  # noqa: E501
        '<div style="background:linear-gradient(135deg,#0f172a 0%,#1e293b 100%);'
        'border-radius:10px;padding:14px 20px;margin-bottom:12px;border:1px solid #334155">'
        '<div style="display:flex;align-items:center;gap:16px">'
        f'<div style="flex:1;text-align:right;font-size:1.05em;font-weight:700;color:{_C_HOME};'
        f'overflow:hidden;text-overflow:ellipsis;white-space:nowrap">{home}</div>'
        '<div style="text-align:center;white-space:nowrap;padding:0 8px">'
        f'<span style="font-size:2em;font-weight:900;color:{h_col}">{hs}</span>'
        '<span style="font-size:1.4em;color:#475569;margin:0 6px">–</span>'
        f'<span style="font-size:2em;font-weight:900;color:{a_col}">{as_}</span>'
        "</div>"
        f'<div style="flex:1;font-size:1.05em;font-weight:700;color:{_C_AWAY};'
        f'overflow:hidden;text-overflow:ellipsis;white-space:nowrap">{away}</div>'
        "</div>"
        f"{sub_html}"
        "</div>"
    )


# ── Helper: widget momentum SofaScore ──────────────────────────────────────────


def _render_momentum_widget(event_id: int) -> None:
    """Embeds il widget Attack Momentum ufficiale di SofaScore."""
    import streamlit.components.v1 as components

    components.html(
        f'<iframe width="100%" height="286" '
        f'src="https://widgets.sofascore.com/it/embed/attackMomentum?id={event_id}&widgetTheme=light" '  # noqa: E501
        f'frameborder="0" scrolling="no"></iframe>',
        height=286,
    )


# ── Helper: statistiche avanzate ───────────────────────────────────────────────


def _render_live_stats(
    stats: dict,
    home: str,
    away: str,
    home_goals: int = 0,
    away_goals: int = 0,
) -> None:
    home_s = stats.get("home", {})
    away_s = stats.get("away", {})

    if not home_s and not away_s:
        st.caption("Statistiche non ancora disponibili")
        return

    def _v(d, key):
        v = d.get(key)
        if v is None:
            return None
        try:
            return float(v)
        except (ValueError, TypeError):
            return None

    sh, sa = _v(home_s, "shots_total"), _v(away_s, "shots_total")
    soh, soa = _v(home_s, "shots_on_target"), _v(away_s, "shots_on_target")
    ch, ca = _v(home_s, "corners"), _v(away_s, "corners")
    fh, fa = _v(home_s, "fouls"), _v(away_s, "fouls")
    svh, sva = _v(home_s, "saves"), _v(away_s, "saves")
    yh, ya = _v(home_s, "yellow_cards"), _v(away_s, "yellow_cards")
    rh, ra = _v(home_s, "red_cards"), _v(away_s, "red_cards")
    xgh, xga = _v(home_s, "xg"), _v(away_s, "xg")
    offh, offa = _v(home_s, "offsides"), _v(away_s, "offsides")
    ph, pa = _v(home_s, "possession"), _v(away_s, "possession")

    # xG superiore di >1 unità ai gol → highlight ambra
    hl_xg_h = xgh is not None and xgh > home_goals + 1
    hl_xg_a = xga is not None and xga > away_goals + 1

    # Possesso
    poss_html = ""
    if ph is not None and pa is not None:
        ph_i, pa_i = int(round(ph)), int(round(pa))
        total_p = ph_i + pa_i or 1
        hp = ph_i / total_p * 100
        poss_html = (  # noqa: E501
            '<div style="margin:0 0 8px 0">'
            '<div style="display:flex;align-items:center;gap:8px;font-size:.9em">'
            f'<div style="width:46px;text-align:right;font-weight:700;color:{_C_HOME}">{ph_i}%</div>'  # noqa: E501
            f'<div style="flex:1;height:8px;border-radius:4px;'
            f'background:linear-gradient(to right,{_C_HOME} {hp:.0f}%,{_C_AWAY} {hp:.0f}%)"></div>'
            f'<div style="width:46px;font-weight:700;color:{_C_AWAY}">{pa_i}%</div>'
            '<div style="min-width:88px;font-size:.8em;color:#94a3b8">Possesso</div>'
            "</div></div>"
        )

    bars = "".join([
        _stat_bar_html("xGoals", xgh, xga, dec=2, hl_home=hl_xg_h, hl_away=hl_xg_a),
        _stat_bar_html("Tiri totali", sh, sa),
        _stat_bar_html("In porta", soh, soa),
        _stat_bar_html("Corner", ch, ca),
        _stat_bar_html("Parate", svh, sva),
        _stat_bar_html("Falli", fh, fa),
        _stat_bar_html("Fuorigioco", offh, offa),
        _stat_bar_html("Gialli 🟨", yh, ya),
        _stat_bar_html("Rossi 🟥", rh, ra),
    ])

    header_html = (  # noqa: E501
        '<div style="display:flex;justify-content:space-between;font-size:.78em;margin-bottom:6px">'
        f'<span style="color:{_C_HOME};font-weight:600">{home[:16]}</span>'
        '<span style="color:#64748b">📊 Statistiche</span>'
        f'<span style="color:{_C_AWAY};font-weight:600">{away[:16]}</span>'
        "</div>"
    )
    st.markdown(
        '<div style="background:#0f172a;border-radius:8px;padding:10px 14px;border:1px solid #1e293b">'  # noqa: E501
        + header_html + poss_html + bars + "</div>",
        unsafe_allow_html=True,
    )

    if hl_xg_h:
        st.caption(f"🟡 **{home}** xG {xgh:.2f} vs {home_goals} gol — potenziale non sfruttato")
    if hl_xg_a:
        st.caption(f"🟡 **{away}** xG {xga:.2f} vs {away_goals} gol — potenziale non sfruttato")


# ── Helper: distribuzione FT condizionata al punteggio HT ─────────────────────


def _render_ht_ft_distribution(
    df_hist: pd.DataFrame,
    home: str,
    away: str,
    ht_h: int,
    ht_a: int,
) -> None:
    h2h_mask = ((df_hist["home"] == home) & (df_hist["away"] == away)) | (
        (df_hist["home"] == away) & (df_hist["away"] == home)
    )
    h2h_all = df_hist[h2h_mask].copy()
    if h2h_all.empty:
        return

    def ht_matches_row(row: pd.Series) -> bool:
        if row["home"] == home:
            return row["hthg"] == ht_h and row["htag"] == ht_a
        return row["hthg"] == ht_a and row["htag"] == ht_h

    matching = h2h_all[h2h_all.apply(ht_matches_row, axis=1)]
    if len(matching) < 2:
        return

    with st.expander(
        f"📊 FT con HT {ht_h}–{ht_a} in H2H ({len(matching)} precedenti)",
        expanded=False,
    ):
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Campione", len(matching))
        c2.metric("Over 2.5", f"{matching['over_2_5'].mean() * 100:.0f}%")
        c3.metric("GNG", f"{matching['btts'].mean() * 100:.0f}%")
        c4.metric("Media gol", f"{matching['total_goals'].mean():.1f}")

        outcomes = []
        for _, row in matching.iterrows():
            ft_h = int(row["hg"]) if row["home"] == home else int(row["ag"])
            ft_a = int(row["ag"]) if row["home"] == home else int(row["hg"])
            outcomes.append(f"{ft_h}–{ft_a}")

        ft_counts = pd.Series(outcomes).value_counts().reset_index()
        ft_counts.columns = ["FT", "Freq."]
        st.dataframe(ft_counts, width="stretch", hide_index=True)


# ── Helper: singola card partita ───────────────────────────────────────────────


def _render_match_card(
    m: dict,
    df_hist: pd.DataFrame,
    alert_threshold: int,
    stats: dict | None = None,
    lineups: dict | None = None,
) -> None:
    home = m["home_team"]
    away = m["away_team"]
    hs = m["home_score"] if m["home_score"] is not None else 0
    as_ = m["away_score"] if m["away_score"] is not None else 0
    minute = m["minute"] or 0
    ht_h = m["home_ht_score"]
    ht_a = m["away_ht_score"]

    # Status label per lo score card
    _slbl = m.get("status_label", "")
    if _slbl and not any(c.isdigit() for c in _slbl):
        display_status = _slbl  # testo puro (es. "Halftime", "PT")
    elif minute > 0:
        display_status = f"{minute}'"
    else:
        display_status = ""

    alert = hs == 0 and as_ == 0 and minute >= alert_threshold

    # Score header (full-width)
    st.markdown(
        _score_card_html(home, away, hs, as_, display_status, ht_h, ht_a, alert),
        unsafe_allow_html=True,
    )

    # Layout: stats a sx | marcatori + H2H a dx
    col_stats, col_right = st.columns([1.4, 1])

    with col_right:
        if m["goals"]:
            st.markdown("**⚽ Marcatori**")
            for g in sorted(m["goals"], key=lambda x: x["minute"] or 0):
                side_icon = "🏠" if g["team"] == home else "✈️"
                sfx = {"OWN": " *(aut.)*", "PENALTY": " *(rig.)*"}.get(g["type"], "")
                assist = f" — _{g['assist']}_" if g.get("assist") else ""
                st.markdown(f"{side_icon} **{g['minute']}'** {g['scorer']}{sfx}{assist}")
        elif hs + as_ > 0:
            st.caption("Marcatori non disponibili")

        if not df_hist.empty:
            h2h_df = head_to_head(df_hist, home, away)
            h2h_sum = h2h_summary(h2h_df, home) if not h2h_df.empty else {}
            if h2h_sum:
                st.markdown("**⚔️ H2H**")
                c1, c2, c3 = st.columns(3)
                c1.metric(f"🏠 {home[:5]}", f"{h2h_sum['win_pct']:.0f}%")
                c2.metric("X", f"{h2h_sum['draw_pct']:.0f}%")
                c3.metric(f"✈️ {away[:5]}", f"{h2h_sum['loss_pct']:.0f}%")
                c4, c5 = st.columns(2)
                c4.metric("O2.5", f"{h2h_sum['over_2_5_pct']:.0f}%")
                c5.metric("GNG", f"{h2h_sum['goal_pct']:.0f}%")
                st.caption(f"Su {h2h_sum['campione']} scontri")
            else:
                st.caption("Nessun precedente disponibile")

    with col_stats:
        if stats is not None:
            _render_live_stats(stats, home, away, home_goals=hs, away_goals=as_)
        else:
            st.caption("Statistiche avanzate non disponibili.")
        event_id = m.get("event_id")
        if event_id:
            _render_momentum_widget(event_id)

    if minute > 45 and not df_hist.empty and ht_h is not None and ht_a is not None:
        _render_ht_ft_distribution(df_hist, home, away, ht_h, ht_a)

    # Formazioni
    with st.expander("👥 Formazioni", expanded=False):
        if lineups is None:
            st.caption("Formazioni non ancora disponibili.")
        else:
            _confirmed = lineups.get("confirmed", False)
            _badge = "🟢" if _confirmed else "🟡"
            _source = "UFFICIALI" if _confirmed else "PROBABILI"
            st.caption(f"{_badge} **{_source}** (SofaScore)")

            # Età media XI titolari
            _age_h = lineups.get("home", {}).get("avg_age")
            _age_a = lineups.get("away", {}).get("avg_age")
            if _age_h is not None or _age_a is not None:
                _gap = abs((_age_h or 0) - (_age_a or 0)) if _age_h and _age_a else 0
                _age_col_h, _age_col_a = st.columns(2)
                with _age_col_h:
                    if _age_h is not None:
                        _warn = " 👶" if _age_a is not None and _gap >= 4 and _age_h < _age_a else ""  # noqa: E501
                        st.caption(f"Età media XI: **{_age_h:.1f} anni**{_warn}")
                with _age_col_a:
                    if _age_a is not None:
                        _warn = " 👶" if _age_h is not None and _gap >= 4 and _age_a < _age_h else ""  # noqa: E501
                        st.caption(f"Età media XI: **{_age_a:.1f} anni**{_warn}")

            _col_lh, _col_la = st.columns(2)
            with _col_lh:
                _sd = lineups.get("home", {})
                _render_lineup(
                    _sd.get("formation"), _sd.get("players", []),
                    True, f"🏠 {home}", _badge,
                )
            with _col_la:
                _sd = lineups.get("away", {})
                _render_lineup(
                    _sd.get("formation"), _sd.get("players", []),
                    False, f"✈️ {away}", _badge,
                )


# ── Helper: lista partite live ─────────────────────────────────────────────────


def _render_live_matches(
    live_matches: list[dict],
    df_hist_by_league: dict[str, pd.DataFrame],
    alert_threshold: int,
    stats_map: dict[tuple[str, str], dict],
    lineups_map: dict[tuple[str, str], dict | None] | None = None,
) -> None:
    n = len(live_matches)
    st.subheader(f"In corso — {n} {'partita' if n == 1 else 'partite'}")

    _SS_STATUS_LABELS = {
        "IN_PLAY": lambda m: f"{m['minute']}'" if m.get("minute") is not None else "In corso",
        "PAUSED": lambda m: f"PT ({m['minute']}')" if m.get("minute") is not None else "PT",
    }

    sorted_matches = sorted(live_matches, key=lambda m: m.get("league_name", ""))
    for league_name, group in groupby(sorted_matches, key=lambda m: m.get("league_name", "")):
        if league_name:
            st.markdown(f"#### 🏆 {league_name}")

        for m in group:
            home = m["home_team"]
            away = m["away_team"]
            hs = m["home_score"] if m["home_score"] is not None else 0
            as_ = m["away_score"] if m["away_score"] is not None else 0
            minute = m["minute"] or 0
            status = m["status"]

            status_lbl = m.get("status_label") or _SS_STATUS_LABELS.get(status, lambda m: status)(m)

            badges = ""
            if hs == 0 and as_ == 0 and minute >= alert_threshold:
                badges += " ⚠️"
            if hs + as_ > 0:
                badges += " ⚽"

            title = f"**{home}** {hs} – {as_} **{away}**  ·  {status_lbl}{badges}"
            df_hist = df_hist_by_league.get(m.get("league_key", ""), pd.DataFrame())
            stats = stats_map.get((home, away))
            lineups = (lineups_map or {}).get((home, away))

            with st.expander(title, expanded=True):
                _render_match_card(m, df_hist, alert_threshold, stats=stats, lineups=lineups)


# ── Helper: schedule di oggi ───────────────────────────────────────────────────


def _render_today_schedule(today_matches: list[dict]) -> None:
    st.info("Nessuna partita in corso al momento.")

    if not today_matches:
        st.caption("Nessuna partita in programma oggi in nessun campionato.")
        return

    upcoming = [m for m in today_matches if m["status"] in ("SCHEDULED", "TIMED")]
    finished = [m for m in today_matches if m["status"] == "FINISHED"]

    def show_group(matches: list[dict], header: str) -> None:
        if not matches:
            return
        st.markdown(f"**{header}**")
        sorted_m = sorted(matches, key=lambda x: (x.get("league_name", ""), x["local_date"]))
        for league_name, group in groupby(sorted_m, key=lambda m: m.get("league_name", "")):
            st.markdown(f"*🏆 {league_name}*")
            for m in group:
                kick = m["local_date"].strftime("%H:%M") if m.get("local_date") else "??"
                if m["status"] in ("SCHEDULED", "TIMED"):
                    st.markdown(
                        f"&emsp;🕐 **{kick}** &nbsp; {m['home_team']} – {m['away_team']}"
                    )
                elif m["status"] == "FINISHED":
                    hs = m["home_score"] or 0
                    as_ = m["away_score"] or 0
                    st.markdown(
                        f"&emsp;✅ **{kick}** &nbsp; "
                        f"{m['home_team']} **{hs}–{as_}** {m['away_team']}"
                    )

    show_group(upcoming, "📋 Programmate oggi")
    if upcoming and finished:
        st.markdown("---")
    show_group(finished, "✅ Risultati di oggi")


# ── Fragment con auto-refresh ogni 60s ────────────────────────────────────────


@st.fragment(run_every=60)
def live_section(threshold: int) -> None:
    from datetime import datetime

    now = datetime.now(_TZ_ROME)

    st.caption(
        f"Aggiornato alle **{now.strftime('%H:%M:%S')}** · "
        "refresh automatico ogni 60s · SofaScore"
    )

    # Step 1: partite live con xG (qualsiasi campionato worldwide)
    try:
        live_matches = load_all_live_matches()
    except Exception as e:
        st.error(f"Errore nel recupero dati live (SofaScore): {e}")
        return

    if not live_matches:
        # Nessuna partita live: mostra schedule di oggi per i campionati configurati
        try:
            all_today = load_all_today_matches()
        except Exception:
            all_today = []
        _render_today_schedule(all_today)
        return

    # Step 2: statistiche live + formazioni per ogni partita in corso
    stats_map: dict[tuple[str, str], dict] = {}
    lineups_map: dict[tuple[str, str], dict | None] = {}
    for m in live_matches:
        eid = m.get("event_id")
        if not eid:
            continue
        key = (m["home_team"], m["away_team"])
        try:
            raw = load_sofascore_stats(eid)
            if raw:
                stats_map[key] = _ss_stats_to_live_format(raw)
        except Exception as e_s:
            logger.warning(f"Stats non disponibili evento {eid}: {e_s}")
        try:
            lineups_map[key] = load_sofascore_lineups(eid)
        except Exception as e_l:
            logger.warning(f"Lineups non disponibili evento {eid}: {e_l}")
            lineups_map[key] = None

    # Step 3: dati storici solo per i campionati noti con match live
    # (league_key è None per competizioni non in SS_TOURNAMENT_IDS)
    active_leagues = {m["league_key"] for m in live_matches if m.get("league_key")}
    df_hist_by_league: dict[str, pd.DataFrame] = {
        lk: load_historical(lk) for lk in active_leagues
    }

    _render_live_matches(live_matches, df_hist_by_league, threshold, stats_map, lineups_map)


# ── Avvio ─────────────────────────────────────────────────────────────────────

live_section(alert_threshold)

st.caption("Score + statistiche avanzate: **SofaScore** (via Playwright)")
