"""
Storico Pronostici
──────────────────
Pick completati con esiti, statistiche globali (hit rate, ROI)
e dettaglio per giornata/campionato.
"""

from __future__ import annotations

import datetime as _dt

import pandas as pd
import streamlit as st

from config import HISTORICAL_SEASONS, LEAGUES
from db import SessionLocal

st.set_page_config(page_title="Pronostici | Statstonk", layout="wide")
st.title("📊 Pronostici")
st.caption("Pick automatici con esito già valutato dallo scheduler.")

# ── Sidebar ───────────────────────────────────────────────────────────────────

with st.sidebar:
    st.header("Filtri")

    all_league_keys = list(LEAGUES.keys())
    selected_leagues = st.multiselect(
        "Campionato",
        all_league_keys,
        default=all_league_keys,
        format_func=lambda k: LEAGUES[k]["name"],
    )

    season = st.selectbox("Stagione", HISTORICAL_SEASONS, index=0)

    esito_filter = st.radio(
        "Esito",
        ["Tutti", "Solo vincenti", "Solo perdenti", "In attesa"],
        index=0,
    )

    if st.button("🔄 Aggiorna"):
        st.rerun()

# ── Caricamento dati ──────────────────────────────────────────────────────────


@st.cache_data(ttl=60)
def load_picks(leagues: tuple, _season: str) -> pd.DataFrame:
    from models.picks import Pick

    session = SessionLocal()
    try:
        q = session.query(Pick).filter(Pick.season == _season)
        if leagues:
            q = q.filter(Pick.league.in_(list(leagues)))
        picks = q.order_by(Pick.match_date.desc()).all()
    finally:
        session.close()

    if not picks:
        return pd.DataFrame()

    rows = []
    for p in picks:
        rows.append({
            "Campionato":   LEAGUES.get(p.league, {}).get("name", p.league),
            "Giornata":     p.matchday if p.matchday else "—",
            "data_dt":      p.match_date,
            "data_giorno":  p.match_date.date() if p.match_date else None,
            "Partita":      f"{p.home_team} – {p.away_team}",
            "Mercato":      p.mercato,
            "Segnali":      p.segnali,
            "Quota":        p.quota,
            "_esito_raw":   p.esito,
        })

    return pd.DataFrame(rows)


df_full = load_picks(
    tuple(sorted(selected_leagues or all_league_keys)),
    season,
)

if df_full.empty:
    st.info("Nessun pick trovato con i filtri selezionati.")
    st.stop()

_MIN_STAKE_QUOTA = 1.50  # sotto questa quota non si punta (escluso dalle statistiche)


# ── Selettore segnali (sidebar, dopo il caricamento dati) ────────────────────

def _sig_label(sig: int, df: pd.DataFrame) -> str:
    """Costruisce la label '≥N  —  X pick · YY% HR' per il radio."""
    sub   = df[df["Segnali"] >= sig]
    sub_c = sub[sub["_esito_raw"].notna() & (sub["Quota"] >= _MIN_STAKE_QUOTA)]
    n_pick = len(sub)
    if len(sub_c) > 0:
        n_won = (sub_c["_esito_raw"] == True).sum()  # noqa: E712
        hit_str = f"{n_won / len(sub_c) * 100:.0f}% HR"
    else:
        hit_str = "nessun esito"
    return f"≥{sig}  —  {n_pick} pick · {hit_str}"


_max_sig = int(df_full["Segnali"].max()) if not df_full.empty else 3
_sig_options = list(range(3, max(_max_sig, 5) + 1))
_sig_labels  = {s: _sig_label(s, df_full) for s in _sig_options}

with st.sidebar:
    st.markdown("---")
    _sig_choice = st.radio(
        "Segnali minimi",
        options=_sig_options,
        format_func=lambda s: _sig_labels[s],
        index=0,
    )
    st.markdown("---")
    st.subheader("Simulazione bankroll")
    bankroll_init = st.number_input(
        "Bankroll iniziale (€)",
        min_value=10,
        max_value=100_000,
        value=10_000,
        step=100,
    )
    st.caption("Stake per numero di segnali (% del bankroll)")
    _sc1, _sc2 = st.columns(2)
    with _sc1:
        stake_3 = st.number_input("3 segnali", min_value=0.1, max_value=20.0, value=1.0, step=0.5, format="%.1f", key="st3")  # noqa: E501
        stake_5 = st.number_input("5+ segnali", min_value=0.1, max_value=20.0, value=3.0, step=0.5, format="%.1f", key="st5")  # noqa: E501
    with _sc2:
        stake_4 = st.number_input("4 segnali", min_value=0.1, max_value=20.0, value=2.0, step=0.5, format="%.1f", key="st4")  # noqa: E501

min_segnali = _sig_choice
stake_map: dict[int, float] = {3: stake_3, 4: stake_4, 5: stake_5}


def _stake_pct(segnali: int) -> float:
    """Restituisce la percentuale di stake per il numero di segnali dato."""
    thresholds = sorted((s for s in stake_map if s <= segnali), reverse=True)
    return stake_map[thresholds[0]] if thresholds else stake_map[min(stake_map)]


def _stake_eur(segnali: int, bankroll: float) -> float:
    """Restituisce lo stake in € = bankroll × %."""
    return bankroll * _stake_pct(segnali) / 100


# ── Applica filtri ────────────────────────────────────────────────────────────

df = df_full[df_full["Segnali"] >= min_segnali].copy()

if df.empty:
    st.info(f"Nessun pick con almeno {min_segnali} segnali.")
    st.stop()

if esito_filter == "Solo vincenti":
    df = df[df["_esito_raw"] == True]   # noqa: E712
elif esito_filter == "Solo perdenti":
    df = df[df["_esito_raw"] == False]  # noqa: E712
elif esito_filter == "In attesa":
    df = df[df["_esito_raw"].isna()]

if df.empty:
    st.info("Nessun pick con l'esito selezionato.")
    st.stop()

# ── Helper metriche ───────────────────────────────────────────────────────────


def _metriche(sub: pd.DataFrame) -> tuple[int, int, int, int, float, float]:
    """Ritorna (totale, vinti, persi, in_attesa, hit_rate%, roi%).
    HR e ROI escludono pick con quota < _MIN_STAKE_QUOTA (solo monitoraggio)."""
    total   = len(sub)
    pending = sub["_esito_raw"].isna().sum()

    # Solo pick puntabili per le statistiche di performance
    puntabili = sub[sub["Quota"].notna() & (sub["Quota"] >= _MIN_STAKE_QUOTA)]
    won  = (puntabili["_esito_raw"] == True).sum()   # noqa: E712
    lost = (puntabili["_esito_raw"] == False).sum()  # noqa: E712
    completed = won + lost
    hit_rate = (won / completed * 100) if completed > 0 else 0.0

    if completed > 0:
        profit = sum(
            row["Quota"] - 1 if row["_esito_raw"] else -1
            for _, row in puntabili[puntabili["_esito_raw"].notna()].iterrows()
        )
        roi = profit / completed * 100
    else:
        roi = 0.0

    return total, int(won), int(lost), int(pending), hit_rate, roi


def _bankroll_sim(sub: pd.DataFrame, bankroll: float) -> tuple[pd.DataFrame, float]:
    """
    Simula l'andamento del bankroll con stake variabile (% del bankroll iniziale).
    Ritorna (DataFrame con curva, totale_investito).
    """
    completed = sub[
        sub["_esito_raw"].notna() & sub["Quota"].notna() & (sub["Quota"] >= _MIN_STAKE_QUOTA)
    ].copy()
    if completed.empty:
        return pd.DataFrame(), 0.0
    completed = completed.sort_values("data_dt")
    bk = bankroll
    total_staked = 0.0
    rows = []
    for _, row in completed.iterrows():
        stake = _stake_eur(int(row["Segnali"]), bankroll)
        total_staked += stake
        if row["_esito_raw"]:
            bk += stake * (row["Quota"] - 1)
        else:
            bk -= stake
        rows.append({"Data": row["data_dt"], "Bankroll (€)": round(bk, 2)})
    return pd.DataFrame(rows).set_index("Data"), total_staked


def _fmt_esito(val) -> str:
    if val is True:
        return "✅"
    elif val is False:
        return "❌"
    return "⏳"


def _style_esito(val: str) -> str:
    if val == "✅":
        return "background-color:#2dc65333"
    elif val == "❌":
        return "background-color:#e6394633"
    return ""


def _style_netto(val: str) -> str:
    if val and val.startswith("+"):
        return "background-color:#2dc65333"
    elif val and val.startswith("−"):
        return "background-color:#e6394633"
    return ""


def _render_table(sub: pd.DataFrame, bankroll: float) -> None:
    disp = sub[["Campionato", "Partita", "Mercato", "Segnali", "Quota", "_esito_raw"]].copy()

    # Calcola Puntata e Netto (€) prima di convertire Quota in stringa
    def _puntata_str(row) -> str:
        q = row["Quota"]
        if pd.isna(q) or q < _MIN_STAKE_QUOTA:
            return "quota da multipla"
        return f"€ {_stake_eur(int(row['Segnali']), bankroll):.0f}"

    disp["Puntata (€)"] = disp.apply(_puntata_str, axis=1)

    def _netto(row) -> str:
        if pd.isna(row["_esito_raw"]):
            return "—"
        q = row["Quota"] if pd.notna(row["Quota"]) else 1.0
        if q < _MIN_STAKE_QUOTA:
            return "—"
        stake = _stake_eur(int(row["Segnali"]), bankroll)
        v = stake * (q - 1) if row["_esito_raw"] else -stake
        return f"+€{v:.0f}" if v >= 0 else f"−€{abs(v):.0f}"

    disp["Netto (€)"] = disp.apply(_netto, axis=1)
    disp["Quota"] = disp["Quota"].apply(lambda q: f"{q:.2f}" if pd.notna(q) else "—")
    disp["Esito"] = disp["_esito_raw"].apply(_fmt_esito)
    disp = disp.drop(columns=["_esito_raw"])
    st.dataframe(
        disp.style.map(_style_esito, subset=["Esito"]).map(_style_netto, subset=["Netto (€)"]),
        width="stretch",
        hide_index=True,
        column_config={
            "Campionato":  st.column_config.TextColumn("Campionato", width="small"),
            "Partita":     st.column_config.TextColumn("Partita", width="medium"),
            "Mercato":     st.column_config.TextColumn("Mercato", width="medium"),
            "Segnali":     st.column_config.NumberColumn("Segnali", width="small"),
            "Quota":       st.column_config.TextColumn("Quota", width="small"),
            "Puntata (€)": st.column_config.TextColumn("Puntata", width="small"),
            "Esito":       st.column_config.TextColumn("Esito", width="small"),
            "Netto (€)":   st.column_config.TextColumn("Netto (€)", width="small"),
        },
    )


# ── Metriche globali ──────────────────────────────────────────────────────────

total, won, lost, pending, hit_rate, roi = _metriche(df)

c1, c2, c3, c4, c5 = st.columns(5)
with c1: st.metric("Pick totali", total)  # noqa: E701
with c2: st.metric("Vincenti ✅", won)  # noqa: E701
with c3: st.metric("Perdenti ❌", lost)  # noqa: E701
with c4: st.metric("In attesa ⏳", pending)  # noqa: E701
with c5: st.metric("Hit rate", f"{hit_rate:.1f}%", delta=f"ROI {roi:+.1f}%")  # noqa: E701

# ── Simulazione bankroll ───────────────────────────────────────────────────────

sim_df, total_staked = _bankroll_sim(df, bankroll_init)
if not sim_df.empty:
    final_bk = sim_df["Bankroll (€)"].iloc[-1]
    profit_eur = final_bk - bankroll_init
    roi_bk = profit_eur / total_staked * 100 if total_staked > 0 else 0.0

    stake_range = f"€{_stake_eur(2, bankroll_init):.0f} – €{_stake_eur(5, bankroll_init):.0f}"
    cb1, cb2, cb3 = st.columns(3)
    with cb1:
        st.metric("Bankroll finale", f"€ {final_bk:,.0f}", delta=f"€ {profit_eur:+,.0f}")
    with cb2:
        st.metric("Totale investito", f"€ {total_staked:,.0f}", help=f"Range stake: {stake_range}")
    with cb3:
        st.metric("ROI sul capitale rischiato", f"{roi_bk:+.1f}%")

    with st.expander("📈 Curva del bankroll", expanded=True):
        st.line_chart(sim_df, y="Bankroll (€)", height=220)

st.markdown("---")

# ── Picks per giorno ──────────────────────────────────────────────────────────

_today = _dt.date.today()
giorni_ordinati = sorted(df["data_giorno"].dropna().unique(), reverse=True)
# Giorni futuri (>= domani) ordinati crescenti — i primi 2 vanno aperti
_future_days = sorted([d for d in giorni_ordinati if d >= _today])
_open_days = set(_future_days[:2])

_GIORNI_IT = ["Lunedì", "Martedì", "Mercoledì", "Giovedì", "Venerdì", "Sabato", "Domenica"]
_MESI_IT   = ["", "gennaio", "febbraio", "marzo", "aprile", "maggio", "giugno",
              "luglio", "agosto", "settembre", "ottobre", "novembre", "dicembre"]


def _fmt_giorno(d) -> str:
    return f"{_GIORNI_IT[d.weekday()]} {d.day} {_MESI_IT[d.month]} {d.year}"


for i, giorno in enumerate(giorni_ordinati):
    day_df = df[df["data_giorno"] == giorno].copy()
    day_total, day_won, day_lost, day_pending, day_hit, day_roi = _metriche(day_df)

    # Label testuale del giorno con mini-stats
    hit_str = f"{day_hit:.0f}% HR" if (day_won + day_lost) > 0 else "⏳"
    label = (
        f"**{_fmt_giorno(giorno)}** — "
        f"{day_total} pick | ✅ {day_won} · ❌ {day_lost} · ⏳ {day_pending} | {hit_str}"
    )

    # Espandi di default i primi 2 giorni futuri (con partite ancora da giocare)
    with st.expander(label, expanded=(giorno in _open_days)):
        _render_table(day_df, bankroll_init)

# ── Riepilogo per campionato ──────────────────────────────────────────────────

st.markdown("---")
st.subheader("Riepilogo per campionato")

_completed = df[df["_esito_raw"].notna() & df["Quota"].notna() & (df["Quota"] >= _MIN_STAKE_QUOTA)]
if not _completed.empty:
    summary_rows = []
    for camp, group in _completed.groupby("Campionato"):
        g_won  = (group["_esito_raw"] == True).sum()   # noqa: E712
        g_lost = (group["_esito_raw"] == False).sum()  # noqa: E712
        g_total = len(group)
        g_hit  = g_won / g_total * 100 if g_total > 0 else 0.0
        if g_total > 0:
            g_profit = sum(
                row["Quota"] - 1 if row["_esito_raw"] else -1
                for _, row in group.iterrows()
            )
            g_roi = g_profit / g_total * 100
        else:
            g_roi = 0.0
        summary_rows.append({
            "Campionato":  camp,
            "Pick":        g_total,
            "Vincenti":    int(g_won),
            "Perdenti":    int(g_lost),
            "Hit rate":    f"{g_hit:.1f}%",
            "ROI stimato": f"{g_roi:+.1f}%",
        })

    summary_df = pd.DataFrame(summary_rows).sort_values("Pick", ascending=False)
    st.dataframe(summary_df, width="stretch", hide_index=True)
else:
    st.info("Nessun pick completato ancora per il riepilogo per campionato.")

# ── Calibrazione modello ───────────────────────────────────────────────────────

st.markdown("---")
with st.expander("🔧 Calibrazione modello", expanded=False):
    from analytics.calibration import (
        MARKET_BASE_MIN_SIGNALS,
        effective_min_signals,
        load_calibration,
    )

    calib = load_calibration()
    blocked = calib.get("blocked_markets", [])

    # ── Stato mercati bloccati / soglie alterate ───────────────────────────────
    col_b, col_w = st.columns(2)
    with col_b:
        if blocked:
            st.error(f"**Mercati bloccati ({len(blocked)}):** " + ", ".join(blocked))
        else:
            st.success("Nessun mercato bloccato.")
    with col_w:
        if calib:
            updated_at = calib.get("updated_at", "")[:16].replace("T", " ")
            st.caption(f"Calibrazione dinamica aggiornata: {updated_at} UTC")
        else:
            st.info("Nessuna calibrazione dinamica ancora. Applicate solo soglie base.")

    # ── Tabella per mercato: difficoltà + soglia effettiva + performance ───────
    st.markdown("##### Soglie e performance per mercato")

    # Tutti i mercati noti (base) + quelli con dati storici
    market_stats = calib.get("markets", {}) if calib else {}
    all_markets = sorted(
        set(MARKET_BASE_MIN_SIGNALS.keys()) | set(market_stats.keys())
    )

    cal_rows = []
    for m in all_markets:
        base = MARKET_BASE_MIN_SIGNALS.get(m, 2)
        eff = effective_min_signals(m, calib)
        s = market_stats.get(m)
        if m in blocked:
            stato = "🚫 Bloccato"
        elif eff > base:
            stato = f"⚠️ +{eff - base} dinamico"
        else:
            stato = "✅ OK"
        cal_rows.append({
            "Mercato":          m,
            "Base":             base,
            "Effettiva":        eff,
            "Pick":             str(s["n"]) if s else "—",
            "HR":               f"{s['hr'] * 100:.0f}%" if s else "—",
            "ROI":              f"{s['roi'] * 100:+.1f}%" if s else "—",
            "Quota media":      f"{s['avg_quota']:.2f}" if s and s["avg_quota"] else "—",
            "Stato":            stato,
        })

    st.dataframe(
        pd.DataFrame(cal_rows),
        width="stretch",
        hide_index=True,
        column_config={
            "Base":      st.column_config.NumberColumn("Base", width="small"),
            "Effettiva": st.column_config.NumberColumn("Eff.", width="small"),
        },
    )
