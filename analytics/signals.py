"""
Generazione segnali pre-partita e gestione picks.

Funzioni pure (senza dipendenze Streamlit) che possono essere richiamate
sia dalla dashboard sia dallo scheduler headless.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    pass


# ── Statistiche stagione corrente per sede ────────────────────────────────────


def season_form_stats(df: pd.DataFrame, team: str, venue: str) -> dict:
    """
    Statistiche su tutte le partite della stagione corrente in casa o in trasferta.
    Restituisce {} (con "n": 0) se non ci sono dati.
    """
    if df.empty:
        return {"n": 0}
    tl = team.lower()
    if venue == "home":
        d = df[df["home"].str.lower() == tl]
        if d.empty:
            return {"n": 0}
        st_goals = (d["hg"] - d["hthg"]) + (d["ag"] - d["htag"])
        return {
            "n": len(d),
            "won": (d["result"] == "H").mean(),
            "draw": (d["result"] == "D").mean(),
            "over_1_5": d["over_1_5"].mean(),
            "over_2_5": d["over_2_5"].mean(),
            "over_3_5": d["over_3_5"].mean(),
            "btts": d["btts"].mean(),
            "scored": (d["hg"] > 0).mean(),
            "scored_2plus": (d["hg"] >= 2).mean(),
            "cs": (d["ag"] == 0).mean(),
            "won_by_2plus": (d["hg"] - d["ag"] >= 2).mean(),
            "corners": d["total_corners"].mean(),
            "own_corners": d["hc"].mean(),
            "avg_gf": d["hg"].mean(),
            "avg_gs": d["ag"].mean(),
            "avg_sot": d["hst"].mean(),
            "avg_match_yellow": d["total_yellow"].mean(),
            "avg_match_fouls": (d["hf"] + d["af"]).mean(),
            "ht_over_0_5": ((d["hthg"] + d["htag"]) > 0).mean(),
            "ht_scored": (d["hthg"] > 0).mean(),
            "ht_btts": ((d["hthg"] > 0) & (d["htag"] > 0)).mean(),
            "st_over_0_5": (st_goals > 0).mean(),
            "st_over_1_5": (st_goals > 1).mean(),
            "red_card_pct": ((d["hr"] + d["ar"]) > 0).mean(),
            "over_9_5_corners": (d["total_corners"] > 9).mean(),
            "over_10_5_corners": (d["total_corners"] > 10).mean(),
            "over_8_5_corners": (d["total_corners"] > 8).mean(),
            "over_11_5_corners": (d["total_corners"] > 11).mean(),
            "over_2_5_yellow": (d["total_yellow"] > 2).mean(),
            "over_3_5_yellow": (d["total_yellow"] > 3).mean(),
            "over_4_5_yellow": (d["total_yellow"] > 4).mean(),
            "own_yellow_over_1_5": (d["hy"] > 1).mean(),
            "ht_over_1_5": ((d["hthg"] + d["htag"]) >= 2).mean(),
            "st_btts": (((d["hg"] - d["hthg"]) > 0) & ((d["ag"] - d["htag"]) > 0)).mean(),
            "own_fouls": d["hf"].mean(),
        }
    else:
        d = df[df["away"].str.lower() == tl]
        if d.empty:
            return {"n": 0}
        st_goals = (d["hg"] - d["hthg"]) + (d["ag"] - d["htag"])
        return {
            "n": len(d),
            "won": (d["result"] == "A").mean(),
            "draw": (d["result"] == "D").mean(),
            "over_1_5": d["over_1_5"].mean(),
            "over_2_5": d["over_2_5"].mean(),
            "over_3_5": d["over_3_5"].mean(),
            "btts": d["btts"].mean(),
            "scored": (d["ag"] > 0).mean(),
            "scored_2plus": (d["ag"] >= 2).mean(),
            "cs": (d["hg"] == 0).mean(),
            "won_by_2plus": (d["ag"] - d["hg"] >= 2).mean(),
            "corners": d["total_corners"].mean(),
            "own_corners": d["ac"].mean(),
            "avg_gf": d["ag"].mean(),
            "avg_gs": d["hg"].mean(),
            "avg_sot": d["ast"].mean(),
            "avg_match_yellow": d["total_yellow"].mean(),
            "avg_match_fouls": (d["hf"] + d["af"]).mean(),
            "ht_over_0_5": ((d["hthg"] + d["htag"]) > 0).mean(),
            "ht_scored": (d["htag"] > 0).mean(),
            "ht_btts": ((d["hthg"] > 0) & (d["htag"] > 0)).mean(),
            "st_over_0_5": (st_goals > 0).mean(),
            "st_over_1_5": (st_goals > 1).mean(),
            "red_card_pct": ((d["hr"] + d["ar"]) > 0).mean(),
            "over_9_5_corners": (d["total_corners"] > 9).mean(),
            "over_10_5_corners": (d["total_corners"] > 10).mean(),
            "over_8_5_corners": (d["total_corners"] > 8).mean(),
            "over_11_5_corners": (d["total_corners"] > 11).mean(),
            "over_2_5_yellow": (d["total_yellow"] > 2).mean(),
            "over_3_5_yellow": (d["total_yellow"] > 3).mean(),
            "over_4_5_yellow": (d["total_yellow"] > 4).mean(),
            "own_yellow_over_1_5": (d["ay"] > 1).mean(),
            "ht_over_1_5": ((d["hthg"] + d["htag"]) >= 2).mean(),
            "st_btts": (((d["hg"] - d["hthg"]) > 0) & ((d["ag"] - d["htag"]) > 0)).mean(),
            "own_fouls": d["af"].mean(),
        }


# ── Generazione segnali incrociati ────────────────────────────────────────────


def generate_signals(
    home: str,
    away: str,
    fh: dict,
    fa: dict,
    h2h_sum: dict,
    hd,
    ad,
    days_home: int | None,
    days_away: int | None,
    injuries: dict | None,
    next_match_home: dict | None = None,
    next_match_away: dict | None = None,
) -> list[dict]:
    """
    Genera lista di segnali incrociati per la partita.
    Ogni segnale: {"verdict": "✅"|"❌"|"➖", "text": str, "mercato": str|None}

    Può essere chiamata sia dalla dashboard sia dallo scheduler headless.
    injuries può essere None o {"home": [], "away": []} (nessun dato API-Football).
    next_match_home/away: dict {days: int, league_key: str, league_name: str} oppure None.
      Usato per rilevare turnover se la prossima partita è ravvicinata (S44).
    """
    sigs: list[dict] = []

    def add(verdict: str, text: str, mercato: str | None = None) -> None:
        sigs.append({"verdict": verdict, "text": text, "mercato": mercato})

    has_form = fh.get("n", 0) >= 3 and fa.get("n", 0) >= 3

    # ── 1. Casa segna ─────────────────────────────────────────────────────────
    if has_form:
        h_scored = fh.get("scored", 0)
        a_concedes = 1 - fa.get("cs", 0)
        hs_pct = int(h_scored * 100)
        ac_pct = int(a_concedes * 100)
        if h_scored >= 0.60 and a_concedes >= 0.60:
            add(
                "✅",
                f"Casa segna {hs_pct}% · Avv. subisce {ac_pct}% → Gol casa probabile",
                "Casa Segna (almeno 1 gol)",
            )
        elif h_scored < 0.40 or a_concedes < 0.40:
            add("❌", f"Casa segna {hs_pct}% · Avv. subisce {ac_pct}% → Gol casa improbabile")
        else:
            add("➖", f"Casa segna {hs_pct}% · Avv. subisce {ac_pct}% → Incerto")

    # ── 2. Trasferta segna ────────────────────────────────────────────────────
    if has_form:
        a_scored = fa.get("scored", 0)
        h_concedes = 1 - fh.get("cs", 0)
        as_pct = int(a_scored * 100)
        hc_pct = int(h_concedes * 100)
        if a_scored >= 0.60 and h_concedes >= 0.60:
            add(
                "✅",
                f"Trasf. segna {as_pct}% · Casa subisce {hc_pct}% → Gol trasferta probabile",
                "Trasferta Segna (almeno 1 gol)",
            )
        elif a_scored < 0.40 or h_concedes < 0.40:
            add("❌", f"Trasf. segna {as_pct}% · Casa subisce {hc_pct}% → Gol trasf. improbabile")
        else:
            add("➖", f"Trasf. segna {as_pct}% · Casa subisce {hc_pct}% → Incerto")

    # ── 3. Over 2.5 ───────────────────────────────────────────────────────────
    if has_form:
        over_avg = (fh.get("over_2_5", 0) + fa.get("over_2_5", 0)) / 2
        if over_avg >= 0.55:
            add("✅", f"Over 2.5: media {int(over_avg * 100)}% → Partita aperta", "Over 2.5 Gol")
        elif over_avg < 0.45:
            add("❌", f"Over 2.5: media {int(over_avg * 100)}% → Pochi gol attesi")
        else:
            add("➖", f"Over 2.5: media {int(over_avg * 100)}% → Equilibrio")

    # ── 4. Under 2.5 (solo se supportato) ────────────────────────────────────
    if has_form:
        over_avg = (fh.get("over_2_5", 0) + fa.get("over_2_5", 0)) / 2
        if over_avg < 0.45:
            add(
                "✅",
                f"Under 2.5: {int((1 - over_avg) * 100)}% partite sotto 3 gol → Under favorito",
                "Under 2.5 Gol",
            )

    # ── 5. BTTS ───────────────────────────────────────────────────────────────
    if has_form:
        btts_avg = (fh.get("btts", 0) + fa.get("btts", 0)) / 2
        if btts_avg >= 0.50:
            add(
                "✅",
                f"BTTS: media {int(btts_avg * 100)}% → Entrambe segnano spesso",
                "Goal — Entrambe Segnano",
            )
        elif btts_avg < 0.45:
            add("❌", f"BTTS: media {int(btts_avg * 100)}% → Solo una segna spesso")
        else:
            add("➖", f"BTTS: media {int(btts_avg * 100)}% → Equilibrio")

    # ── 6. No Goal (solo se supportato) ──────────────────────────────────────
    if has_form:
        btts_avg = (fh.get("btts", 0) + fa.get("btts", 0)) / 2
        if btts_avg < 0.45:
            add(
                "✅",
                f"No Goal: {int((1 - btts_avg) * 100)}% senza entrambe segnare → No Goal favorito",
                "No Goal — Solo una Segna",
            )

    # ── 7. 1X2 Casa ───────────────────────────────────────────────────────────
    if has_form:
        h_won = fh.get("won", 0)
        a_won = fa.get("won", 0)
        hw_pct = int(h_won * 100)
        aw_pct = int(a_won * 100)
        if h_won >= 0.50 and a_won < 0.50:
            add("✅", f"Casa vince {hw_pct}% in casa → Favorita", "1 — Vittoria Casa")
        elif a_won >= 0.50:
            add("❌", f"Casa vince {hw_pct}% in casa · Trasf. forte ({aw_pct}%) → Svantaggio casa")
        else:
            add("➖", f"1X2: Casa {hw_pct}% · Trasf. {aw_pct}% → Equilibrio")

    # ── 8. 1X2 Trasferta ──────────────────────────────────────────────────────
    if has_form:
        a_won = fa.get("won", 0)
        h_won = fh.get("won", 0)
        aw_pct = int(a_won * 100)
        hw_pct = int(h_won * 100)
        if a_won >= 0.45 and h_won < 0.50:
            add("✅", f"Trasf. vince {aw_pct}% in trasferta → Candidata", "2 — Vittoria Trasferta")
        elif h_won >= 0.50:
            add("❌", f"Trasf. vince {aw_pct}% in trasf. · Casa forte ({hw_pct}%) → Casa favorita")

    # ── 8b. Favorita netta ────────────────────────────────────────────────────
    # Segnale autonomo: se una squadra vince ≥65% delle partite nel proprio campo
    # indipendentemente dall'avversario, è un indicatore forte di superiorità.
    if has_form:
        if fh.get("won", 0) >= 0.65:
            hw_pct = int(fh["won"] * 100)
            add(
                "✅",
                f"Casa favorita netta: vince {hw_pct}% delle partite in casa",
                "1 — Vittoria Casa",
            )
        if fa.get("won", 0) >= 0.65:
            aw_pct = int(fa["won"] * 100)
            add(
                "✅",
                f"Trasferta favorita netta: vince {aw_pct}% delle partite in trasferta",
                "2 — Vittoria Trasferta",
            )

    # ── 9. Corner ─────────────────────────────────────────────────────────────
    if has_form and fh.get("corners") is not None and fa.get("corners") is not None:
        avg_c = (fh["corners"] + fa["corners"]) / 2
        if avg_c >= 10.5:
            add("✅", f"Corner: media {avg_c:.1f} → Over 10.5 probabile", "Corner O/U Over 10.5")
        elif avg_c < 9.5:
            add("✅", f"Corner: media {avg_c:.1f} → Under 9.5 probabile", "Corner O/U Under 9.5")
        else:
            add("➖", f"Corner: media {avg_c:.1f} → Equilibrio (9.5–10.5)")

    # ── 10. Ritardo Over/Under ────────────────────────────────────────────────
    if hd is not None and ad is not None:
        try:
            max_over = max(int(hd["Ritardo Over 2.5"]), int(ad["Ritardo Over 2.5"]))
            if max_over >= 3:
                add(
                    "✅",
                    f"Ritardo Over 2.5: {max_over} gare consecutive senza → Atteso",
                    "Over 2.5 Gol",
                )
        except Exception:
            pass
        try:
            max_under = max(int(hd["Ritardo Under 2.5"]), int(ad["Ritardo Under 2.5"]))
            if max_under >= 3:
                add(
                    "✅",
                    f"Ritardo Under 2.5: {max_under} gare consecutive senza → Atteso",
                    "Under 2.5 Gol",
                )
        except Exception:
            pass

    # ── 11. Ritardo Goal/No Goal ──────────────────────────────────────────────
    if hd is not None and ad is not None:
        try:
            max_goal = max(int(hd["Ritardo Goal"]), int(ad["Ritardo Goal"]))
            if max_goal >= 3:
                add(
                    "✅",
                    f"Ritardo Goal: {max_goal} gare consecutive senza BTTS → Atteso",
                    "Goal — Entrambe Segnano",
                )
        except Exception:
            pass
        try:
            max_ng = max(int(hd["Ritardo No Goal"]), int(ad["Ritardo No Goal"]))
            if max_ng >= 3:
                add(
                    "✅",
                    f"Ritardo No Goal: {max_ng} gare consecutive con BTTS → No Goal atteso",
                    "No Goal — Solo una Segna",
                )
        except Exception:
            pass

    # ── 12. H2H risultato ─────────────────────────────────────────────────────
    if h2h_sum and h2h_sum.get("campione", 0) >= 4:
        win_pct = h2h_sum.get("win_pct", 0)
        loss_pct = h2h_sum.get("loss_pct", 0)
        draw_pct = h2h_sum.get("draw_pct", 0)
        n = h2h_sum["campione"]
        if win_pct >= 60:
            add(
                "✅",
                f"H2H: Casa vince {win_pct:.0f}% ({n}g) → Trend storico favorevole casa",
                "1 — Vittoria Casa",
            )
        elif loss_pct >= 60:
            add(
                "✅",
                f"H2H: Trasferta vince {loss_pct:.0f}% ({n}g) → Trend storico favorevole trasferta",
                "2 — Vittoria Trasferta",
            )
        else:
            add(
                "➖",
                f"H2H: 1={win_pct:.0f}% X={draw_pct:.0f}% 2={loss_pct:.0f}% ({n}g) → Nessun trend",
            )

    # ── 13. H2H Over/BTTS ─────────────────────────────────────────────────────
    if h2h_sum and h2h_sum.get("campione", 0) >= 4:
        over_pct = h2h_sum.get("over_2_5_pct", 0)
        goal_pct = h2h_sum.get("goal_pct", 0)
        n = h2h_sum["campione"]
        if over_pct >= 60:
            add(
                "✅",
                f"H2H Over 2.5: {over_pct:.0f}% ({n}g) → Partite prolifiche nei precedenti",
                "Over 2.5 Gol",
            )
        if goal_pct >= 60:
            add(
                "✅",
                f"H2H Goal: {goal_pct:.0f}% ({n}g) → Entrambe segnano nei precedenti",
                "Goal — Entrambe Segnano",
            )

    # ── 14. Riposo ────────────────────────────────────────────────────────────
    if days_home is not None and days_away is not None:
        home_tired = days_home < 4
        away_tired = days_away < 4
        if home_tired and not away_tired:
            add(
                "✅",
                f"Riposo: Casa {days_home}gg (stanca) · Trasf. {days_away}gg → Vantagg. trasferta",
                "2 — Vittoria Trasferta",
            )
        elif away_tired and not home_tired:
            add(
                "✅",
                f"Riposo: Trasf. {days_away}gg (stanca) · Casa {days_home}gg → Vantaggio casa",
                "1 — Vittoria Casa",
            )
        elif home_tired and away_tired:
            add(
                "✅",
                f"Riposo: Entrambe stanche ({days_home}gg/{days_away}gg) → Attesi meno gol",
                "Under 2.5 Gol",
            )
        else:
            add("➖", f"Riposo: Casa {days_home}gg · Trasf. {days_away}gg → Nessun vantaggio")

    # ── 15. Assenti ───────────────────────────────────────────────────────────
    if injuries is not None:
        home_out = sum(1 for p in injuries.get("home", []) if p["type"] == "Missing Fixture")
        away_out = sum(1 for p in injuries.get("away", []) if p["type"] == "Missing Fixture")
        if away_out >= 2 and away_out > home_out:
            add(
                "✅",
                f"Assenti: Trasf. {away_out} indisponibili vs Casa {home_out} → Vantaggio casa",
                "1 — Vittoria Casa",
            )
        elif home_out >= 2 and home_out > away_out:
            add(
                "✅",
                f"Assenti: Casa {home_out} ind. vs Trasf. {away_out} → Vantaggio trasferta",
                "2 — Vittoria Trasferta",
            )
        elif (home_out + away_out) >= 3:
            add("✅", f"Assenti: {home_out + away_out} totali → Attesi meno gol", "Under 2.5 Gol")
        elif home_out + away_out > 0:
            add("➖", f"Assenti: Casa {home_out} · Trasf. {away_out} → Impatto limitato")

    # ── 16. Over/Under 1.5 gol ────────────────────────────────────────────────
    if has_form:
        over1_avg = (fh.get("over_1_5", 0) + fa.get("over_1_5", 0)) / 2
        if over1_avg >= 0.75:
            add(
                "✅",
                f"Over 1.5: media {int(over1_avg * 100)}% → Almeno 2 gol molto probabile",
                "Over 1.5 Gol",
            )
        elif over1_avg < 0.55:
            add(
                "✅",
                f"Under 1.5: {int((1 - over1_avg) * 100)}% partite con ≤1 gol → Under favorito",
                "Under 1.5 Gol",
            )
        else:
            add("➖", f"Over 1.5: media {int(over1_avg * 100)}% → Equilibrio")

    # ── 17. Casa segna 2+ gol (Over 1.5 casa) ────────────────────────────────
    if has_form:
        h_2plus = fh.get("scored_2plus", 0)
        if h_2plus >= 0.40:
            add(
                "✅",
                f"Casa segna 2+ nel {int(h_2plus * 100)}% delle partite in casa",
                "Casa Over 1.5 Gol",
            )
        elif h_2plus < 0.20:
            add("❌", f"Casa segna 2+ solo nel {int(h_2plus * 100)}% → Difficile Over 1.5 casa")
        else:
            add("➖", f"Casa segna 2+ nel {int(h_2plus * 100)}% → Incerto")

    # ── 18. Trasferta segna 2+ gol (Over 1.5 trasferta) ──────────────────────
    if has_form:
        a_2plus = fa.get("scored_2plus", 0)
        if a_2plus >= 0.40:
            add(
                "✅",
                f"Trasf. segna 2+ nel {int(a_2plus * 100)}% delle partite in trasf.",
                "Trasferta Over 1.5 Gol",
            )
        elif a_2plus < 0.20:
            add("❌", f"Trasf. segna 2+ solo nel {int(a_2plus * 100)}% → Difficile Over 1.5 trasf.")
        else:
            add("➖", f"Trasf. segna 2+ nel {int(a_2plus * 100)}% → Incerto")

    # ── 19. Tiri in porta (qualità attacco) ──────────────────────────────────
    if has_form and fh.get("avg_sot") is not None and fa.get("avg_sot") is not None:
        sot_sum = fh["avg_sot"] + fa["avg_sot"]
        if sot_sum >= 8.0:
            add("✅", f"Tiri in porta: {sot_sum:.1f} avg combined → Partita aperta", "Over 2.5 Gol")
        elif sot_sum <= 5.0:
            add(
                "✅",
                f"Tiri in porta: {sot_sum:.1f} avg combined → Pochi gol attesi",
                "Under 2.5 Gol",
            )
        else:
            add("➖", f"Tiri in porta: {sot_sum:.1f} avg combined → Equilibrio")

    # ── 20. Cartellini Gialli Over/Under 3.5 ─────────────────────────────────
    if has_form and fh.get("avg_match_yellow") is not None:
        yellow_avg = (fh["avg_match_yellow"] + fa["avg_match_yellow"]) / 2
        if yellow_avg >= 4.0:
            add(
                "✅",
                f"Gialli: media {yellow_avg:.1f} per partita → Over 3.5 probabile",
                "Cartellini Gialli Over 3.5",
            )
        elif yellow_avg <= 3.0:
            add(
                "✅",
                f"Gialli: media {yellow_avg:.1f} per partita → Under 3.5 probabile",
                "Cartellini Gialli Under 3.5",
            )
        else:
            add("➖", f"Gialli: media {yellow_avg:.1f} per partita → Equilibrio (3.0–4.0)")

    # ── 21. Primo tempo Over 0.5 ──────────────────────────────────────────────
    if has_form and fh.get("ht_over_0_5") is not None:
        ht_avg = (fh["ht_over_0_5"] + fa["ht_over_0_5"]) / 2
        if ht_avg >= 0.65:
            add(
                "✅",
                f"Gol nel 1° tempo: {int(ht_avg * 100)}% delle partite → Over 0.5 PT",
                "Over 0.5 Primo Tempo",
            )
        elif ht_avg < 0.45:
            add(
                "✅",
                f"Gol nel 1° tempo: solo {int(ht_avg * 100)}% → Under 0.5 PT probabile",
                "Under 0.5 Primo Tempo",
            )
        else:
            add("➖", f"Gol nel 1° tempo: {int(ht_avg * 100)}% → Equilibrio")

    # ── 22. Over/Under 3.5 gol totale ────────────────────────────────────────
    if has_form:
        over3_avg = (fh.get("over_3_5", 0) + fa.get("over_3_5", 0)) / 2
        if over3_avg >= 0.45:
            add(
                "✅",
                f"Over 3.5: media {int(over3_avg * 100)}% → Partita molto prolifera",
                "Over 3.5 Gol",
            )
        elif over3_avg < 0.25:
            add(
                "✅",
                f"Under 3.5: {int((1 - over3_avg) * 100)}% → Pochi gol attesi",
                "Under 3.5 Gol",
            )
        else:
            add("➖", f"Over 3.5: media {int(over3_avg * 100)}% → Equilibrio")

    # ── 23. Pareggio X ────────────────────────────────────────────────────────
    if has_form:
        draw_avg = (fh.get("draw", 0) + fa.get("draw", 0)) / 2
        if draw_avg >= 0.30:
            add("✅", f"Pareggio: media {int(draw_avg * 100)}% → X probabile", "X — Pareggio")
        elif draw_avg < 0.20:
            add("❌", f"Pareggio: media {int(draw_avg * 100)}% → X improbabile")
        else:
            add("➖", f"Pareggio: media {int(draw_avg * 100)}% → Incerto")

    # ── 24. Doppia chance 1X / X2 ─────────────────────────────────────────────
    if has_form:
        dc_1x = fh.get("won", 0) + fh.get("draw", 0)
        dc_x2 = fa.get("won", 0) + fa.get("draw", 0)
        if dc_1x >= 0.65:
            add(
                "✅",
                f"Doppia chance 1X: Casa vince/pareggia {int(dc_1x * 100)}%",
                "1X — Doppia Chance Casa",
            )
        if dc_x2 >= 0.60:
            add(
                "✅",
                f"Doppia chance X2: Trasf. vince/pareggia {int(dc_x2 * 100)}%",
                "X2 — Doppia Chance Trasferta",
            )

    # ── 27. Corner casa / trasferta separati ─────────────────────────────────
    if has_form and fh.get("own_corners") is not None and fa.get("own_corners") is not None:
        h_c = fh["own_corners"]
        a_c = fa["own_corners"]
        if h_c >= 6.0:
            add(
                "✅", f"Corner casa: media {h_c:.1f} → Casa Over 5.5 corner", "Corner Casa Over 5.5"
            )
        if a_c >= 5.5:
            add(
                "✅",
                f"Corner trasferta: media {a_c:.1f} → Trasf. Over 4.5 corner",
                "Corner Trasferta Over 4.5",
            )

    # ── 28. Falli totali Over/Under ───────────────────────────────────────────
    if has_form and fh.get("avg_match_fouls") is not None and fa.get("avg_match_fouls") is not None:
        fouls_avg = (fh["avg_match_fouls"] + fa["avg_match_fouls"]) / 2
        if fouls_avg >= 25:
            add("✅", f"Falli: media {fouls_avg:.1f} → Over 24.5 falli", "Over 24.5 Falli")
        elif fouls_avg < 20:
            add("✅", f"Falli: media {fouls_avg:.1f} → Under 20.5 falli", "Under 20.5 Falli")
        else:
            add("➖", f"Falli: media {fouls_avg:.1f} → Equilibrio")

    # ── 29. Handicap europeo ──────────────────────────────────────────────────
    if has_form:
        h_2plus_win = fh.get("won_by_2plus", 0)
        a_2plus_win = fa.get("won_by_2plus", 0)
        if h_2plus_win >= 0.40:
            add(
                "✅",
                f"Casa vince 2+ gol nel {int(h_2plus_win * 100)}% → Handicap −1 casa",
                "Handicap −1 Casa",
            )
        elif a_2plus_win >= 0.35:
            add(
                "✅",
                f"Trasf. vince 2+ gol nel {int(a_2plus_win * 100)}% → Handicap +1 trasferta",
                "Handicap +1 Trasferta",
            )

    # ── 30. Primo tempo Goal/No Goal ──────────────────────────────────────────
    if has_form and fh.get("ht_btts") is not None and fa.get("ht_btts") is not None:
        ht_btts_avg = (fh["ht_btts"] + fa["ht_btts"]) / 2
        if ht_btts_avg >= 0.35:
            add(
                "✅",
                f"BTTS nel 1° T: {int(ht_btts_avg * 100)}% → Goal PT probabile",
                "Goal Primo Tempo",
            )
        elif ht_btts_avg < 0.20:
            add(
                "✅",
                f"BTTS nel 1° T: solo {int(ht_btts_avg * 100)}% → No Goal PT probabile",
                "No Goal Primo Tempo",
            )
        else:
            add("➖", f"BTTS nel 1° T: {int(ht_btts_avg * 100)}% → Equilibrio")

    # ── 31. Secondo tempo Over 0.5 / 1.5 ─────────────────────────────────────
    if has_form and fh.get("st_over_0_5") is not None and fa.get("st_over_0_5") is not None:
        st05_avg = (fh["st_over_0_5"] + fa["st_over_0_5"]) / 2
        st15_avg = (fh.get("st_over_1_5", 0) + fa.get("st_over_1_5", 0)) / 2
        if st05_avg >= 0.80:
            add(
                "✅",
                f"Gol nel 2° T: {int(st05_avg * 100)}% → Over 0.5 ST probabile",
                "Over 0.5 Secondo Tempo",
            )
        if st15_avg >= 0.55:
            add(
                "✅",
                f"2+ gol nel 2° T: {int(st15_avg * 100)}% → Over 1.5 ST probabile",
                "Over 1.5 Secondo Tempo",
            )

    # ── 32. Rossi sì/no ───────────────────────────────────────────────────────
    if has_form and fh.get("red_card_pct") is not None and fa.get("red_card_pct") is not None:
        red_avg = (fh["red_card_pct"] + fa["red_card_pct"]) / 2
        if red_avg >= 0.25:
            add(
                "✅",
                f"Rosso in {int(red_avg * 100)}% delle partite → Almeno 1 rosso probabile",
                "Almeno 1 Rosso",
            )
        else:
            add("➖", f"Rosso in {int(red_avg * 100)}% delle partite → Improbabile")

    # ── 33. Tiri in porta per squadra ────────────────────────────────────────
    if has_form and fh.get("avg_sot") is not None and fa.get("avg_sot") is not None:
        if fh["avg_sot"] >= 4.0:
            add(
                "✅",
                f"Tiri in porta casa: {fh['avg_sot']:.1f} → Casa Over 3.5 SOT",
                "Casa Over 3.5 Tiri in Porta",
            )
        if fa["avg_sot"] >= 3.5:
            add(
                "✅",
                f"Tiri in porta trasf.: {fa['avg_sot']:.1f} → Trasf. Over 3.5 SOT",
                "Trasferta Over 3.5 Tiri in Porta",
            )

    # ── 34. 12 — Escludi pareggio ─────────────────────────────────────────────
    if has_form:
        draw_avg = (fh.get("draw", 0) + fa.get("draw", 0)) / 2
        h_won = fh.get("won", 0)
        a_won = fa.get("won", 0)
        if draw_avg < 0.22 and (h_won + a_won) >= 0.70:
            add(
                "✅",
                f"12: pareggio raro ({int(draw_avg * 100)}%) · {int((h_won + a_won) * 100)}%"
                " esiti decisi → Escludi X",
                "12 — Escludi Pareggio",
            )

    # ── 35. Gialli Over/Under 2.5 ─────────────────────────────────────────────
    if has_form and fh.get("over_2_5_yellow") is not None and fa.get("over_2_5_yellow") is not None:
        y25_avg = (fh["over_2_5_yellow"] + fa["over_2_5_yellow"]) / 2
        if y25_avg >= 0.70:
            add(
                "✅",
                f"Gialli Over 2.5: {int(y25_avg * 100)}% → Partita con molti gialli",
                "Cartellini Gialli Over 2.5",
            )
        elif y25_avg < 0.40:
            add(
                "✅",
                f"Gialli Under 2.5: solo {int(y25_avg * 100)}% oltre 2 gialli → Pochi gialli",
                "Cartellini Gialli Under 2.5",
            )

    # ── 36. Gialli Over 4.5 ───────────────────────────────────────────────────
    if has_form and fh.get("over_4_5_yellow") is not None and fa.get("over_4_5_yellow") is not None:
        y45_avg = (fh["over_4_5_yellow"] + fa["over_4_5_yellow"]) / 2
        if y45_avg >= 0.40:
            add(
                "✅",
                f"Gialli Over 4.5: {int(y45_avg * 100)}% → Partita molto tesa",
                "Cartellini Gialli Over 4.5",
            )
        elif y45_avg < 0.20:
            add(
                "✅",
                f"Gialli Under 4.5: solo {int(y45_avg * 100)}% → Pochi gialli attesi",
                "Cartellini Gialli Under 4.5",
            )

    # ── 37. Gialli casa / trasf. Over 1.5 ────────────────────────────────────
    if (
        has_form
        and fh.get("own_yellow_over_1_5") is not None
        and fa.get("own_yellow_over_1_5") is not None
    ):
        if fh["own_yellow_over_1_5"] >= 0.55:
            add(
                "✅",
                f"Gialli casa Over 1.5: {int(fh['own_yellow_over_1_5'] * 100)}% → Casa fa molti falli",  # noqa: E501
                "Gialli Casa Over 1.5",
            )
        if fa["own_yellow_over_1_5"] >= 0.55:
            add(
                "✅",
                f"Gialli trasf. Over 1.5: {int(fa['own_yellow_over_1_5'] * 100)}% → Trasf. fa molti falli",  # noqa: E501
                "Gialli Trasferta Over 1.5",
            )

    # ── 38. Corner Over 8.5 / Under 8.5 ──────────────────────────────────────
    if (
        has_form
        and fh.get("over_8_5_corners") is not None
        and fa.get("over_8_5_corners") is not None
    ):
        avg_85 = (fh["over_8_5_corners"] + fa["over_8_5_corners"]) / 2
        if avg_85 < 0.40:
            add(
                "✅",
                f"Corner Under 8.5: solo {int(avg_85 * 100)}% supera 8 corner → Pochi corner",
                "Corner O/U Under 8.5",
            )

    # ── 39. Corner Over 11.5 ─────────────────────────────────────────────────
    if (
        has_form
        and fh.get("over_11_5_corners") is not None
        and fa.get("over_11_5_corners") is not None
    ):
        avg_115 = (fh["over_11_5_corners"] + fa["over_11_5_corners"]) / 2
        if avg_115 >= 0.40:
            add(
                "✅",
                f"Corner Over 11.5: {int(avg_115 * 100)}% → Molti corner attesi",
                "Corner O/U Over 11.5",
            )

    # ── 40. Over 1.5 primo tempo ──────────────────────────────────────────────
    if has_form and fh.get("ht_over_1_5") is not None and fa.get("ht_over_1_5") is not None:
        ht15_avg = (fh["ht_over_1_5"] + fa["ht_over_1_5"]) / 2
        if ht15_avg >= 0.30:
            add(
                "✅",
                f"Over 1.5 PT: {int(ht15_avg * 100)}% → Almeno 2 gol nel 1° tempo",
                "Over 1.5 Primo Tempo",
            )
        elif ht15_avg < 0.15:
            add(
                "✅",
                f"Under 1.5 PT: solo {int(ht15_avg * 100)}% → Pochi gol nel 1° tempo",
                "Under 1.5 Primo Tempo",
            )

    # ── 41. Casa / Trasferta segna nel primo tempo ────────────────────────────
    if has_form and fh.get("ht_scored") is not None and fa.get("ht_scored") is not None:
        if fh["ht_scored"] >= 0.60:
            add(
                "✅",
                f"Casa segna nel 1° T: {int(fh['ht_scored'] * 100)}% → Gol casa PT probabile",
                "Casa Segna Primo Tempo",
            )
        if fa["ht_scored"] >= 0.55:
            add(
                "✅",
                f"Trasf. segna nel 1° T: {int(fa['ht_scored'] * 100)}% → Gol trasf. PT probabile",
                "Trasferta Segna Primo Tempo",
            )

    # ── 42. Goal / No Goal secondo tempo ──────────────────────────────────────
    if has_form and fh.get("st_btts") is not None and fa.get("st_btts") is not None:
        st_btts_avg = (fh["st_btts"] + fa["st_btts"]) / 2
        if st_btts_avg >= 0.45:
            add(
                "✅",
                f"Goal nel 2° T: entrambe segnano nel {int(st_btts_avg * 100)}% → Goal ST",
                "Goal Secondo Tempo",
            )
        elif st_btts_avg < 0.25:
            add(
                "✅",
                f"No Goal nel 2° T: solo {int(st_btts_avg * 100)}% → No Goal ST",
                "No Goal Secondo Tempo",
            )

    # ── 43. Falli casa / trasferta Over 10 ───────────────────────────────────
    if has_form and fh.get("own_fouls") is not None and fa.get("own_fouls") is not None:
        if fh["own_fouls"] >= 13.0:
            add(
                "✅",
                f"Falli casa: media {fh['own_fouls']:.1f} → Casa Over 12.5 falli",
                "Falli Casa Over 12.5",
            )
        if fa["own_fouls"] >= 12.0:
            add(
                "✅",
                f"Falli trasf.: media {fa['own_fouls']:.1f} → Trasf. Over 11.5 falli",
                "Falli Trasferta Over 11.5",
            )

    # ── 44. Prossima partita ravvicinata (turnover/rotazione) ─────────────────
    # Se una squadra ha un match importante entro pochi giorni, il manager
    # tende a ruotare i titolari → meno intensità → Under 2.5 Gol favorito.
    # Competizioni europee = rotazione quasi certa; match ravvicinati = probabile.
    _EUROPEAN = {
        "champions_league", "europa_league", "conference_league", "copa_libertadores"
    }
    for _nm, _team_label, _opponent_mercato in [
        (next_match_home, home, "2 — Vittoria Trasferta"),
        (next_match_away, away, "1 — Vittoria Casa"),
    ]:
        if _nm is None:
            continue
        days_gap   = _nm.get("days")
        is_euro    = _nm.get("league_key") in _EUROPEAN
        league_nm  = _nm.get("league_name", "prossima gara")

        if days_gap is None:
            continue

        if days_gap <= 3 and is_euro:
            add(
                "✅",
                f"{_team_label}: {league_nm} tra {days_gap}gg → Rotazione quasi certa → "
                "Intensità ridotta",
                "Under 2.5 Gol",
            )
        elif days_gap <= 3:
            add(
                "➖",
                f"{_team_label}: prossima gara tra {days_gap}gg → Possibile turnover",
            )
        elif days_gap <= 5 and is_euro:
            add(
                "➖",
                f"{_team_label}: {league_nm} tra {days_gap}gg → Attenzione gestione rosa",
            )

    return sigs


# ── Filtro mercati mutuamente esclusivi ───────────────────────────────────────

CONFLICT_GROUPS: list[frozenset] = [
    frozenset({"Over 1.5 Gol",  "Under 1.5 Gol"}),
    frozenset({"Over 2.5 Gol",  "Under 2.5 Gol"}),
    frozenset({"Over 3.5 Gol",  "Under 3.5 Gol"}),
    frozenset({"Goal — Entrambe Segnano", "No Goal — Solo una Segna"}),
    frozenset({"1 — Vittoria Casa", "X — Pareggio", "2 — Vittoria Trasferta"}),
    frozenset({"Handicap −1 Casa", "Handicap +1 Trasferta"}),
    frozenset({"Cartellini Gialli Over 2.5", "Cartellini Gialli Under 2.5"}),
    frozenset({"Cartellini Gialli Over 3.5", "Cartellini Gialli Under 3.5"}),
    frozenset({"Cartellini Gialli Over 4.5", "Cartellini Gialli Under 4.5"}),
]


def filter_conflicts(counts: dict[str, int]) -> dict[str, int]:
    """
    Per i mercati mutuamente esclusivi:
    - Se un membro ha più segnali degli altri → vince, i perdenti vengono rimossi.
    - In caso di parità → tutti rimossi (vero contrasto, nessun pick affidabile).
    """
    to_remove: set[str] = set()
    for group in CONFLICT_GROUPS:
        present = {m: counts[m] for m in group if m in counts}
        if len(present) < 2:
            continue
        max_count = max(present.values())
        winners = [m for m, c in present.items() if c == max_count]
        if len(winners) == 1:
            # Un chiaro vincitore: elimina solo i perdenti
            to_remove |= {m for m in present if m != winners[0]}
        else:
            # Parità: segnali davvero in antitesi, elimina tutto
            to_remove |= set(present.keys())
    return {m: c for m, c in counts.items() if m not in to_remove}


# ── Salvataggio picks qualificati ─────────────────────────────────────────────


def save_picks(
    home: str,
    away: str,
    match_date,
    matchday: int | None,
    league_key: str,
    counts: dict[str, int],
    match_odds: dict[str, float],
) -> None:
    """
    Sincronizza in DB i pick per una partita:
    - Upsert dei mercati con ≥2 segnali e quota SofaScore ≥1.30
    - Elimina i pick pendenti che non soddisfano più i criteri

    Solo i pick con esito=None vengono toccati.
    Può essere chiamata sia dalla dashboard sia dallo scheduler headless.
    """
    from analytics.calibration import effective_min_signals, load_calibration
    from config import CURRENT_SEASON
    from db import get_session
    from models.picks import Pick

    calib = load_calibration()
    blocked = set(calib.get("blocked_markets", []))

    qualifying = {
        mercato: count
        for mercato, count in counts.items()
        if mercato not in blocked
        and count >= effective_min_signals(mercato, calib)
        and match_odds.get(mercato) is not None
        and match_odds[mercato] >= 1.30
    }

    match_dt = match_date.to_pydatetime() if hasattr(match_date, "to_pydatetime") else match_date
    # Normalizza a naive UTC per coerenza col DB (DateTime senza timezone=True)
    if getattr(match_dt, "tzinfo", None) is not None:
        from datetime import UTC as _UTC
        match_dt = match_dt.astimezone(_UTC).replace(tzinfo=None)

    try:
        with get_session() as session:
            for mercato, count in qualifying.items():
                quota = match_odds[mercato]
                existing = (
                    session.query(Pick)
                    .filter(
                        Pick.home_team == home,
                        Pick.away_team == away,
                        Pick.match_date == match_dt,
                        Pick.mercato == mercato,
                    )
                    .first()
                )
                if existing:
                    existing.segnali = count
                    existing.quota = quota
                    existing.updated_at = datetime.utcnow()
                else:
                    session.add(
                        Pick(
                            league=league_key,
                            season=CURRENT_SEASON,
                            matchday=matchday,
                            home_team=home,
                            away_team=away,
                            match_date=match_dt,
                            mercato=mercato,
                            quota=quota,
                            segnali=count,
                            esito=None,
                        )
                    )

            stale = (
                session.query(Pick)
                .filter(
                    Pick.home_team == home,
                    Pick.away_team == away,
                    Pick.match_date == match_dt,
                    Pick.esito.is_(None),
                    Pick.mercato.notin_(list(qualifying.keys())),
                )
                .all()
            )
            for p in stale:
                session.delete(p)

    except Exception:
        pass  # silent — non blocca il chiamante
