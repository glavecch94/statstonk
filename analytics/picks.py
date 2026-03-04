"""
Logica di valutazione esito post-partita per i pick.

Usato dallo scheduler per aggiornare esito=True/False dopo che la partita
è terminata e i dati sono stati sincronizzati nel DB.
"""

from __future__ import annotations

# Mappa mercato → funzione(hg, ag, hc, ac, hy, ay) -> bool
# hg/ag: gol casa/trasferta, hc/ac: corner, hy/ay: cartellini gialli
OUTCOME_MAP: dict = {
    "1 — Vittoria Casa":           lambda hg, ag, hc, ac, hy, ay: hg > ag,
    "X — Pareggio":                lambda hg, ag, hc, ac, hy, ay: hg == ag,
    "2 — Vittoria Trasferta":      lambda hg, ag, hc, ac, hy, ay: ag > hg,
    "Over 1.5 Gol":                lambda hg, ag, hc, ac, hy, ay: hg + ag >= 2,
    "Over 2.5 Gol":                lambda hg, ag, hc, ac, hy, ay: hg + ag >= 3,
    "Over 3.5 Gol":                lambda hg, ag, hc, ac, hy, ay: hg + ag >= 4,
    "Under 1.5 Gol":               lambda hg, ag, hc, ac, hy, ay: hg + ag <= 1,
    "Under 2.5 Gol":               lambda hg, ag, hc, ac, hy, ay: hg + ag <= 2,
    "Under 3.5 Gol":               lambda hg, ag, hc, ac, hy, ay: hg + ag <= 3,
    "Goal — Entrambe Segnano":     lambda hg, ag, hc, ac, hy, ay: hg > 0 and ag > 0,
    "No Goal — Solo una Segna":    lambda hg, ag, hc, ac, hy, ay: not (hg > 0 and ag > 0),
    "Corner O/U Over 9.5":         lambda hg, ag, hc, ac, hy, ay: (hc or 0) + (ac or 0) > 9,
    "Corner O/U Over 10.5":        lambda hg, ag, hc, ac, hy, ay: (hc or 0) + (ac or 0) > 10,
    "Corner O/U Under 8.5":        lambda hg, ag, hc, ac, hy, ay: (hc or 0) + (ac or 0) <= 8,
    "Corner O/U Under 9.5":        lambda hg, ag, hc, ac, hy, ay: (hc or 0) + (ac or 0) <= 9,
    "Corner O/U Over 11.5":        lambda hg, ag, hc, ac, hy, ay: (hc or 0) + (ac or 0) > 11,
    "Cartellini Gialli Over 3.5":  lambda hg, ag, hc, ac, hy, ay: (hy or 0) + (ay or 0) > 3,
    "Cartellini Gialli Over 2.5":  lambda hg, ag, hc, ac, hy, ay: (hy or 0) + (ay or 0) > 2,
    "Cartellini Gialli Over 4.5":  lambda hg, ag, hc, ac, hy, ay: (hy or 0) + (ay or 0) > 4,
    "Cartellini Gialli Under 2.5": lambda hg, ag, hc, ac, hy, ay: (hy or 0) + (ay or 0) <= 2,
    "Cartellini Gialli Under 3.5": lambda hg, ag, hc, ac, hy, ay: (hy or 0) + (ay or 0) <= 3,
    "Cartellini Gialli Under 4.5": lambda hg, ag, hc, ac, hy, ay: (hy or 0) + (ay or 0) <= 4,
    # Mercati 1X2 estesi
    "1X — Doppia Chance Casa":     lambda hg, ag, hc, ac, hy, ay: hg >= ag,
    "X2 — Doppia Chance Trasferta": lambda hg, ag, hc, ac, hy, ay: ag >= hg,
    "12 — Escludi Pareggio":       lambda hg, ag, hc, ac, hy, ay: hg != ag,
}


def evaluate_pick_outcome(
    mercato: str,
    hg: int | None,
    ag: int | None,
    hc: int | None = None,
    ac: int | None = None,
    hy: int | None = None,
    ay: int | None = None,
) -> bool | None:
    """
    Valuta l'esito di un pick dato il risultato finale della partita.

    Args:
        mercato: label del mercato (es. "Over 2.5 Gol")
        hg, ag:  gol casa / trasferta (None se partita non ancora giocata)
        hc, ac:  corner casa / trasferta (None se dato non disponibile)
        hy, ay:  cartellini gialli casa / trasferta

    Returns:
        True se pick vincente, False se perdente, None se dati insufficienti
        o mercato non mappato.
    """
    if hg is None or ag is None:
        return None

    fn = OUTCOME_MAP.get(mercato)
    if fn is None:
        return None

    try:
        return bool(fn(hg, ag, hc, ac, hy, ay))
    except Exception:
        return None
