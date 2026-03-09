"""
Calibrazione automatica dei segnali basata sulle performance storiche dei pick.

Dopo ogni ciclo di aggiornamento esiti, analizza i pick completati e:
- Calcola hit rate e ROI per mercato e per campionato
- Genera soglie minime di segnali per mercato (aumentate se la performance è scadente)
- Identifica mercati da bloccare (performance consistentemente negativa)
- Salva i risultati in data/calibration.json

La calibrazione viene letta da save_picks() per filtrare i pick in uscita.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

_CALIB_PATH = Path("data/calibration.json")

# Soglie per la correzione dinamica (basata su performance storiche)
_MIN_STAKE_QUOTA = 1.50     # sotto questa quota: pick "solo monitoraggio", escluso dalle stats
_MIN_PICKS_FOR_CALIB = 15   # minimo per applicare +1 segnale dinamico
_MIN_PICKS_FOR_BLOCK = 25   # minimo per bloccare un mercato
_HR_WARN = 0.40             # sotto questa soglia: +1 segnale rispetto alla base
_HR_BLOCK = 0.27            # sotto questa soglia (con ≥25 pick): blocco
_ROI_BLOCK = -0.22          # ROI medio sotto −22% con ≥25 pick: blocco

# ── Difficoltà base per mercato ───────────────────────────────────────────────
# Segnali minimi pre-calibrazione dinamica.
# Riflette quanto ogni mercato sia intrinsecamente difficile da prevedere
# con indicatori statistici di forma.
#
# 2 = standard (mercati goal/BTTS con ampio segnale statistico)
# 3 = difficile (1X2 secco, mercati estremi, corner)
# 4 = molto difficile (cartellini, pareggio)

MARKET_BASE_MIN_SIGNALS: dict[str, int] = {
    # ── Risultato 1X2 ──────────────────────────────────────────────────────────
    "1 — Vittoria Casa":            3,   # dipende da matchup, forma volatile
    "X — Pareggio":                 4,   # mercato altamente imprevedibile
    "2 — Vittoria Trasferta":       3,
    "1X — Doppia Chance Casa":      3,
    "X2 — Doppia Chance Trasferta": 3,
    "12 — Escludi Pareggio":        2,
    # ── Goal totali ────────────────────────────────────────────────────────────
    "Over 1.5 Gol":                 2,
    "Over 2.5 Gol":                 2,
    "Over 3.5 Gol":                 3,   # evento raro, richiede evidenza forte
    "Under 1.5 Gol":                3,   # evento raro
    "Under 2.5 Gol":                2,
    "Under 3.5 Gol":                2,
    # ── BTTS ───────────────────────────────────────────────────────────────────
    "Goal — Entrambe Segnano":      2,
    "No Goal — Solo una Segna":     3,
    # ── Corner (alta varianza, poca correlazione con la forma) ─────────────────
    "Corner O/U Over 9.5":          3,
    "Corner O/U Over 10.5":         3,
    "Corner O/U Over 11.5":         4,
    "Corner O/U Under 8.5":         4,
    "Corner O/U Under 9.5":         3,
    # ── Cartellini (molto rumorosi, dipendono dall'arbitro) ────────────────────
    "Cartellini Gialli Over 2.5":   4,
    "Cartellini Gialli Over 3.5":   4,
    "Cartellini Gialli Over 4.5":   4,
    "Cartellini Gialli Under 2.5":  4,
    "Cartellini Gialli Under 3.5":  4,
    "Cartellini Gialli Under 4.5":  4,
    # ── Handicap ───────────────────────────────────────────────────────────────
    "Handicap −1 Casa":             3,
    "Handicap +1 Trasferta":        3,
}


# ── Calcolo statistiche ───────────────────────────────────────────────────────


def compute_market_stats() -> dict[str, dict]:
    """
    Calcola statistiche per ogni mercato sui pick completati.
    Ritorna dict {mercato: {n, won, lost, hr, roi, avg_quota}}.
    """
    from db import SessionLocal
    from models.picks import Pick

    session = SessionLocal()
    try:
        completed = session.query(Pick).filter(Pick.esito.isnot(None)).all()
    finally:
        session.close()

    raw: dict[str, dict] = {}
    for p in completed:
        if not p.quota or p.quota < _MIN_STAKE_QUOTA:
            continue  # escludi pick non puntabili dalle statistiche
        if p.segnali < 3:
            continue  # soglia minima di segnali per le statistiche
        m = p.mercato
        if m not in raw:
            raw[m] = {"n": 0, "won": 0, "roi_sum": 0.0, "quota_sum": 0.0}
        s = raw[m]
        s["n"] += 1
        if p.esito is True:
            s["won"] += 1
            s["roi_sum"] += p.quota - 1
        else:
            s["roi_sum"] -= 1.0
        s["quota_sum"] += p.quota

    result: dict[str, dict] = {}
    for m, s in raw.items():
        n = s["n"]
        hr = s["won"] / n if n > 0 else 0.0
        roi = s["roi_sum"] / n if n > 0 else 0.0
        avg_q = s["quota_sum"] / n if n > 0 else 0.0
        result[m] = {
            "n": n,
            "won": s["won"],
            "lost": n - s["won"],
            "hr": round(hr, 4),
            "roi": round(roi, 4),
            "avg_quota": round(avg_q, 4),
        }
    return result


def compute_league_stats() -> dict[str, dict]:
    """Calcola statistiche di hit rate e ROI per campionato."""
    from db import SessionLocal
    from models.picks import Pick

    session = SessionLocal()
    try:
        completed = session.query(Pick).filter(Pick.esito.isnot(None)).all()
    finally:
        session.close()

    raw: dict[str, dict] = {}
    for p in completed:
        if not p.quota or p.quota < _MIN_STAKE_QUOTA:
            continue  # escludi pick non puntabili dalle statistiche
        if p.segnali < 3:
            continue  # soglia minima di segnali per le statistiche
        lg = p.league
        if lg not in raw:
            raw[lg] = {"n": 0, "won": 0, "roi_sum": 0.0}
        s = raw[lg]
        s["n"] += 1
        if p.esito is True:
            s["won"] += 1
            s["roi_sum"] += p.quota - 1
        else:
            s["roi_sum"] -= 1.0

    return {
        lg: {
            "n": s["n"],
            "won": s["won"],
            "hr": round(s["won"] / s["n"], 4) if s["n"] > 0 else 0.0,
            "roi": round(s["roi_sum"] / s["n"], 4) if s["n"] > 0 else 0.0,
        }
        for lg, s in raw.items()
    }


# ── Statistiche segnale età ───────────────────────────────────────────────────


def compute_age_signal_stats() -> dict:
    """
    Calcola le performance dei pick aggiustati dal segnale età.
    Legge age_adjusted_matches.json e incrocia con i pick completati nel DB.

    Ritorna:
    {
        "boosted":  {n, won, hr, roi},   # pick +1 (squadra senior)
        "penalized": {n, won, hr, roi},  # pick -1 (squadra giovane)
        "matches": N,                    # partite con age signal
    }
    """
    import json as _json

    from config import DATA_DIR as _DATA_DIR
    from db import SessionLocal
    from models.picks import Pick

    age_file = _DATA_DIR / "age_adjusted_matches.json"
    if not age_file.exists():
        return {}

    try:
        raw = _json.loads(age_file.read_text())
        if isinstance(raw, list):
            return {}  # vecchio formato, non ancora migrabile
    except Exception:
        return {}

    # Costruisce set di (home, away, date_str, senior) per lookup rapido
    age_matches: list[dict] = list(raw.values())
    if not age_matches:
        return {}

    session = SessionLocal()
    try:
        completed = session.query(Pick).filter(Pick.esito.isnot(None)).all()
    finally:
        session.close()

    # Indice per lookup: (home, away, date[:10]) → entry
    idx: dict[tuple, dict] = {}
    for entry in age_matches:
        k = (entry.get("home", ""), entry.get("away", ""), entry.get("date", ""))
        idx[k] = entry

    _MIN_Q = _MIN_STAKE_QUOTA
    boosted   = {"n": 0, "won": 0, "roi_sum": 0.0}
    penalized = {"n": 0, "won": 0, "roi_sum": 0.0}

    for p in completed:
        if not p.quota or p.quota < _MIN_Q or p.segnali < 3:
            continue
        date_str = p.match_date.strftime("%Y-%m-%d") if p.match_date else ""
        entry = idx.get((p.home_team, p.away_team, date_str))
        if not entry:
            continue

        senior = entry.get("senior", "")
        _AGE_H = {"1 — Vittoria Casa", "1X — Doppia Chance Casa", "Handicap −1 Casa"}
        _AGE_A = {"2 — Vittoria Trasferta", "X2 — Doppia Chance Trasferta", "Handicap +1 Trasferta"}

        home_is_senior = senior == p.home_team
        if p.mercato in _AGE_H:
            bucket = boosted if home_is_senior else penalized
        elif p.mercato in _AGE_A:
            bucket = boosted if not home_is_senior else penalized
        else:
            continue  # mercato non direzionale, non tracciato

        bucket["n"] += 1
        if p.esito:
            bucket["won"] += 1
            bucket["roi_sum"] += p.quota - 1
        else:
            bucket["roi_sum"] -= 1.0

    def _fmt(b: dict) -> dict:
        n = b["n"]
        return {
            "n": n,
            "won": b["won"],
            "hr": round(b["won"] / n, 4) if n > 0 else 0.0,
            "roi": round(b["roi_sum"] / n, 4) if n > 0 else 0.0,
        }

    return {
        "matches": len(age_matches),
        "boosted": _fmt(boosted),
        "penalized": _fmt(penalized),
    }


# ── Calcolo aggiustamenti ─────────────────────────────────────────────────────


def _compute_adjustments(
    market_stats: dict[str, dict],
) -> tuple[dict[str, int], list[str]]:
    """
    Dato il dict di statistiche per mercato, calcola:
    - market_min_segnali: {mercato → soglia dinamica}  (già incorpora la base)
    - blocked_markets: lista mercati da escludere dai pick

    La soglia dinamica parte sempre dalla base di MARKET_BASE_MIN_SIGNALS,
    e cresce ulteriormente se le performance storiche sono scadenti.
    """
    market_min_segnali: dict[str, int] = {}
    blocked_markets: list[str] = []

    for m, s in market_stats.items():
        n, hr, roi = s["n"], s["hr"], s["roi"]
        base = MARKET_BASE_MIN_SIGNALS.get(m, 2)

        if n >= _MIN_PICKS_FOR_BLOCK and (hr < _HR_BLOCK or roi < _ROI_BLOCK):
            blocked_markets.append(m)
            logger.warning(
                "Calibrazione: mercato '%s' BLOCCATO (n=%d, HR=%.0f%%, ROI=%.1f%%)",
                m, n, hr * 100, roi * 100,
            )
        elif n >= _MIN_PICKS_FOR_CALIB and hr < _HR_WARN:
            extra = 1 if (_HR_WARN - hr) < 0.12 else 2
            dynamic = base + extra
            market_min_segnali[m] = dynamic
            logger.info(
                "Calibrazione: mercato '%s' → min_segnali=%d (base=%d +%d) "
                "(n=%d, HR=%.0f%%, ROI=%.1f%%)",
                m, dynamic, base, extra, n, hr * 100, roi * 100,
            )

    return market_min_segnali, blocked_markets


def effective_min_signals(mercato: str, calib: dict | None = None) -> int:
    """
    Restituisce la soglia effettiva di segnali minimi per un mercato,
    combinando la difficoltà base statica e l'aggiustamento dinamico.

    Priorità: max(base_statica, aggiustamento_dinamico)
    """
    base = MARKET_BASE_MIN_SIGNALS.get(mercato, 2)
    if calib:
        dynamic = calib.get("market_min_segnali", {}).get(mercato, base)
        return max(base, dynamic)
    return base


# ── Salvataggio / caricamento ─────────────────────────────────────────────────


def save_calibration() -> dict:
    """
    Calcola le statistiche sui pick completati, determina gli aggiustamenti
    e salva il tutto in data/calibration.json.
    Ritorna il dict salvato.
    """
    _CALIB_PATH.parent.mkdir(parents=True, exist_ok=True)

    market_stats = compute_market_stats()
    league_stats = compute_league_stats()
    age_signal_stats = compute_age_signal_stats()
    market_min_segnali, blocked_markets = _compute_adjustments(market_stats)

    calib = {
        "updated_at": datetime.utcnow().isoformat(),
        "markets": market_stats,
        "leagues": league_stats,
        "age_signal": age_signal_stats,
        "market_min_segnali": market_min_segnali,
        "blocked_markets": blocked_markets,
    }

    with open(_CALIB_PATH, "w", encoding="utf-8") as f:
        json.dump(calib, f, ensure_ascii=False, indent=2)

    logger.info(
        "Calibrazione salvata: %d mercati analizzati, %d con soglia elevata, %d bloccati",
        len(market_stats),
        len(market_min_segnali),
        len(blocked_markets),
    )
    return calib


def load_calibration() -> dict:
    """Carica la calibrazione da file. Ritorna dict vuoto se non esiste o non leggibile."""
    if not _CALIB_PATH.exists():
        return {}
    try:
        with open(_CALIB_PATH, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}
