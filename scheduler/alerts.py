"""
Alert Telegram per partite live con xG.

Job: check_live_alerts() — schedulato ogni 2 minuti.

Alert inviati solo per partite "interessanti" (situazioni live):
  1. xG > gol + _XG_GAP per almeno una squadra
  2. Dangerous attacks dominance ≥ _DA_DOM_RATIO per una squadra
  3. Partita calda: min ≥ _HOT_MATCH_MIN, punteggio stretto (±1), DA dominance ≥ _HOT_DA_RATIO

Condizioni monitorate (una sola notifica per evento per partita):
  ⚽  Goal segnato          — cambio score
  🟥  Cartellino rosso     — aumento conteggio rossi
  🟡  xG dominante         — xG > gol + 1.0 (reset se la squadra segna)
  ⚠️  0-0 prolungato       — 0-0 dopo 70 minuti
  🎯  xG elevato su 0-0    — xG totale ≥ 1.8 con score 0-0 (reset se la squadra segna)
  ⚡  DA dominance          — una squadra controlla ≥ 65% degli attacchi pericolosi
  🔥  Partita calda         — 70'+, punteggio stretto, una squadra domina gli attacchi

Lo stato viene aggiornato per tutte le partite live (evita duplicati futuri),
ma gli alert vengono inviati solo per le partite interessanti.

Stato persistente: data/live_alerts_state.json
  {
    "<event_id>": {
      "score": "1-0",
      "rc_h": 0, "rc_a": 0,
      "xg_home": true,      # alert xG già inviato per la casa
      "xg_away": true,
      "zero_zero": true,
      "xg_zero": true,      # alert xG elevato su 0-0 già inviato
      "da_dom": true,       # alert DA dominance già inviato
      "hot_match": true     # alert partita calda già inviato
    }, ...
  }
Pulizia automatica delle partite non più live ad ogni run.
"""

from __future__ import annotations

import json
import logging
import time
import urllib.request
from datetime import UTC, date, datetime
from zoneinfo import ZoneInfo

from config import DATA_DIR, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID

logger = logging.getLogger(__name__)

_STATE_FILE = DATA_DIR / "live_alerts_state.json"
_DAILY_JOBS_FILE = DATA_DIR / "daily_jobs_last_run.json"
_INTERVENTIONS_FILE = DATA_DIR / "interventions.json"


def _get_daily_last_run() -> dict:
    try:
        if _DAILY_JOBS_FILE.exists():
            return json.loads(_DAILY_JOBS_FILE.read_text())
    except Exception:
        pass
    return {}


def _set_daily_last_run(key: str) -> None:
    data = _get_daily_last_run()
    data[key] = date.today().isoformat()
    try:
        _DAILY_JOBS_FILE.write_text(json.dumps(data, indent=2))
    except Exception as exc:
        logger.warning("[alerts] Salvataggio daily_jobs fallito: %s", exc)
# ── Interventi (tracking ok/ko) ────────────────────────────────────────────────


def _load_interventions() -> list:
    try:
        if _INTERVENTIONS_FILE.exists():
            return json.loads(_INTERVENTIONS_FILE.read_text())
    except Exception:
        pass
    return []


def _save_interventions(interventions: list) -> None:
    try:
        _INTERVENTIONS_FILE.write_text(json.dumps(interventions, indent=2))
    except Exception as exc:
        logger.warning("[alerts] Salvataggio interventions fallito: %s", exc)


def _record_intervention(event_id: str, alert_type: str, team: str, match: str, comp: str, minute: int) -> None:
    """Registra un nuovo intervento come pending."""
    interventions = _load_interventions()
    interventions.append({
        "event_id": event_id,
        "match": match,
        "comp": comp,
        "alert_type": alert_type,
        "team": team,
        "minute": minute,
        "date": date.today().isoformat(),
        "outcome": "pending",
    })
    _save_interventions(interventions)


def _resolve_intervention(event_id: str, alert_type: str, outcome: str) -> None:
    """Risolve il primo intervento pending per evento + tipo."""
    interventions = _load_interventions()
    for inv in interventions:
        if inv["event_id"] == event_id and inv["alert_type"] == alert_type and inv["outcome"] == "pending":
            inv["outcome"] = outcome
            break
    _save_interventions(interventions)


def _resolve_all_pending(event_id: str, outcome: str) -> None:
    """Risolve tutti gli interventi pending per un dato evento."""
    interventions = _load_interventions()
    changed = False
    for inv in interventions:
        if inv["event_id"] == event_id and inv["outcome"] == "pending":
            inv["outcome"] = outcome
            changed = True
    if changed:
        _save_interventions(interventions)


def _interventions_stats_line() -> str:
    """Riga riassuntiva degli interventi totali: ✅ N · ❌ N · ⏳ N."""
    invs = _load_interventions()
    ok  = sum(1 for i in invs if i["outcome"] == "ok")
    ko  = sum(1 for i in invs if i["outcome"] == "ko")
    pnd = sum(1 for i in invs if i["outcome"] == "pending")
    return f"📊 Interventi: ✅ {ok} · ❌ {ko} · ⏳ {pnd}"


_XG_GAP = 1.2          # xG > gol + questa soglia → alert / partita interessante
_NO_LIVE_SKIP_SEC = 600  # secondi da attendere tra check quando non ci sono partite live
_XG_ZERO_TOTAL = 1.8   # xG totale (home+away) ≥ soglia con score 0-0 → alert
_DA_DOM_RATIO = 0.65   # dangerous attacks ratio ≥ soglia → alert / interessante


# ── Telegram ───────────────────────────────────────────────────────────────────


def _send(text: str) -> None:
    """Invia un messaggio HTML via Telegram Bot API. Silenzioso in caso di errore."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = json.dumps({
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text,
            "parse_mode": "HTML",
        }).encode()
        req = urllib.request.Request(
            url, data=payload, headers={"Content-Type": "application/json"}
        )
        urllib.request.urlopen(req, timeout=10)
        logger.debug("[alerts] Telegram inviato: %s", text[:60])
    except Exception as exc:
        logger.warning("[alerts] Telegram error: %s", exc)


# ── Stato ─────────────────────────────────────────────────────────────────────


def _load_state() -> dict:
    try:
        if _STATE_FILE.exists():
            return json.loads(_STATE_FILE.read_text())
    except Exception:
        pass
    return {}


def _save_state(state: dict) -> None:
    try:
        _STATE_FILE.write_text(json.dumps(state, indent=2))
    except Exception as exc:
        logger.warning("[alerts] Salvataggio stato fallito: %s", exc)


# ── Helper parsing evento raw SofaScore ───────────────────────────────────────


def _minute(e: dict) -> int:
    """Minuto corrente dall'evento raw SofaScore (0 se non calcolabile)."""
    status_type = e.get("status", {}).get("type", "")
    if status_type == "halftime":
        return 45
    if status_type != "inprogress":
        return 0
    period_start = e.get("time", {}).get("currentPeriodStartTimestamp")
    if not period_start:
        return 0
    elapsed = (int(time.time()) - period_start) // 60
    desc = e.get("status", {}).get("description", "").lower()
    if "2nd" in desc or "second" in desc:
        return min(90, 45 + elapsed)
    if "extra" in desc or "overtime" in desc:
        return min(120, 90 + elapsed)
    return min(45, elapsed)


def _stat(alltime: dict, stat_name: str, side: str) -> float | None:
    """Estrae un valore numerico dalle statistiche ALL di una partita."""
    v = alltime.get(stat_name, {}).get(side)
    if v is None:
        return None
    try:
        return float(str(v).rstrip("%"))
    except (ValueError, TypeError):
        return None



def _send_live_alert(text: str, ev: dict | None = None) -> None:
    """Invia un alert live con intestazione standard."""
    _send(f"Intervento possibile 📶\n{text}")


def _has_prior_alert(ev: dict) -> bool:
    """True se ci sono ancora alert attivi (non risolti via follow-up) per questa partita."""
    return any(ev.get(k) for k in ("xg_home", "xg_away", "xg_zero", "da_dom"))


def _send_final_message(event_id: str, ev: dict) -> None:
    """Invia il messaggio di fine partita con esito dell'alert e risolve gli interventi pending."""
    home = ev.get("home", "?")
    away = ev.get("away", "?")
    comp = ev.get("comp", "")
    last_score = ev.get("score", "0-0")
    try:
        h_goals, a_goals = (int(x) for x in last_score.split("-"))
    except Exception:
        h_goals, a_goals = 0, 0
    score_disp = f"{h_goals}–{a_goals}"
    total_goals = h_goals + a_goals

    header = f"🏁 <b>Fine partita</b>\n{comp} · {home} {score_disp} {away}"

    if ev.get("xg_zero"):
        xg_str = f" (xG totale {ev['xg_total']:.2f})" if ev.get("xg_total") else ""
        if total_goals > 0:
            outcome = "✅ Gol arrivato"
        else:
            outcome = f"❌ Finita 0-0{xg_str}"
    elif ev.get("xg_home") and not ev.get("xg_away"):
        goals_at_alert = ev.get("xg_home_goals", 0)
        if h_goals > goals_at_alert:
            outcome = f"✅ {home} ha segnato"
        else:
            outcome = f"❌ {home} non ha segnato"
    elif ev.get("xg_away") and not ev.get("xg_home"):
        goals_at_alert = ev.get("xg_away_goals", 0)
        if a_goals > goals_at_alert:
            outcome = f"✅ {away} ha segnato"
        else:
            outcome = f"❌ {away} non ha segnato"
    elif ev.get("xg_home") and ev.get("xg_away"):
        home_goals_at_alert = ev.get("xg_home_goals", 0)
        away_goals_at_alert = ev.get("xg_away_goals", 0)
        if h_goals > home_goals_at_alert or a_goals > away_goals_at_alert:
            outcome = "✅ Gol arrivato"
        else:
            outcome = "❌ Nessun gol"
    elif ev.get("da_dom"):
        dominant = ev.get("dominant", "")
        if dominant:
            dom_won = (
                (dominant == home and h_goals > a_goals)
                or (dominant == away and a_goals > h_goals)
            )
            if dom_won:
                outcome = f"✅ {dominant} ha vinto"
            else:
                outcome = f"❌ {dominant} non ha vinto"
        else:
            return
    else:
        return  # nessun alert rilevante, non inviare nulla

    _resolve_all_pending(event_id, "ok" if outcome.startswith("✅") else "ko")
    _send(f"{header}\n{outcome}\n{_interventions_stats_line()}")


# ── Job principale ─────────────────────────────────────────────────────────────


def check_live_alerts() -> None:
    """
    Controlla tutte le partite live e invia alert Telegram per eventi nuovi.
    Perimetro: tutti i match live (qualsiasi lega). Alert xG solo per partite con hasXg=True.
    Usa una singola sessione Playwright per tutti gli eventi (efficiente).
    Se l'ultimo check non aveva partite live, salta finché non sono trascorsi 10 minuti.
    """
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.debug("[alerts] Telegram non configurato — skip")
        return

    # Throttle: se l'ultimo check era vuoto e non sono passati 10 min, salta
    state_now = _load_state()
    no_live_until = state_now.get("__no_live_until__", 0)
    if time.time() < no_live_until:
        logger.info("[alerts] Nessuna partita live al check precedente — skip (throttle 10min)")
        return

    from scrapers.sofascore import SofaScoreScraper

    _active_statuses = ("inprogress", "halftime")

    # ── Step 1: recupera tutti gli eventi live ─────────────────────────────────
    try:
        with SofaScoreScraper() as ss:
            raw_live = ss.get_live_events()
            live = [
                e for e in raw_live
                if e.get("status", {}).get("type") in _active_statuses
            ]

            if not live:
                state_now["__no_live_until__"] = time.time() + _NO_LIVE_SKIP_SEC
                _save_state(state_now)
                logger.info("[alerts] Nessuna partita live — prossimo check tra 10 min")
                return

            # Step 2: statistiche per ogni partita (stessa sessione browser)
            stats_by_id: dict[int, dict] = {}
            for e in live:
                try:
                    raw = ss.get_statistics(e["id"])
                    stats_by_id[e["id"]] = raw.get("ALL", {})
                except Exception:
                    stats_by_id[e["id"]] = {}

    except Exception:
        logger.exception("[alerts] Errore scraping live")
        return

    # ── Step 3: carica stato, invia finali per partite terminate, pulisce ────────
    state = _load_state()
    active_ids = {e["id"] for e in live}

    for eid_str, ev in state.items():
        try:
            eid_int = int(eid_str)
        except (ValueError, TypeError):
            continue
        if eid_int not in active_ids and _has_prior_alert(ev):
            _send_final_message(eid_str, ev)

    state = {k: v for k, v in state.items() if k.lstrip("-").isdigit() and int(k) in active_ids}
    _save_state(state)  # persiste subito la rimozione dei match terminati

    # ── Step 4: valuta condizioni per ogni partita ────────────────────────────
    for e in live:
        try:
            eid = e["id"]
        except (KeyError, TypeError):
            continue
        ev = state.setdefault(str(eid), {})
        try:
            comp = e.get("tournament", {}).get("name", "")
            home = e.get("homeTeam", {}).get("name", "?")
            away = e.get("awayTeam", {}).get("name", "?")
            ev["comp"] = comp
            ev["home"] = home
            ev["away"] = away
            hs = (e.get("homeScore") or {}).get("current") or 0
            as_ = (e.get("awayScore") or {}).get("current") or 0
            min_ = _minute(e)

            score = f"{hs}-{as_}"
            score_disp = f"{hs}–{as_}"
            min_tag = f"  ·  {min_}'" if min_ else ""

            # ── ⚽ Gol (aggiornamento solo se segna la squadra attesa) ──────────
            prev = ev.get("score", "")
            if prev and score != prev:
                ph, pa = (int(x) for x in prev.split("-"))
                home_scored = hs > ph
                away_scored = as_ > pa
                # home ha segnato ed era la squadra attesa
                if home_scored and (ev.get("xg_home") or ev.get("xg_zero") or ev.get("dominant") == home):
                    if ev.get("xg_home"):
                        _resolve_intervention(str(eid), "xg_home", "ok")
                    if ev.get("xg_zero"):
                        _resolve_intervention(str(eid), "xg_zero", "ok")
                    ev.pop("xg_home", None)
                    ev.pop("xg_zero", None)
                    _send(f"Aggiornamento intervento 🔄\n⚽ <b>GOAL{min_tag}</b>\n{comp}\n<b>{home}</b> {score_disp} {away}\n{_interventions_stats_line()}")  # noqa: E501
                # away ha segnato ed era la squadra attesa
                if away_scored and (ev.get("xg_away") or ev.get("xg_zero") or ev.get("dominant") == away):
                    if ev.get("xg_away"):
                        _resolve_intervention(str(eid), "xg_away", "ok")
                    if ev.get("xg_zero"):
                        _resolve_intervention(str(eid), "xg_zero", "ok")
                    ev.pop("xg_away", None)
                    ev.pop("xg_zero", None)
                    _send(f"Aggiornamento intervento 🔄\n⚽ <b>GOAL{min_tag}</b>\n{comp}\n{home} {score_disp} <b>{away}</b>\n{_interventions_stats_line()}")  # noqa: E501
            ev["score"] = score

            has_xg = bool(e.get("hasXg"))
            alltime = stats_by_id.get(eid, {})
            xg_h = _stat(alltime, "Expected goals", "home") if has_xg else None
            xg_a = _stat(alltime, "Expected goals", "away") if has_xg else None
            da_h = _stat(alltime, "Dangerous attacks", "home")
            da_a = _stat(alltime, "Dangerous attacks", "away")
            sot_h = _stat(alltime, "Shots on target", "home")
            sot_a = _stat(alltime, "Shots on target", "away")

            # DA ratio: percentuale home (0.0–1.0), None se dati assenti
            da_total = (da_h or 0) + (da_a or 0)
            da_ratio_h: float | None = (da_h / da_total) if da_total > 0 and da_h is not None and da_a is not None else None  # noqa: E501

            # SOT ratio: percentuale home (0.0–1.0), None se dati assenti o totale < 3
            sot_total = (sot_h or 0) + (sot_a or 0)
            sot_ratio_h: float | None = (sot_h / sot_total) if sot_total >= 3 and sot_h is not None and sot_a is not None else None  # noqa: E501

            has_da_dominance = (
                da_ratio_h is not None
                and (da_ratio_h >= _DA_DOM_RATIO or da_ratio_h <= 1 - _DA_DOM_RATIO)
            )
            has_sot_dominance = (
                sot_ratio_h is not None
                and (sot_ratio_h >= _DA_DOM_RATIO or sot_ratio_h <= 1 - _DA_DOM_RATIO)
            )

            # ── 🟡 xG dominante ───────────────────────────────────────────────
            # Non inviare se la partita è già decisa (perdendo di 2+ gol al 80'+)
            # Non inviare nei minuti finali del tempo regolamentare (86'–90')
            home_match_decided = as_ >= hs + 2 and min_ >= 80
            away_match_decided = hs >= as_ + 2 and min_ >= 80
            in_regular_time_endgame = 86 <= min_ <= 90

            if xg_h is not None and xg_h > hs + _XG_GAP and not ev.get("xg_home") and not home_match_decided and not in_regular_time_endgame:  # noqa: E501
                _send_live_alert(
                    f"{comp} · {home} {score_disp} {away}  ·  {min_}'\n"
                    f"🟡 xG alto {home} ({xg_h:.2f} xG ma {hs} gol) → gol {home} probabile",
                )
                ev["xg_home"] = True
                ev["xg_home_goals"] = hs
                _record_intervention(str(eid), "xg_home", home, f"{home} vs {away}", comp, min_)

            if xg_a is not None and xg_a > as_ + _XG_GAP and not ev.get("xg_away") and not away_match_decided and not in_regular_time_endgame:  # noqa: E501
                _send_live_alert(
                    f"{comp} · {home} {score_disp} {away}  ·  {min_}'\n"
                    f"🟡 xG alto {away} ({xg_a:.2f} xG ma {as_} gol) → gol {away} probabile",
                )
                ev["xg_away"] = True
                ev["xg_away_goals"] = as_
                _record_intervention(str(eid), "xg_away", away, f"{home} vs {away}", comp, min_)

            # ── 🎯 xG elevato su 0-0 ──────────────────────────────────────────
            if (
                has_xg and hs == 0 and as_ == 0
                and xg_h is not None and xg_a is not None
                and xg_h + xg_a >= _XG_ZERO_TOTAL
                and not ev.get("xg_zero")
            ):
                _send_live_alert(
                    f"🎯 <b>xG elevato su 0-0 — {xg_h:.2f} + {xg_a:.2f} = {xg_h + xg_a:.2f}</b>\n"
                    f"{comp}\n"
                    f"{home} 0–0 {away}{min_tag}",
                )
                ev["xg_zero"] = True
                ev["xg_total"] = round(xg_h + xg_a, 2)
                _record_intervention(str(eid), "xg_zero", f"{home}+{away}", f"{home} vs {away}", comp, min_)

            # ── ⚡ DA / SOT dominance ──────────────────────────────────────────
            if (has_da_dominance or has_sot_dominance) and not ev.get("da_dom") and da_ratio_h is not None:
                if da_ratio_h >= _DA_DOM_RATIO:
                    dominant, da_pct = home, int(da_ratio_h * 100)
                else:
                    dominant, da_pct = away, int((1 - da_ratio_h) * 100)
                sot_str = f" · Tiri {int(sot_h or 0)}–{int(sot_a or 0)}" if sot_ratio_h is not None else ""
                _send_live_alert(
                    f"⚡ <b>Dominio — {dominant}{min_tag}</b>\n"
                    f"{comp} · {home} {score_disp} {away}\n"
                    f"DA {da_pct}%{sot_str}",
                )
                ev["da_dom"] = True
                ev["dominant"] = dominant
                _record_intervention(str(eid), "da_dom", dominant, f"{home} vs {away}", comp, min_)

        except Exception:
            logger.exception("[alerts] Errore processing evento %d", eid)

    state.pop("__no_live_until__", None)  # ci sono partite live: torna al check ogni 2 min
    _save_state(state)
    logger.info("[alerts] check completato — %d partite live", len(live))


# ── Risultati giornalieri ───────────────────────────────────────────────────────


def send_daily_results() -> None:
    """
    Invia via Telegram i risultati dei pick di ieri (esito noto).
    Schedulato alle 08:30, dopo update_pick_outcomes delle 08:15.
    """
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.debug("[alerts] Telegram non configurato — skip daily results")
        return

    from collections import defaultdict
    from datetime import timedelta

    from db import SessionLocal
    from models.picks import Pick

    session = SessionLocal()
    try:
        yesterday = datetime.combine(date.today() - timedelta(days=1), datetime.min.time())
        today = yesterday + timedelta(days=1)
        picks = (
            session.query(Pick)
            .filter(
                Pick.esito.isnot(None),
                Pick.match_date >= yesterday,
                Pick.match_date < today,
                Pick.segnali >= 3,
            )
            .order_by(Pick.league, Pick.home_team, Pick.segnali.desc())
            .all()
        )
    finally:
        session.close()

    date_label = (date.today() - timedelta(days=1)).strftime("%a %d/%m")

    if not picks:
        _send(f"📊 <b>Risultati {date_label}</b>\n\nNessun pick da valutare.")
        _set_daily_last_run("daily_results")
        return

    from config import LEAGUES as _LEAGUES

    by_league: dict[str, dict[str, list]] = defaultdict(lambda: defaultdict(list))
    for p in picks:
        match_key = f"{p.home_team} – {p.away_team}"
        league_name = _LEAGUES.get(p.league, {}).get("name", p.league)
        by_league[league_name][match_key].append(p)

    _MIN_QUOTA = 1.50  # coerente con _MIN_STAKE_QUOTA degli altri moduli

    lines = [f"📊 <b>Risultati {date_label}</b>"]
    vinti, totale, netto = 0, 0, 0.0
    for league in sorted(by_league):
        lines.append(f"  <i>{league}</i>")
        for match_label, mps in sorted(by_league[league].items()):
            lines.append(f"  ⚽ <b>{match_label}</b>")
            for p in sorted(mps, key=lambda x: -x.segnali):
                icon = "✅" if p.esito else "❌"
                quota_str = f"@{p.quota:.2f}" if p.quota else "@—"
                seg_str = "★" * p.segnali
                puntabile = p.quota and p.quota >= _MIN_QUOTA
                mon_str = "" if puntabile else "  <i>(monit.)</i>"
                lines.append(f"    {icon} {p.mercato}  {quota_str}  {seg_str}{mon_str}")
                if puntabile:
                    totale += 1
                    if p.esito:
                        vinti += 1
                        netto += p.quota - 1
                    else:
                        netto -= 1

    netto_str = f"+{netto:.2f}" if netto >= 0 else f"{netto:.2f}"
    lines.append(f"\n<b>{vinti}/{totale} ✅  —  {netto_str} unità</b>")
    _send("\n".join(lines))
    _set_daily_last_run("daily_results")
    logger.info("[alerts] Daily results inviati: %d pick (%d vinti)", totale, vinti)


# ── Pick giornalieri ────────────────────────────────────────────────────────────

_MIN_STAKE_QUOTA = 1.50  # sotto questa quota: solo monitoraggio


def _stake_pct(segnali: int) -> int:
    """Percentuale bankroll suggerita in base al numero di segnali."""
    if segnali >= 5:
        return 3
    if segnali == 4:
        return 2
    return 1  # 3 segnali


def send_daily_picks() -> None:
    """
    Invia via Telegram i pick pendenti a partire da oggi, raggruppati per data e lega.
    Schedulato ogni mattina dopo generate_picks_all_leagues().
    Invia messaggi multipli se il contenuto supera il limite Telegram (4096 char).
    """
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.debug("[alerts] Telegram non configurato — skip daily picks")
        return

    from collections import defaultdict

    from db import SessionLocal
    from models.picks import Pick

    session = SessionLocal()
    try:
        from datetime import timedelta
        today = datetime.combine(date.today(), datetime.min.time())
        tomorrow = today + timedelta(days=1)
        picks = (
            session.query(Pick)
            .filter(
                Pick.esito.is_(None),
                Pick.match_date >= today,
                Pick.match_date < tomorrow,
                Pick.segnali >= 3,
            )
            .order_by(Pick.match_date, Pick.league, Pick.home_team, Pick.segnali.desc())
            .all()
        )
    finally:
        session.close()

    if not picks:
        logger.info("[alerts] Nessun pick pendente da inviare")
        _send("📅 <b>Pick del giorno</b>\n\nNessun pick disponibile.")
        _set_daily_last_run("daily_picks")
        return

    from config import LEAGUES as _LEAGUES

    def _league_name(key: str) -> str:
        return _LEAGUES.get(key, {}).get("name", key)

    # Raggruppa per lega → partita
    by_league: dict[str, dict[str, list]] = defaultdict(lambda: defaultdict(list))
    for p in picks:
        match_key = f"{p.home_team} – {p.away_team}"
        by_league[_league_name(p.league)][match_key].append(p)

    _tz_rome = ZoneInfo("Europe/Rome")

    def _build_league_block(league: str, matches: dict) -> list[str]:
        block: list[str] = [f"\n<i>{league}</i>"]
        for match_label, mps in sorted(matches.items()):
            kickoff = mps[0].match_date.replace(tzinfo=UTC).astimezone(_tz_rome).strftime("%H:%M")
            block.append(f"⚽ <b>{match_label}</b>  ({kickoff})")
            for p in sorted(mps, key=lambda x: -x.segnali):
                quota_str = f"{p.quota:.2f}" if p.quota else "—"
                seg_str = "★" * p.segnali
                if p.quota and p.quota >= _MIN_STAKE_QUOTA:
                    pct = _stake_pct(p.segnali)
                    stake_str = f"→ {pct}% bankroll"
                else:
                    stake_str = "→ quota da multipla"
                block.append(f"• {p.mercato}  @{quota_str}  {seg_str}  {stake_str}")
        return block

    # Costruisce messaggi rispettando il limite di 4096 caratteri
    _MAX = 4000  # margine di sicurezza
    header = "📅 <b>Pick del giorno</b>"
    current_lines: list[str] = [header]
    msg_count = 0

    for league in sorted(by_league):
        league_block = _build_league_block(league, by_league[league])
        block_text = "\n".join(league_block)
        candidate = "\n".join(current_lines) + block_text
        if len(candidate) > _MAX and len(current_lines) > 1:
            _send("\n".join(current_lines))
            msg_count += 1
            current_lines = ["📅 <b>Pick del giorno</b> (continua)"] + league_block
        else:
            current_lines.extend(league_block)

    if current_lines:
        _send("\n".join(current_lines))
        msg_count += 1

    _set_daily_last_run("daily_picks")
    logger.info("[alerts] Daily picks inviati: %d pick in %d messaggi", len(picks), msg_count)
