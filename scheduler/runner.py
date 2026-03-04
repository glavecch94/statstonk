"""
Scheduler principale — avvia APScheduler e registra i job.

Uso:
    python scheduler/runner.py          # avvia lo scheduler (gira in continuo)
    python scheduler/runner.py --now    # esegue subito il sync e poi avvia lo scheduler
    python scheduler/runner.py --once   # esegue il sync una volta sola ed esce

Schedule di default:
    - Ogni giorno alle 08:00 (Europe/Rome)
      Copre le partite del giorno precedente (sera/notte italiana).
    - Ogni giorno alle 23:30 (Europe/Rome)
      Passata sufficiente di solito per avere i risultati delle partite serali.
"""

from __future__ import annotations

import logging
import sys

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from db import init_db
from scheduler.alerts import (
    _get_daily_last_run,
    check_live_alerts,
    send_daily_picks,
    send_daily_results,
)
from scheduler.jobs import (
    generate_picks_all_leagues,
    sync_all_leagues,
    sync_historical_all,
    update_lineups,
    update_pick_outcomes,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

TIMEZONE = "Europe/Rome"


def _register_jobs(scheduler) -> None:
    """Registra tutti i job su un'istanza scheduler (blocking o background)."""

    # Mattina: cattura partite del pomeriggio/sera precedente (tutte le leghe)
    scheduler.add_job(
        sync_all_leagues,
        CronTrigger(hour=8, minute=0, timezone=TIMEZONE),
        id="sync_morning",
        name="Sync mattina (tutte le leghe)",
        misfire_grace_time=3600,  # se il sistema era spento, esegue entro 1h dal trigger
        replace_existing=True,
    )

    # Mattina: aggiorna esiti pick dopo il sync
    scheduler.add_job(
        update_pick_outcomes,
        CronTrigger(hour=8, minute=15, timezone=TIMEZONE),
        id="update_picks_morning",
        name="Aggiorna esiti pick (mattina)",
        misfire_grace_time=3600,
        replace_existing=True,
    )

    # Sera tardi: cattura partite infrasettimanali finali (tutte le leghe)
    scheduler.add_job(
        sync_all_leagues,
        CronTrigger(hour=23, minute=30, timezone=TIMEZONE),
        id="sync_evening",
        name="Sync sera (tutte le leghe)",
        misfire_grace_time=3600,
        replace_existing=True,
    )

    # Sera tardi: aggiorna esiti pick dopo il sync serale
    scheduler.add_job(
        update_pick_outcomes,
        CronTrigger(hour=23, minute=45, timezone=TIMEZONE),
        id="update_picks_evening",
        name="Aggiorna esiti pick (sera)",
        misfire_grace_time=3600,
        replace_existing=True,
    )

    # Mattina: genera picks per tutte le leghe (dopo sync e aggiornamento esiti)
    scheduler.add_job(
        generate_picks_all_leagues,
        CronTrigger(hour=9, minute=0, timezone=TIMEZONE),
        id="generate_picks_morning",
        name="Genera picks tutte le leghe (mattina)",
        misfire_grace_time=3600,
        replace_existing=True,
    )

    # Pomeriggio: rigenera picks (quote SofaScore aggiornate nel corso della giornata)
    scheduler.add_job(
        generate_picks_all_leagues,
        CronTrigger(hour=17, minute=0, timezone=TIMEZONE),
        id="generate_picks_afternoon",
        name="Genera picks tutte le leghe (pomeriggio)",
        misfire_grace_time=3600,
        replace_existing=True,
    )

    # Formazioni SofaScore: ogni 30 minuti
    scheduler.add_job(
        update_lineups,
        IntervalTrigger(minutes=30, timezone=TIMEZONE),
        id="update_lineups",
        name="Aggiorna formazioni SofaScore",
        misfire_grace_time=600,
        replace_existing=True,
    )

    # Alert live Telegram: ogni 3 minuti
    scheduler.add_job(
        check_live_alerts,
        IntervalTrigger(minutes=3, timezone=TIMEZONE),
        id="live_alerts",
        name="Alert live Telegram",
        misfire_grace_time=60,
        replace_existing=True,
    )

    # Risultati di ieri Telegram: alle 8:30 (dopo update_pick_outcomes delle 8:15)
    scheduler.add_job(
        send_daily_results,
        CronTrigger(hour=8, minute=30, timezone=TIMEZONE),
        id="daily_results_telegram",
        name="Risultati pick ieri Telegram",
        misfire_grace_time=1800,
        replace_existing=True,
    )

    # Pick giornalieri Telegram: alle 9:30 (dopo generate_picks alle 9:00)
    scheduler.add_job(
        send_daily_picks,
        CronTrigger(hour=9, minute=30, timezone=TIMEZONE),
        id="daily_picks_telegram",
        name="Pick giornalieri Telegram",
        misfire_grace_time=1800,
        replace_existing=True,
    )


def _run_missed_daily_jobs() -> None:
    """
    Esegue subito i job giornalieri che non sono stati eseguiti oggi.
    Chiamata all'avvio dello scheduler per recuperare esecuzioni perse (es. Mac spento).

    - daily_results: inviato se ora >= 08:30 e non già eseguito oggi
    - daily_picks:   inviato se ora >= 09:30 e non già eseguito oggi
    """
    from datetime import datetime as dt
    now = dt.now()
    today = now.date().isoformat()
    last_run = _get_daily_last_run()

    if last_run.get("daily_results") != today and now.hour * 60 + now.minute >= 8 * 60 + 30:
        logger.info("Startup: daily_results non inviato oggi — invio ora.")
        send_daily_results()

    if last_run.get("daily_picks") != today and now.hour * 60 + now.minute >= 9 * 60 + 30:
        logger.info("Startup: daily_picks non inviato oggi — invio ora.")
        send_daily_picks()


def build_scheduler() -> BlockingScheduler:
    """Scheduler bloccante per uso standalone da CLI."""
    scheduler = BlockingScheduler(timezone=TIMEZONE)
    _register_jobs(scheduler)
    return scheduler


def start_background_scheduler() -> BackgroundScheduler:
    """
    Avvia uno scheduler non bloccante da usare all'interno della dashboard Streamlit.
    Usa @st.cache_resource per garantire un'unica istanza per processo.

    Esegue generate_picks_all_leagues() in un thread separato al primo avvio,
    così i pick sono disponibili subito senza aspettare il prossimo job schedulato.
    Il rate limit di 7s su api.football-data.org garantisce di restare entro 10 req/min.
    """
    import threading

    init_db()
    scheduler = BackgroundScheduler(timezone=TIMEZONE)
    _register_jobs(scheduler)
    scheduler.start()
    logger.info("Background scheduler avviato dalla dashboard.")

    # Genera picks subito in background (non blocca il caricamento della UI)
    t = threading.Thread(target=generate_picks_all_leagues, daemon=True, name="picks-init")
    t.start()

    # Invia in background i messaggi giornalieri persi (es. Mac era spento)
    t2 = threading.Thread(target=_run_missed_daily_jobs, daemon=True, name="missed-daily")
    t2.start()

    return scheduler


def main() -> None:
    args = set(sys.argv[1:])
    init_db()

    if "--once" in args:
        logger.info("Modalità --once: eseguo sync, picks e uscita.")
        sync_all_leagues()
        update_pick_outcomes()
        generate_picks_all_leagues()
        return

    if "--historical" in args:
        logger.info("Modalità --historical: carica dati storici da SofaScore ed esce.")
        sync_historical_all(include_stats_seasons=2)
        return

    scheduler = build_scheduler()
    _run_missed_daily_jobs()

    if "--now" in args:
        logger.info("Modalità --now: eseguo sync immediato prima di avviare lo scheduler.")
        sync_all_leagues()
        update_pick_outcomes()
        generate_picks_all_leagues()

    logger.info("Scheduler avviato. Job registrati:")
    for job in scheduler.get_jobs():
        next_run = getattr(job, "next_run_time", None) or "calcolata all'avvio"
        logger.info(f"  • {job.name} → prossima esecuzione: {next_run}")

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler fermato.")


if __name__ == "__main__":
    main()
