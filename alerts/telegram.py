from __future__ import annotations

import logging

from telegram import Bot
from telegram.constants import ParseMode

from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID

logger = logging.getLogger(__name__)


async def send_alert(message: str) -> None:
    """
    Invia un messaggio HTML al chat Telegram configurato.

    Esempio di messaggio:
        "<b>⚽ ALERT LIVE</b>\n"
        "Inter vs Milan — 67'\n"
        "xG gap: 2.1 vs 0.4\n"
        "Quota goal prossimo: 1.85 (Bet365)"
    """
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("Telegram non configurato — alert saltato.")
        return

    bot = Bot(token=TELEGRAM_BOT_TOKEN)
    async with bot:
        await bot.send_message(
            chat_id=TELEGRAM_CHAT_ID,
            text=message,
            parse_mode=ParseMode.HTML,
        )
    logger.info("Alert Telegram inviato.")
