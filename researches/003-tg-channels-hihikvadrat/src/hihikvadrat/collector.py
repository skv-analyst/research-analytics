import logging
import sqlite3
import sys
from datetime import datetime, timezone

from telethon import TelegramClient

from . import config, db

logger = logging.getLogger(__name__)


def parse_message(message, sender) -> dict:
    """Map a Telethon Message (+ its resolved sender) to a DB record.

    Pure function so it can be unit-tested without hitting the network.
    """
    return {
        "message_id": message.id,
        "user_id": getattr(sender, "id", None),
        "username": getattr(sender, "username", None),
        "first_name": getattr(sender, "first_name", None),
        "last_name": getattr(sender, "last_name", None),
        "message_date": message.date.astimezone(timezone.utc).isoformat(),
        "text": message.raw_text or "",
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }


async def run_collection(conn: sqlite3.Connection, incremental: bool) -> None:
    mode = "incremental" if incremental else "backfill"
    min_id = db.get_last_message_id(conn) if incremental else None
    logger.info("collector.run_collection: starting %s run (min_id=%s)", mode, min_id)

    found = 0
    inserted = 0

    client = TelegramClient(
        config.TELEGRAM_SESSION_NAME,
        int(config.TELEGRAM_API_ID),
        config.TELEGRAM_API_HASH,
    )
    await client.connect()
    try:
        if not await client.is_user_authorized():
            if sys.stdin.isatty():
                # First-ever login: interactively ask for phone/code/2FA and
                # persist the result into the .session file.
                logger.info(
                    "collector.run_collection: no authorized session found, starting "
                    "interactive Telegram login"
                )
                await client.start()
            else:
                raise RuntimeError(
                    "collector.run_collection: Telegram session is not authorized and "
                    "no interactive terminal is available to log in. Run "
                    "`uv run python -m hihikvadrat --backfill` once from a terminal "
                    "with TTY and copy the resulting .session file here — see "
                    "README.md ('Первый логин')."
                )

        kwargs = {"search": config.SEARCH_TAG}
        if min_id:
            kwargs["min_id"] = min_id

        try:
            async for message in client.iter_messages(config.TELEGRAM_CHAT, **kwargs):
                found += 1
                try:
                    sender = await message.get_sender()
                    record = parse_message(message, sender)
                    if db.insert_intro(conn, record):
                        inserted += 1
                except Exception:
                    logger.exception(
                        "collector.run_collection: failed to process message_id=%s",
                        message.id,
                    )
        except Exception:
            logger.exception(
                "collector.run_collection: iteration over chat=%s failed",
                config.TELEGRAM_CHAT,
            )
            raise
    finally:
        await client.disconnect()

    logger.info(
        "collector.run_collection: finished %s run — found=%d inserted=%d",
        mode,
        found,
        inserted,
    )
