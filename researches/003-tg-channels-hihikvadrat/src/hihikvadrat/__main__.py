import argparse
import asyncio
import logging
import sys

from . import config, db
from .collector import run_collection


def _setup_logging() -> None:
    logging.basicConfig(
        level=getattr(logging, config.LOG_LEVEL.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def main() -> int:
    _setup_logging()
    logger = logging.getLogger(__name__)

    parser = argparse.ArgumentParser(
        description="Collect #о_себе intro messages from a Telegram chat into SQLite."
    )
    parser.add_argument(
        "--backfill",
        action="store_true",
        help="Full one-time backfill instead of incremental collection since the last run.",
    )
    args = parser.parse_args()

    try:
        config.validate()
    except RuntimeError as exc:
        logger.error("main: %s", exc)
        return 1

    conn = db.get_connection(config.DB_PATH)
    try:
        asyncio.run(run_collection(conn, incremental=not args.backfill))
    except Exception:
        logger.exception("main: collection run failed")
        return 1
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
