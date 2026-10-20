import argparse
import logging
import sys

from . import collector, config, db


def _setup_logging() -> None:
    logging.basicConfig(
        level=getattr(logging, config.LOG_LEVEL.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def main() -> int:
    _setup_logging()
    logger = logging.getLogger(__name__)

    parser = argparse.ArgumentParser(
        description="Collect State Duma roll-call votes (all созывы) into SQLite."
    )
    parser.add_argument(
        "--stage",
        choices=["all", "periods", "votes", "details", "deputies"],
        default="all",
        help="Which stage to run. All stages are resumable/idempotent, so re-running "
        "'all' after an interrupted run just picks up where it left off.",
    )
    parser.add_argument(
        "--detail-limit",
        type=int,
        default=None,
        help="Cap the number of vote details fetched this run (useful for a quick test pass).",
    )
    parser.add_argument(
        "--convocation",
        type=int,
        default=None,
        help="For --stage votes: scope to this созыв's date range (looked up from the "
        "periods table — run --stage periods first). voteSearch's own convocation "
        "filter doesn't work, so this passes from/to dates instead.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="For --stage votes: cap the total number of votes fetched this run "
        "(quick test pass without pulling the whole matched range).",
    )
    args = parser.parse_args()

    try:
        config.validate()
    except RuntimeError as exc:
        logger.error("main: %s", exc)
        return 1

    conn = db.get_connection(config.DB_PATH)
    try:
        if args.stage == "all":
            collector.run_all(conn, detail_limit=args.detail_limit)
        elif args.stage == "periods":
            collector.sync_periods(conn)
        elif args.stage == "votes":
            from_date = to_date = None
            if args.convocation is not None:
                period = db.get_period_by_number(conn, args.convocation)
                if period is None:
                    logger.error(
                        "main: no period found for созыв %d — run --stage periods first",
                        args.convocation,
                    )
                    return 1
                from_date = period["start_date"].split(" ")[0]
                to_date = period["end_date"].split(" ")[0]
            collector.sync_votes_list(
                conn, from_date=from_date, to_date=to_date, limit=args.limit
            )
        elif args.stage == "details":
            collector.sync_vote_details(conn, limit=args.detail_limit)
        elif args.stage == "deputies":
            collector.sync_deputy_details(conn)
    except Exception:
        logger.exception("main: collection run failed")
        return 1
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
