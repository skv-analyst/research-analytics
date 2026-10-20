import logging
import math
import re
import sqlite3
import time
from datetime import datetime, timezone

from . import api_client, config, db

logger = logging.getLogger(__name__)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _to_bool(value) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("true", "1")


def _to_int(value):
    if value is None or value == "":
        return None
    return int(value)


def sozd_url(law_number: str | None) -> str | None:
    """СОЗД (sozd.duma.gov.ru) uses the bill's lawNumber directly as its URL id —
    no separate API call needed to resolve the link.
    """
    return f"https://sozd.duma.gov.ru/bill/{law_number}" if law_number else None


def _parse_convocation_number(name: str | None) -> int | None:
    match = re.match(r"\s*(\d+)", name or "")
    return int(match.group(1)) if match else None


def sync_periods(conn: sqlite3.Connection) -> None:
    """Store the full созыв reference list (id, number, name, date range) so a
    specific созыв's date range can be picked by hand before scoping the vote
    collection to it (voteSearch's own `convocation` filter doesn't work).
    """
    periods = api_client.get_periods()
    for period in periods:
        db.upsert_period(
            conn,
            {
                "id": period["id"],
                "number": _parse_convocation_number(period.get("name")),
                "name": period.get("name"),
                "start_date": period.get("startDate"),
                "end_date": period.get("endDate"),
            },
        )
    logger.info("collector.sync_periods: stored %d periods", len(periods))


def sync_votes_list(
    conn: sqlite3.Connection,
    from_date: str | None = None,
    to_date: str | None = None,
    limit: int | None = None,
) -> None:
    """Paginate voteSearch and upsert the vote skeleton rows.

    voteSearch's documented `convocation` filter does not actually restrict
    results (verified: id/number and bare number both come back with the same
    unfiltered totalCount) — but `from`/`to` do work, so scoping to one созыв
    means passing that period's date range (see periods table / --convocation
    in the CLI) rather than its id.

    `limit` caps the total number of votes fetched this call, for a quick test
    pass without pulling the whole matched range.
    """
    page = 1
    total_count = None
    seen = 0
    while total_count is None or seen < total_count:
        if limit and seen >= limit:
            break
        result = api_client.search_votes(
            page=page,
            limit=config.SEARCH_PAGE_LIMIT,
            sort="date_asc",
            from_date=from_date,
            to_date=to_date,
        )
        total_count = int(result["totalCount"])
        votes = result.get("votes", [])
        if not votes:
            break
        if limit:
            votes = votes[: max(limit - seen, 0)]
        for vote in votes:
            db.upsert_vote_summary(
                conn,
                {
                    "id": vote["id"],
                    "subject": vote.get("subject"),
                    "vote_date": vote.get("voteDate"),
                    "result_type": vote.get("resultType"),
                    "resolution": int(_to_bool(vote.get("result"))),
                    "for_count": _to_int(vote.get("forCount")),
                    "against_count": _to_int(vote.get("againstCount")),
                    "abstain_count": _to_int(vote.get("abstainCount")),
                    "absent_count": _to_int(vote.get("absentCount")),
                },
            )
        seen += len(votes)
        logger.info(
            "collector.sync_votes_list: page %d/%d, %d/%d votes",
            page,
            math.ceil(total_count / config.SEARCH_PAGE_LIMIT),
            seen,
            total_count,
        )
        page += 1


def sync_vote_details(conn: sqlite3.Connection, limit: int | None = None) -> None:
    """Fetch vote/:id for every vote missing detail — the expensive, resumable
    stage. On a per-vote failure, logs and leaves fetched_detail_at NULL so the
    next run retries it.
    """
    vote_ids = db.get_vote_ids_missing_detail(conn)
    if limit:
        vote_ids = vote_ids[:limit]
    total = len(vote_ids)
    logger.info("collector.sync_vote_details: %d votes need detail", total)

    for i, vote_id in enumerate(vote_ids, start=1):
        try:
            detail = api_client.get_vote_detail(vote_id)
            law_number = detail.get("lawNumber")
            vote_record = {
                "id": vote_id,
                "law_number": law_number,
                "subject": detail.get("subject"),
                "vote_date": detail.get("date"),
                "resolution": int(_to_bool(detail.get("resolution"))),
                "for_count": _to_int(detail.get("for")),
                "against_count": _to_int(detail.get("against")),
                "abstain_count": _to_int(detail.get("abstain")),
                "absent_count": _to_int(detail.get("absent")),
                "transcript_link": detail.get("transcriptLink"),
                "sozd_url": sozd_url(law_number),
                "fetched_detail_at": _now(),
            }
            faction_records = [
                {
                    "vote_id": vote_id,
                    "faction_code": f.get("code"),
                    "faction_name": f.get("name"),
                    "faction_abbr": f.get("abbr"),
                    "total": _to_int(f.get("total")),
                    "for_count": _to_int(f.get("for")),
                    "against_count": _to_int(f.get("against")),
                    "abstain_count": _to_int(f.get("abstain")),
                    "absent_count": _to_int(f.get("absent")),
                }
                for f in detail.get("resultsByFaction", [])
            ]
            deputy_records = [
                {
                    "vote_id": vote_id,
                    "deputy_code": d.get("code"),
                    "result": d.get("result"),
                    "faction_code": d.get("factionCode"),
                    "family": d.get("family"),
                    "name": d.get("name"),
                    "patronymic": d.get("patronymic"),
                }
                for d in detail.get("resultsByDeputy", [])
            ]
            db.save_vote_detail(conn, vote_record, faction_records, deputy_records)
        except Exception:
            logger.exception(
                "collector.sync_vote_details: failed on vote_id=%s (%d/%d)",
                vote_id,
                i,
                total,
            )

        if i % 200 == 0 or i == total:
            logger.info("collector.sync_vote_details: %d/%d done", i, total)

        time.sleep(config.REQUEST_DELAY_SECONDS)


def sync_deputy_details(conn: sqlite3.Connection) -> None:
    """Fetch deputy.json for every deputy code seen in votes_by_deputy that we
    don't have bio detail for yet.
    """
    deputy_codes = db.get_deputy_codes_missing_detail(conn)
    total = len(deputy_codes)
    logger.info("collector.sync_deputy_details: %d deputies need detail", total)

    for i, deputy_id in enumerate(deputy_codes, start=1):
        try:
            detail = api_client.get_deputy_detail(deputy_id)
            record = {
                "id": deputy_id,
                "family": detail.get("family"),
                "name": detail.get("name"),
                "patronymic": detail.get("patronymic"),
                "birthdate": detail.get("birthdate"),
                "credentials_start": detail.get("credentialsStart"),
                "credentials_end": detail.get("credentialsEnd"),
                "faction_id": detail.get("factionId"),
                "faction_name": detail.get("factionName"),
                "faction_role": detail.get("factionRole"),
                "faction_region": detail.get("factionRegion"),
                "is_actual": int(_to_bool(detail.get("isActual"))),
                "regions": db.to_json_text(detail.get("regions") or detail.get("region")),
                "lawcount": _to_int(detail.get("lawcount")),
                "speach_count": _to_int(detail.get("speachCount")),
                "vote_link": detail.get("voteLink"),
                "transcript_link": detail.get("transcriptLink"),
                "educations": db.to_json_text(detail.get("educations")),
                "degrees": db.to_json_text(detail.get("degrees")),
                "ranks": db.to_json_text(detail.get("ranks")),
                "activity": db.to_json_text(detail.get("activity")),
                "detail_fetched_at": _now(),
            }
            db.upsert_deputy_detail(conn, record)
        except Exception:
            logger.exception(
                "collector.sync_deputy_details: failed on deputy_id=%s (%d/%d)",
                deputy_id,
                i,
                total,
            )

        if i % 50 == 0 or i == total:
            logger.info("collector.sync_deputy_details: %d/%d done", i, total)

        time.sleep(config.REQUEST_DELAY_SECONDS)


def run_all(conn: sqlite3.Connection, detail_limit: int | None = None) -> None:
    sync_votes_list(conn)
    sync_vote_details(conn, limit=detail_limit)
    sync_deputy_details(conn)
