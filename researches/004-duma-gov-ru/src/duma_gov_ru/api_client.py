"""Thin client for api.duma.gov.ru — see researches/004-duma-gov-ru/api/*.html for the
saved reference docs this is based on.

JSON responses are unwrapped (no top-level "result" key, unlike XML) but return most
numbers as strings — callers normalize types themselves.
"""

import logging

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from . import config

logger = logging.getLogger(__name__)

_session: requests.Session | None = None


def _get_session() -> requests.Session:
    global _session
    if _session is None:
        session = requests.Session()
        retry = Retry(
            total=5,
            backoff_factor=1.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        session.mount("http://", HTTPAdapter(max_retries=retry))
        session.mount("https://", HTTPAdapter(max_retries=retry))
        _session = session
    return _session


def _request(request_name: str, params: dict | None = None) -> dict | list:
    url = f"{config.BASE_URL}/{config.DUMA_API_KEY}/{request_name}.json"
    query = {"app_token": config.DUMA_APP_KEY, **(params or {})}
    response = _get_session().get(url, params=query, timeout=config.REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()
    return response.json()


def get_periods() -> list[dict]:
    """List of созывы/сессии. Each period: id, name, startDate, endDate, sessions[].
    Not currently used to scope collection — see search_votes.
    """
    return _request("periods")


def search_votes(
    page: int,
    limit: int = 100,
    sort: str = "date_asc",
    from_date: str | None = None,
    to_date: str | None = None,
) -> dict:
    """One page of votes. Returns dict with totalCount, page, pageSize, votes[].

    voteSearch's documented `convocation` filter does not actually restrict
    results on the live API (verified against both the full period id and the
    bare созыв number — totalCount/rows were identical and unfiltered either
    way). `from`/`to` (vote date bounds) do work — verified against the 8th
    созыв's period range, totalCount came back as exactly 16689 — so that's
    how to scope to one созыв instead.
    """
    params = {"page": page, "limit": limit, "sort": sort}
    if from_date:
        params["from"] = from_date
    if to_date:
        params["to"] = to_date
    return _request("voteSearch", params)


def get_vote_detail(vote_id: int) -> dict:
    """Full detail for one vote: date, lawNumber, subject, resolution, counts,
    transcriptLink, resultsByFaction[], resultsByDeputy[].
    """
    return _request(f"vote/{vote_id}")


def get_deputy_detail(deputy_id: str) -> dict:
    """Full bio for one deputy: faction, region(s), education, degrees, ranks, activity."""
    return _request("deputy", {"id": deputy_id})
