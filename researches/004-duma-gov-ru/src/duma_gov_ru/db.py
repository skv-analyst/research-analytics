import json
import logging
import sqlite3

logger = logging.getLogger(__name__)

SCHEMA = """
CREATE TABLE IF NOT EXISTS periods (
    id TEXT PRIMARY KEY,
    number INTEGER,
    name TEXT,
    start_date TEXT,
    end_date TEXT
);

CREATE TABLE IF NOT EXISTS votes (
    id INTEGER PRIMARY KEY,
    convocation_id TEXT,
    law_number TEXT,
    subject TEXT,
    vote_date TEXT,
    result_type TEXT,
    resolution INTEGER,
    for_count INTEGER,
    against_count INTEGER,
    abstain_count INTEGER,
    absent_count INTEGER,
    transcript_link TEXT,
    sozd_url TEXT,
    fetched_detail_at TEXT
);

CREATE TABLE IF NOT EXISTS votes_by_faction (
    vote_id INTEGER NOT NULL,
    faction_code TEXT NOT NULL,
    faction_name TEXT,
    faction_abbr TEXT,
    total INTEGER,
    for_count INTEGER,
    against_count INTEGER,
    abstain_count INTEGER,
    absent_count INTEGER,
    PRIMARY KEY (vote_id, faction_code)
);

CREATE TABLE IF NOT EXISTS votes_by_deputy (
    vote_id INTEGER NOT NULL,
    deputy_code TEXT NOT NULL,
    result TEXT,
    faction_code TEXT,
    family TEXT,
    name TEXT,
    patronymic TEXT,
    PRIMARY KEY (vote_id, deputy_code)
);

CREATE TABLE IF NOT EXISTS deputies (
    id TEXT PRIMARY KEY,
    family TEXT,
    name TEXT,
    patronymic TEXT,
    birthdate TEXT,
    credentials_start TEXT,
    credentials_end TEXT,
    faction_id TEXT,
    faction_name TEXT,
    faction_role TEXT,
    faction_region TEXT,
    is_actual INTEGER,
    regions TEXT,
    lawcount INTEGER,
    speach_count INTEGER,
    vote_link TEXT,
    transcript_link TEXT,
    educations TEXT,
    degrees TEXT,
    ranks TEXT,
    activity TEXT,
    detail_fetched_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_votes_law_number ON votes (law_number);
CREATE INDEX IF NOT EXISTS idx_votes_by_deputy_deputy ON votes_by_deputy (deputy_code);
"""

_UPSERT_PERIOD_SQL = """
INSERT INTO periods (id, number, name, start_date, end_date)
VALUES (:id, :number, :name, :start_date, :end_date)
ON CONFLICT (id) DO UPDATE SET
    number=excluded.number,
    name=excluded.name,
    start_date=excluded.start_date,
    end_date=excluded.end_date
"""

_UPSERT_VOTE_SUMMARY_SQL = """
INSERT INTO votes
    (id, subject, vote_date, result_type, resolution,
     for_count, against_count, abstain_count, absent_count)
VALUES
    (:id, :subject, :vote_date, :result_type, :resolution,
     :for_count, :against_count, :abstain_count, :absent_count)
ON CONFLICT (id) DO UPDATE SET
    subject=excluded.subject,
    vote_date=excluded.vote_date,
    result_type=excluded.result_type,
    resolution=excluded.resolution,
    for_count=excluded.for_count,
    against_count=excluded.against_count,
    abstain_count=excluded.abstain_count,
    absent_count=excluded.absent_count
"""

_UPDATE_VOTE_DETAIL_SQL = """
UPDATE votes SET
    law_number=:law_number,
    subject=:subject,
    vote_date=:vote_date,
    resolution=:resolution,
    for_count=:for_count,
    against_count=:against_count,
    abstain_count=:abstain_count,
    absent_count=:absent_count,
    transcript_link=:transcript_link,
    sozd_url=:sozd_url,
    fetched_detail_at=:fetched_detail_at
WHERE id=:id
"""

_UPSERT_DEPUTY_DETAIL_SQL = """
INSERT INTO deputies
    (id, family, name, patronymic, birthdate, credentials_start, credentials_end,
     faction_id, faction_name, faction_role, faction_region, is_actual, regions,
     lawcount, speach_count, vote_link, transcript_link, educations, degrees, ranks,
     activity, detail_fetched_at)
VALUES
    (:id, :family, :name, :patronymic, :birthdate, :credentials_start, :credentials_end,
     :faction_id, :faction_name, :faction_role, :faction_region, :is_actual, :regions,
     :lawcount, :speach_count, :vote_link, :transcript_link, :educations, :degrees, :ranks,
     :activity, :detail_fetched_at)
ON CONFLICT (id) DO UPDATE SET
    family=excluded.family,
    name=excluded.name,
    patronymic=excluded.patronymic,
    birthdate=excluded.birthdate,
    credentials_start=excluded.credentials_start,
    credentials_end=excluded.credentials_end,
    faction_id=excluded.faction_id,
    faction_name=excluded.faction_name,
    faction_role=excluded.faction_role,
    faction_region=excluded.faction_region,
    is_actual=excluded.is_actual,
    regions=excluded.regions,
    lawcount=excluded.lawcount,
    speach_count=excluded.speach_count,
    vote_link=excluded.vote_link,
    transcript_link=excluded.transcript_link,
    educations=excluded.educations,
    degrees=excluded.degrees,
    ranks=excluded.ranks,
    activity=excluded.activity,
    detail_fetched_at=excluded.detail_fetched_at
"""


def get_connection(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.executescript(SCHEMA)
    conn.commit()
    return conn


def upsert_vote_summary(conn: sqlite3.Connection, record: dict) -> None:
    """Insert/refresh a vote row from a voteSearch page — does not touch
    fetched_detail_at, so it never un-does progress made by the detail stage.
    """
    conn.execute(_UPSERT_VOTE_SUMMARY_SQL, record)
    conn.commit()


def upsert_period(conn: sqlite3.Connection, record: dict) -> None:
    conn.execute(_UPSERT_PERIOD_SQL, record)
    conn.commit()


def get_period_by_number(conn: sqlite3.Connection, number: int) -> dict | None:
    row = conn.execute(
        "SELECT id, number, name, start_date, end_date FROM periods WHERE number=?",
        (number,),
    ).fetchone()
    if row is None:
        return None
    return {
        "id": row[0],
        "number": row[1],
        "name": row[2],
        "start_date": row[3],
        "end_date": row[4],
    }


def get_vote_ids_missing_detail(conn: sqlite3.Connection) -> list[int]:
    rows = conn.execute(
        "SELECT id FROM votes WHERE fetched_detail_at IS NULL ORDER BY id"
    ).fetchall()
    return [row[0] for row in rows]


def save_vote_detail(
    conn: sqlite3.Connection,
    vote_record: dict,
    faction_records: list[dict],
    deputy_records: list[dict],
) -> None:
    """Persist a fetched vote/:id response: update the vote row and replace its
    faction/deputy breakdown rows.
    """
    conn.execute(_UPDATE_VOTE_DETAIL_SQL, vote_record)
    vote_id = vote_record["id"]
    conn.execute("DELETE FROM votes_by_faction WHERE vote_id=?", (vote_id,))
    conn.execute("DELETE FROM votes_by_deputy WHERE vote_id=?", (vote_id,))
    conn.executemany(
        """
        INSERT INTO votes_by_faction
            (vote_id, faction_code, faction_name, faction_abbr, total,
             for_count, against_count, abstain_count, absent_count)
        VALUES
            (:vote_id, :faction_code, :faction_name, :faction_abbr, :total,
             :for_count, :against_count, :abstain_count, :absent_count)
        """,
        faction_records,
    )
    conn.executemany(
        """
        INSERT INTO votes_by_deputy
            (vote_id, deputy_code, result, faction_code, family, name, patronymic)
        VALUES
            (:vote_id, :deputy_code, :result, :faction_code, :family, :name, :patronymic)
        """,
        deputy_records,
    )
    conn.commit()


def get_deputy_codes_missing_detail(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute(
        """
        SELECT DISTINCT vd.deputy_code
        FROM votes_by_deputy vd
        LEFT JOIN deputies d ON d.id = vd.deputy_code
        WHERE d.id IS NULL OR d.detail_fetched_at IS NULL
        """
    ).fetchall()
    return [row[0] for row in rows]


def upsert_deputy_detail(conn: sqlite3.Connection, record: dict) -> None:
    conn.execute(_UPSERT_DEPUTY_DETAIL_SQL, record)
    conn.commit()


def to_json_text(value) -> str:
    return json.dumps(value, ensure_ascii=False)
