import logging
import sqlite3

logger = logging.getLogger(__name__)

SCHEMA = """
CREATE TABLE IF NOT EXISTS intros (
    message_id INTEGER PRIMARY KEY,
    user_id INTEGER,
    username TEXT,
    first_name TEXT,
    last_name TEXT,
    message_date TEXT NOT NULL,
    text TEXT NOT NULL,
    fetched_at TEXT NOT NULL,
    -- placeholders for a future classification/segmentation pass
    level TEXT,
    stack TEXT,
    projects TEXT
);
"""

_INSERT_SQL = """
INSERT OR IGNORE INTO intros
    (message_id, user_id, username, first_name, last_name, message_date, text, fetched_at)
VALUES
    (:message_id, :user_id, :username, :first_name, :last_name, :message_date, :text, :fetched_at)
"""


def get_connection(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute(SCHEMA)
    conn.commit()
    return conn


def insert_intro(conn: sqlite3.Connection, record: dict) -> bool:
    """Insert a record, deduplicating by message_id. Returns True if a new row was inserted."""
    cursor = conn.execute(_INSERT_SQL, record)
    conn.commit()
    inserted = cursor.rowcount > 0
    if not inserted:
        logger.debug("db.insert_intro: skipped duplicate message_id=%s", record.get("message_id"))
    return inserted


def get_last_message_id(conn: sqlite3.Connection) -> int | None:
    row = conn.execute("SELECT MAX(message_id) FROM intros").fetchone()
    return row[0] if row else None
