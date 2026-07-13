import sys
from pathlib import Path

# Общие для всех researches настройки (пути, TG API ID/hash, TELEGRAM_CHAT) живут в корневом
# config.py репозитория. Этот проект — установленный пакет, запускается как `python -m hihikvadrat`
# из своей папки/venv, поэтому корень репозитория сам по себе не попадает в sys.path — подключаем
# его явно вместо .env/python-dotenv.
_ROOT_DIR = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(_ROOT_DIR))

import config as root_config  # noqa: E402


def _parse_chat(value: str | None):
    """Telethon only resolves a numeric chat_id if it's an int, not a numeric
    string — cast when it looks numeric. Usernames ("@name"/"name") and invite
    links are left as-is.
    """
    if value is None:
        return None
    try:
        return int(value)
    except ValueError:
        return value


TELEGRAM_API_ID = root_config.TG_USER_APP_API_ID
TELEGRAM_API_HASH = root_config.TG_USER_APP_API_HASH
TELEGRAM_CHAT = _parse_chat(root_config.TELEGRAM_CHAT)

# Сессия Telethon — общая для всех researches (см. root config.PATH_TO_SESSION),
TELEGRAM_SESSION_NAME = str(root_config.PATH_TO_SESSION / "telegram")

DB_PATH = "hihikvadrat.db"
SEARCH_TAG = "#о_себе"
LOG_LEVEL = "INFO"

_REQUIRED_VARS = {
    "TELEGRAM_API_ID": TELEGRAM_API_ID,
    "TELEGRAM_API_HASH": TELEGRAM_API_HASH,
    "TELEGRAM_CHAT": TELEGRAM_CHAT,
}


def validate() -> None:
    """Fail fast with a clear message if required config values are missing."""
    missing = [name for name, value in _REQUIRED_VARS.items() if not value]
    if missing:
        raise RuntimeError(
            "config.validate: missing required values — set TELEGRAM_USER_APP_API_ID / "
            "TELEGRAM_USER_APP_API_HASH env vars (see root config.py): "
            + ", ".join(missing)
        )
