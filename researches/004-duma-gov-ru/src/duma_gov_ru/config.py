from pathlib import Path
import os

from dotenv import load_dotenv

# Unlike the shared Telegram creds in the repo root config.py, the Duma API keys
# are only used by this project — kept in a project-local .env instead.
_ROOT_DIR = Path(__file__).resolve().parents[2]
load_dotenv(_ROOT_DIR / ".env")

DUMA_API_KEY = os.getenv("DUMA_API_KEY")
DUMA_APP_KEY = os.getenv("DUMA_APP_KEY")

BASE_URL = "http://api.duma.gov.ru/api"
DB_PATH = str(_ROOT_DIR / "duma_gov_ru.db")

REQUEST_TIMEOUT_SECONDS = 30
# Delay between vote-detail calls (the expensive stage, ~16-17k requests) to stay polite
# with the public API rather than only relying on the 50k/day cap.
REQUEST_DELAY_SECONDS = float(os.getenv("DUMA_REQUEST_DELAY_SECONDS", "0.1"))
SEARCH_PAGE_LIMIT = 100

LOG_LEVEL = os.getenv("DUMA_LOG_LEVEL", "INFO")

_REQUIRED_VARS = {
    "DUMA_API_KEY": DUMA_API_KEY,
    "DUMA_APP_KEY": DUMA_APP_KEY,
}


def validate() -> None:
    """Fail fast with a clear message if required config values are missing."""
    missing = [name for name, value in _REQUIRED_VARS.items() if not value]
    if missing:
        raise RuntimeError(
            "config.validate: missing required values — copy .env.example to .env and "
            "fill in keys from http://api.duma.gov.ru/key-request: " + ", ".join(missing)
        )
