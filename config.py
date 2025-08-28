import os
from pathlib import Path

# Main paths
ROOT_DIR = Path(__file__).resolve().parents[0]
PATH_TO_RESEARCHES = ROOT_DIR / 'researches'
PATH_TO_SESSION = ROOT_DIR / "tg-session"
os.makedirs(PATH_TO_SESSION, exist_ok=True)

# Telegram API
TG_USER_APP_API_ID = os.getenv("TELEGRAM_USER_APP_API_ID")
TG_USER_APP_API_HASH = os.getenv("TELEGRAM_USER_APP_API_HASH")

print(ROOT_DIR, PATH_TO_RESEARCHES)
