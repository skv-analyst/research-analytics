# research-analytics

Репозиторий с исследовательскими проектами. Каждое исследование живёт в своей папке под
`researches/`, изолировано своим `.venv` на `uv` и не зависит от остальных.

## Структура репозитория

```
research-analytics/
├── config.py                        # общие пути и секреты
├── setup_research.sh                # скрипт для старта нового исследования
├── tg-session/                      # общая сессия Telethon (telegram.session) вне git
└── researches/
    └── NNN-название-исследования/
        ├── data/
        ├── notebooks/
        ├── report/
        ├── src/
        ├── pyproject.toml
        └── README.md
```

## Как начать новое исследование

```bash
./setup_research.sh название_исследования
```

Скрипт создаёт `researches/название_исследования/` со структурой `data/`, `notebooks/`, `report/`,
базовым `pyproject.toml` (jupyter, pandas, requests, plotly) и `README.md`, ставит зависимости через
`uv add`. Название — без пробелов, по аналогии с существующими папками стоит добавлять
порядковый номер-префикс (`004-...`).

После генерации:

```bash
cd researches/название_исследования
uv sync
```

## Общий config.py

Корневой `config.py` хранит то, что нужно нескольким исследованиям сразу:
- `PATH_TO_SESSION` — путь к общей папке с файлом сессии Telethon (`tg-session/telegram.session`),
  чтобы не логиниться в Telegram отдельно в каждом проекте.
- `TG_USER_APP_API_ID` / `TG_USER_APP_API_HASH` — Telegram API-ключи (с https://my.telegram.org),
  читаются из переменных окружения `TELEGRAM_USER_APP_API_ID` / `TELEGRAM_USER_APP_API_HASH` —
  их нужно экспортировать в шелле самостоятельно, в репозитории их нет.
- Константы отдельных проектов (например `TELEGRAM_CHAT`), если код проекта должен смотреть в
  общий конфиг вместо своего `.env`.

Код внутри `researches/.../` подключает его, добавляя корень репозитория в `sys.path`.