# hihikvadrat

Разовый сбор интро (`#о_себе`) из Telegram-чата аналитиков в SQLite и их просмотр. Контекст и
исходные договорённости — в `project-brief.md`.

## Локальная настройка

```bash
uv sync
```

Проект берёт настройки из корневого `config.py` репозитория, а не из `.env`:
- `TG_USER_APP_API_ID`, `TG_USER_APP_API_HASH` — читаются из env-переменных
  `TELEGRAM_USER_APP_API_ID` / `TELEGRAM_USER_APP_API_HASH` (см. корневой `config.py`)
- `TELEGRAM_CHAT` — задан константой в корневом `config.py` (`@hihikvadrat`)

Свои значения `TELEGRAM_USER_APP_API_ID` / `TELEGRAM_USER_APP_API_HASH` (с https://my.telegram.org,
API development tools) нужно экспортировать в окружении перед запуском — они общие для всех
researches в этом репозитории.

## Первый логин в Telegram

Telethon авторизуется как обычный аккаунт (не бот) и хранит сессию в файле `.session` — это
SQLite-файл с ключами авторизации, эквивалент активной сессии в Telegram. Проект использует общую
для всех researches сессию из корневого `config.PATH_TO_SESSION` (`tg-session/telegram.session`).

Если этой сессии ещё нет или она протухла, логин запросится интерактивно при запуске:

```bash
uv run python -m hihikvadrat --backfill
```

Telethon спросит номер телефона, код из Telegram и, если включена 2FA, пароль.

## Запуск вручную

```bash
uv run python -m hihikvadrat            # инкрементальный сбор нового с последнего запуска
uv run python -m hihikvadrat --backfill # разовый проход по всей истории чата
```

## Просмотр собранных интро

`notebooks/intros_browser.ipynb` — Jupyter-ноутбук для чтения `hihikvadrat.db` без ручных SQL-запросов.
Выводит интро, сгруппированные по `level`, от самой маленькой группы к самой большой; внутри группы —
`username`, `stack`, `projects` и полный текст.

```bash
uv sync                      # подтягивает ipykernel из dev-группы
uv run jupyter lab notebooks/intros_browser.ipynb
```

Открыть можно и через IDE (DataSpell, VS Code и т.п.) — она должна подхватить `.venv` проекта как ядро.