from __future__ import annotations

import argparse
import json
import re
import time
from pathlib import Path
from typing import Any, cast

import pandas as pd
import requests
from bs4 import BeautifulSoup

from config import settings


# Колонки расширенных данных по игре, которые сохраняются в итоговый CSV.
DETAIL_COLUMNS = [
    "product_id",
    "country",
    "base_price",
    "discounted_price",
    "genres",
    "publisher",
    "star_rating",
    "age_rating",
    "in_ps_plus",
    "voice_langs",
    "screen_langs",
    "offline_players",
]

# Поля, которые проверяются в режиме --retry-missing.
# Если все три пустые (None/NaN) — игра попадает в повторный сбор.
RETRY_NULL_FIELDS = ["voice_langs", "screen_langs", "offline_players"]


def build_details_headers(country_code: str) -> dict[str, str]:
    """
    Формирует HTTP-заголовки для запросов к PlayStation Store
    с подставленной локалью (country_code).

    Копирует базовые заголовки из конфига и переопределяет:
    - accept-language — язык контента ответа
    - x-psn-store-locale-override — регион магазина
    - x-apollo-operation-name — имя GraphQL-операции для деталей продукта

    Args:
        country_code: Локаль региона в формате xx-xx (например, en-in, tr-tr).

    Returns:
        Словарь HTTP-заголовков для requests.get().
    """
    headers = cast(dict[str, str], dict(settings.HEADERS))
    headers["accept-language"] = country_code
    headers["x-psn-store-locale-override"] = country_code
    headers["x-apollo-operation-name"] = settings.DETAILS_OPERATION_NAME
    return headers


def build_details_params(product_id: str) -> dict[str, str]:
    """
    Формирует query-параметры GraphQL-запроса для получения деталей продукта.

    Использует persisted query (кэшированный запрос по SHA256-хэшу),
    что позволяет не передавать полное тело запроса каждый раз.

    Args:
        product_id: Уникальный идентификатор игры в PlayStation Store
                    (например, UP0006-PPSA19534_00-SANTIAGOSTANDARD).

    Returns:
        Словарь параметров для requests.get().
    """
    variables = {"productId": product_id}
    extensions = {
        "persistedQuery": {
            "version": settings.DETAILS_PERSISTED_QUERY_VERSION,
            "sha256Hash": settings.DETAILS_PERSISTED_QUERY_SHA256,
        }
    }
    return {
        "operationName": settings.DETAILS_OPERATION_NAME,
        "variables": json.dumps(variables, separators=(",", ":")),
        "extensions": json.dumps(extensions, separators=(",", ":")),
    }


def empty_details_row(product_id: str, country_code: str) -> dict[str, Any]:
    """
    Создаёт шаблон строки деталей с None во всех полях.

    Используется как базовое значение перед заполнением:
    если какой-либо источник данных не вернул значение,
    поля останутся None, а не вызовут KeyError.

    Args:
        product_id: ID игры.
        country_code: Локаль (регион) сбора данных.

    Returns:
        Словарь с ключами из DETAIL_COLUMNS и None-значениями.
    """
    return {
        "product_id": product_id,
        "country": country_code,
        "base_price": None,
        "discounted_price": None,
        "genres": None,
        "publisher": None,
        "star_rating": None,
        "age_rating": None,
        "in_ps_plus": False,
        "voice_langs": None,
        "screen_langs": None,
        "offline_players": None,
    }


def extract_release_languages(soup: BeautifulSoup, qa_suffix_pattern: str) -> str | None:
    """
    Извлекает список языков (озвучки или субтитров) из HTML страницы продукта.

    Sony использует разные суффиксы в data-qa в зависимости от платформы:
    - voice-value → озвучка PS4
    - ps5Voice-value → озвучка PS5
    - subtitles-value → субтитры PS4
    - ps5Subtitles-value → субтитры PS5

    Паттерн qa_suffix_pattern с regex позволяет захватить все варианты сразу.
    Если одна страница содержит несколько блоков (PS4 + PS5) — языки объединяются
    без дублей с сохранением порядка.

    Args:
        soup: Распарсенный HTML страницы продукта.
        qa_suffix_pattern: Regex-паттерн для суффикса data-qa атрибута.
                           Например: r"#(?:[a-z0-9]+)?voice-value$"

    Returns:
        Строка с языками через запятую или None, если данные не найдены.
    """
    pattern = re.compile(qa_suffix_pattern, flags=re.IGNORECASE)
    values: list[str] = []

    for node in soup.find_all("dd", attrs={"data-qa": True}):
        data_qa = str(node.get("data-qa", ""))
        if "gameInfo#releaseInformation#" not in data_qa:
            continue
        if not pattern.search(data_qa):
            continue

        raw_value = node.get_text(strip=True)
        if raw_value:
            values.append(raw_value)

    if not values:
        return None

    # Некоторые страницы отдают несколько блоков (например platform-specific).
    # Склеиваем без дублей, сохраняя порядок языков.
    unique_parts: list[str] = []
    seen: set[str] = set()
    for value in values:
        for part in [item.strip() for item in value.split(",") if item.strip()]:
            lowered = part.lower()
            if lowered in seen:
                continue
            seen.add(lowered)
            unique_parts.append(part)

    return ", ".join(unique_parts) if unique_parts else None


def get_full_game_details(product_id: str, country_code: str) -> dict[str, Any]:
    """
    Собирает полные данные по одной игре из двух источников:

    1. GraphQL API (/api/graphql/v1/op) — цены, жанры, рейтинги, PS Plus.
    2. HTML страницы продукта (store.playstation.com/{country}/product/{id}) —
       языки озвучки, субтитров и количество офлайн-игроков через BeautifulSoup.

    Для offline_players сначала проверяем data-qa элементы compatText
    (исключая онлайн-строки), затем делаем fallback по тексту страницы
    через regex для диапазонов вида "1 - 4 players".

    При ошибке запроса данные остаются None (не бросается исключение),
    чтобы один упавший продукт не прерывал весь сбор.

    Args:
        product_id: ID игры в PlayStation Store.
        country_code: Локаль региона (влияет на URL и заголовки запроса).

    Returns:
        Словарь с полями из DETAIL_COLUMNS. Незаполненные поля — None.
    """
    details = empty_details_row(product_id=product_id, country_code=country_code)
    headers = build_details_headers(country_code=country_code)

    try:
        # --- Источник 1: GraphQL API ---
        api_response = requests.get(
            settings.API_URL,
            headers=headers,
            params=build_details_params(product_id=product_id),
            timeout=settings.DETAILS_TIMEOUT_SECONDS,
        )
        api_response.raise_for_status()
        api_payload = api_response.json()
        product = api_payload.get("data", {}).get("productRetrieve", {})

        if product:
            details["star_rating"] = product.get("starRating", {}).get("averageRating")
            details["age_rating"] = product.get("contentRating", {}).get("name")
            details["genres"] = ", ".join(
                genre.get("value", "")
                for genre in product.get("localizedGenres", [])
                if genre.get("value")
            ) or None

            # webctas — блок с кнопками покупки; первый элемент — основная цена
            ctas = product.get("webctas", [])
            if ctas:
                price_info = ctas[0].get("price", {})
                details["base_price"] = price_info.get("basePrice")
                details["discounted_price"] = price_info.get("discountedPrice")
                branding = price_info.get("serviceBranding", [])
                details["in_ps_plus"] = any("PS_PLUS" in item for item in branding)

        # --- Источник 2: HTML страницы продукта ---
        web_url = f"https://store.playstation.com/{country_code}/product/{product_id}"
        web_response = requests.get(
            web_url,
            headers=headers,
            timeout=settings.DETAILS_TIMEOUT_SECONDS,
        )

        if web_response.status_code == 200:
            soup = BeautifulSoup(web_response.text, "html.parser")

            # Языки озвучки: voice-value или ps5Voice-value
            details["voice_langs"] = extract_release_languages(
                soup=soup,
                qa_suffix_pattern=r"#(?:[a-z0-9]+)?voice-value$",
            )
            # Языки субтитров: subtitles-value или ps5Subtitles-value
            details["screen_langs"] = extract_release_languages(
                soup=soup,
                qa_suffix_pattern=r"#(?:[a-z0-9]+)?subtitles-value$",
            )

            # Количество офлайн-игроков из блока совместимости
            # Пропускаем онлайн-строки ("online play required" и подобные)
            compat_nodes = soup.select('span[data-qa*="mfe-compatibility-notices#notices#notice"][data-qa$="#compatText"]')
            for node in compat_nodes:
                value = node.get_text(strip=True)
                if not value:
                    continue
                lowered = value.lower()
                if "online" in lowered:
                    continue
                if re.search(r"\b\d+\s*-\s*\d+\s*players?\b|\b\d+\s*players?\b", lowered):
                    details["offline_players"] = value
                    break

            # Fallback: ищем диапазон игроков в тексте всей страницы
            # (на случай если блок compatText содержит нестандартный формат)
            if not details["offline_players"]:
                page_text = soup.get_text(strip=True)
                range_match = re.search(r"\b\d+\s*[-–—]\s*\d+\s*players?\b", page_text, flags=re.IGNORECASE)
                if range_match:
                    details["offline_players"] = range_match.group(0)

    except requests.RequestException as exc:
        print(f"Ошибка HTTP для {product_id}: {exc}")
    except Exception as exc:
        print(f"Ошибка обработки {product_id}: {exc}")

    return details


def load_games(csv_path: Path) -> pd.DataFrame:
    """
    Загружает CSV со списком игр (output get_games.py) и нормализует колонку id.

    Удаляет строки с пустым id и дубли по id — чтобы каждая игра
    обрабатывалась ровно один раз.

    Args:
        csv_path: Путь к CSV-файлу со списком игр (ps-store-games.csv).

    Returns:
        DataFrame с уникальными играми.

    Raises:
        FileNotFoundError: Если файл не существует или пуст.
        ValueError: Если в файле нет колонки 'id'.
    """
    if not csv_path.exists() or csv_path.stat().st_size == 0:
        raise FileNotFoundError(f"Файл со списком игр не найден или пуст: {csv_path}")

    games_df = cast(pd.DataFrame, pd.read_csv(csv_path))
    if "id" not in games_df.columns:
        raise ValueError(f"В файле {csv_path} отсутствует колонка 'id'")

    games_df = games_df.dropna(subset=["id"]).copy()
    games_df["id"] = games_df["id"].astype(str).str.strip()
    games_df = games_df[games_df["id"] != ""].drop_duplicates(subset=["id"])

    return games_df


def load_processed_keys(details_csv_path: Path) -> set[tuple[str, str]]:
    """
    Читает уже собранные пары (product_id, country) из CSV деталей.

    Используется для инкрементального сбора: при следующем запуске
    уже обработанные игры (для данного региона) пропускаются.
    Ключ уникальности — пара, а не только product_id, чтобы одну игру
    можно было собрать для нескольких регионов независимо.

    Args:
        details_csv_path: Путь к CSV с деталями (ps-store-games-details.csv).

    Returns:
        Множество кортежей (product_id, country) уже обработанных игр.
        Возвращает пустое множество если файл не существует или пуст.
    """
    if not details_csv_path.exists() or details_csv_path.stat().st_size == 0:
        return set()

    try:
        details_df = cast(pd.DataFrame, pd.read_csv(details_csv_path))
    except ValueError:
        return set()

    if "product_id" not in details_df.columns:
        return set()

    if "country" not in details_df.columns:
        details_df["country"] = ""

    normalized = details_df[["product_id", "country"]].copy()
    normalized["product_id"] = normalized["product_id"].astype(str).str.strip()
    normalized["country"] = normalized["country"].fillna("").astype(str).str.strip().str.lower()
    normalized = normalized[normalized["product_id"] != ""]

    return set(
        zip(normalized["product_id"].tolist(), normalized["country"].tolist(), strict=False)
    )


def load_games_with_missing_fields(
    games_df: pd.DataFrame,
    details_csv_path: Path,
    country_code: str,
    fields: list[str],
) -> pd.DataFrame:
    """
    Находит игры в CSV деталях, у которых в указанных полях пусто для данного региона.

    Используется в режиме --retry-missing для повторного сбора записей,
    которые были пустыми из-за блокировки по IP или временных сбоев сервера.

    Пустыми считаются: NaN, пустая строка, строки "nan"/"none"/"null".

    Args:
        games_df: Полный список игр (из ps-store-games.csv).
        details_csv_path: Путь к CSV с деталями.
        country_code: Регион для фильтрации (сравнивается в нижнем регистре).
        fields: Список колонок для проверки на пустоту.

    Returns:
        DataFrame с играми из games_df, которые имеют пустые поля
        в CSV деталей для данного региона.
    """
    if not details_csv_path.exists() or details_csv_path.stat().st_size == 0:
        return pd.DataFrame(columns=games_df.columns)

    details_df = cast(pd.DataFrame, pd.read_csv(details_csv_path))
    if "product_id" not in details_df.columns or "country" not in details_df.columns:
        return pd.DataFrame(columns=games_df.columns)

    details_df = details_df.copy()
    details_df["product_id"] = details_df["product_id"].astype(str).str.strip()
    details_df["country"] = details_df["country"].fillna("").astype(str).str.strip().str.lower()

    country_rows = details_df[details_df["country"] == country_code.strip().lower()].copy()
    if country_rows.empty:
        return pd.DataFrame(columns=games_df.columns)

    missing_mask = pd.Series(False, index=country_rows.index)
    for field in fields:
        if field not in country_rows.columns:
            missing_mask = pd.Series(True, index=country_rows.index)
            break

        col = country_rows[field]
        empty_text = col.astype(str).str.strip().str.lower().isin({"", "nan", "none", "null"})
        missing_mask = missing_mask | col.isna() | empty_text

    target_ids = set(country_rows.loc[missing_mask, "product_id"].tolist())
    if not target_ids:
        return pd.DataFrame(columns=games_df.columns)

    return games_df[games_df["id"].astype(str).isin(target_ids)].copy()


def pick_games_for_run(
    games_df: pd.DataFrame,
    product_id: str | None,
    country_code: str,
    retry_missing: bool,
    test_mode: bool,
    limit: int,
    details_csv_path: Path,
) -> pd.DataFrame:
    """
    Определяет список игр для текущего запуска в зависимости от режима.

    Приоритет режимов (от высшего к низшему):
    1. --product-id: одна конкретная игра, все остальные флаги игнорируются.
    2. --retry-missing: только игры с пустыми полями для данного региона.
       Если также указан --test, берётся первые limit игр из них.
    3. --test: первые limit игр из полного списка.
    4. Обычный режим: все игры, которых ещё нет в CSV деталей для данного региона.

    Args:
        games_df: Полный DataFrame со списком игр.
        product_id: ID конкретной игры или None.
        country_code: Регион для фильтрации.
        retry_missing: Флаг режима повторного сбора.
        test_mode: Флаг тестового режима.
        limit: Максимальное количество игр в тестовом режиме.
        details_csv_path: Путь к CSV с уже собранными деталями.

    Returns:
        DataFrame с играми для обработки в текущем запуске.
    """
    if product_id:
        selected = games_df[games_df["id"] == product_id].copy()
        if selected.empty:
            raise ValueError(f"Игра с id '{product_id}' не найдена в {settings.OUTPUT_GAMES_CSV}")
        return selected

    if retry_missing:
        selected = load_games_with_missing_fields(
            games_df=games_df,
            details_csv_path=details_csv_path,
            country_code=country_code,
            fields=RETRY_NULL_FIELDS,
        )
        if test_mode:
            return selected.head(limit).copy()
        return selected

    if test_mode:
        return games_df.head(limit).copy()

    # Обычный инкрементальный режим: пропускаем уже обработанные (product_id, country)
    processed_keys = load_processed_keys(details_csv_path)
    if not processed_keys:
        return games_df.copy()

    current_country = country_code.strip().lower()
    pending_mask = ~games_df["id"].astype(str).map(lambda game_id: (game_id, current_country) in processed_keys)
    return games_df[pending_mask].copy()


def append_detail_row(game_row: pd.Series, details: dict[str, Any], output_path: Path) -> None:
    """
    Дозаписывает одну строку деталей в CSV (режим append).

    Объединяет метаданные из исходного CSV игр (name, platforms) с собранными
    деталями. Заголовки пишутся только при создании нового файла.

    Запись после каждой игры (а не батчами в конце) гарантирует,
    что прогресс не теряется при прерывании скрипта.

    Args:
        game_row: Строка из DataFrame games_df (содержит id, name, platforms).
        details: Словарь с собранными деталями игры.
        output_path: Путь к выходному CSV-файлу.
    """
    game_data = game_row.to_dict()
    game_data["product_id"] = game_data.pop("id")
    row_df = pd.DataFrame([{**game_data, **details}])

    output_path.parent.mkdir(parents=True, exist_ok=True)
    file_exists = output_path.exists() and output_path.stat().st_size > 0
    row_df.to_csv(output_path, mode="a", header=not file_exists, index=False, encoding="utf-8")


def deduplicate_output(output_path: Path) -> int:
    """
    Перезаписывает CSV деталей без дублей по ключу (product_id, country).

    В процессе сбора одна игра может быть записана несколько раз
    (например, при повторном запуске с --retry-missing). Эта функция
    оставляет только последнюю (keep="last") версию каждой записи.

    Вызывается автоматически в конце collect_details().

    Args:
        output_path: Путь к CSV-файлу для дедупликации.

    Returns:
        Итоговое количество строк после дедупликации (0 если файл пуст).
    """
    if not output_path.exists() or output_path.stat().st_size == 0:
        return 0

    df = cast(pd.DataFrame, pd.read_csv(output_path))
    if "product_id" not in df.columns:
        return len(df)

    if "country" not in df.columns:
        # Обратная совместимость: старые файлы без колонки country
        deduped = df.drop_duplicates(subset=["product_id"], keep="last")
        deduped.to_csv(output_path, index=False, encoding="utf-8")
        return len(deduped)

    deduped = df.drop_duplicates(subset=["product_id", "country"], keep="last")
    deduped.to_csv(output_path, index=False, encoding="utf-8")
    return len(deduped)


def collect_details(games_df: pd.DataFrame, country_code: str, output_path: Path, sleep_seconds: float) -> None:
    """
    Основной цикл сбора деталей: итерирует по играм, собирает данные и
    сразу дозаписывает каждую строку в CSV.

    После завершения всех игр вызывает дедупликацию файла.

    Args:
        games_df: DataFrame с играми для обработки.
        country_code: Локаль региона для запросов.
        output_path: Путь к выходному CSV-файлу.
        sleep_seconds: Пауза между запросами (защита от ограничений Sony).
    """
    total = len(games_df)
    saved = 0

    for index, (_, game_row) in enumerate(games_df.iterrows(), start=1):
        product_id = str(game_row["id"])
        print(f"[{index}/{total}] Сбор деталей для {product_id}")
        details = get_full_game_details(product_id=product_id, country_code=country_code)
        append_detail_row(game_row=game_row, details=details, output_path=output_path)
        saved += 1

        if index < total:
            time.sleep(sleep_seconds)

    total_rows = deduplicate_output(output_path)
    print(f"Сохранено/обновлено строк: {saved}; всего в файле: {total_rows} -> {output_path}")


def parse_args() -> argparse.Namespace:
    """
    Определяет и разбирает аргументы командной строки.

    Доступные аргументы:
    - --country: регион (en-in, en-pl, tr-tr и т.д.)
    - --product-id: собрать только одну конкретную игру
    - --retry-missing: пересобрать игры с пустыми voice/screen/offline
    - --test: тестовый режим на первых N играх
    - --limit: количество игр в тестовом режиме
    - --sleep-seconds: пауза между запросами (переопределяет config)

    Returns:
        Namespace с распарсенными аргументами.
    """
    parser = argparse.ArgumentParser(description="Сбор расширенных данных по играм PlayStation Store")
    parser.add_argument("--retry-missing", action="store_true", help="Пересобрать только игры, где voice/screen/offline пустые")
    parser.add_argument("--test", action="store_true", help="Тестовый запуск на ограниченном числе игр")
    parser.add_argument("--limit", type=int, default=settings.DETAILS_TEST_LIMIT, help="Лимит игр для тестового запуска")
    parser.add_argument("--country", default=settings.DETAILS_COUNTRY, help="Локаль стора в формате en-in")
    parser.add_argument("--sleep-seconds", type=float, default=None, help="Пауза между играми (секунды), например 1.2")
    parser.add_argument("--product-id", help="Собрать данные только по одному product_id")
    return parser.parse_args()


def main() -> None:
    """
    Точка входа скрипта. Разбирает аргументы, определяет список игр
    для обработки и запускает цикл сбора деталей.

    Алгоритм:
    1. Читаем аргументы CLI.
    2. Загружаем полный список игр из ps-store-games.csv.
    3. Фильтруем игры через pick_games_for_run() в соответствии с режимом.
    4. Запускаем collect_details() — сбор с записью после каждой игры.
    """
    args = parse_args()
    test_mode = bool(args.test)
    retry_missing = bool(args.retry_missing)
    limit = max(1, int(args.limit))
    country_code = str(args.country).lower()
    product_id = str(args.product_id).strip() if args.product_id else None
    sleep_seconds = settings.DETAILS_SLEEP_SECONDS if args.sleep_seconds is None else max(0.0, float(args.sleep_seconds))

    if product_id and test_mode:
        raise ValueError("Используйте либо --product-id, либо --test")

    all_games_df = load_games(
        csv_path=settings.OUTPUT_GAMES_CSV,
    )

    games_df = pick_games_for_run(
        games_df=all_games_df,
        product_id=product_id,
        country_code=country_code,
        retry_missing=retry_missing,
        test_mode=test_mode,
        limit=limit,
        details_csv_path=settings.OUTPUT_GAMES_DETAILS_CSV,
    )

    if games_df.empty:
        print("Новых игр для обработки нет")
        return

    print(f"Загружено игр для обработки: {len(games_df)}")
    if retry_missing:
        print("Режим повторного сбора: только записи с пустыми voice/screen/offline")
    if test_mode:
        print(f"Тестовый режим включен: первые {len(games_df)} игр")
    if product_id:
        print(f"Точечный режим: только {product_id}")
    print(f"Пауза между запросами: {sleep_seconds} сек")

    collect_details(
        games_df=games_df,
        country_code=country_code,
        output_path=settings.OUTPUT_GAMES_DETAILS_CSV,
        sleep_seconds=sleep_seconds,
    )


if __name__ == "__main__":
    main()
