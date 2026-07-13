from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from config import settings


# Колонки итогового CSV. offset нужен для чекпоинта — чтобы при следующем
# запуске продолжить с последней успешно скачанной страницы.
CSV_COLUMNS = ["id", "name", "platforms", "offset"]


def build_request_params(offset: int, size: int) -> dict[str, str]:
    """
    Формирует словарь query-параметров для GraphQL-запроса к PlayStation Store API.

    Параметры variables и extensions сериализуются в JSON-строки, потому что
    API ожидает их именно в таком виде (не как вложенные объекты query string).

    Args:
        offset: Порядковый номер первой игры на запрашиваемой странице.
        size: Количество игр на странице (обычно PAGE_SIZE из конфига).

    Returns:
        Словарь параметров, готовых для передачи в requests.get().
    """
    variables = {
        "id": settings.CATEGORY_ID,
        "pageArgs": {"size": size, "offset": offset},
        "sortBy": settings.SORT_BY,
        "filterBy": settings.FILTER_BY,
        "facetOptions": settings.FACET_OPTIONS,
    }
    extensions = {
        "persistedQuery": {
            "version": settings.PERSISTED_QUERY_VERSION,
            "sha256Hash": settings.PERSISTED_QUERY_SHA256,
        }
    }
    return {
        "operationName": settings.OPERATION_NAME,
        "variables": json.dumps(variables, separators=(",", ":")),
        "extensions": json.dumps(extensions, separators=(",", ":")),
    }


def get_games_page(offset: int, size: int) -> dict[str, Any] | None:
    """
    Выполняет HTTP-запрос к PlayStation Store API и возвращает JSON-ответ
    для одной страницы каталога игр.

    При сетевой ошибке или не-2xx статусе печатает сообщение и возвращает None,
    чтобы основной цикл мог корректно остановиться.

    Args:
        offset: Смещение для пагинации (номер первой игры на странице).
        size: Количество игр на странице.

    Returns:
        Распарсенный JSON-ответ или None в случае ошибки.
    """
    params = build_request_params(offset=offset, size=size)
    try:
        response = requests.get(
            settings.API_URL,
            headers=settings.HEADERS,
            params=params,
            timeout=settings.REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        return response.json()
    except requests.RequestException as exc:
        print(f"Ошибка запроса на offset {offset}: {exc}")
        return None


def get_start_offset(csv_path: Path) -> int:
    """
    Определяет, с какого offset продолжать сбор данных.

    Читает уже существующий CSV (если он есть), находит максимальный
    сохранённый offset и возвращает следующий (max + PAGE_SIZE).
    Если файл не существует или пуст — возвращает 0 (начинаем сначала).

    Это позволяет возобновлять прерванный сбор без повторной загрузки
    уже скачанных страниц.

    Args:
        csv_path: Путь к CSV-файлу с ранее собранными играми.

    Returns:
        Offset, с которого нужно начать следующую сессию сбора.
    """
    if not csv_path.exists() or csv_path.stat().st_size == 0:
        return 0

    try:
        existing_df = pd.read_csv(csv_path, usecols=["offset"])
    except ValueError:
        return 0
    except Exception as exc:
        print(f"Ошибка чтения чекпоинта: {exc}. Начинаем с 0.")
        return 0

    offsets = pd.to_numeric(existing_df["offset"], errors="coerce").dropna()
    if offsets.empty:
        return 0

    last_offset = int(offsets.max())
    next_offset = last_offset + settings.PAGE_SIZE
    print(f"Файл найден. Последний сохраненный offset: {last_offset}. Продолжаем с {next_offset}.")
    return next_offset


def parse_products_to_df(payload: dict[str, Any], offset: int) -> tuple[pd.DataFrame, bool]:
    """
    Извлекает список игр из JSON-ответа API и преобразует его в DataFrame.

    Также возвращает флаг isLast — признак того, что это последняя страница
    в каталоге. Если флаг True, основной цикл должен остановиться.

    Args:
        payload: Распарсенный JSON-ответ от API.
        offset: Текущее смещение (сохраняется в колонке offset каждой строки
                для последующего восстановления чекпоинта).

    Returns:
        Кортеж (DataFrame с играми, флаг последней страницы).

    Raises:
        KeyError: Если в ответе отсутствует ожидаемая структура ключей.
    """
    try:
        grid_data = payload["data"]["categoryGridRetrieve"]
        products = grid_data.get("products", [])
        page_info = grid_data.get("pageInfo", {})
    except KeyError as exc:
        raise KeyError(f"Ошибка разбора JSON. Отсутствует ключ: {exc}") from exc

    if not products:
        return pd.DataFrame(columns=CSV_COLUMNS), bool(page_info.get("isLast", False))

    rows = [
        {
            "id": product.get("id", ""),
            "name": product.get("name", ""),
            "platforms": ", ".join(product.get("platforms", [])),
            "offset": offset,
        }
        for product in products
    ]
    batch_df = pd.DataFrame(rows, columns=CSV_COLUMNS)
    return batch_df, bool(page_info.get("isLast", False))


def append_batch_to_csv(batch_df: pd.DataFrame, csv_path: Path) -> None:
    """
    Дозаписывает батч игр в CSV-файл (режим append).

    Заголовки колонок записываются только при создании нового файла.
    Если DataFrame пустой — ничего не делает.

    Args:
        batch_df: DataFrame с играми текущей страницы.
        csv_path: Путь к выходному CSV-файлу.
    """
    if batch_df.empty:
        return

    csv_path.parent.mkdir(parents=True, exist_ok=True)
    file_exists = csv_path.exists() and csv_path.stat().st_size > 0
    batch_df.to_csv(csv_path, mode="a", header=not file_exists, index=False, encoding="utf-8")


def main() -> None:
    """
    Основной цикл сбора игр из каталога PlayStation Store.

    Алгоритм:
    1. Определяем стартовый offset через get_start_offset() — продолжаем
       с последней записанной страницы или начинаем с 0.
    2. В цикле запрашиваем страницы по PAGE_SIZE игр — пока не достигнем
       последней страницы или лимита MAX_PAGES_PER_RUN.
    3. Каждый батч сразу дозаписывается в CSV — прогресс не теряется
       при прерывании скрипта.
    4. Между запросами делаем паузу REQUEST_SLEEP_SECONDS.
    """
    csv_path = settings.OUTPUT_GAMES_CSV
    start_offset = get_start_offset(csv_path)
    current_offset = start_offset
    pages_processed = 0
    total_collected = 0
    is_last_page = False

    if start_offset == 0 and (not csv_path.exists() or csv_path.stat().st_size == 0):
        print(f"Будет создан новый CSV: {csv_path}")

    print(f"\nЗапуск сбора: максимум {settings.MAX_PAGES_PER_RUN} страниц...")

    while not is_last_page and pages_processed < settings.MAX_PAGES_PER_RUN:
        print(f"Запрашиваем страницу {pages_processed + 1} (offset: {current_offset})...")
        payload = get_games_page(offset=current_offset, size=settings.PAGE_SIZE)

        if not payload or "data" not in payload:
            print("Получен пустой или некорректный ответ. Остановка цикла.")
            break

        try:
            batch_df, is_last_page = parse_products_to_df(payload, offset=current_offset)
        except KeyError as exc:
            print(str(exc))
            break

        if batch_df.empty:
            print("Игры закончились.")
            break

        append_batch_to_csv(batch_df, csv_path)
        total_collected += len(batch_df)

        current_offset += settings.PAGE_SIZE
        pages_processed += 1
        time.sleep(settings.REQUEST_SLEEP_SECONDS)

    print("\nСбор сессии завершен!")
    print(f"Добавлено игр: {total_collected}")
    print(f"Обработано страниц: {pages_processed}")
    if is_last_page:
        print("Достигнут абсолютный конец списка игр в категории!")


if __name__ == "__main__":
    main()
