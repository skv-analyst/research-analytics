from __future__ import annotations

from pathlib import Path
from typing import Any

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Централизованная конфигурация проекта PlayStation Store Research.

    Параметры читаются из переменных окружения или файла .env (если он есть).
    Все значения по умолчанию рассчитаны на работу с индийским регионом (en-IN).

    Группы параметров:
    - Базовые URL и идентификаторы категории стора
    - Параметры пагинации и таймингов для get_games.py
    - Параметры запроса деталей для get_details.py
    - Пути к выходным CSV-файлам
    - HTTP-заголовки для запросов к PlayStation Store
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # --- Базовые URL и идентификаторы ---
    # WEB_URL используется для навигации / формирования ссылок на страницы продуктов
    WEB_URL: str = "https://store.playstation.com/en-in/category/4cbf39e2-5749-4970-ba81-93a489e4570c"
    # API_URL — GraphQL endpoint PlayStation Store
    API_URL: str = "https://web.np.playstation.com/api/graphql/v1//op"
    # Имя GraphQL-операции для получения списка игр в категории
    OPERATION_NAME: str = "categoryGridRetrieve"
    # UUID категории «All Games» в PlayStation Store
    CATEGORY_ID: str = "4cbf39e2-5749-4970-ba81-93a489e4570c"
    # Локаль по умолчанию для заголовков запросов списка игр
    COUNTRY: str = "en-IN"

    # --- Параметры пагинации (get_games.py) ---
    # Количество игр на одну страницу (максимум, который принимает API)
    PAGE_SIZE: int = 24
    # Максимальное количество страниц за один запуск скрипта
    MAX_PAGES_PER_RUN: int = 386
    # Таймаут HTTP-запроса в секундах
    REQUEST_TIMEOUT_SECONDS: float = 20.0
    # Пауза между запросами страниц (вежливый парсинг, защита от бана)
    REQUEST_SLEEP_SECONDS: float = 1.5

    # --- Параметры GraphQL для запроса деталей (get_details.py) ---
    # Имя GraphQL-операции для получения данных конкретного продукта
    DETAILS_OPERATION_NAME: str = "queryRetrieveTelemetryDataPDPProduct"
    # Версия persisted query (обычно не меняется)
    DETAILS_PERSISTED_QUERY_VERSION: int = 1
    # SHA256-хэш persisted query для запроса деталей продукта
    DETAILS_PERSISTED_QUERY_SHA256: str = "71375f8f3dba0de83520ecd474069e037f8ea23b4efcb6778aef58894cd4452d"
    # Локаль по умолчанию для сбора деталей (формат: xx-xx)
    DETAILS_COUNTRY: str = "en-in"
    # Таймаут HTTP-запроса деталей в секундах
    DETAILS_TIMEOUT_SECONDS: float = 10.0
    # Пауза между запросами деталей (защита от ограничений Sony)
    DETAILS_SLEEP_SECONDS: float = 0.5
    # Количество игр в тестовом запуске (--test)
    DETAILS_TEST_LIMIT: int = 5

    # --- Параметры сортировки и фильтрации (get_games.py) ---
    SORT_BY: str | None = None
    FILTER_BY: list[str] = Field(default_factory=list)
    FACET_OPTIONS: list[dict[str, Any]] = Field(default_factory=list)

    # --- Параметры persisted query для списка игр ---
    PERSISTED_QUERY_VERSION: int = 1
    PERSISTED_QUERY_SHA256: str = "257713466fc3264850aa473409a29088e3a4115e6e69e9fb3e061c8dd5b9f5c6"

    # --- Пути к выходным файлам ---
    # Список игр, собранных через get_games.py
    OUTPUT_GAMES_CSV: Path = Path(__file__).resolve().parents[1] / "data/ps-store-games.csv"
    # Расширённые данные по играм, собранные через get_details.py
    OUTPUT_GAMES_DETAILS_CSV: Path = Path(__file__).resolve().parents[1] / "data/ps-store-games-details.csv"

    # --- HTTP-заголовки ---
    # Используются во всех запросах к PlayStation Store.
    # accept-language и x-psn-store-locale-override обновляются динамически
    # через model_validator sync_locale_headers при инициализации.
    HEADERS: dict[str, str] = Field(
        default_factory=lambda: {
            "accept": "application/json",
            "accept-language": "en-IN",
            "apollographql-client-name": "@sie-ppr-web-store/app",
            "apollographql-client-version": "0.109.0",
            "content-type": "application/json",
            "origin": "https://store.playstation.com",
            "referer": "https://store.playstation.com/",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36",
            "x-psn-app-ver": "@sie-ppr-web-store/app/0.109.0-",
            "x-psn-store-locale-override": "en-IN",
        }
    )

    @field_validator("PAGE_SIZE", "MAX_PAGES_PER_RUN", "DETAILS_TEST_LIMIT", "DETAILS_PERSISTED_QUERY_VERSION")
    @classmethod
    def validate_positive_ints(cls, value: int) -> int:
        """Проверяет, что целочисленные параметры строго положительны."""
        if value <= 0:
            raise ValueError("Value must be greater than zero")
        return value

    @field_validator("REQUEST_TIMEOUT_SECONDS", "REQUEST_SLEEP_SECONDS", "DETAILS_TIMEOUT_SECONDS", "DETAILS_SLEEP_SECONDS")
    @classmethod
    def validate_positive_floats(cls, value: float) -> float:
        """Проверяет, что параметры времени (таймауты и паузы) строго положительны."""
        if value <= 0:
            raise ValueError("Value must be greater than zero")
        return value

    @field_validator("PERSISTED_QUERY_SHA256", "DETAILS_PERSISTED_QUERY_SHA256")
    @classmethod
    def validate_sha256(cls, value: str) -> str:
        """
        Проверяет, что SHA256-хэш persisted query корректен:
        - ровно 64 символа
        - только hex-символы (0-9, a-f)
        Возвращает хэш в нижнем регистре.
        """
        if len(value) != 64 or any(ch not in "0123456789abcdef" for ch in value.lower()):
            raise ValueError("PERSISTED_QUERY_SHA256 must be a 64-char hex string")
        return value.lower()

    @field_validator("HEADERS")
    @classmethod
    def validate_headers(cls, headers: dict[str, str]) -> dict[str, str]:
        """
        Проверяет наличие всех обязательных HTTP-заголовков.
        Если хотя бы один ключ отсутствует — выбрасывает ValidationError
        со списком недостающих заголовков.
        """
        required_keys = {
            "accept",
            "accept-language",
            "apollographql-client-name",
            "apollographql-client-version",
            "content-type",
            "origin",
            "referer",
            "user-agent",
            "x-psn-app-ver",
            "x-psn-store-locale-override",
        }
        missing = required_keys - set(headers)
        if missing:
            raise ValueError(f"HEADERS missing required keys: {sorted(missing)}")
        return headers

    @field_validator("DETAILS_COUNTRY")
    @classmethod
    def validate_details_country(cls, value: str) -> str:
        """
        Проверяет формат локали для сбора деталей.
        Допустимый формат: xx-xx (например, en-in, tr-tr, en-us).
        Нормализует значение к нижнему регистру.
        """
        normalized = value.strip().lower()
        if len(normalized) != 5 or normalized[2] != "-":
            raise ValueError("DETAILS_COUNTRY must look like en-in, tr-tr, en-us")
        return normalized

    @model_validator(mode="after")
    def sync_locale_headers(self) -> "Settings":
        """
        После инициализации синхронизирует accept-language и
        x-psn-store-locale-override в HEADERS со значением COUNTRY.
        Это гарантирует, что заголовки всегда соответствуют выбранной локали,
        даже если COUNTRY переопределён через .env или переменную окружения.
        """
        self.HEADERS["accept-language"] = self.COUNTRY
        self.HEADERS["x-psn-store-locale-override"] = self.COUNTRY
        return self


settings = Settings()

