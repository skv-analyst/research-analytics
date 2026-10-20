#!/usr/bin/env python3
"""
Каталог «Компьютерной литературы» piter.com + разметка для аналитика через Claude.

    pip install requests beautifulsoup4 pandas openpyxl anthropic
    export ANTHROPIC_API_KEY=...

    python piter_books.py scrape   # книги -> data/books.jsonl (+ data/categories.json)
    python piter_books.py tag      # LLM-разметка -> data/tags.jsonl
    python piter_books.py export   # piter_books.xlsx и piter_books.csv (для Google Sheets)

Все шаги можно перезапускать: докачивается только то, чего ещё нет.
"""
import json
import os
import re
import sys
import time
from pathlib import Path

import requests
from bs4 import BeautifulSoup

BASE = "https://www.piter.com"
ROOT = "kompyutery-i-internet"
DELAY = 0.7  # пауза между запросами, чтобы не нагружать сайт

# Подразделы — только чтобы проставить книгам «родные» категории сайта
SUBCOLLECTIONS = {
    "biblioteka-programmista": "Библиотека программиста",
    "operatsionnye-sistemy": "ОС",
    "linux": "Linux",
    "devops": "DevOps",
    "hranenie-i-obrabotka-dannyh": "Хранение и обработка данных",
    "bazy-dannyh": "Базы данных",
    "data-science": "Data science",
    "programmirovanie-razrabotka-programnogo-obespecheniya": "Разработка ПО",
    "programmirovanie-osnovy-i-algoritmy": "Основы и алгоритмы",
    "veb-programmirovanie": "Веб",
    "prilozheniya-dlya-mobilnyh-ustroystv": "Мобильная разработка",
    "programmirovanie-igr": "Геймдев",
    "yazyki-programmirovaniya": "Языки программирования",
    "python": "Python",
    "java": "Java",
    "javascript": "JavaScript",
    "go": "Go",
    "yazyk-programmirovaniya-ss-s": "C/C++/C#",
    "mashinnoe-obuchenie-ii": "ML/ИИ",
    "computer-science-i-algoritmy": "CS и алгоритмы",
    "klassika-computer-science": "Классика CS",
    "upravlenie-it-proektami": "Управление IT-проектами",
    "dlya-professionalov": "Для профессионалов",
    "administrirovanie": "Администрирование",
    "informatsionnye-sistemy": "Информационные системы",
    "informatsionnaya-bezopasnost": "Безопасность",
    "grafika-i-dizayn": "Графика и дизайн",
    "seriya-grokaem": "Грокаем",
    "bestsellery-oreilly": "O'Reilly",
}

# Профиль, под который LLM оценивает полезность — поправьте под себя
ANALYST_PROFILE = (
    "Продуктовый/BI-аналитик в крупной компании: SQL, Databricks (SQL, PySpark), "
    "Kafka, KQL, Python; продуктовые метрики, A/B-тесты, статистика, событийная "
    "аналитика, контракты данных, data engineering, визуализация, базовый ML, "
    "общение с бизнесом и разработкой, рост до Principal."
)
TAG_VOCAB = [
    "SQL", "базы данных", "data engineering", "архитектура данных", "статистика",
    "A/B-тесты", "продуктовая аналитика", "BI/визуализация", "Python", "Spark",
    "ML", "LLM/GenAI", "алгоритмы", "математика", "архитектура ПО", "DevOps/инфра",
    "soft skills/карьера", "менеджмент", "безопасность", "веб/фронтенд",
    "мобильная разработка", "геймдев", "другие языки", "для начинающих",
]

DATA = Path("data")
DATA.mkdir(exist_ok=True)
BOOKS, TAGS, CATS = DATA / "books.jsonl", DATA / "tags.jsonl", DATA / "categories.json"

S = requests.Session()
S.headers["User-Agent"] = "Mozilla/5.0 (personal book catalog script)"
PRODUCT_RE = re.compile(r"/product/([^/?#]+)")


def get(url, **kw):
    for attempt in range(4):
        try:
            r = S.get(url, timeout=30, **kw)
            time.sleep(DELAY)
            if r.status_code == 200:
                return r
            if r.status_code == 404:
                return None
        except requests.RequestException:
            pass
        time.sleep(2 ** attempt + 1)
    return None


def read_jsonl(path):
    if not path.exists():
        return []
    return [json.loads(l) for l in path.open(encoding="utf-8") if l.strip()]


def append_jsonl(path, obj):
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")


def html_to_text(html):
    return BeautifulSoup(html or "", "html.parser").get_text(" ", strip=True)


# ---------------------------------------------------------------- scrape
def list_collection(slug, max_pages=300):
    handles = []
    for page in range(1, max_pages + 1):
        r = get(f"{BASE}/collection/{slug}", params={"page": page})
        if not r:
            break
        soup = BeautifulSoup(r.text, "html.parser")
        new = []
        for a in soup.select("a[href*='/product/']"):
            m = PRODUCT_RE.search(a.get("href", ""))
            if m and m.group(1) not in handles and m.group(1) not in new:
                new.append(m.group(1))
        if not new:  # страницы кончились (InSales отдаёт последнюю повторно/пустую)
            break
        handles += new
        print(f"  {slug}: стр. {page}, всего {len(handles)}")
    return handles


def extract_meta(text):
    out = {}
    for key, pat in {
        "year": r"Год(?: издания)?\s*[:\-]?\s*((?:19|20)\d{2})",
        "pages": r"(?:Кол-во страниц|Страниц|Количество страниц)\s*[:\-]?\s*(\d{2,4})",
        "isbn": r"ISBN\s*[:\-]?\s*([\d\-X]{10,17})",
        "author": r"Автор(?:ы)?\s*[:\-]\s*([^\n:]{3,120}?)(?:\s{2,}|\s(?:Год|ISBN|Серия|Переплет|Страниц)|$)",
    }.items():
        m = re.search(pat, text)
        if m:
            out[key] = m.group(1).strip()
    return out


def fetch_product(handle):
    url = f"{BASE}/collection/{ROOT}/product/{handle}"
    book = {"handle": handle, "url": url}

    # 1) JSON-эндпоинт InSales — самый чистый источник
    r = get(f"{BASE}/products/{handle}.json")
    if r:
        try:
            p = r.json()
            p = p.get("product", p)
            names = {pr.get("id"): pr.get("title") for pr in p.get("properties") or []}
            props = {}
            for c in p.get("characteristics") or []:
                k = names.get(c.get("property_id")) or str(c.get("property_id"))
                props.setdefault(k, []).append(str(c.get("title")))
            props = {k: ", ".join(v) for k, v in props.items()}
            variants = p.get("variants") or [{}]
            book.update(
                title=p.get("title"),
                description=html_to_text(p.get("description") or p.get("short_description")),
                price=variants[0].get("price"),
                props=props,
                source="json",
            )
            meta_text = " ".join(f"{k}: {v}" for k, v in props.items())
            book.update({k: v for k, v in extract_meta(meta_text).items() if v})
            if book.get("title") and book.get("description"):
                return book
        except ValueError:
            pass

    # 2) Фолбэк — HTML страницы товара
    r = get(url)
    if not r:
        return book if book.get("title") else None
    soup = BeautifulSoup(r.text, "html.parser")
    h1 = soup.find("h1")
    desc = None
    for sel in ["[itemprop=description]", ".product-description", "#tab-description",
                ".product__description", ".description"]:
        el = soup.select_one(sel)
        if el and len(el.get_text(strip=True)) > 50:
            desc = el.get_text(" ", strip=True)
            break
    if not desc:
        og = soup.find("meta", property="og:description") or soup.find("meta", attrs={"name": "description"})
        desc = og.get("content") if og else ""
    page_text = soup.get_text("\n", strip=True)
    book.setdefault("title", h1.get_text(strip=True) if h1 else handle)
    if not book.get("description"):
        book["description"] = desc
    for k, v in extract_meta(page_text).items():
        book.setdefault(k, v)
    book["source"] = book.get("source", "html")
    return book


def cmd_scrape():
    if CATS.exists():
        cats = json.loads(CATS.read_text(encoding="utf-8"))
    else:
        print("Собираю список книг…")
        cats = {h: [] for h in list_collection(ROOT)}
        for slug, name in SUBCOLLECTIONS.items():
            for h in list_collection(slug):
                cats.setdefault(h, []).append(name)
        CATS.write_text(json.dumps(cats, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"Книг в списке: {len(cats)}")

    done = {b["handle"] for b in read_jsonl(BOOKS)}
    todo = [h for h in cats if h not in done]
    for i, h in enumerate(todo, 1):
        b = fetch_product(h)
        if b:
            append_jsonl(BOOKS, b)
        print(f"[{i}/{len(todo)}] {(b or {}).get('title', h)[:80]}")


# ---------------------------------------------------------------- tag
SYSTEM = f"""Ты помогаешь аналитику данных разобрать каталог IT-книг.
Профиль читателя: {ANALYST_PROFILE}

Для каждой книги верни объект:
- "handle": как во входе
- "summary": 1–2 предложения по-русски, о чём книга и для кого (своими словами)
- "relevance": 0–3 (0 — бесполезна этому аналитику, 1 — по касательной, 2 — полезна, 3 — must read)
- "level": "начальный" | "средний" | "продвинутый"
- "tags": 1–4 тега СТРОГО из списка: {", ".join(TAG_VOCAB)}
- "why": до 15 слов — почему такая оценка

Ответ — только JSON-массив, без пояснений и без markdown."""


def cmd_tag(batch_size=15):
    import anthropic

    client = anthropic.Anthropic()
    model = os.getenv("TAG_MODEL", "claude-haiku-5-5")
    done = {t["handle"] for t in read_jsonl(TAGS)}
    books = [b for b in read_jsonl(BOOKS) if b["handle"] not in done]
    print(f"К разметке: {len(books)}")
    for i in range(0, len(books), batch_size):
        chunk = books[i:i + batch_size]
        payload = [{"handle": b["handle"], "title": b.get("title"),
                    "description": (b.get("description") or "")[:1500]} for b in chunk]
        for attempt in range(3):
            try:
                resp = client.messages.create(
                    model=model, max_tokens=4000, system=SYSTEM,
                    messages=[{"role": "user", "content": json.dumps(payload, ensure_ascii=False)}],
                )
                text = "".join(c.text for c in resp.content if c.type == "text")
                text = re.sub(r"^```(?:json)?|```$", "", text.strip(), flags=re.M).strip()
                for t in json.loads(text):
                    append_jsonl(TAGS, t)
                break
            except Exception as e:  # noqa: BLE001
                print(f"  батч {i}: ошибка {e!r}, повтор…")
                time.sleep(5)
        print(f"Размечено {min(i + batch_size, len(books))}/{len(books)}")


# ---------------------------------------------------------------- export
def cmd_export():
    import pandas as pd

    cats = json.loads(CATS.read_text(encoding="utf-8")) if CATS.exists() else {}
    books = pd.DataFrame(read_jsonl(BOOKS)).drop_duplicates("handle")
    tags = pd.DataFrame(read_jsonl(TAGS))
    if not tags.empty:
        tags = tags.drop_duplicates("handle", keep="last")
        books = books.merge(tags, on="handle", how="left")
    for col in ["author", "year", "pages", "price", "relevance", "tags", "level", "summary", "why"]:
        if col not in books:
            books[col] = None
    books["categories"] = books["handle"].map(lambda h: ", ".join(cats.get(h, [])))
    books["tags"] = books["tags"].map(lambda t: ", ".join(t) if isinstance(t, list) else t)
    books["description"] = books["description"].fillna("").str.slice(0, 2000)

    out = books.rename(columns={
        "title": "Название", "author": "Автор", "year": "Год", "pages": "Страниц",
        "price": "Цена", "categories": "Категории сайта", "relevance": "Полезность (0-3)",
        "tags": "Теги", "level": "Уровень", "summary": "Кратко", "why": "Почему",
        "description": "Описание", "url": "Ссылка",
    })[["Название", "Автор", "Год", "Страниц", "Цена", "Полезность (0-3)", "Теги",
        "Уровень", "Категории сайта", "Кратко", "Почему", "Описание", "Ссылка"]]
    out = out.sort_values(["Полезность (0-3)", "Год"], ascending=[False, False])
    out.to_excel("piter_books.xlsx", index=False)
    out.to_csv("piter_books.csv", index=False, encoding="utf-8-sig")
    print(f"Готово: {len(out)} книг → piter_books.xlsx / piter_books.csv")


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else ""
    {"scrape": cmd_scrape, "tag": cmd_tag, "export": cmd_export}.get(
        cmd, lambda: print(__doc__))()
