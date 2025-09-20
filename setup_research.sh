#!/bin/bash

# Проверка аргумента
if [ -z "$1" ]; then
  echo "Ошибка: укажите название исследования (без пробелов!)"
  echo "Пример: ./setup_research.sh market_analysis"
  exit 1
fi

RESEARCH_NAME=$1
RESEARCH_DIR="researches/$RESEARCH_NAME"

# Создаём структуру папок
mkdir -p "$RESEARCH_DIR"/{data,notebooks,report}

# Создаём pyproject.toml
cat > "$RESEARCH_DIR/pyproject.toml" <<EOF
[project]
name = "$RESEARCH_NAME"
version = "0.1.0"
description = "Аналитическое исследование"
readme = "README.md"
requires-python = ">=3.13"

dependencies = [
    "jupyter",
    "pandas",
    "plotly"
]
EOF

# Переходим в директорию исследования и устанавливаем пакеты
cd "$RESEARCH_DIR" && uv add --quiet jupyter pandas requests plotly

# Возвращаемся обратно в корневую директорию
cd -

# Создаём базовый README
echo "# $RESEARCH_NAME" > "$RESEARCH_DIR/README.md"

echo "--------------------------------------------------"
echo "✅ Исследование '$RESEARCH_NAME' создано!"
echo "• Папка: $(pwd)/$RESEARCH_DIR"
echo "--------------------------------------------------"