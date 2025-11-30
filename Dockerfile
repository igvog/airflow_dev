# Используем официальный образ
FROM apache/airflow:2.9.2

# Копируем наш файл зависимостей
COPY pyproject.toml .

# ВАЖНО: Мы НЕ переключаемся на root. Мы остаемся пользователем airflow.
# 1. Устанавливаем uv через обычный pip
# 2. Устанавливаем наши библиотеки через uv
RUN pip install --no-cache-dir uv && \
    uv pip install --no-cache -r pyproject.toml