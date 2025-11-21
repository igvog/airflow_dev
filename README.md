# Airflow ETL -> DW (Star Schema)

Проект поднимает Airflow 2.9.2 (Docker) и DAGи для выгрузки данных в Postgres и построения звёздной схемы.
- api_to_dw_star_schema: JSONPlaceholder -> Postgres -> звезда (staging -> dims/fact).
- olist_to_dw_star_schema: Olist CSV -> Postgres -> звезда (staging -> dims/fact).

Телеграм-алерты (опционально): TELEGRAM_BOT_TOKEN и TELEGRAM_CHAT_ID в .env (вместе с ALERT_EMAILS/SMTP).

## Olist DAG (olist_to_dw_star_schema)
- Датасет Olist скачивается в data/olist (монтируется как /opt/airflow/data/olist в контейнере). Чтобы перекачать файлы, передайте refresh_data_files=true при запуске DAG.
- Параметры: run_date (логическая дата загрузки, по умолчанию 2024-01-01), full_refresh (true по умолчанию), refresh_data_files (false по умолчанию), force_fail (тест алертов).
- Таблицы: staging (staging_*), затем dim (dim_customers, dim_sellers, dim_products, dim_dates) и факты (fact_order_items, fact_payments) в базе postgres_etl_target.
- Backfill: Trigger DAG с нужным run_date или airflow dags backfill; при full_refresh=true staging/dim/fact пересоздаются.

## Что внутри
- dags/api_to_dw_star_schema.py — параметризованный DAG (backfill/refill, run_date, алерты, логи).
- dags/olist_to_dw_star_schema.py — DAG для Olist CSV -> DW.
- docker-compose.yaml + .env.example — окружение Airflow + отдельный Postgres для витрины.
- pyproject.toml, poetry.lock — зависимости (Poetry), requirements.txt — экспорт для отладки.
- plugins/.gitkeep — заготовка под плагины.

## Быстрый старт
Требования: Docker Desktop, Docker Compose.

1) Перейти в каталог:
   cd airflow_dev

2) Создать .env:
   cp .env.example .env
   # На Linux/macOS: заменить AIRFLOW_UID на вывод id -u; при необходимости указать ALERT_EMAILS через запятую
   # Для Telegram: TELEGRAM_BOT_TOKEN и TELEGRAM_CHAT_ID

3) Поднять окружение (Docker Desktop должен быть запущен):
   docker-compose up --build -d

Airflow UI: http://localhost:8080 (login/password: admin / admin)
Postgres витрины: localhost:5433, БД etl_db, пользователь etl_user, пароль etl_pass.

## Параметры DAG
- run_date — логическая дата загрузки (например, 2024-01-01). По умолчанию data_interval_end, backfill работает штатно.
- full_refresh — true/false, пересоздавать staging и витрину перед загрузкой (по умолчанию true).
- Подключение к БД создаётся автоматически, если в .env есть AIRFLOW_CONN_POSTGRES_ETL_TARGET_CONN=postgresql+psycopg2://etl_user:etl_pass@postgres-etl-target:5432/etl_db. Альтернатива — создать connection postgres_etl_target_conn через UI Admin -> Connections (те же параметры).

## Backfill / re-fill
- Запуск за конкретную дату через UI: Trigger DAG -> run_date.
- Серия дат через CLI контейнера:
   docker exec -it airflow_services \
     airflow dags backfill api_to_dw_star_schema \
     -s 2024-01-01 -e 2024-01-05
- Перезапуск за день: удалить дагран в UI или выполнить backfill тем же диапазоном — staging/drop обеспечивает идемпотентность.

## Структура данных
- Staging: staging_posts, staging_users, staging_comments (сырые данные + loaded_at).
- Dimensions: dim_users, dim_dates (для api DAG); dim_customers, dim_sellers, dim_products, dim_dates (для olist DAG).
- Facts: fact_posts (метрики: длина текста, word count, число комментариев); fact_order_items/fact_payments (olist).

## Алерты и логирование
- Логи через стандартный Airflow логгер.
- _alert_on_failure шлёт письмо на ALERT_EMAILS и сообщение в Telegram (если TELEGRAM_BOT_TOKEN/CHAT_ID заданы).

## Работа с зависимостями (Poetry)
В контейнере выполняется poetry install --no-root (см. docker-compose.yaml).
Актуализация зависимостей:
   poetry lock
   poetry export -f requirements.txt --without-hashes -o requirements.txt

## Проверка результата
1) В UI включить DAG api_to_dw_star_schema и сделать Trigger.
2) Убедиться, что задачи зелёные (для отчёта — скрин Grid/Graph).
3) Проверить данные:
   SELECT COUNT(*) FROM staging_posts;
   SELECT * FROM fact_posts ORDER BY post_id LIMIT 5;
4) Для Olist — запросы из sql/olist_reporting_queries.sql (row counts, статусы, выручка, топы и т.п.).

## Остановка
   docker-compose down
   docker-compose down -v   # остановить и удалить данные в volume
