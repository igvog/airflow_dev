# Airflow ETL → DW (Star Schema)

Проект поднимает Airflow 2.9.2 (Docker) и DAG `api_to_dw_star_schema`, который выгружает данные из публичного API JSONPlaceholder, складывает их в Postgres и строит звёздную схему (staging → dims/fact).

## Olist DAG (olist_to_dw_star_schema)
- Датасет Olist скачивается в `data/olist` (монтируется как `/opt/airflow/data/olist` в контейнере). Чтобы перекачать файлы, передайте параметр `refresh_data_files=true` при запуске DAG.
- DAG: `olist_to_dw_star_schema`. Параметры: `run_date` (логическая дата загрузки, по умолчанию `2024-01-01`), `full_refresh` (по умолчанию `true`), `refresh_data_files` (по умолчанию `false`).
- Таблицы: staging (`staging_*`), затем dim (`dim_customers`, `dim_sellers`, `dim_products`, `dim_dates`) и факты (`fact_order_items`, `fact_payments`) в базе `postgres_etl_target`.
- Для backfill: Trigger DAG с нужным `run_date` или `airflow dags backfill`; при `full_refresh=true` staging/dim/fact таблицы пересоздаются.

## Что внутри
- `dags/api_to_dw_star_schema.py` — параметризованный DAG (поддержка backfill/refill, дата запуска через `run_date`, алерты, логи).
- `docker-compose.yaml` + `.env.example` — окружение Airflow + отдельный Postgres для витрины.
- `pyproject.toml`, `poetry.lock` — зависимости через Poetry, `requirements.txt` экспортирован для отладки вне контейнера.
- `plugins/.gitkeep` — заготовка под плагины.

## Быстрый старт
Требования: Docker Desktop, Docker Compose, Python 3.10+ (только если хотите управлять зависимостями локально).

1) Склонировать/развернуть ветку `final-project` и перейти в каталог:
```bash
cd airflow_dev-main
```

2) Создать `.env`:
```bash
cp .env.example .env
# На Linux/macOS: заменить AIRFLOW_UID на вывод `id -u`
# При необходимости указать ALERT_EMAILS через запятую
```

3) Поднять окружение (Docker Desktop должен быть запущен):
```bash
docker-compose up --build -d
```

Airflow UI: http://localhost:8080 (логин/пароль: `admin` / `admin`).  
Postgres витрины: `localhost:5433`, БД `etl_db`, пользователь `etl_user`, пароль `etl_pass`.

## Параметры DAG
- `run_date` — логическая дата загрузки (например, `2024-01-01`). По умолчанию берётся `data_interval_end`, поэтому backfill работает штатно.
- `full_refresh` — `true/false`, пересоздавать staging и витрину перед загрузкой (по умолчанию `true`).
- Подключение к БД создаётся автоматически, если в `.env` есть `AIRFLOW_CONN_POSTGRES_ETL_TARGET_CONN=postgresql+psycopg2://etl_user:etl_pass@postgres-etl-target:5432/etl_db`. Альтернатива — завести connection `postgres_etl_target_conn` через UI **Admin → Connections** (те же параметры).

## Backfill / re-fill
- Запуск за конкретную дату через UI: **Trigger DAG** → задать `run_date`.
- Серия дат через CLI контейнера:
```bash
docker exec -it airflow_services \
  airflow dags backfill api_to_dw_star_schema \
  -s 2024-01-01 -e 2024-01-05
```
Перезапуск за день: удалить дагран в UI или выполнить backfill тем же диапазоном — таблицы staging/drop перезаливка обеспечивают идемпотентность.

## Структура данных
- Staging: `staging_posts`, `staging_users`, `staging_comments` (сырые данные + `loaded_at` по параметру даты).
- Dimensions: `dim_users`, `dim_dates`.
- Fact: `fact_posts` (метрики: длина текста, word count, количество комментариев).

## Алерты и логирование
- Логи на уровне DAG-а через стандартный Airflow логгер.
- При фейле таска вызывает `_alert_on_failure` и шлёт письмо на `ALERT_EMAILS` (нужен настроенный `smtp_default` или иной e-mail backend в Airflow).

## Работа с зависимостями (Poetry)
В контейнере выполняется `poetry install --no-root` (см. `docker-compose.yaml`).  
Актуализация зависимостей:
```bash
poetry lock
poetry export -f requirements.txt --without-hashes -o requirements.txt
```

## Проверка результата
1) В UI включить DAG `api_to_dw_star_schema` и сделать Trigger.
2) Убедиться, что задачи зелёные (скриншот для отчёта).
3) Проверить данные:
```sql
SELECT COUNT(*) FROM staging_posts;
SELECT * FROM fact_posts ORDER BY post_id LIMIT 5;
```
4) Для отчёта — скриншоты UI (grid/graph + расписание) и таблиц (`staging_*`, `dim_*`, `fact_posts`).

## Остановка
```bash
docker-compose down      # остановить
docker-compose down -v   # остановить и удалить данные в volume
```
