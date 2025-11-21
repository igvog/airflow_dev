# Airflow ETL > DW (star schema)

Docker-compose окружение с Airflow 2.9.2 и двумя DAGами:
- `api_to_dw_star_schema`: JSONPlaceholder > Postgres > звёздная схема (dim_users, dim_dates, fact_posts).
- `olist_to_dw_star_schema`: Olist CSV > Postgres > звёздная схема (dim_customers/sellers/products/dates, fact_order_items/fact_payments).

## Быстрый старт
Требования: Docker Desktop.

1) Скопируй пример окружения и при необходимости подправь UID/GID (на Linux `id -u`/`id -g`):
```bash
cp .env.example .env
```
Опционально: пропиши `ALERT_EMAILS`, SMTP (если нужны алерты), свой путь к данным Olist через `OLIST_DATA_DIR` (по умолчанию `/opt/airflow/data/olist`, в репо данных нет).

2) Подними сервисы:
```bash
docker-compose up --build -d
```
Airflow UI: http://localhost:8080 (admin/admin).
Target Postgres витрины: `localhost:5433`, БД `etl_db`, user `etl_user`, pass `etl_pass`.

3) Создай connection `postgres_etl_target_conn` в Airflow (Admin > Connections) с параметрами:
- Host: `postgres-etl-target`
- Port: `5432` (внутри сети Docker)
- Database: `etl_db`
- User/Password: `etl_user` / `etl_pass`
Тип: Postgres. (Переменная `AIRFLOW_CONN_POSTGRES_ETL_TARGET_CONN` в `.env` создаст его автоматически.)

4) Данные Olist: положи CSV в `data/olist` (монтируется как `/opt/airflow/data/olist`). Если пусто, запусти DAG с `refresh_data_files=true` — он скачает их сам.

## Параметры DAGов
- `api_to_dw_star_schema`:
  - `run_date` (логическая дата загрузки, по умолчанию `data_interval_end`),
  - `full_refresh` (пересоздать staging/dim/fact при истине).
  Backfill: UI Trigger или CLI внутри контейнера: `airflow dags backfill api_to_dw_star_schema -s 2024-01-01 -e 2024-01-05`.
- `olist_to_dw_star_schema`:
  - `run_date` (default `2024-01-01`),
  - `full_refresh` (drop/recreate),
  - `refresh_data_files` (перекачать CSV),
  - `force_fail` (проверка алертов).

## Проверка данных
Подключись к `localhost:5433` (etl_db/etl_user/etl_pass) и выполни, например:
```sql
SELECT COUNT(*) FROM fact_posts;
SELECT COUNT(*) FROM fact_order_items;
SELECT COUNT(*) FROM fact_payments;
```
или запросы из `sql/olist_reporting_queries.sql`.

## Остановка
```bash
docker-compose down        # остановить
docker-compose down -v     # остановить и удалить volume с данными
```
