# 📦 Final Project: Airflow + Olist E-commerce Data Warehouse (DWH)

Итоговый проект по курсу Data Engineering.  
Реализован полный ETL-процесс: загрузка e-commerce датасета Olist в PostgreSQL, формирование staging-слоя и построение аналитического хранилища данных (DWH) по звёздной схеме.

Проект подготовлен для запуска в Docker + Airflow, полностью воспроизводим и соответствует всем требованиям финального задания.

---

# 🔍 1. Постановка задачи

Необходимо:

- Использовать настоящий e-commerce датасет (Kaggle — Olist).
- Загрузить сырые CSV-файлы в PostgreSQL (staging-уровень).
- Построить Data Warehouse по модели *звезда* (dim/fact).
- Реализовать ETL в Airflow:
  - логирование,
  - обработку ошибок (`try/except`),
  - кастомный Telegram alerting,
  - backfill и re-fill,
  - параметризацию (передача бизнес-даты),
  - поддержку `execution_date` как бизнес-даты.

---

# 📁 2. Используемый датасет

Источник:  
**Brazilian E-Commerce Public Dataset by Olist (Kaggle)**  

Используются CSV-файлы:

- `olist_orders_dataset.csv`
- `olist_order_items_dataset.csv`
- `olist_order_payments_dataset.csv`
- `olist_order_reviews_dataset.csv`
- `olist_customers_dataset.csv`
- `olist_geolocation_dataset.csv`

Все файлы должны находиться локально по пути:

./data/olist/

Копировать код

В контейнере мапятся в:

/opt/airflow/data/olist/

yaml
Копировать код

---

# 🧱 3. Архитектура решения

Проект строится в два слоя:

RAW CSV → STAGING (Postgres) → DWH (STAR SCHEMA)

markdown
Копировать код

## 3.1. Staging слой

Содержит таблицы:

- `stg_orders`
- `stg_order_items`
- `stg_order_payments`
- `stg_order_reviews`
- `stg_customers`
- `stg_geolocation`

Особенности:

- структура почти 1-в-1 как CSV;
- безопасная загрузка через `PostgresHook.insert_rows`;
- обработка пустых значений;
- батчевые вставки (5000 строк);
- try/except + логирование ошибок;
- кастомный Telegram alert при падении задачи.

## 3.2. DWH слой (звезда)

### Измерения (DIM):

1. `dim_customer`
2. `dim_geolocation`
3. `dim_payment_type`
4. `dim_date`  
   заполняется через `generate_series` (календарь на 20 лет)

### Факты (FACT):

- `fact_order_items`
- `fact_order_payments`
- `fact_order_reviews`

Особенности:

- Surrogate keys (`SERIAL`)
- Business keys уникальны
- Foreign keys на DIM таблицы
- Индексация для аналитики
- Идемпотентность: удаление + вставка (re-fill per date)

---

# 🏗 4. DAG #1 — RAW → STAGING  
**Файл:** `dags/ecommerce_raw_to_staging.py`

Основные задачи:

### ✔ create_staging_tables
Создаёт 6 таблиц `stg_*` при первом запуске.

### ✔ load_csv_to_staging (универсальный загрузчик)
- читает файл через Pandas,
- заменяет NaN на None,
- вставляет строки пачками в Postgres,
- логирует объём загруженных данных,
- ловит ошибки и отправляет Telegram alert.

### ✔ отдельные задачи загрузки:
- load_orders  
- load_order_items  
- load_order_payments  
- load_order_reviews  
- load_customers  
- load_geolocation  

Каждый task загружает свой CSV в свою staging таблицу.

### ✔ зависимости:
create_staging_tables → все load_* задачи

yaml
Копировать код

---

# 🌟 5. DAG #2 — STAGING → DWH (STAR SCHEMA)  
**Файл:** `dags/ecommerce_dwh_star_schema.py`

Основные компоненты:

### ✔ try/except decorator (`safe_execute`)
Оборачивает все Python-таски  
→ детальное логирование ошибок  
→ исключения не подавляются.

### ✔ create_dw_schema
Создаёт dim и fact таблицы.

### ✔ populate_dim_customer
- upsert по `customer_id`
- никакого дублирования

### ✔ populate_dim_geolocation
- DISTINCT по ZIP-кодам  
- ON CONFLICT DO NOTHING

### ✔ populate_dim_payment_type
- создаёт справочник типов оплаты

### ✔ populate_dim_date
- generate_series(2010–2030)
- форматирование surrogate key `YYYYMMDD`

### ✔ populate_fact_order_items / payments / reviews
Каждая таблица фактов:

1) сначала очищает записи за бизнес-день:

```sql
DELETE FROM fact_order_items WHERE date_key = ...
затем вставляет актуальные данные за этот день.

✔ Параметризация:
csharp
Копировать код
params: { "run_for_date": nullable string }
Если не передано вручную, используется execution_date.

Это позволяет:

backfill

re-fill

выборочное перестроение конкретного дня

⏳ 6. Backfill и Re-fill
В DAG #2 включено:

ini
Копировать код
catchup=True
start_date=2016-01-01
Backfill:
Airflow выполнит DAG за все исторические даты.

Re-fill:
Если запустить DAG вручную:

ini
Копировать код
run_for_date = "2017-10-04"
FACT таблицы пересоберутся только за этот день.

📣 7. Telegram alerting
Подключение в Airflow:
Connection:

Conn Id: telegram_conn

Conn Type: HTTP

Host: https://api.telegram.org

Login: <CHAT_ID>

Password: <BOT_TOKEN>

Встроенный alert:
Используется callback:

python
Копировать код
"on_failure_callback": telegram_alert
Сообщение содержит:

имя DAG

имя task

ошибку

статус FAIL

⚙️ 8. Docker окружение
Проект запускается через docker-compose:

Копировать код
docker-compose up -d
Сервисы:

Сервис	Назначение
postgres-airflow-db	БД метаданных Airflow
postgres-etl-target	БД с staging + dwh
airflow-services	webserver + scheduler

Airflow UI:

arduino
Копировать код
http://localhost:8080
📦 9. Package manager (UV)
Поддержан пункт 4 задания:
"Technical add.work: package manager to UV or poetry"

Файл:

Копировать код
pyproject.toml
Используется для декларации зависимостей:

toml
Копировать код
[project]
name = "airflow-project"
requires-python = ">=3.10"
dependencies = [
    "apache-airflow",
    "apache-airflow-providers-postgres",
    "apache-airflow-providers-http"
]
Установка:

bash
Копировать код
uv sync
Docker использует requirements.txt, UV — альтернативный менеджер.

🧪 10. Проверка результатов
После запуска DAG:

Проверка staging:
sql
Копировать код
SELECT COUNT(*) FROM stg_orders;
SELECT COUNT(*) FROM stg_order_items;
SELECT COUNT(*) FROM stg_order_payments;
SELECT COUNT(*) FROM stg_customers;
SELECT COUNT(*) FROM stg_geolocation;
Проверка dim:
sql
Копировать код
SELECT COUNT(*) FROM dim_customer;
SELECT COUNT(*) FROM dim_geolocation;
SELECT COUNT(*) FROM dim_payment_type;
SELECT COUNT(*) FROM dim_date;
Проверка fact:
sql
Копировать код
SELECT COUNT(*) FROM fact_order_items;
SELECT COUNT(*) FROM fact_order_payments;
SELECT COUNT(*) FROM fact_order_reviews;
Пример аналитического запроса:
sql
Копировать код
SELECT 
    d.year,
    d.month,
    SUM(price + freight_value) AS revenue
FROM fact_order_items f
JOIN dim_date d ON d.date_key = f.date_key
GROUP BY d.year, d.month
ORDER BY d.year, d.month;
🎯 11. Итоги проекта
Реализовано:

Полный ETL pipeline из RAW → STAGING → DWH.

Два DAG:

загрузка CSV,

построение звезды.

Идемпотентность:

upsert в DIM,

delete+insert в FACT.

Telegram оповещения.

Логирование и try/except.

Backfill и выборочная пересборка по датам.

Поддержка UV.

Полностью dockerized окружение.

Проект готов к использованию преподавателем без дополнительной настройки.

📌 Автор:
Arman A.
2025