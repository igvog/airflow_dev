# Final Project: Airflow + Olist E-commerce DWH

Итоговый проект по курсу Data Engineering: DAG в Airflow, который загружает e-commerce датасет Olist в Postgres и строит звездообразную схему (DWH) вокруг факта заказов.

## 1. Постановка задачи

Необходимо:

- Взять реальный e-commerce датасет с Kaggle.
- Загрузить сырые CSV в Postgres (staging-слой).
- Построить DWH-схему типа «звезда» с измерениями (dim) и фактом (fact).
- Реализовать DAG в Airflow с:
  - логированием,
  - обработкой ошибок (try/except),
  - алертами (Telegram),
  - поддержкой backfill и re-fill по дате,
  - использованием `execution_date` как бизнес-даты.

Базовый шаблон окружения и Docker-конфигурация взяты из репозитория `igvog/airflow_dev`.

## 2. Используемый датасет

Источник: Kaggle - **Brazilian E-Commerce Public Dataset by Olist**  

Датасет включает много таблиц. В рамках проекта используется подмножество, непосредственно связанное с фактом заказов:

- `olist_orders_dataset.csv` - заказы (ядро факта),
- `olist_order_items_dataset.csv` - позиции заказов (order lines),
- `olist_products_dataset.csv` - товары,
- `olist_customers_dataset.csv` - покупатели,
- `olist_order_payments_dataset.csv` - платежи.

Остальные таблицы (например, продавцы, отзывы, геолокация и т.д.) в этот DWH *специально не включались*, чтобы сфокусироваться на витрине продаж вокруг центра фактов - заказов.

### Размещение файлов

Локально CSV-файлы нужно положить в каталог:

```text
./dags/data/
```

В docker-compose эта папка монтируется в контейнер Airflow как:

```text
/opt/airflow/dags/data
```

Именно отсюда DAG читает файлы (`DATA_DIR = "/opt/airflow/dags/data"`).

## 3. Архитектура решения

### 3.1. Staging-слой (Postgres)

Создаются промежуточные таблицы (staging) в базе `etl_db` (контейнер `postgres-etl-target`):

- `final_staging_orders`
- `final_staging_order_items`
- `final_staging_products`
- `final_staging_customers`
- `final_staging_order_payments`

Структура таблиц близка к исходным CSV + технические поля:

- `raw_data JSONB` - полный исходный ряд в JSON-формате,
- `loaded_at TIMESTAMP` - момент загрузки.

Загрузка реализована отдельными Python-задачами:

- `load_orders_to_staging`
- `load_order_items_to_staging`
- `load_products_to_staging`
- `load_customers_to_staging`
- `load_payments_to_staging`

Особенности:

- чтение через `csv.DictReader`,
- батчевые вставки в Postgres через `PostgresHook.insert_rows` (`BATCH_SIZE = 5000`),
- обработка пустых значений (`check_null_values` → `NULL`),
- `try/except` вокруг чтения и вставки, логирование ошибок через `logging`.

Точка входа: `create_staging_tables` - дропает и пересоздаёт все `final_staging_*`.

### 3.2. DWH (звездообразная схема)

Схема строится в той же базе `etl_db`.

**Измерения:**

1. `final_dim_date`
   - `date_key` (PK, формат `YYYYMMDD`),
   - `full_date`, `year`, `quarter`, `month`, `month_name`,
   - `day`, `day_of_week`, `day_name`, `is_weekend`.
   - Заполняется из `generate_series('2016-01-01' .. '2018-12-31')` с `ON CONFLICT (date_key) DO NOTHING`.

2. `final_dim_customer`
   - `customer_key` (PK, surrogate),
   - `customer_id` (business key, `UNIQUE`),
   - `customer_unique_id`, `customer_zip_code_prefix`, `customer_city`, `customer_state`,
   - `created_at`, `updated_at`.
   - Заполняется из `final_staging_customers` с `ON CONFLICT (customer_id) DO UPDATE` (upsert, без дублей).

3. `final_dim_product`
   - `product_key` (PK, surrogate),
   - `product_id` (business key, `UNIQUE`),
   - `product_category_name`,
   - `product_name_lenght`, `product_description_lenght`, `product_photos_qty`,
   - `product_weight_g`, `product_length_cm`, `product_height_cm`, `product_width_cm`,
   - `created_at`, `updated_at`.
   - Заполняется из `final_staging_products` с `ON CONFLICT (product_id) DO UPDATE`.

**Факт:**

`final_fact_orders` - таблица фактов по строкам заказов.

- Grain: **строка заказа** (`order_id` + `order_item_id`).
- Ключи:
  - `order_item_key` - surrogate PK,
  - `customer_key` → `final_dim_customer`,
  - `product_key` → `final_dim_product`,
  - `order_purchase_date_key` → `final_dim_date`.
- Поля:
  - из orders: статус, даты покупки/подтверждения/доставки, ETA,
  - из order_items: `shipping_limit_date`, `price`, `freight_value`,
  - из payments: `payment_type`, `payment_installments`, `payment_value`,
  - `load_datetime` - время загрузки строки факта.
- Уникальность строки:
  - `UNIQUE (order_id, order_item_id)`.

Индексы:

- по `customer_key`, `product_key`, `order_purchase_date_key`, `order_id`
  для типовых аналитических запросов.

### 3.3. DAG и планировщик

**Файл DAG:**  
`dags/iskander_final_project_dag.py`

**DAG id:**  
`iskander_final_project_dag`

**Подключение к целевой БД:**  
`postgres_etl_target_conn` (Postgres, БД `etl_db`, контейнер `postgres-etl-target`).

**Основные настройки DAG:**

- `schedule_interval='@daily'` - ежедневный запуск,
- `start_date=datetime(2017, 1, 1)` - начало бизнес-календаря
- `catchup=True` - поддержка backfill по историческим датам,
- `max_active_runs=1` - избегаем гонок при пересоздании/обновлении таблиц,
- `max_active_tasks=4` - ограничение параллелизма.

Основные группы задач:

1. Staging:
   - `create_staging_tables`
   - `load_orders_to_staging`
   - `load_order_items_to_staging`
   - `load_products_to_staging`
   - `load_customers_to_staging`
   - `load_payments_to_staging`

2. DWH:
   - `create_dw_schema` - `CREATE TABLE IF NOT EXISTS` для всех dim/fact.
   - `populate_dim_date`
   - `populate_dim_customer`
   - `populate_dim_product`
   - `populate_fact_orders`

Связи:

- Все `load_*` зависят от `create_staging_tables`.
- `create_dw_schema` зависит от всех `load_*`.
- `populate_dim_date` зависит от `create_dw_schema`.
- `populate_dim_customer` зависит от `create_dw_schema` и `load_customers`.
- `populate_dim_product` зависит от `create_dw_schema` и `load_products`.
- `populate_fact_orders` зависит от:
  - `populate_dim_date`,
  - `populate_dim_customer`,
  - `populate_dim_product`,
  - `load_orders`, `load_order_items`, `load_payments`.

### 3.4. Backfill и re-fill (идемпотентность по дате)

Задача `populate_fact_orders`:

1. Определяет бизнес-день:

   ```python
   logical_dt = (
       context.get("data_interval_start")
       or context.get("logical_date")
       or context["execution_date"]
   )
   target_date = logical_dt.date()
   target_date_str = target_date.strftime("%Y-%m-%d")
   date_key = int(target_date.strftime("%Y%m%d"))
   ```

2. Перед вставкой факта делает очистку за этот день:

   ```sql
   DELETE FROM final_fact_orders
   WHERE order_purchase_date_key = %(date_key)s;
   ```

3. Затем вставляет все строки факта за этот день, собранные из staging и измерений:

   ```sql
   WHERE o.order_purchase_timestamp::date = %(target_date)s;
   ```

Это обеспечивает:

- **backfill** - при включённом `catchup=True` можно прогнать весь период с 2017-01-01 до текущей даты;
- **re-fill** - повторный запуск DAG за ту же дату перезапишет данные за день без дублей.

## 4. Алерты: только Telegram

Изначально планировались email-алерты через `EmailOperator`, но от них отказался:

- для локального окружения требуется настраивать SMTP, порты, доступы к внешнему почтовому серверу;
- это добавляет лишнюю инфраструктурную сложность и не укладывается в сроки итогового проекта.

В результате:

- **email-алерты отключены** (нет `EmailOperator`, `email_on_failure=False`),
- оставлены только уведомления в Telegram через `SimpleHttpOperator` и Telegram Bot API.

### Настройка Telegram-алертов

В DAG добавлена задача:

```python
alert_telegram = SimpleHttpOperator(
    task_id="alert_telegram_on_failure",
    http_conn_id="telegram_api",
    endpoint="bot{{ var.value.TELEGRAM_BOT_TOKEN }}/sendMessage",
    method="POST",
    headers={"Content-Type": "application/json"},
    data=json.dumps(
        {
            "chat_id": "{{ var.value.TELEGRAM_CHAT_ID }}",
            "text": "DAG {{ dag.dag_id }} failed on {{ ds }} (run_id={{ run_id }})",
        }
    ),
    trigger_rule="one_failed",
)
```

Алерт срабатывает, если любой из основных тасков DAG упал (`trigger_rule="one_failed"`).

**Что требуется настроить в Airflow:**

1. **Connection** (Admin → Connections):

   - Conn Id: `telegram_api`
   - Conn Type: `HTTP`
   - Host: `https://api.telegram.org`

2. **Variables** (Admin → Variables):

   - `TELEGRAM_BOT_TOKEN` - токен бота (от BotFather),
   - `TELEGRAM_CHAT_ID` - id чата или группы, куда слать уведомления.

## 5. Пакетный менеджер: pyproject.toml и uv

Для фиксации зависимостей и в соответствии с требованием «Technical add.work: package manager to UV or poetry» используется `pyproject.toml` с секцией `[tool.uv]`.

Пример содержимого:

```toml
[project]
name = "airflow-dev"
version = "0.1.0"
description = "Capstone final project: Airflow + Olist data source + DWH"
requires-python = ">=3.10"

dependencies = [
    "apache-airflow",
    "apache-airflow-providers-postgres",
    "apache-airflow-providers-http",  # нужен для SimpleHttpOperator (Telegram)
]

[tool.uv]
```

Также есть `requirements.txt`, который используется в docker-compose для установки зависимостей внутри контейнера Airflow:

```yaml
- ./requirements.txt:/requirements.txt
...
command:
  - -c
  - |
    pip install -r /requirements.txt;
    airflow db init;
    airflow users create ...;
    (airflow webserver & airflow scheduler)
```

Таким образом:

- зависимости явно задекларированы и для uv (`pyproject.toml`),
- и для Docker-окружения (`requirements.txt`).

## 6. Запуск окружения и DAG

### 6.1. Docker Compose

Запуск стандартный (аналогично README из `igvog/airflow_dev`):

```bash
docker-compose up -d
```

Основные сервисы:

- `postgres-airflow-db` - БД метаданных Airflow,
- `postgres-etl-target` - целевая БД `etl_db` для DWH,
- `airflow-services` - webserver + scheduler Airflow.

### 6.2. Подготовка данных

1. Скачать CSV-файлы Olist (из Kaggle).
2. Положить их локально в:

   ```text
   ./dags/data/
   ```

3. Убедиться, что в контейнере по пути `/opt/airflow/dags/data` файлы видны.

### 6.3. Настройка Airflow

1. Создать Connection `postgres_etl_target_conn` (если не создан автоматически):

   - Conn Type: `Postgres`
   - Host: `postgres-etl-target`
   - Port: `5432`
   - Schema: `etl_db`
   - Login: `etl_user`
   - Password: `etl_pass`

2. По желанию - настроить Telegram-алерты (см. раздел 4).

### 6.4. Запуск DAG

1. В Airflow UI включить DAG `iskander_final_project_dag`.
2. Для проверки можно сначала запустить вручную за конкретную дату, например:

   - `execution_date = 2017-10-02` (дата, где в датасете точно есть заказы).

3. При включённом `catchup=True` Airflow может автоматически прогнать весь диапазон от `start_date` до текущей даты.

## 7. Проверка результатов

Примеры запросов в БД `etl_db`:

```sql
-- Проверка, что staging заполнен
SELECT COUNT(*) FROM final_staging_orders;
SELECT COUNT(*) FROM final_staging_order_items;
SELECT COUNT(*) FROM final_staging_customers;
SELECT COUNT(*) FROM final_staging_products;
SELECT COUNT(*) FROM final_staging_order_payments;

-- Проверка, что размерности заполнены
SELECT COUNT(*) FROM final_dim_date;
SELECT COUNT(*) FROM final_dim_customer;
SELECT COUNT(*) FROM final_dim_product;

-- Проверка факта
SELECT COUNT(*) FROM final_fact_orders;
```

Пример простой витрины выручки по годам и месяцам:

```sql
SELECT
    d.year,
    d.month,
    SUM(f.price + f.freight_value) AS gross_revenue
FROM final_fact_orders f
JOIN final_dim_date d
  ON d.date_key = f.order_purchase_date_key
GROUP BY d.year, d.month
ORDER BY d.year, d.month;
```

Для отчётности по проекту подготовлены скриншоты и возникшие трудности в ходе работы с их решениями.
