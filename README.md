# Final Project: Airflow + Olist E-commerce DWH

Итоговый проект по курсу Data Engineering: DAG в Airflow, который загружает e-commerce датасет Olist в Postgres и строит простую звездообразную схему (DWH).

## 1. Описание задачи

- Взять реальный e-commerce датасет с Kaggle.  
- Загрузить сырые CSV в Postgres (staging-слой).  
- Построить схему DWH типа "звезда" с измерениями (dim) и фактом (fact).  
- Реализовать DAG в Airflow c:
  - логированием,
  - обработкой ошибок (try/except),
  - алертами (email/Telegram),
  - поддержкой backfill и re-fill,
  - использованием `execution_date` в качестве бизнес-даты.

## 2. Используемый датасет

Источник: Kaggle - **Brazilian E-Commerce Public Dataset by Olist**  
(подробнее см. по ссылке в README шаблонного репозитория `igvog/airflow_dev`).

Используемые файлы:

- `olist_orders_dataset.csv`
- `olist_order_items_dataset.csv`
- `olist_products_dataset.csv`
- `olist_customers_dataset.csv`
- `olist_order_payments_dataset.csv`

Все файлы ожидаются внутри контейнера Airflow по пути:

/opt/airflow/dags/data/

Локально - положить CSV в:

./dags/data/

## 3. Архитектура решения

### 3.1. Staging-слой (Postgres)

Создаются временные таблицы для сырых CSV:

- `final_staging_orders`
- `final_staging_order_items`
- `final_staging_products`
- `final_staging_customers`
- `final_staging_order_payments`

Особенности:

- Структура близка к CSV + поле `raw_data JSONB` и `loaded_at TIMESTAMP`.
- Загрузка через `PostgresHook.insert_rows` батчами (`BATCH_SIZE = 5000`).
- Везде используется `try/except` и логирование через `logging`.

### 3.2. DWH (звездообразная схема)

**Измерения:**

- `final_dim_date`  
  - `date_key` (формат `YYYYMMDD`, PK)  
  - `full_date`, `year`, `quarter`, `month`, `day`, `day_of_week`, `is_weekend` и т.п.
- `final_dim_customer`  
  - `customer_key` (PK, surrogate)  
  - `customer_id` (business key, UNIQUE)  
  - `customer_unique_id`, `zip`, `city`, `state`, audit-поля `created_at`, `updated_at`.
- `final_dim_product`  
  - `product_key` (PK, surrogate)  
  - `product_id` (business key, UNIQUE)  
  - `product_category_name`, длины названия/описания, количество фото, размеры/вес.

**Факт:**

- `final_fact_orders`  
  - Grain: **строка заказа** (`order_id` + `order_item_id`).  
  - Ключи:
    - `order_item_key` - surrogate PK,
    - `customer_key` → `final_dim_customer`,
    - `product_key` → `final_dim_product`,
    - `order_purchase_date_key` → `final_dim_date`.
  - Поля:
    - из orders: статус, даты покупки/доставки/оценки,
    - из order_items: `price`, `freight_value`, `shipping_limit_date`,
    - из payments: тип оплаты, количество платежей, `payment_value`,
    - `load_datetime` для аудита.
  - Уникальность строки факта:
    - `UNIQUE (order_id, order_item_id)`.

## 4. DAG в Airflow

**Файл:** `dags/iskander_final_project_dag.py`  
**DAG id:** `iskander_final_project_dag`  
**Расписание:** `@daily`  
**start_date:** `2016-10-04` (под диапазон дат Olist)  
**catchup:** `True` (поддержка backfill)

Основные задачи:

1. `create_staging_tables`  
   - Дропает и создаёт `final_staging_*` таблицы.

2. `load_orders_to_staging`  
3. `load_order_items_to_staging`  
4. `load_products_to_staging`  
5. `load_customers_to_staging`  
6. `load_payments_to_staging`  
   - Читают CSV через `csv.DictReader`.
   - Батчевые вставки в staging, логируют количество строк.
   - Обрабатывают пустые значения (`check_null_values`).

7. `create_dw_schema`  
   - Дропает и пересоздаёт `final_dim_date`, `final_dim_customer`, `final_dim_product`, `final_fact_orders`.
   - Создаёт PK/FK и индексы для факта.

8. `populate_dim_date`  
   - Заполняет календарь `final_dim_date` на диапазон `2016-01-01` … `2018-12-31`
     через `generate_series`.
   - `ON CONFLICT (date_key) DO NOTHING` - безопасный повторный запуск.

9. `populate_dim_customer`  
   - Загружает уникальных клиентов из `final_staging_customers`.  
   - `ON CONFLICT (customer_id) DO UPDATE` - upsert, без дублей.

10. `populate_dim_product`  
    - Аналогично для товаров из `final_staging_products`.  

11. `populate_fact_orders`  
    - Определяет бизнес-дату:
      - берётся `data_interval_start` / `execution_date` DAG’а,
      - вычисляется `target_date` и `date_key = YYYYMMDD`.
    - Логика re-fill:
      - сначала `DELETE FROM final_fact_orders WHERE order_purchase_date_key = :date_key`,
      - затем вставка всех строк факта за этот день из staging + dim’ов.
    - Идемпотентность по дате: повторный прогон за одну и ту же дату не создаёт дублей.

12. `alert_email_on_failure`  
    - `EmailOperator` с `trigger_rule="one_failed"`.
    - Отправляет письмо при падении любого из основных тасков.

13. `alert_telegram_on_failure` (опционально)  
    - `SimpleHttpOperator` → Telegram Bot API.  
    - Отправляет сообщение в чат при ошибке DAG.

## 5. Поднятие окружения

Порядок поднятия Docker-окружения, настройки Airflow и Postgres - как в README шаблонного репозитория `igvog/airflow_dev`.

В рамках этого проекта дополнительно требуется только:

1. Локально положить CSV-файлы Olist в `./dags/data/`.  
2. Настроить email-alerting (или отключить его):

   - В Airflow UI (Admin → Variables) создать переменную:
     - `ALERT_EMAIL` = `sedinkar@gmail.com` (или другой адрес).
   - В Admin → Connections настроить SMTP (`smtp_default`) с вашим app-паролем.

3. (Опционально) Настроить Telegram-алерты:

   - В Admin → Connections:
     - `telegram_api` - HTTP, host: `https://api.telegram.org`.
   - В Admin → Variables:
     - `TELEGRAM_BOT_TOKEN` - токен бота от BotFather.
     - `TELEGRAM_CHAT_ID` - id чата/группы для уведомлений.

Если алерты не нужны, можно:

- в `default_args` поставить `email_on_failure=False`,  
- или закомментировать таски `alert_email_on_failure` / `alert_telegram_on_failure`  
  и соответствующие зависимости.

## 6. Backfill и запуск на любую дату

- DAG настроен на `@daily` и `catchup=True`:  
  при включении можно прогнать исторические даты в диапазоне от `start_date` до текущей.

- Для запуска за конкретную дату (например, `2017-05-01`):
  - в Airflow Web UI создать manual run с `execution_date = 2017-05-01`,
  - DAG возьмёт эту дату как бизнес-день,
  - `populate_fact_orders` пересчитает факт только за этот день.

- Благодаря `DELETE ... WHERE order_purchase_date_key = :date_key`  
  перед вставкой данные за день всегда перезаписываются и не дублируются.

## 7. Проверка результата

Примеры запросов в целевой БД (`etl_db`):

SELECT COUNT(*) FROM final_dim_date;
SELECT COUNT(*) FROM final_dim_customer;
SELECT COUNT(*) FROM final_dim_product;

SELECT COUNT(*) FROM final_fact_orders;

SELECT
    d.year,
    d.month,
    SUM(f.price + f.freight_value) AS gross_revenue
FROM final_fact_orders f
JOIN final_dim_date d
  ON d.date_key = f.order_purchase_date_key
GROUP BY d.year, d.month
ORDER BY d.year, d.month;

После успешного прогона DAG’а:

- В Airflow UI можно показать граф зависимостей и статус задач.  
- В Postgres - скриншоты таблиц `final_dim_*` и `final_fact_orders` с данными.
