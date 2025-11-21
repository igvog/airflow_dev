# 🚀 Airflow ETL Pipeline: Olist E-Commerce Data Warehouse

![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.9.2-blue?style=for-the-badge&logo=apache-airflow)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-13+-336791?style=for-the-badge&logo=postgresql)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=for-the-badge&logo=docker)
![Python](https://img.shields.io/badge/Python-3.9+-3776AB?style=for-the-badge&logo=python)

## 📖 Описание проекта

Этот проект реализует полный **ETL-пайплайн** (Extract, Transform, Load) для обработки данных электронной коммерции (Brazilian E-Commerce Public Dataset by Olist).

Система автоматически скачивает данные, загружает их в промежуточную область (Staging) и преобразует в аналитическое хранилище (DWH) по схеме **«Звезда» (Star Schema)**.

### Ключевые возможности

* ✅ **Оркестрация:** Полностью автоматизировано через Apache Airflow.
* ✅ **Архитектура:** Staging Area → Dimensions & Facts.
* ✅ **Идемпотентность:** Поддержка `UPSERT` (безопасные перезапуски).
* ✅ **Мониторинг:** Уведомления об ошибках через **Email** и **Telegram**.
* ✅ **Data Quality:** Встроенные проверки качества данных.

---

## 🏗 Архитектура

Проект состоит из двух основных DAG:

1. **`olist_to_dw_star_schema`** (Основной) — Обработка реальных данных Olist (8 таблиц).
2. **`api_to_dw_star_schema`** (Демо) — Пример работы с JSONPlaceholder API.

### Структура данных (Star Schema)

* **Facts (Факты):** `fact_order_items`, `fact_payments`.
* **Dimensions (Измерения):** `dim_customers`, `dim_sellers`, `dim_products`, `dim_dates`.

---

## 🚀 Быстрый старт

Для запуска вам понадобятся установленные **Docker Desktop** и **Docker Compose**.

### 1. Клонирование и настройка

```bash
# Перейдите в папку проекта
cd airflow_dev

# Создайте файл конфигурации из примера
cp .env.example .env
```

### 2. Настройка переменных (.env)

Откройте файл `.env` и убедитесь, что `AIRFLOW_UID` соответствует вашему пользователю (для Linux/macOS выполните `id -u`).

Для настройки алертов (опционально) укажите:

```ini
ALERT_EMAILS=ваша_почта@example.com
TELEGRAM_BOT_TOKEN=ваш_токен
TELEGRAM_CHAT_ID=ваш_chat_id
```

### 3. Запуск контейнеров

```bash
docker-compose up --build -d
```

После запуска (подождите 1–2 минуты) сервисы будут доступны по адресам:

| Сервис           | URL / Хост              | Логин / Пароль   |
|------------------|-------------------------|------------------|
| **Airflow UI**   | `http://localhost:8080` | `admin` / `admin`|
| **Postgres DWH** | `localhost:5433`        | `etl_user` / `etl_pass` |
| **Database Name**| `etl_db`                | -                |

---

## ⚙️ Управление DAG (Olist)

**DAG:** `olist_to_dw_star_schema`

### Параметры запуска (Configuration JSON)

При ручном запуске (Trigger DAG w/ config) можно передать параметры:

```json
{
  "run_date": "2024-01-01",
  "full_refresh": true,
  "refresh_data_files": false,
  "force_fail": false
}
```

* `run_date`: Логическая дата загрузки (влияет на поле `load_date`).
* `full_refresh`: `true` — пересоздать таблицы (DROP/CREATE), `false` — только UPSERT.
* `refresh_data_files`: `true` — принудительно перекачать CSV файлы с источника.
* `force_fail`: `true` — вызвать искусственную ошибку для теста алертов.

### Backfill (Историческая загрузка)

Загрузка данных за диапазон дат через CLI контейнера:

```bash
docker exec -it airflow_services \
    airflow dags backfill olist_to_dw_star_schema \
    -s 2024-01-01 -e 2024-01-07
```

---

## 🛠 Технические детали

### Файловая структура

```text
.
├── dags/
│   ├── olist_to_dw_star_schema.py  # Основной ETL пайплайн
│   └── api_to_dw_star_schema.py    # Демо пайплайн
├── data/olist/                     # Сюда скачиваются CSV (mounted volume)
├── sql/                            # SQL запросы для аналитики
├── docker-compose.yaml             # Описание инфраструктуры
├── requirements.txt                # Python зависимости
└── README.md                       # Документация
```

### Работа с зависимостями (Poetry)

Проект использует Poetry. При сборке Docker-образа выполняется установка зависимостей.  
Для обновления `requirements.txt` локально:

```bash
poetry lock
poetry export -f requirements.txt --without-hashes -o requirements.txt
```

---

## 📊 Проверка результатов

1. Зайдите в Airflow UI, активируйте DAG `olist_to_dw_star_schema` и нажмите **Trigger**.
2. Дождитесь, пока все квадратики в Grid View станут тёмно-зелёными.
3. Подключитесь к базе данных (DBeaver/DataGrip) и выполните проверочный запрос:

```sql
-- Топ категорий по выручке
SELECT 
    dp.product_category_name_english, 
    SUM(foi.price) AS revenue 
FROM fact_order_items AS foi
JOIN dim_products AS dp ON foi.product_key = dp.product_key
GROUP BY 1 
ORDER BY 2 DESC 
LIMIT 5;
```

---

## 🛑 Остановка проекта

```bash
# Остановить контейнеры
docker-compose down

# Остановить и удалить данные (полная очистка)
docker-compose down -v
```
