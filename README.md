# Currency Aggregator ETL – Airflow Project

## 1. Описание проекта

Проект агрегирует курсы валют из трёх источников:

* **Frankfurter API**
* **CurrencyAPI.net**
* **NBK RSS**

Данные собираются, нормализуются и сохраняются в Postgres в виде **dim/fact таблиц**.
Реализовано обнаружение аномалий и отправка уведомлений по Email и Telegram.

**Цель проекта:** автоматизация сбора и обработки валютных курсов, возможность работы с историческими данными через backfill, удобное логирование и контроль аномалий.

Проект построен с использованием:

* **Airflow** – для оркестрации ETL;
* **PostgreSQL** – как целевая база данных;
* **Docker & Docker Compose** – для изоляции и быстрого запуска;
* **Poetry** – для управления зависимостями Python.

---

## 2. Структура проекта

```
.
├── dags/
│   └── currency_aggregator_etl.py    # основной DAG
├── logs/                             # логи Airflow
│   ├── scheduler/
│   ├── webserver/
│   └── task_logs/
├── plugins/                          # плагины Airflow (пусто)
├── docker-compose.yml                # конфигурация Docker и Airflow
├── .env                              # переменные окружения
├── pyproject.toml                     # конфигурация Poetry
├── poetry.lock                        # lock-файл зависимостей
├── README.md                         # эта инструкция
└── requirements.txt                  # зависимости Python (для Docker build)
```

---

## 3. Настройка и запуск проекта

### 3.1 Подготовка

1. Убедитесь, что установлены **Docker**, **Docker Compose** и **Poetry**.
2. Клонируйте репозиторий и перейдите в папку проекта:

```bash
git clone <URL репозитория>
cd airflow_dev
```

3. Настройте `.env` файл:

```env
AIRFLOW_UID=1000   # замените на ваш локальный UID (id -u)
ALERT_EMAIL=youremail@example.com
TG_TOKEN=<ваш_telegram_bot_token>
TG_CHAT=<ваш_chat_id>
```

4. Создайте папки для логов:

```powershell
mkdir logs
mkdir logs\scheduler
mkdir logs\webserver
mkdir logs\task_logs
```

5. Установите зависимости через **Poetry** (локально, если нужно):

```bash
poetry install
```

---

### 3.2 Запуск контейнеров

```bash
docker-compose up -d
```

* Контейнеры: Postgres для Airflow, Postgres для ETL, Airflow webserver + scheduler.
* После запуска в Airflow UI можно создавать DAG, проверять логи и запускать задачи.

Откройте Airflow UI:

```
http://localhost:8080
```

Логин: `admin`
Пароль: `admin`

---

### 3.3 Создание подключения к Postgres ETL

1. В Airflow UI → **Admin → Connections → +**
2. Настройте так:

| Поле      | Значение                 |
| --------- | ------------------------ |
| Conn Id   | postgres_etl_target_conn |
| Conn Type | Postgres                 |
| Host      | postgres-etl-target      |
| Schema    | etl_db                   |
| Login     | etl_user                 |
| Password  | etl_pass                 |
| Port      | 5432                     |

3. Нажмите **Test**, должно быть "Connection successfully tested."

---

### 3.4 Запуск DAG

* **Ручной запуск:**
  Airflow UI → DAGs → `currency_aggregator_etl` → Play ▶

* **Backfill / Re-run для исторических данных:**

```bash
docker exec -it airflow_services airflow dags backfill currency_aggregator_etl -s 2024-01-01 -e 2024-01-02
```

* **Проверка таблиц в Postgres:**

```sql
SELECT * FROM dim_api_source;

SELECT * FROM dim_currency;

SELECT  * FROM fact_alerts_log;

SELECT * FROM fact_crypto_rate;

SELECT  * FROM  fact_exchange_rate;

SELECT  * FROM  fact_daily_stats;
```

---

### 3.5 Логи

* Задачи → `logs/task_logs/<dag_id>/<task_id>/`
* Scheduler → `logs/scheduler/`
* Webserver → `logs/webserver/`

Все задачи используют `logging.info()` и `logging.warning()`.

---

### 3.6 Остановка проекта

```bash
docker-compose down
```

Для удаления данных БД:

```bash
docker-compose down -v
```

---

## 4. Особенности реализации

* DAG параметризуется через `dag_run.conf`, можно задать `target_date`.
* Настроен **alerting** через Email и Telegram.
* Реализован **backfill / re-fill** для исторических данных.
* Используется **Poetry** для управления зависимостями.
* Логи задач, scheduler и webserver сохраняются в локальные папки для удобного мониторинга.
* Таблицы **dim/fact** создаются автоматически через PythonOperators при первом запуске.

---

### 5. Результат работы

* В Airflow UI виден DAG с зелёными задачами.
* В базе данных созданы таблицы `dim_currency`, `dim_api_source`, `fact_exchange_rate`, `fact_daily_stats`, `fact_alerts_log`,`fact_crypto_rate`
* Данные загружены и агрегированы, аномалии выявляются и отправляются в виде уведомлений.
---
