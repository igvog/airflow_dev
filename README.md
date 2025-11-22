# Car Sales Star Schema DWH | Airflow ETL Project

Полноценный ETL-пайплайн на Apache Airflow, который загружает исторические данные о продажах автомобилей (2018–2024) из CSV-файла в PostgreSQL в формате **звёздной схемы (Star Schema)**.

**Особенности проекта:**
- Полная звёздная схема (5 измерений + таблица фактов)
- Full refresh (TRUNCATE + COPY) — быстро и надёжно
- Telegram-уведомления о успехе и ошибках каждой задачи
- Безопасное хранение секретов через Airflow Variables
- Логирование, try/except, graceful cleanup
- Поддержка backfill (catchup=True)
- Параметризация (можно запускать на конкретную дату)
- Готов к продакшену

## Архитектура данных
staging_car_sales → dim_date, dim_customer, dim_car, dim_salesperson, dim_region → fact_car_sales
text### Таблицы измерений (Dimensions)
- `dim_date` — календарь (date_key, год, квартал, месяц, сезон и т.д.)
- `dim_customer` — клиенты + возрастная группа
- `dim_car` — автомобили (марка, модель, год)
- `dim_salesperson` — продавцы
- `dim_region` — регионы продаж

### Таблица фактов
- `fact_car_sales` — продажи (кол-во, цена, прибыль, комиссия и т.д.)

## Структура проекта
.
├── dags/
│   └── car_sales_star_schema_dwh.py          ← основной DAG
├── data/
│   └── car_sales_2018_2024_enhanced.csv      ← датасет (положи сюда!)
├── logs/                                     ← создаётся автоматически
├── docker-compose.yaml                       ← Airflow + 2 Postgres БД
├── .env                                      ← UID для избежания проблем с правами
├── requirements.txt                          ← pandas, requests и т.д.
└── README.md                                 ← ты читаешь его
text## Как запустить (Windows / Linux / Mac — всё одинаково)

### 1. Подготовка

```bash
# Клонируй / распакуй проект
cd airflow_car_sales_dwh

# Узнай свой UID (Linux/Mac) или оставь 1000 (Windows WSL)
id -u
# → запомни число (обычно 1000)

# Открой .env и замени 1000 на своё число
nano .env
# AIRFLOW_UID=1000  → AIRFLOW_UID=твоё_число
2. Запуск окружения
Bashdocker-compose up -d
Первый запуск займёт 5–10 минут (скачивает образы, ставит зависимости).
Проверь:
Bashdocker-compose ps
# Должны быть Up: postgres, postgres-etl-target, webserver, scheduler
3. Доступ к Airflow UI
Открой в браузере: http://localhost:8080
Логин / пароль: admin / admin
4. Настройка подключения к целевой БД
Admin → Connections → +
Заполни точно так:
ПолеЗначениеConnection Idpostgres_etl_target_connConnection TypePostgresHostpostgres-etl-targetSchemaetl_dbLoginetl_userPasswordetl_passPort5432
→ Test → Save
5. Добавь Telegram-уведомления (по желанию, но красиво)
Admin → Variables → + (два раза)
Важно: сначала напиши своему боту в Telegram /start, иначе он не сможет тебе писать!
6. Положи датасет
Скачай/положи файл car_sales_2018_2024_enhanced.csv в папку ./data/
7. Запуск полного ETL (один раз)
В UI:

Найди DAG → car_sales_star_schema_dwh
Нажми Trigger DAG
Поставь галку Catchup
Укажи даты: Start 2022-01-01, End — сегодня
→ Trigger

Или через терминал (быстрее):
Bashdocker-compose exec webserver airflow dags trigger car_sales_star_schema_dwh
Готово! Через 3–10 минут (зависит от размера CSV) всё загрузится.
8. Проверка результата
Подключись к целевой базе через любой клиент:

Host: localhost
Port: 5433
Database: etl_db
User: etl_user
Password: etl_pass

Выполни:
SQLSELECT COUNT(*) FROM fact_car_sales;
SELECT * FROM dim_customer LIMIT 10;
Должны быть десятки тысяч строк!
9. Ежедневная загрузка (автоматически)
После первого запуска DAG будет запускаться каждый день в 00:00 и перезаписывать DWH актуальными данными из CSV.
Остановка
Bashdocker-compose down        # остановить
docker-compose down -v     # удалить все данные (осторожно!)
Автор
Aliaskar — Data Engineer
Готов к новым вызовам
Проект сделан с любовью к чистому коду и надёжным пайплайнам.