# COVID ETL → Postgres (Star Schema) + Error Logging

Этот проект содержит Airflow DAG, который:

-   загружает COVID-19 CSV данные,
-   сохраняет их в PostgreSQL (staging),
-   трансформирует в star schema (DW),
-   записывает ошибки в таблицу `etl_error_logs`,
-   при падении таска отправляет email для быстрой реакции на ошибки.

## Структура проекта

AIRFLOW_DEV/ │ ├── dags/ │ ├── **pycache**/ │ └──
api_to_dw_star_schema.py │ ├── logs/ │ ├── plugins/ │ ├── .env ├──
.gitignore ├── docker-compose.yaml ├── README.md └── requirements.txt

## Запуск Airflow

### 1. Установи зависимости

    pip install -r requirements.txt

### 2. Создай .env файл (если нет)

    AIRFLOW_UID = 
    AIRFLOW__SMTP__SMTP_HOST=smtp.gmail.com
    AIRFLOW__SMTP__SMTP_STARTTLS=True
    AIRFLOW__SMTP__SMTP_SSL=False
    AIRFLOW__SMTP__SMTP_PORT=587
    AIRFLOW__SMTP__SMTP_USER=your_email@gmail.com
    AIRFLOW__SMTP__SMTP_PASSWORD=SECRET KEY
    AIRFLOW__SMTP__SMTP_MAIL_FROM=your_email@gmail.com



### 3. Запусти Airflow через Docker

    docker compose up --build

Airflow будет доступен по адресу:

http://localhost:8080

## PostgreSQL

Создай подключение в Airflow:

-   Conn ID: `postgres_etl_target_conn`
-   Host: `postgres-etl-target`
-   User: `etl_user`
-   Password: `etl_pass`
-   Port: `5432`
-   DB: `etl_db`

## Создаваемые таблицы

### staging

-   staging_covid

### dimension tables

-   dim_country\
-   dim_dates

### fact table

-   fact_covid

### error logs

-   etl_error_logs

## Расписание

DAG выполняется ежедневно:

    schedule_interval=timedelta(days=1)

## Логи ошибок

При любой ошибке вставляется запись в таблицу:

    etl_error_logs (dag_id, task_id, execution_date, try_number, error_message)


## Зачем нужны таблицы в хранилище данных

### staging_covid  
Промежуточная таблица, куда попадает «сырые» данные без изменений.  
Нужна для:
- сохранения исходных данных,
- повторной загрузки без скачивания,
- упрощения дальнейших трансформаций.

### dim_country  
Справочник стран.  
Содержит уникальные страны, чтобы не хранить строковое название в миллионах записей фактов.  
Ускоряет запросы и уменьшает размер данных.

### dim_dates  
Календарная таблица.  
Позволяет делать аналитические запросы по датам: год, месяц, неделя, праздник, квартал.  
Обычно используется для удобного построения отчётов.

### fact_covid  
Фактологическая таблица.  
Содержит измеренные значения: случаи, смерти, вакцинация и т.д.  
Использует ключи на `dim_country` и `dim_dates`.  
Отсюда строятся дашборды, отчёты, BI-аналитика.

### etl_error_logs  
Логирование всех ошибок ETL.  
Позволяет:
- отслеживать, где сломался процесс,
- сохранять текст ошибки,
- не искать вручную по логам Airflow.

### Email-уведомления при ошибках

DAG настроен так, что при падении любого таска автоматически отправляется email на указанный адрес. 
Это помогает быстро реагировать на ошибки и отслеживать состояние ETL-процесса.


