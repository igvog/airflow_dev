# Airflow Project with PostgreSQL DWH and MinIO

Этот проект демонстрирует построение ETL-пайплайна на Apache Airflow с использованием MinIO как хранилища данных и PostgreSQL как DWH.

## Сервисы

Проект включает следующие компоненты:

* **Apache Airflow** — оркестрация задач
* **PostgreSQL DWH** — хранилище данных
* **MinIO** — объектное хранилище (аналог S3)
* **Docker / Docker Compose** — инфраструктура

## Предварительные требования

* Docker
* Docker Compose
* 4GB+ оперативной памяти
* 10GB+ свободного места на диске

## Файл `.env`

Должен содержать:

```
AIRFLOW_UID=1000
TELEGRAM_TOKEN=
TELEGRAM_CHAT_ID=
```

## Запуск проекта

### 1. Поднять все сервисы

```
docker-compose up -d
```

### 2. Проверить запущенные контейнеры

```
docker ps
```

## Доступ к сервисам

### Airflow Web UI

* **Логин:** airflow
* **Пароль:** airflow

### MinIO

* **Логин:** minioadmin
* **Пароль:** minioadmin

### PostgreSQL

* **База:** postgres
* **Пользователь:** postgres
* **Пароль:** postgres

## Настройка подключений в Airflow UI

### PostgreSQL Connection

* **Connection Id:** `pg_conn`
* **Connection Type:** postgres
* **Host:** postgres_dwh
* **Database:** postgres
* **Login:** postgres
* **Password:** postgres
* **Port:** 5432

### MinIO Connection

* **Connection Id:** `minio_conn`
* **Connection Type:** amazon web server
* **Extra:**

```
{
  "aws_access_key_id": "minioadmin",
  "aws_secret_access_key": "minioadmin",
  "endpoint_url": "http://minio:9000"
}
```

## Запуск DAG'ов

Порядок запуска:

1. `raw_from_api_to_minio`
2. `etl_raw_to_dwh`

## Структура проекта

```
project/
│── dags/
│   ├── raw_from_api_to_minio.py
│   ├── etl_raw_to_dwh.py
│── docker-compose.yml
│── .env
│── README.md
```

## Назначение пайплайна

* Выгрузка данных из API SpaceX
* Сохранение сырых данных в MinIO
* Обработка и загрузка в PostgreSQL DWH
