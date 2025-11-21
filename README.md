# """Airflow Project with PostgreSQL DWH and MinIO"""
Этот проект использует Apache Airflow для оркестрации данных с дополнительными сервисами PostgreSQL DWH и MinIO.

# Предварительные требования:
-Docker
-Docker Compose
-4GB+ оперативной памяти
-10GB+ свободного места на диске

# .env должен содержать 
AIRFLOW_UID=1000
TELEGRAM_TOKEN=
TELEGRAM_CHAT_ID=

# Запуск всех сервисов
docker-compose up -d

# Проверка сервисов
docker ps

<!-- Airflow UI: http://localhost:8080 -->
Логин: airflow
Пароль: airflow

<!-- MinIO Console: http://localhost:9001 -->
Логин: minioadmin
Пароль: minioadmin

<!-- PostgreSQL DWH: localhost:5433 -->
База: postgres
Пользователь: postgres
Пароль: postgres

# Postgres Conn UI Airflow
Connection Id: pg_conn
Connection Type: postgres
Host: postgres_dwh
Database: postgres
Login: postgres
Password: postgres
Port: 5432

# Minio Conn UI Airflow
​Connection Id: minio_conn
Connection Type: amazon web server
Extr: 
{
  "aws_access_key_id": "minioadmin",
  "aws_secret_access_key": "minioadmin",
  "endpoint_url": "http://minio:9000"
}

# Запуск DAG
Порядок запуска дага вручную
raw_from_api_to_minio -> etl_raw_to_dwh
