ETL-проект для датасета Olist (Airflow + PostgreSQL)

Этот проект реализует полный ETL-процесс для e-commerce датасета Olist с использованием Apache Airflow и PostgreSQL.
Пайплайн автоматически загружает сырые CSV-файлы, преобразует их в измерения и факты, а затем записывает в DWH.

Проект выполнен как учебная работа для понимания построения дата-пайплайнов и DWH-моделирования.

Структура проекта
```
airflow-docker/
│
├── dags/
│   ├── etl_olist_ecommerce.py      # главный DAG проекта
│   ├── sql/
│   │   └── create_olist_dwh.sql    # DWH схема (таблицы Dim/Fact)
│   └── data/                       # исходные CSV Olist
│
├── docker-compose.yaml             # запуск Airflow
├── .gitignore
└── README.md
```

## Step 1: Update .env File

Before you start, find your local user ID by running this in your terminal:

```bash
id -u
```

Open the `.env` file and replace `1000` with the number your terminal printed. This prevents file permission errors inside the Docker container.

## Step 2: Start the Environment

With Docker Desktop running, open a terminal in the project directory and run:

```bash
docker-compose up -d
```

This will:
- Pull the Postgres and Airflow images
- Start the two Postgres databases (one for Airflow, one for the ETL)
- Build the Airflow image, installing the Python packages from `requirements.txt`
- Start the Airflow webserver and scheduler

> **Note:** The first launch can take a few minutes as it downloads images and builds.

## Step 3: Access Airflow

Open your web browser and go to:

**http://localhost:8080**

Log in with the default credentials (set in the `docker-compose.yml`):
- **Username:** `airflow`
- **Password:** `airflow`

## Stopping the Environment

To stop all the containers, run:

```bash
docker-compose down
```

To stop and remove the database volumes (deleting all your data), run:

```bash
docker-compose down -v
```

