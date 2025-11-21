# Airflow ETL Shugyla Assan

This guide walks you through setting up and running the Airflow environment defined in the `docker-compose.yml` file.
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
- **Username:** `admin`
- **Password:** `admin`

## Step 4: Create the Postgres Connection

This is the most important step for the ETL to work. You need to tell Airflow how to connect to the `postgres-etl-target` database.

1. In the Airflow UI, go to **Admin → Connections**
2. Click the **+** button to add a new connection
3. Fill in the form with these exact values:

   | Field | Value | Notes |
   |-------|-------|-------|
   | **Connection Id** | `postgres_etl_target_conn` | This must match the `ETL_POSTGRES_CONN_ID` in the DAG file |
   | **Connection Type** | `Postgres` | |
   | **Host** | `postgres-etl-target` | This is the service name from `docker-compose.yml` |
   | **Schema** | `etl_db` | From the `postgres-etl-target` environment variables |
   | **Login** | `etl_user` | From the `postgres-etl-target` environment variables |
   | **Password** | `etl_pass` | From the `postgres-etl-target` environment variables |
   | **Port** | `5432` | This is the port inside the Docker network, not the 5433 host port |

4. Click **Test**. It should show "Connection successfully tested."
5. Click **Save**.

## Step 5: Run Your ETL DAG

1. Go back to the Airflow DAGs dashboard
2. Find the `movie_ratings_etl_dag` DAG
3. Click the **Play** button (▶) on the right to trigger a manual run
4. You can click on the DAG name to watch the tasks run in the "Grid" or "Graph" view. If all goes well, all four tasks will turn green.

## Step 6: Verify the Data

How do you know it worked? Let's connect to the target database and check.

You can use any SQL client (like DBeaver, TablePlus, or pgAdmin) to connect to the `postgres-etl-target` database using these details:

- **Host:** `localhost`
- **Port:** `5433` (This is the host port you defined in `docker-compose.yml`)
- **Database:** `etl_db`
- **User:** `etl_user`
- **Password:** `etl_pass`


Movie Ratings ETL Project

Этот проект реализует ETL-процесс для загрузки данных о фильмах и пользовательских рейтингах в Data Warehouse (Star Schema).


## Ключевые компоненты:

Staging tables- промежуточные таблицы для исходных CSV-файлов.
Dimension tables (dim_users, dim_movies, dim_date) - справочные таблицы для аналитики.
Fact table (fact_ratings) - таблица фактов с рейтингами пользователей.
Airflow DAG -автоматизация загрузки, трансформации и обновления данных.

## Данные

Данные проекта взяты из публичного датасета на Kaggle: The Movies Dataset
https://www.kaggle.com/datasets/rounakbanik/the-movies-dataset/data?select=movies_metadata.csv 

## Используются файлы:
movies_metadata.csv - информация о фильмах: title, genres, release_date, budget, revenue, tagline и др.
ratings.csv -пользовательские рейтинги: userId, movieId, rating, timestamp.
для работы DAG файлы должны быть помещены в папку dags/files/.

## Структура проекта
│
├── dags/
│   ├── movie_ratings_star_schema_etl.py  
│   └── files/
│       ├── movies_metadata.csv
|       |   └── movies_metadata.csv       
│       └── ratings.csv
│         └── ratings.csv      
├── README.md
└── requirements.txt 

Установка и запуск

Клонировать репозиторий и перейти в ветку проекта

git clone <url_репозитория>
git checkout <твоя_ветка>





Настроить Airflow (step 3)
Настройте соединение с PostgreSQL через Airflow (postgres_etl_target_conn). (step 4)
Создайте базу данных для ETL.

Запустить DAG

DAG movie_ratings_star_schema_etl автоматически выполняет:
Загрузку CSV в staging.
Очистку и создание таблиц (staging + DW).
Заполнение dimension tables (dim_users, dim_movies, dim_date).
Заполнение fact table (fact_ratings).
DAG поддерживает параметризацию даты запуска (run_date), безопасный backfill и upsert для измерений.

Результат
Полностью подготовленный Star Schema DW, готовый к аналитике и построению отчетов.

## Технические особенности

Chunking — загрузка больших CSV частями для экономии памяти.
JSON parsing — обработка жанров фильмов в формате JSON.
Try-Catch — обработка ошибок при загрузке данных с логированием.
Parameterized DAG — можно запускать ETL для конкретной даты.
Upsert — обновление данных измерений при повторной загрузке.
Логирование и ошибки
Используется Python logging для отслеживания прогресса и ошибок.
При ошибках загрузки строки пропускаются с помощью AirflowSkipException.

Ensure your files are arranged as follows:




## Stopping the Environment

To stop all the containers, run:

```bash
docker-compose down
```

To stop and remove the database volumes (deleting all your data), run:

```bash
docker-compose down -v
```


Expected project output:
1. Code
2. Airflow DAG UI
3. Dataset in DB