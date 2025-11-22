from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import json
import csv
import ast

DATA_PATH = "/opt/airflow/data/movies/"


def create_tables():
    hook = PostgresHook(postgres_conn_id="postgres_etl_target_conn")
    sql = """
    CREATE SCHEMA IF NOT EXISTS dw;

    CREATE TABLE IF NOT EXISTS dw.dim_movie (
        movie_key BIGSERIAL  PRIMARY KEY,
        movie_id BIGINT UNIQUE,
        title TEXT,
        release_year BIGINT,
        genres TEXT
    );

    CREATE TABLE IF NOT EXISTS dw.dim_user (
        user_key SERIAL PRIMARY KEY,
        user_id INT UNIQUE
    );

    CREATE TABLE IF NOT EXISTS dw.fact_rating (
        rating_key SERIAL PRIMARY KEY,
        user_key INT,
        movie_key BIGINT,
        rating FLOAT,
        rating_timestamp TIMESTAMP,
        FOREIGN KEY(user_key) REFERENCES dw.dim_user(user_key),
        FOREIGN KEY(movie_key) REFERENCES dw.dim_movie(movie_key)
    );
    """
    hook.run(sql)


def load_dim_movie():
    hook = PostgresHook(postgres_conn_id="postgres_etl_target_conn")
    conn = hook.get_conn()
    cur = conn.cursor()

    df = pd.read_csv(DATA_PATH + "movies_metadata.csv", low_memory=False)

    # keep only numeric ids
    df = df[df["id"].str.isnumeric()]
    df["movie_id"] = df["id"].astype("Int64")  # use nullable integer

    # extract year
    df["release_year"] = pd.to_datetime(df["release_date"], errors="coerce").dt.year

    # genres field is JSON-like list
    def parse_genres(x):
        try:
            glist = ast.literal_eval(x)
            return ", ".join([g["name"] for g in glist])
        except:
            return None

    df["genres_clean"] = df["genres"].apply(parse_genres)

    for _, row in df.iterrows():
        # replace pd.NA or NaN with None for psycopg2
        movie_id = int(row["movie_id"]) if pd.notna(row["movie_id"]) else None
        title = row["title"] if pd.notna(row["title"]) else None
        release_year = int(row["release_year"]) if pd.notna(row["release_year"]) else None
        genres = row["genres_clean"] if pd.notna(row["genres_clean"]) else None

        if movie_id is None:
            continue  # skip rows without valid movie_id

        cur.execute("""
            INSERT INTO dw.dim_movie (movie_id, title, release_year, genres)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (movie_id) DO NOTHING;
        """, (movie_id, title, release_year, genres))

    conn.commit()


def load_dim_user():
    hook = PostgresHook(postgres_conn_id="postgres_etl_target_conn")
    conn = hook.get_conn()
    cur = conn.cursor()

    ratings = pd.read_csv(DATA_PATH + "ratings.csv")
    users = ratings[["userId"]].drop_duplicates()

    for _, row in users.iterrows():
        if pd.isna(row["userId"]):
            continue
        user_id = int(row["userId"])  # вот ключевое преобразование

        cur.execute("""
            INSERT INTO dw.dim_user (user_id)
            VALUES (%s)
            ON CONFLICT (user_id) DO NOTHING;
        """, (user_id,))

    conn.commit()
    


def load_fact_rating():
   

    hook = PostgresHook(postgres_conn_id="postgres_etl_target_conn")
    conn = hook.get_conn()
    cur = conn.cursor()

    ratings_path = "/opt/airflow/data/movies/ratings.csv"
    temp_path = "/opt/airflow/data/movies/ratings_prepared.csv"

    # -------- 1. Загружаем словари ключей --------
    cur.execute("SELECT user_id, user_key FROM dw.dim_user")
    user_map = {row[0]: row[1] for row in cur.fetchall()}

    cur.execute("SELECT movie_id, movie_key FROM dw.dim_movie")
    movie_map = {row[0]: row[1] for row in cur.fetchall()}

    # -------- 2. Готовим файл для COPY (пишем построчно, без Pandas) --------
    with open(ratings_path, "r") as infile, open(temp_path, "w", newline="") as outfile:
        reader = csv.DictReader(infile)
        writer = csv.writer(outfile)

        # header
        writer.writerow(["user_key", "movie_key", "rating", "rating_timestamp"])

        for row in reader:
            try:
                user_id = int(row["userId"])
                movie_id = int(row["movieId"])

                user_key = user_map.get(user_id)
                movie_key = movie_map.get(movie_id)

                # пропускаем строки, где нет соответствий
                if user_key is None or movie_key is None:
                    continue

                rating = float(row["rating"])
                ts = int(row["timestamp"])

                writer.writerow([user_key, movie_key, rating, ts])

            except Exception:
                continue

    # -------- 3. COPY в fact_rating -----------
    with open(temp_path, "r") as f:
        cur.copy_expert("""
            COPY dw.fact_rating(user_key, movie_key, rating, rating_timestamp)
            FROM STDIN WITH CSV HEADER
        """, f)

    conn.commit()
    cur.close()
    conn.close()

    print("fact_rating загружен успешно! (стрим, без падений по памяти)")


with DAG(
    dag_id="movies_dataset_dwh_etl",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False
):
    create = PythonOperator(
        task_id="create_tables",
        python_callable=create_tables
    )

    load_movies = PythonOperator(
        task_id="load_dim_movie",
        python_callable=load_dim_movie
    )

    load_users = PythonOperator(
        task_id="load_dim_user",
        python_callable=load_dim_user
    )

    load_ratings = PythonOperator(
        task_id="load_fact_rating",
        python_callable=load_fact_rating
    )

    create >> [load_movies, load_users] >> load_ratings
