"""
ETL DAG: API to Data Warehouse Star Schema
This DAG extracts data from JSONPlaceholder API, loads it into Postgres,
and transforms it into a star schema data warehouse model.
"""


from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
import requests
import json
import logging
import time
from dotenv import load_dotenv
import os
from airflow.utils.helpers import chain

load_dotenv()  

API_KEY = os.getenv("API_KEY")

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
with DAG(
    'api_to_dw_star_schema',
    default_args=default_args,
    description='ETL pipeline: API → Postgres → Star Schema DW',
    schedule_interval=timedelta(days=1),  # Run daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['etl', 'api', 'datawarehouse', 'star-schema'],
) as dag:

    # ========== STAGING LAYER ==========
    
    def create_staging_tables(**context):
        """Create staging tables for raw API data"""
        hook = PostgresHook(postgres_conn_id='postgres_etl_target_conn')
        
        # Drop existing staging tables
        drop_staging = """
        DROP TABLE IF EXISTS stg_movies  CASCADE;
        DROP TABLE IF EXISTS stg_actors  CASCADE;
        DROP TABLE IF EXISTS stg_credits  CASCADE;
        DROP TABLE IF EXISTS stg_genres CASCADE;
        DROP TABLE IF EXISTS stg_companies CASCADE;
        DROP TABLE IF EXISTS stg_language CASCADE;
        DROP TABLE IF EXISTS stg_countries CASCADE;
        """
        
        # Create staging tables
        create_staging = """
        CREATE TABLE IF NOT EXISTS stg_movies (
            movie_id INTEGER PRIMARY KEY,
            title TEXT,
            original_title TEXT,
            adult BOOLEAN,
            overview TEXT,
            status TEXT,
            tagline TEXT,
            original_language TEXT,
            runtime INTEGER,
            budget BIGINT,
            revenue BIGINT,
            popularity FLOAT,
            vote_average FLOAT,
            vote_count INT,
            homepage TEXT,
            release_date DATE,
            raw_json JSONB,
            loaded_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );


        CREATE TABLE IF NOT EXISTS stg_actors (
            actor_id INTEGER PRIMARY KEY,
            name TEXT,
            gender INT,
            popularity FLOAT,
            known_for_department TEXT,
            birthday DATE,
            deathday DATE,
            place_of_birth TEXT,
            biography TEXT,
            raw_json JSONB,
            loaded_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );


        CREATE TABLE IF NOT EXISTS stg_credits (
            credit_id SERIAL PRIMARY KEY,
            movie_id INTEGER,
            actor_id INTEGER,
            character_name TEXT,
            job TEXT,
            department TEXT,
            raw_json JSONB,
            loaded_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );


        CREATE TABLE IF NOT EXISTS stg_genres (
            genre_id INTEGER PRIMARY KEY,
            name TEXT,
            raw_json JSONB,
            loaded_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        
        CREATE TABLE IF NOT EXISTS stg_companies (
            company_id INTEGER PRIMARY KEY,
            name TEXT,
            origin_country TEXT,
            raw_json JSONB,
            loaded_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        
        CREATE TABLE IF NOT EXISTS stg_languages (
            language_code TEXT PRIMARY KEY,
            english_name TEXT,
            name TEXT,
            raw_json JSONB,
            loaded_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );


        CREATE TABLE IF NOT EXISTS stg_countries (
            country_code TEXT PRIMARY KEY,
            english_name TEXT,
            name TEXT,
            raw_json JSONB,
            loaded_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        """
        
        hook.run(drop_staging)
        hook.run(create_staging)
        logging.info("Staging tables created successfully")
    
    create_staging = PythonOperator(
        task_id='create_staging_tables',
        python_callable=create_staging_tables,
    )
    
    def fetch_api_data(**context):
        """Fetch data from JSONPlaceholder API"""
        BASE_URL = "https://api.themoviedb.org/3"
        
        try:
            
            # Fetch genres
            genres_resp = requests.get(f"{BASE_URL}/genre/movie/list", params={"api_key": API_KEY})
            genres_resp.raise_for_status()
            genres = genres_resp.json().get("genres", [])
            logging.info(f'Fetched {len(genres)} genres from API')

            # Fetch languages
            lang_resp = requests.get(f"{BASE_URL}/configuration/languages", params={"api_key": API_KEY})
            lang_resp.raise_for_status()
            lang = lang_resp.json()
            logging.info(f'Fetched {len(lang)} genres from API')


            # Fetch countries
            countries_resp = requests.get(f"{BASE_URL}/configuration/countries", params={"api_key": API_KEY})
            countries_resp.raise_for_status()
            countries = countries_resp.json()
            logging.info(f"Fetched {len(countries)} countries from API")

            movies = []

            # Fetch total pages
            first_page = requests.get(
                f"{BASE_URL}/discover/movie",
                params={"api_key": API_KEY, "page": 1, "sort_by": "popularity.desc"}
            )
            first_page.raise_for_status()

            first_page_data = first_page.json()
            total_pages = first_page_data.get("total_pages", 1)
            logging.info(f"Total pages available: {total_pages}")


            max_pages = min(total_pages, 250)
            logging.info(f"Will fetch {max_pages} pages (~{max_pages*20} movies).")

            # Fetching first page
            movies.extend(first_page_data.get("results", []))
            
            # Fetch all pages
            for page in range(2, max_pages + 1):
                page_resp = requests.get(f"{BASE_URL}/discover/movie",
                                            params={"api_key": API_KEY, "page": page})
                page_resp.raise_for_status()
                movies.extend(page_resp.json().get("results", []))

            logging.info(f"Fetched total {len(movies)} movies.")

            # Store in XCom for next tasks
            context['ti'].xcom_push(key='genres_data', value=genres)
            context['ti'].xcom_push(key='languages_data', value=lang)
            context['ti'].xcom_push(key='movies_data', value=movies)
            context['ti'].xcom_push(key='countries_data', value=countries)

            return {
                    "genres_count": len(genres),
                    "languages_count": len(lang),
                    "movies_count": len(movies)
                }
        except Exception as e:
            logging.error(f"Error fetching API data: {str(e)}")
            raise
    
    fetch_data = PythonOperator(
        task_id='fetch_api_data',
        python_callable=fetch_api_data,
    )
    

    def fetch_movie_details_and_cast(**context):
        """
        Takes movie list from fetch_api_data() XCom and enriches it:
        - movie_details: full metadata
        - movie_cast: actors per movie
        """

        BASE_URL = "https://api.themoviedb.org/3"
        ti = context["ti"]

        
        # pull movies from previous task
        movies = ti.xcom_pull(task_ids="fetch_api_data", key="movies_data")

        movie_details = []
        movie_cast = []

        
        for m in movies:
            try:
                movie_id = m["id"]

                
                detail_resp = requests.get(
                    f"{BASE_URL}/movie/{movie_id}",
                    params={"api_key": API_KEY}
                )
                detail_resp.raise_for_status()
                details = detail_resp.json()

                movie_details.append(details)


                credits_resp = requests.get(
                    f"{BASE_URL}/movie/{movie_id}/credits",
                    params={"api_key": API_KEY}
                )
                credits_resp.raise_for_status()
                credits = credits_resp.json().get("cast", [])

                movie_cast.append({
                    "movie_id": movie_id,
                    "cast": credits
                })

                logging.info(f"Fetched details + cast for movie {movie_id}")

            except requests.exceptions.RequestException as e:
                logging.error(f"Ошибка при запросе к API для movie_id {movie_id}: {e}")


        

        # Push results to XCom
        ti.xcom_push(key="movie_details_data", value=movie_details)
        ti.xcom_push(key="movie_cast_data", value=movie_cast)

        return {
            "details_count": len(movie_details),
            "cast_count": len(movie_cast)
        }

    fetch_data_details = PythonOperator(
        task_id='fetch_movie_details_and_cast',
        python_callable=fetch_movie_details_and_cast,
    )

    def load_movies_to_staging(**context):
        """Load movies data into staging table"""

        hook = PostgresHook(postgres_conn_id='postgres_etl_target_conn')
        movies = context['ti'].xcom_pull(
            task_ids='fetch_movie_details_and_cast',
            key='movie_details_data'
        )

        if not movies:
            logging.warning('No movies data found in XCom')
            return

        insert_query = """
        INSERT INTO stg_movies (
            movie_id, title, original_title, adult, overview, status, tagline,
            original_language, runtime, budget, revenue, popularity,
            vote_average, vote_count, homepage, release_date, raw_json
        ) VALUES (
            %(id)s, %(title)s, %(original_title)s, %(adult)s, %(overview)s, %(status)s, %(tagline)s,
            %(original_language)s, %(runtime)s, %(budget)s, %(revenue)s, %(popularity)s,
            %(vote_average)s, %(vote_count)s, %(homepage)s, %(release_date)s, %(raw_json)s
        )
        ON CONFLICT (movie_id) DO NOTHING;
        """

        for m in movies:
            # Convert raw dict → JSON string
            record = m.copy()
            record["raw_json"] = json.dumps(m)

            # Convert release_date → YYYY-MM-DD or None
            rd = m.get("release_date")
            if rd:
                try:
                    record["release_date"] = datetime.strptime(rd, "%Y-%m-%d").date()
                except:
                    record["release_date"] = None
            else:
                record["release_date"] = None

            hook.run(insert_query, parameters=record)

        logging.info(f"Inserted {len(movies)} movies into stg_movies")
    
    load_movies = PythonOperator(
        task_id='load_movies_to_staging',
        python_callable=load_movies_to_staging,
    )
    

    def load_actors_to_staging(**context):
        """Load users data into staging table"""

        hook = PostgresHook(postgres_conn_id='postgres_etl_target_conn')
        movie_cast_list = context['ti'].xcom_pull(
            task_ids='fetch_movie_details_and_cast',
            key='movie_cast_data'
        )

        if not movie_cast_list:
            logging.warning("No movie_cast data found in XCom")
            return

        actors_seen = set()

        insert_sql = """
        INSERT INTO stg_actors (
            actor_id, name, gender, popularity, known_for_department,
            birthday, deathday, place_of_birth, biography, raw_json
        ) VALUES (
            %(actor_id)s, %(name)s, %(gender)s, %(popularity)s, %(known_for_department)s,
            %(birthday)s, %(deathday)s, %(place_of_birth)s, %(biography)s, %(raw_json)s
        )
        ON CONFLICT (actor_id) DO NOTHING;
        """

        for movie_cast in movie_cast_list:
            for actor in movie_cast["cast"]:

                actor_id = actor.get("id")
                if not actor_id or actor_id in actors_seen:
                    continue

                actors_seen.add(actor_id)

                # IMPORTANT: create record with safe defaults
                record = {
                    "actor_id": actor.get("id"),
                    "name": actor.get("name"),
                    "gender": actor.get("gender"),
                    "popularity": actor.get("popularity"),
                    "known_for_department": actor.get("known_for_department"),

                    # credits API does NOT have these fields → use None
                    "birthday": actor.get("birthday"),
                    "deathday": actor.get("deathday"),
                    "place_of_birth": actor.get("place_of_birth"),
                    "biography": actor.get("biography"),

                    "raw_json": json.dumps(actor)
                }

                hook.run(insert_sql, parameters=record)

        logging.info(f"Loaded {len(actors_seen)} actors into stg_actors")
    
    load_actors = PythonOperator(
        task_id='load_actors_to_staging',
        python_callable=load_actors_to_staging,
    )
    

    def load_genres_to_staging(**context):
        """Load genres data into staging table"""

        hook = PostgresHook(postgres_conn_id='postgres_etl_target_conn')
        genres = context['ti'].xcom_pull(key='genres_data', task_ids='fetch_api_data')

        if not genres:
            logging.warning("No genres data found in XCom")
            return

        insert_sql = """
        INSERT INTO stg_genres (genre_id, name, raw_json)
        VALUES (%(id)s, %(name)s, %(raw_json)s)
        ON CONFLICT (genre_id) DO NOTHING;
        """

        for g in genres:
            record = g.copy()
            record['raw_json'] = json.dumps(g)  # <-- сериализация словаря в JSON
            hook.run(insert_sql, parameters=record)

        logging.info(f"Inserted {len(genres)} genres into stg_genres")
        
    load_genres = PythonOperator(
        task_id='load_genres_to_staging',
        python_callable=load_genres_to_staging,
    )


    def load_credits_to_staging(**context):
        """Load credits data into staging table"""

        hook = PostgresHook(postgres_conn_id='postgres_etl_target_conn')
        movie_cast_list = context['ti'].xcom_pull(
            task_ids='fetch_movie_details_and_cast',
            key='movie_cast_data'
        )

        if not movie_cast_list:
            logging.warning("No movie_cast data found in XCom")
            return

        insert_sql = """
        INSERT INTO stg_credits (
            movie_id, actor_id, character_name, job, department, raw_json
        ) VALUES (
            %(movie_id)s, %(actor_id)s, %(character_name)s, %(job)s, %(department)s, %(raw_json)s
        );
        """

        rows = 0

        conn = hook.get_conn()
        cur = conn.cursor()

        for movie_cast in movie_cast_list:
            movie_id = movie_cast["movie_id"]

            for cast in movie_cast["cast"]:
                record = {
                    "movie_id": movie_id,
                    "actor_id": cast.get("id"),
                    "character_name": cast.get("character"),
                    "job": cast.get("job"),
                    "department": cast.get("department"),
                    "raw_json": json.dumps(cast)
                }

                cur.execute(insert_sql, record)
                rows += 1

        conn.commit()  # <-- ключевой момент, сохраняем вставки
        cur.close()

        logging.info(f"Inserted {rows} credits into stg_credits")
    
    load_credits = PythonOperator(
        task_id='load_credits_to_staging',
        python_callable=load_credits_to_staging,
    )


    def load_languages_to_staging(**context):
        """Load languages data into staging table"""
        hook = PostgresHook(postgres_conn_id='postgres_etl_target_conn')
        languages = context['ti'].xcom_pull(key='languages_data', task_ids='fetch_api_data')
        
        if not languages:
            logging.warning("No languages data found in XCom")
            return

        insert_sql = """
        INSERT INTO stg_languages (language_code, english_name, name, raw_json)
        VALUES (%(iso_639_1)s, %(english_name)s, %(name)s, %(raw_json)s)
        ON CONFLICT (language_code) DO NOTHING;
        """

        for l in languages:
            record = l.copy()
            record['raw_json'] = json.dumps(l)  # <-- сериализуем словарь в JSON
            hook.run(insert_sql, parameters=record)

        logging.info(f"Inserted {len(languages)} languages into stg_languages")
    
    load_languages = PythonOperator(
        task_id='load_languages_to_staging',
        python_callable=load_languages_to_staging,
    )
    

    def load_countries_to_staging(**context):
        """Load countries data into staging table"""
        
        hook = PostgresHook(postgres_conn_id='postgres_etl_target_conn')
        countries = context['ti'].xcom_pull(task_ids='fetch_api_data', key='countries_data')

        if not countries:
            logging.warning("No countries data found in XCom")
            return

        insert_sql = """
        INSERT INTO stg_countries (country_code, english_name, raw_json)
        VALUES (%(iso_3166_1)s, %(english_name)s, %(raw_json)s)
        ON CONFLICT (country_code) DO NOTHING;
        """

        conn = hook.get_conn()
        cur = conn.cursor()
        rows = 0

        for c in countries:
            record = c.copy()
            record['raw_json'] = json.dumps(c)  # serialize dictionary to JSON
            cur.execute(insert_sql, record)
            rows += 1

        conn.commit()
        cur.close()

        logging.info(f"Inserted {rows} countries into stg_countries")

    load_countries = PythonOperator(
        task_id='load_countries_to_staging',
        python_callable=load_countries_to_staging,
    )

    def load_companies_to_staging(**context):
        """Load production companies data into staging table"""

        hook = PostgresHook(postgres_conn_id='postgres_etl_target_conn')
        movies = context['ti'].xcom_pull(task_ids='fetch_movie_details_and_cast', key='movie_details_data')

        if not movies:
            logging.warning("No movies data found in XCom")
            return

        insert_sql = """
        INSERT INTO stg_companies (company_id, name, origin_country, raw_json)
        VALUES (%(id)s, %(name)s, %(origin_country)s, %(raw_json)s)
        ON CONFLICT (company_id) DO NOTHING;
        """

        conn = hook.get_conn()
        cur = conn.cursor()
        rows = 0
        companies_seen = set()

        for m in movies:
            for company in m.get('production_companies', []):
                company_id = company.get('id')
                if company_id in companies_seen:
                    continue
                companies_seen.add(company_id)

                record = company.copy()
                record['raw_json'] = json.dumps(company)  # store full JSON
                cur.execute(insert_sql, record)
                rows += 1

        conn.commit()
        cur.close()

        logging.info(f"Inserted {rows} production companies into stg_companies")


    load_companies = PythonOperator(
        task_id='load_companies_to_staging',
        python_callable=load_companies_to_staging,
    )

    # ========== DATA WAREHOUSE LAYER (STAR SCHEMA) ==========
    
    create_dw_schema = PostgresOperator(
        task_id='create_dw_schema',
        postgres_conn_id='postgres_etl_target_conn',
        sql="""
        -- Drop existing DW tables
        DROP TABLE IF EXISTS fact_cast CASCADE;
        DROP TABLE IF EXISTS fact_movie_ratings CASCADE;
        DROP TABLE IF EXISTS dim_movie CASCADE;
        DROP TABLE IF EXISTS dim_actor CASCADE;
        DROP TABLE IF EXISTS dim_genre CASCADE;
        DROP TABLE IF EXISTS dim_company CASCADE;
        DROP TABLE IF EXISTS dim_language CASCADE;
        DROP TABLE IF EXISTS dim_country CASCADE;
        DROP TABLE IF EXISTS bridge_movie_genre CASCADE;
        DROP TABLE IF EXISTS bridge_movie_company CASCADE;
        
        -- Dimension: Actors
        CREATE TABLE dim_actor (
            actor_key SERIAL PRIMARY KEY,
            actor_id INTEGER UNIQUE NOT NULL,
            name VARCHAR(200),
            gender INT,
            known_for_department VARCHAR(100),
            birthday DATE,
            deathday DATE,
            place_of_birth VARCHAR(200),
            biography TEXT,
            popularity FLOAT,
            valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            valid_to TIMESTAMP,
            is_current BOOLEAN DEFAULT TRUE,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Dimension: Movies
        CREATE TABLE dim_movie (
            movie_key SERIAL PRIMARY KEY,
            movie_id INTEGER UNIQUE NOT NULL,
            title VARCHAR(300),
            original_title VARCHAR(300),
            adult BOOLEAN,
            overview TEXT,
            status VARCHAR(50),
            tagline TEXT,
            original_language VARCHAR(10),
            runtime INT,
            budget BIGINT,
            revenue BIGINT,
            popularity FLOAT,
            vote_average FLOAT,
            vote_count INT,
            homepage VARCHAR(300),
            release_date DATE,
            valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            valid_to TIMESTAMP,
            is_current BOOLEAN DEFAULT TRUE,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Dimension: Genre
        CREATE TABLE dim_genre (
            genre_key SERIAL PRIMARY KEY,
            genre_id INTEGER UNIQUE NOT NULL,
            name VARCHAR(100),
            valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            valid_to TIMESTAMP,
            is_current BOOLEAN DEFAULT TRUE,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Dimension: Companies
        CREATE TABLE dim_company (
            company_key SERIAL PRIMARY KEY,
            company_id INTEGER UNIQUE NOT NULL,
            name VARCHAR(200),
            origin_country VARCHAR(10),
            valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            valid_to TIMESTAMP,
            is_current BOOLEAN DEFAULT TRUE,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Dimension: Languages
        CREATE TABLE dim_language (
            language_code VARCHAR(10) PRIMARY KEY,
            english_name VARCHAR(100),
            name VARCHAR(100),
            valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            valid_to TIMESTAMP,
            is_current BOOLEAN DEFAULT TRUE,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Dimension: Countries
        CREATE TABLE dim_country (
            country_code VARCHAR(10) PRIMARY KEY,
            english_name VARCHAR(100),
            name VARCHAR(100),
            valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            valid_to TIMESTAMP,
            is_current BOOLEAN DEFAULT TRUE,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Bridge table: Movie ↔ Genre (many-to-many)
        CREATE TABLE bridge_movie_genre (
            movie_key INT REFERENCES dim_movie(movie_key),
            genre_key INT REFERENCES dim_genre(genre_key),
            PRIMARY KEY(movie_key, genre_key)
        );

        -- Bridge table: Movie ↔ Company (many-to-many)
        CREATE TABLE bridge_movie_company (
            movie_key INT REFERENCES dim_movie(movie_key),
            company_key INT REFERENCES dim_company(company_key),
            PRIMARY KEY(movie_key, company_key)
        );

        -- Fact table: Cast
        CREATE TABLE fact_cast (
            fact_cast_key SERIAL PRIMARY KEY,
            movie_key INT REFERENCES dim_movie(movie_key),
            actor_key INT REFERENCES dim_actor(actor_key),
            character_name VARCHAR(200),
            job VARCHAR(100),
            department VARCHAR(100),
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Fact table: Movie Ratings / Popularity
        CREATE TABLE fact_movie_ratings (
            fact_rating_key SERIAL PRIMARY KEY,
            movie_key INT REFERENCES dim_movie(movie_key),
            vote_average FLOAT,
            vote_count INT,
            popularity FLOAT,
            rating_date DATE DEFAULT CURRENT_DATE
        );

        -- Indexes for faster joins
        CREATE INDEX idx_fact_cast_movie_key ON fact_cast(movie_key);
        CREATE INDEX idx_fact_cast_actor_key ON fact_cast(actor_key);
        CREATE INDEX idx_fact_movie_ratings_movie_key ON fact_movie_ratings(movie_key);
        """
    )
    
    def populate_dim_movies(**context):
        """Populate movies dimension table from staging"""
        hook = PostgresHook(postgres_conn_id='postgres_etl_target_conn')
        
        sql = """
        MERGE INTO dim_movie d
        USING stg_movies s
        ON d.movie_id = s.movie_id AND d.is_current = TRUE

        WHEN MATCHED AND (
            d.title IS DISTINCT FROM s.title
            OR d.original_title IS DISTINCT FROM s.original_title
            OR d.overview IS DISTINCT FROM s.overview
            OR d.status IS DISTINCT FROM s.status
            OR d.tagline IS DISTINCT FROM s.tagline
            OR d.runtime IS DISTINCT FROM s.runtime
            OR d.budget IS DISTINCT FROM s.budget
            OR d.revenue IS DISTINCT FROM s.revenue
            OR d.popularity IS DISTINCT FROM s.popularity
            OR d.vote_average IS DISTINCT FROM s.vote_average
            OR d.vote_count IS DISTINCT FROM s.vote_count
            OR d.homepage IS DISTINCT FROM s.homepage
            OR d.release_date IS DISTINCT FROM s.release_date
        ) THEN
            UPDATE SET 
                valid_to = CURRENT_TIMESTAMP,
                is_current = FALSE

        WHEN NOT MATCHED THEN
            INSERT (
                movie_id, title, original_title, adult, overview, status, tagline,
                original_language, runtime, budget, revenue, popularity,
                vote_average, vote_count, homepage, release_date,
                valid_from, is_current
            )
            VALUES (
                s.movie_id, s.title, s.original_title, s.adult, s.overview, s.status, s.tagline,
                s.original_language, s.runtime, s.budget, s.revenue, s.popularity,
                s.vote_average, s.vote_count, s.homepage, s.release_date,
                CURRENT_TIMESTAMP, TRUE
            );
        """
        
        hook.run(sql)
        logging.info("Populated dim_movie dimension table")
    
    populate_dim_movies_task = PythonOperator(
        task_id='populate_dim_movies',
        python_callable=populate_dim_movies,
    )

    def populate_dim_actors(**context):
        """Populate actors dimension table from staging"""
        hook = PostgresHook(postgres_conn_id='postgres_etl_target_conn')
        
        sql = """
        MERGE INTO dim_actor d
        USING(
            SELECT 
                actor_id,
                name, 
                gender,
                known_for_department,
                birthday,
                deathday,
                place_of_birth,
                biography,
                popularity
            FROM stg_actors
        ) s
        ON d.actor_id = s.actor_id and d.is_current = TRUE

        WHEN MATCHED AND(
                d.name IS DISTINCT FROM s.name
            OR  d.gender IS DISTINCT FROM s.gender
            OR d.known_for_department IS DISTINCT FROM s.known_for_department
            OR d.birthday IS DISTINCT FROM s.birthday
            OR d.deathday IS DISTINCT FROM s.deathday
            OR d.place_of_birth IS DISTINCT FROM s.place_of_birth
            OR d.biography IS DISTINCT FROM s.biography
            OR d.popularity IS DISTINCT FROM s.popularity
        ) THEN
            UPDATE SET
                valid_to = CURRENT_TIMESTAMP,
                is_current = FALSE

        WHEN NOT MATCHED THEN 
            INSERT(
                actor_id, name, gender, known_for_department,
                birthday, deathday, place_of_birth, biography,
                popularity, valid_from, is_current
            )
            VALUES(
                s.actor_id, s.name, s.gender, s.known_for_department,
                s.birthday, s.deathday, s.place_of_birth, s.biography,
                s.popularity, CURRENT_TIMESTAMP, TRUE
            );
        """
        
        hook.run(sql)
        logging.info("Populated dim_actor dimension table")
    
    populate_dim_actors_task = PythonOperator(
        task_id='populate_dim_actors',
        python_callable=populate_dim_actors,
    )
    

    def populate_dim_language(**context):
        """Populate genre dimension table"""
        hook = PostgresHook(postgres_conn_id='postgres_etl_target_conn')        
        
        sql = """
        INSERT INTO dim_language (language_code, english_name, name, valid_from, is_current, updated_at)
        SELECT language_code, english_name, name, NOW(), TRUE, NOW()
        FROM stg_languages
        ON CONFLICT (language_code) DO UPDATE
        SET english_name = EXCLUDED.english_name,
            name = EXCLUDED.name,
            updated_at = NOW();
        """

        hook.run(sql)
        logging.info("Populated dim_language dimension table")

    
    populate_dim_language_task = PythonOperator(
        task_id='populate_dim_language',
        python_callable=populate_dim_language,
    )
    
    def populate_dim_genre(**context):
        """Populate genre dimension table"""
        hook = PostgresHook(postgres_conn_id='postgres_etl_target_conn')

        sql = """
        INSERT INTO dim_genre (genre_id, name, valid_from, is_current, updated_at)
        SELECT genre_id, name, NOW(), TRUE, NOW()
        FROM stg_genres
        ON CONFLICT (genre_id) DO UPDATE
        SET name = EXCLUDED.name,
            updated_at = NOW();
        """

        hook.run(sql)
        logging.info("Populated dim_genre dimension table")
    
    populate_dim_genre_task = PythonOperator(
        task_id='populate_dim_genre',
        python_callable=populate_dim_genre,
    )


    def populate_dim_country(**context):
        """Populate country dimension table"""
        hook = PostgresHook(postgres_conn_id='postgres_etl_target_conn')
    
        sql = """
        INSERT INTO dim_country (country_code, english_name, name, valid_from, is_current, updated_at)
        SELECT country_code, english_name, name, NOW(), TRUE, NOW()
        FROM stg_countries
        ON CONFLICT (country_code) DO UPDATE
        SET english_name = EXCLUDED.english_name,
            name = EXCLUDED.name,
            updated_at = NOW();
        """

        hook.run(sql)
        logging.info("Populated dim_country dimension table")

    populate_dim_country_task = PythonOperator(
        task_id='populate_dim_country',
        python_callable=populate_dim_country,
    )
    

    def populate_dim_company(**context):
        """Populate company dimension table"""
        hook = PostgresHook(postgres_conn_id='postgres_etl_target_conn')

        sql = """
        INSERT INTO dim_company (company_id, name, origin_country, valid_from, is_current, updated_at)
        SELECT company_id, name, origin_country, NOW(), TRUE, NOW()
        FROM stg_companies
        ON CONFLICT (company_id) DO UPDATE
        SET name = EXCLUDED.name,
            origin_country = EXCLUDED.origin_country,
            updated_at = NOW();
        """

        hook.run(sql)
        logging.info("Populated dim_company dimension table")

    populate_dim_company_task = PythonOperator(
        task_id='populate_dim_company',
        python_callable=populate_dim_company,
    )


    def populate_bridge_movie_genre(**context):
        """Populate movie_genre bridge table"""
        hook = PostgresHook(postgres_conn_id='postgres_etl_target_conn')

        sql = """
        WITH movie_genre_pairs AS (
            SELECT
                s.movie_id,
                g.genre_id::int AS genre_id_from_ids,
                (ge.g->>'id')::int AS genre_id_from_objects
            FROM stg_movies s
            LEFT JOIN LATERAL jsonb_array_elements_text(s.raw_json->'genre_ids') AS g(genre_id)
                ON TRUE
            LEFT JOIN LATERAL jsonb_array_elements(s.raw_json->'genres') AS ge(g)
                ON TRUE
            WHERE s.raw_json IS NOT NULL
        )

        INSERT INTO bridge_movie_genre (movie_key, genre_key)
        SELECT DISTINCT
            dm.movie_key,
            dg.genre_key
        FROM movie_genre_pairs mg
        JOIN dim_movie dm
            ON dm.movie_id = mg.movie_id
        JOIN dim_genre dg
            ON dg.genre_id = COALESCE(mg.genre_id_from_ids, mg.genre_id_from_objects)
        WHERE COALESCE(mg.genre_id_from_ids, mg.genre_id_from_objects) IS NOT NULL
        ON CONFLICT DO NOTHING;
        """

        hook.run(sql)
        logging.info("bridge_movie_genre populated")

    populate_bridge_movie_genre_task = PythonOperator(
        task_id='populate_bridge_movie_genre',
        python_callable=populate_bridge_movie_genre,
    )


    def populate_bridge_movie_company(**context):
        """Populate bridge_movie_company bridge table"""

        hook = PostgresHook(postgres_conn_id='postgres_etl_target_conn')

        sql = """
        WITH movie_company_pairs AS (
        SELECT
            s.movie_id,
            (pc.elem->>'id')::int AS company_id
        FROM stg_movies s,
        LATERAL (
            SELECT jsonb_array_elements(s.raw_json->'production_companies') AS elem
        ) pc
        WHERE s.raw_json ? 'production_companies'
        )
        INSERT INTO bridge_movie_company (movie_key, company_key)
        SELECT DISTINCT dm.movie_key, dc.company_key
        FROM movie_company_pairs mc
        JOIN dim_movie dm ON dm.movie_id = mc.movie_id
        JOIN dim_company dc ON dc.company_id = mc.company_id
        ON CONFLICT (movie_key, company_key) DO NOTHING;
        """
        hook.run(sql)
        logging.info("bridge_movie_company populated")

    populate_bridge_movie_company_task = PythonOperator(
        task_id='populate_bridge_movie_company',
        python_callable=populate_bridge_movie_company,
    )


    def populate_fact_movie_ratings(**context):
        """Populate fact_movie_ratings fact table"""
        hook = PostgresHook(postgres_conn_id='postgres_etl_target_conn')

        sql = """
        INSERT INTO fact_movie_ratings (movie_key, vote_average, vote_count, popularity, rating_date)
        SELECT dm.movie_key, s.vote_average, s.vote_count, s.popularity, COALESCE(s.release_date, CURRENT_DATE)
        FROM stg_movies s
        JOIN dim_movie dm ON dm.movie_id = s.movie_id
        WHERE s.vote_average IS NOT NULL OR s.vote_count IS NOT NULL OR s.popularity IS NOT NULL;
        """

        hook.run(sql)
        logging.info("fact_movie_ratings populated")

    populate_fact_movie_ratings_task = PythonOperator(
        task_id='populate_fact_movie_ratings',
        python_callable=populate_fact_movie_ratings,
    )


    def populate_fact_cast(**context):
        """Populate fact_cast fact table"""
        hook = PostgresHook(postgres_conn_id='postgres_etl_target_conn')

        sql = """
        INSERT INTO fact_cast (movie_key, actor_key, character_name, job, department, loaded_at)
        SELECT DISTINCT
            dm.movie_key,
            da.actor_key,
            LEFT(c.character_name, 200),
            LEFT(c.job, 200),
            LEFT(c.department, 200),
            NOW()
        FROM stg_credits c
        JOIN dim_movie dm ON dm.movie_id = c.movie_id
        JOIN dim_actor da ON da.actor_id = c.actor_id
        WHERE c.actor_id IS NOT NULL
        ON CONFLICT DO NOTHING;
        """
        hook.run(sql)
        logging.info("fact_cast populated")

    populate_fact_cast_task = PythonOperator(
        task_id='populate_fact_cast',
        python_callable=populate_fact_cast,
    )


    # ========== TASK DEPENDENCIES ==========
    
    create_staging >> fetch_data
    fetch_data >> fetch_data_details
    fetch_data_details >> [load_movies, load_actors, load_genres, load_languages, load_credits, load_companies, load_countries]

    for task in [load_movies, load_actors, load_genres, load_languages, load_credits, load_companies, load_countries]:
        task >> create_dw_schema

    create_dw_schema >> [populate_dim_actors_task, 
                        populate_dim_movies_task, 
                        populate_dim_language_task, 
                        populate_dim_genre_task, 
                        populate_dim_country_task,
                        populate_dim_company_task]

    for dim_task in [populate_dim_actors_task, populate_dim_movies_task, populate_dim_language_task,
                 populate_dim_genre_task, populate_dim_country_task, populate_dim_company_task]:
        for bridge_task in [populate_bridge_movie_genre_task, populate_bridge_movie_company_task]:
            dim_task >> bridge_task


    for bridge_task in [populate_bridge_movie_genre_task, populate_bridge_movie_company_task]:
        for fact_task in [populate_fact_cast_task, populate_fact_movie_ratings_task]:
            bridge_task >> fact_task

    

