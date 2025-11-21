from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task
import csv
from datetime import datetime
from pendulum import timezone

ALMATY_TZ = timezone("Asia/Almaty")

@task
def create_dw_schema() -> None:
    """
    Create CLEAN, minimal dimension and fact tables 
    for the airline dataset.
    """
    hook = PostgresHook(postgres_conn_id="postgres_connection")

    ddl = """
    -- CLEAN PREVIOUS TABLES
    DROP TABLE IF EXISTS fact_flight CASCADE;
    DROP TABLE IF EXISTS dim_route CASCADE;
    DROP TABLE IF EXISTS dim_airport CASCADE;
    DROP TABLE IF EXISTS dim_airline CASCADE;
    DROP TABLE IF EXISTS dim_date CASCADE;
    -- ===========================
    -- DIMENSIONS (CLEAN)
    -- ===========================

    CREATE TABLE IF NOT EXISTS dim_airline (
        airline_key        SERIAL PRIMARY KEY,
        airline_code       CHAR(2) UNIQUE NOT NULL,
        airline_name       TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS dim_airport (
        airport_key   SERIAL PRIMARY KEY,
        iata_code     CHAR(3) UNIQUE NOT NULL,
        airport_name  TEXT NOT NULL,
        city          TEXT,
        state         TEXT,
        country       TEXT,
        latitude      NUMERIC(9,6),
        longitude     NUMERIC(9,6)
    );

    CREATE TABLE IF NOT EXISTS dim_date (
        date_key      INTEGER PRIMARY KEY,
        full_date     DATE NOT NULL,
        year          INTEGER NOT NULL,
        month         INTEGER NOT NULL,
        day           INTEGER NOT NULL,
        day_of_week   INTEGER NOT NULL,
        is_weekend    BOOLEAN NOT NULL
    );

    CREATE TABLE IF NOT EXISTS dim_route (
        route_key           SERIAL PRIMARY KEY,
        origin_airport_key  INTEGER NOT NULL REFERENCES dim_airport(airport_key),
        dest_airport_key    INTEGER NOT NULL REFERENCES dim_airport(airport_key),
        route_code          VARCHAR(10) NOT NULL,
        UNIQUE (origin_airport_key, dest_airport_key)
    );

    -- ===========================
    -- FACT TABLE (CLEAN)
    -- ===========================

    CREATE TABLE IF NOT EXISTS fact_flight (
        flight_key          BIGSERIAL PRIMARY KEY,

        -- Foreign keys
        flight_date_key     INTEGER NOT NULL REFERENCES dim_date(date_key),
        airline_key         INTEGER NOT NULL REFERENCES dim_airline(airline_key),
        origin_airport_key  INTEGER NOT NULL REFERENCES dim_airport(airport_key),
        dest_airport_key    INTEGER NOT NULL REFERENCES dim_airport(airport_key),
        route_key           INTEGER REFERENCES dim_route(route_key),

        -- Identifiers
        flight_number       VARCHAR(10) NOT NULL,
        tail_number         VARCHAR(10),

        -- Metrics
        departure_delay_min SMALLINT,
        arrival_delay_min   SMALLINT,
        distance_miles      INTEGER,

        -- Cancel status
        cancelled_flag      BOOLEAN NOT NULL,
        cancel_reason       CHAR(1) NULL,

        ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE INDEX IF NOT EXISTS idx_fact_flight_date
        ON fact_flight (flight_date_key);

    CREATE INDEX IF NOT EXISTS idx_fact_flight_route
        ON fact_flight (route_key);

    """

    hook.run(ddl)
    print("DW schema created successfully (clean minimal version).")

@task
def create_staging_tables():
    hook = PostgresHook(postgres_conn_id="postgres_connection")

    ddl = """
    CREATE TABLE IF NOT EXISTS stg_airline (
        iata_code CHAR(2),
        airline_name TEXT
    );

    CREATE TABLE IF NOT EXISTS stg_airport (
        iata_code CHAR(3),
        airport_name TEXT,
        city TEXT,
        state TEXT,
        country TEXT,
        latitude NUMERIC(9,6),
        longitude NUMERIC(9,6)
    );

    CREATE TABLE IF NOT EXISTS stg_flight (
        year INTEGER,
        month INTEGER,
        day INTEGER,
        airline CHAR(2),
        flight_number VARCHAR(10),
        tail_number VARCHAR(10),
        origin_airport CHAR(3),
        dest_airport CHAR(3),
        departure_delay INTEGER,
        arrival_delay INTEGER,
        distance INTEGER,
        cancelled INTEGER,
        cancellation_reason CHAR(1)
    );
    """

    hook.run(ddl)
    print("Staging tables created.")


DATASET_DIR = "/opt/airflow/dags/datasets"

# ==========================
# LOAD AIRLINES CSV
# ==========================
@task
def load_airlines_staging():
    hook = PostgresHook(postgres_conn_id="postgres_connection")
    path = f"{DATASET_DIR}/airlines.csv"

    with hook.get_conn() as conn, conn.cursor() as cur, open(path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            cur.execute(
                """
                INSERT INTO stg_airline (iata_code, airline_name)
                VALUES (%s, %s);
                """,
                (row["IATA_CODE"], row["AIRLINE"])
            )
        conn.commit()
    print("Loaded airlines staging data.")


# ==========================
# LOAD AIRPORTS CSV
# ==========================
@task
def load_airports_staging():
    hook = PostgresHook(postgres_conn_id="postgres_connection")
    path = f"{DATASET_DIR}/airports.csv"

    with hook.get_conn() as conn, conn.cursor() as cur, open(path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:

            lat = float(row["LATITUDE"]) if row["LATITUDE"] not in ("", None) else None
            lon = float(row["LONGITUDE"]) if row["LONGITUDE"] not in ("", None) else None

            cur.execute(
                """
                INSERT INTO stg_airport (
                    iata_code, airport_name, city, state, country,
                    latitude, longitude
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s);
                """,
                (
                    row["IATA_CODE"],
                    row["AIRPORT"],
                    row["CITY"],
                    row["STATE"],
                    row["COUNTRY"],
                    lat,
                    lon
                )
            )
        conn.commit()
    print("Loaded airports staging data.")



# ==========================
# LOAD FLIGHTS SUBSET CSV
# ==========================
@task
def load_flights_staging():
    hook = PostgresHook(postgres_conn_id="postgres_connection")
    path = f"{DATASET_DIR}/flights_subset.csv"

    def to_int(v):
        return int(v) if v not in ("", None) else None

    with hook.get_conn() as conn, conn.cursor() as cur, open(path, "r") as f:
        reader = csv.DictReader(f)

        for row in reader:

            cur.execute(
                """
                INSERT INTO stg_flight (
                    year, month, day,
                    airline, flight_number, tail_number,
                    origin_airport, dest_airport,
                    departure_delay, arrival_delay, distance,
                    cancelled, cancellation_reason
                )
                VALUES (%s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s);
                """,
                (
                    to_int(row["YEAR"]),
                    to_int(row["MONTH"]),
                    to_int(row["DAY"]),
                    row["AIRLINE"],
                    row["FLIGHT_NUMBER"],
                    row["TAIL_NUMBER"],
                    row["ORIGIN_AIRPORT"],
                    row["DESTINATION_AIRPORT"],
                    to_int(row["DEPARTURE_DELAY"]),
                    to_int(row["ARRIVAL_DELAY"]),
                    to_int(row["DISTANCE"]),
                    to_int(row["CANCELLED"]),
                    row["CANCELLATION_REASON"] if row["CANCELLATION_REASON"] else None
                )
            )

        conn.commit()

    print("Loaded flights staging data.")

# ==========================
# POPULATE DIM DATE
# ==========================

@task
def populate_dim_date():
    hook = PostgresHook(postgres_conn_id="postgres_connection")

    sql = """
    INSERT INTO dim_date (
        date_key,
        full_date,
        year,
        month,
        day,
        day_of_week,
        is_weekend
    )
    SELECT
        TO_CHAR(d, 'YYYYMMDD')::INT AS date_key,
        d::DATE AS full_date,
        EXTRACT(YEAR FROM d)::INT AS year,
        EXTRACT(MONTH FROM d)::INT AS month,
        EXTRACT(DAY FROM d)::INT AS day,
        EXTRACT(DOW FROM d)::INT AS day_of_week,
        CASE WHEN EXTRACT(DOW FROM d) IN (0,6) THEN TRUE ELSE FALSE END AS is_weekend
    FROM generate_series(
        '2015-01-01'::DATE,
        '2015-12-31'::DATE,
        '1 day'::INTERVAL
    ) d
    ON CONFLICT (date_key) DO NOTHING;
    """

    hook.run(sql)
    print("dim_date populated.")



# ==========================
# POPULATE DIM AIRLINE 
# ==========================
@task
def populate_dim_airline():
    hook = PostgresHook(postgres_conn_id="postgres_connection")

    sql = """
    INSERT INTO dim_airline (airline_code, airline_name)
    SELECT DISTINCT
        iata_code,
        airline_name
    FROM stg_airline
    WHERE iata_code IS NOT NULL
      AND airline_name IS NOT NULL
    ON CONFLICT (airline_code) DO UPDATE
    SET airline_name = EXCLUDED.airline_name;
    """

    hook.run(sql)
    print("dim_airline populated.")

# ==========================
# POPULATE DIM AIRPORT 
# ==========================

@task
def populate_dim_airport():
    hook = PostgresHook(postgres_conn_id="postgres_connection")

    sql = """
    INSERT INTO dim_airport (
        iata_code,
        airport_name,
        city,
        state,
        country,
        latitude,
        longitude
    )
    SELECT DISTINCT
        iata_code,
        airport_name,
        city,
        state,
        country,
        latitude,
        longitude
    FROM stg_airport
    WHERE iata_code IS NOT NULL
      AND airport_name IS NOT NULL
    ON CONFLICT (iata_code) DO UPDATE
    SET
        airport_name = EXCLUDED.airport_name,
        city = EXCLUDED.city,
        state = EXCLUDED.state,
        country = EXCLUDED.country,
        latitude = EXCLUDED.latitude,
        longitude = EXCLUDED.longitude;
    """

    hook.run(sql)
    print("dim_airport populated.")

# ==========================
# POPULATE DIM ROUTE 
# ==========================
@task
def populate_dim_route():
    hook = PostgresHook(postgres_conn_id="postgres_connection")

    sql = """
    INSERT INTO dim_route (
        origin_airport_key,
        dest_airport_key,
        route_code
    )
    SELECT DISTINCT
        da_origin.airport_key AS origin_airport_key,
        da_dest.airport_key   AS dest_airport_key,
        da_origin.iata_code || '-' || da_dest.iata_code AS route_code
    FROM stg_flight sf
    JOIN dim_airport da_origin 
        ON sf.origin_airport = da_origin.iata_code
    JOIN dim_airport da_dest 
        ON sf.dest_airport = da_dest.iata_code
    WHERE sf.origin_airport IS NOT NULL
      AND sf.dest_airport IS NOT NULL
    ON CONFLICT (origin_airport_key, dest_airport_key) DO NOTHING;
    """

    hook.run(sql)
    print("dim_route populated.")

# ==========================
# POPULATE FACT FLIGHT
# ==========================
@task
def populate_fact_flight():
    hook = PostgresHook(postgres_conn_id="postgres_connection")

    sql = """
    INSERT INTO fact_flight (
        flight_date_key,
        airline_key,
        origin_airport_key,
        dest_airport_key,
        route_key,
        flight_number,
        tail_number,
        departure_delay_min,
        arrival_delay_min,
        distance_miles,
        cancelled_flag,
        cancel_reason
    )
    SELECT
        (sf.year::TEXT || LPAD(sf.month::TEXT, 2, '0') || LPAD(sf.day::TEXT, 2, '0'))::INT AS flight_date_key,
        da.airline_key,
        ao.airport_key AS origin_airport_key,
        ad.airport_key AS dest_airport_key,
        dr.route_key,
        sf.flight_number,
        sf.tail_number,
        sf.departure_delay,
        sf.arrival_delay,
        sf.distance,
        sf.cancelled::BOOLEAN,
        sf.cancellation_reason
    FROM stg_flight sf
    JOIN dim_airline da ON da.airline_code = sf.airline
    JOIN dim_airport ao ON ao.iata_code = sf.origin_airport
    JOIN dim_airport ad ON ad.iata_code = sf.dest_airport
    LEFT JOIN dim_route dr
        ON dr.origin_airport_key = ao.airport_key
       AND dr.dest_airport_key   = ad.airport_key
    WHERE sf.year IS NOT NULL
      AND sf.month IS NOT NULL
      AND sf.day IS NOT NULL
    ON CONFLICT DO NOTHING;
    """

    hook.run(sql)
    print("fact_flight populated.")


@dag(
    dag_id="flights_dw_etl",
    schedule="@once",
    start_date=datetime(2024, 1, 1, tzinfo=ALMATY_TZ),
    catchup=False,
)
def flights_dw_etl():
    schema_task = create_dw_schema()
    staging_task = create_staging_tables()

    load_airlines = load_airlines_staging()
    load_airports = load_airports_staging()
    load_flights = load_flights_staging()

    dim_date_task = populate_dim_date()
    dim_airline_task = populate_dim_airline()
    dim_airport_task = populate_dim_airport()
    dim_route_task = populate_dim_route()

    fact_task = populate_fact_flight()

    schema_task >> staging_task

    # LOAD STAGING TABLES (PARALLEL)
    staging_task >> load_airlines
    staging_task >> load_airports
    staging_task >> load_flights

    # POPULATE DIMENSIONS (WITH CORRECT DEPENDENCIES)
    load_airlines >> dim_airline_task
    load_airports >> dim_airport_task
    load_flights >> dim_route_task   # route requires flight staging
    dim_airport_task >> dim_route_task  # route also requires airport dim

    # DIM_DATE MUST RUN AFTER SCHEMA
    schema_task >> dim_date_task

    # FACT DEPENDS ON EVERY DIMENSION + FLIGHT STAGING
    dim_date_task >> fact_task
    dim_airline_task >> fact_task
    dim_airport_task >> fact_task
    dim_route_task >> fact_task
    load_flights >> fact_task
flights_dw_etl()
