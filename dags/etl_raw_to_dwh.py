import logging
import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

BUCKET_NAME = "storage"

FILE_NAME = {
    "rockets": "raw_rockets.json",
    "launchpads": "raw_launchpads.json",
    "launches": "raw_launches.json",
    "payloads": "raw_payloads.json"
}

DIM_TABLES = {
    "rockets": "dim_rockets",
    "launchpads": "dim_launchpads"
}

FACT_TABLE = "fact_launches"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}
with DAG(
    'etl_raw_to_dwh',
    default_args=default_args,
    description='ETL pipeline: API → Postgres → Star Schema DW',
    schedule_interval=timedelta(days=1),  # Run daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['etl', 'api', 'datawarehouse', 'star-schema'],
) as dag:
    
    def create_staging_tables(**context):
            """Create staging tables for raw Minio data"""
            hook = PostgresHook(postgres_conn_id='pg_conn')
            
            # Drop existing staging tables
            drop_staging = """
            DROP TABLE IF EXISTS staging_rockets CASCADE;
            DROP TABLE IF EXISTS staging_launchpads  CASCADE;
            DROP TABLE IF EXISTS staging_launches CASCADE;
            DROP TABLE IF EXISTS staging_payloads;
            """
            
            # Create staging tables
            create_staging = """
            CREATE TABLE IF NOT EXISTS staging_rockets (
                id TEXT PRIMARY KEY,
                name TEXT,
                type TEXT,
                active BOOLEAN,
                stages INT,
                boosters INT,
                cost_per_launch BIGINT,
                success_rate_pct INT,
                first_flight DATE,
                country TEXT,
                company TEXT,
                raw_data JSONB,
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE TABLE IF NOT EXISTS staging_payloads (
                id TEXT PRIMARY KEY,
                name TEXT,
                type TEXT,
                mass_kg DOUBLE PRECISION,
                mass_lbs DOUBLE PRECISION,
                customers TEXT[],            -- массив заказчиков
                orbit TEXT,
                launch_id TEXT,              -- ссылка на запуск (launches.id)
                rocket_id TEXT,              -- ссылка на ракету (rockets.id)
                raw_data JSONB,
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE TABLE IF NOT EXISTS staging_launchpads (
                id TEXT PRIMARY KEY,
                name TEXT,
                full_name TEXT,
                locality TEXT,
                region TEXT,
                latitude DOUBLE PRECISION,
                longitude DOUBLE PRECISION,
                launch_attempts INT,
                launch_successes INT,
                status TEXT,
                raw_data JSONB,
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE TABLE IF NOT EXISTS staging_launches (
                id TEXT PRIMARY KEY,
                name TEXT,
                date_utc TIMESTAMP,
                rocket TEXT,
                launchpad TEXT,
                success BOOLEAN,
                payload_ids TEXT[],
                cores JSONB,
                failures JSONB,
                raw_data JSONB,
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            
            hook.run(drop_staging)
            hook.run(create_staging)
            logging.info("Staging tables created successfully")
        
    create_staging = PythonOperator(
        task_id='create_staging_tables',
        python_callable=create_staging_tables,
    )
    
# ============ROCKETS====================================
    def load_rockets_to_staging(**context):
        """Load rockets data into staging table"""
        hook = PostgresHook(postgres_conn_id='pg_conn')
        s3 = S3Hook(aws_conn_id="minio_conn")
        
        try:
            json_str_rockets = s3.read_key(key=FILE_NAME["rockets"], bucket_name=BUCKET_NAME)
            rockets_data = json.loads(json_str_rockets)
        except Exception as e:
            logging.error(f"Failed to load rockets: {e}")
            raise
        
        for rocket in rockets_data:
            insert_query = """
            INSERT INTO staging_rockets (
                id, name, type, active, stages, boosters, cost_per_launch,
                success_rate_pct, first_flight, country, company, raw_data
            ) VALUES (
                %(id)s, %(name)s, %(type)s, %(active)s, %(stages)s, %(boosters)s, %(cost_per_launch)s,
                %(success_rate_pct)s, %(first_flight)s, %(country)s, %(company)s, %(raw_data)s
            )
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                type = EXCLUDED.type,
                active = EXCLUDED.active,
                stages = EXCLUDED.stages,
                boosters = EXCLUDED.boosters,
                cost_per_launch = EXCLUDED.cost_per_launch,
                success_rate_pct = EXCLUDED.success_rate_pct,
                first_flight = EXCLUDED.first_flight,
                country = EXCLUDED.country,
                company = EXCLUDED.company,
                raw_data = EXCLUDED.raw_data,
                loaded_at = CURRENT_TIMESTAMP;
            """
            hook.run(
                insert_query,
                parameters={
                    "id": rocket["id"],
                    "name": rocket["name"],
                    "type": rocket["type"],
                    "active": rocket["active"],
                    "stages": rocket["stages"],
                    "boosters": rocket["boosters"],
                    "cost_per_launch": rocket.get("cost_per_launch"),
                    "success_rate_pct": rocket.get("success_rate_pct"),
                    "first_flight": rocket.get("first_flight"),
                    "country": rocket.get("country"),
                    "company": rocket.get("company"),
                    "raw_data": json.dumps(rocket)
                }
            )
        logging.info(f"Loaded {len(rockets_data)} rockets into staging_rockets")

    load_rockets = PythonOperator(
        task_id='load_rockets_to_staging',
        python_callable=load_rockets_to_staging,
    )
    
# ============PAYLOADS====================================    
    def load_payloads_to_staging(**context):
        hook = PostgresHook(postgres_conn_id='pg_conn')
        s3 = S3Hook(aws_conn_id="minio_conn")
        
        try:
            json_str = s3.read_key(key=FILE_NAME["payloads"], bucket_name=BUCKET_NAME)
            payloads = json.loads(json_str)
        except Exception as e:
            logging.error(f"Failed to load payloads: {e}")
            raise
        

        insert_query = """
            INSERT INTO staging_payloads (
                id, name, type, mass_kg, mass_lbs, customers,
                orbit, launch_id, rocket_id, raw_data
            ) VALUES (
                %(id)s, %(name)s, %(type)s, %(mass_kg)s, %(mass_lbs)s, %(customers)s,
                %(orbit)s, %(launch_id)s, %(rocket_id)s, %(raw_data)s
            )
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                type = EXCLUDED.type,
                mass_kg = EXCLUDED.mass_kg,
                mass_lbs = EXCLUDED.mass_lbs,
                customers = EXCLUDED.customers,
                orbit = EXCLUDED.orbit,
                launch_id = EXCLUDED.launch_id,
                rocket_id = EXCLUDED.rocket_id,
                raw_data = EXCLUDED.raw_data,
                loaded_at = CURRENT_TIMESTAMP;
        """

        for p in payloads:
            hook.run(
                insert_query,
                parameters={
                    "id": p["id"],
                    "name": p.get("name"),
                    "type": p.get("type"),
                    "mass_kg": p.get("mass_kg"),
                    "mass_lbs": p.get("mass_lbs"),
                    "customers": p.get("customers"),
                    "orbit": p.get("orbit"),
                    "launch_id": p.get("launch"),
                    "rocket_id": p.get("rocket"),
                    "raw_data": json.dumps(p),
                },
            )

        logging.info(f"Loaded {len(payloads)} payloads into staging_payloads")
    
    load_payloads = PythonOperator(
        task_id='load_payloads_to_staging',
        python_callable=load_payloads_to_staging,
    )
# ============LAUNCHPADS====================================         
    def load_launchpads_to_staging(**context):
        hook = PostgresHook(postgres_conn_id='pg_conn')
        s3 = S3Hook(aws_conn_id="minio_conn")

        try:
            json_str = s3.read_key(key=FILE_NAME["launchpads"], bucket_name=BUCKET_NAME)
            launchpads = json.loads(json_str)
        except Exception as e:
            logging.error(f"Failed to load launchpads: {e}")
            raise
        
        

        insert_query = """
            INSERT INTO staging_launchpads (
                id, name, full_name, locality, region, latitude, longitude,
                launch_attempts, launch_successes, status, raw_data
            ) VALUES (
                %(id)s, %(name)s, %(full_name)s, %(locality)s, %(region)s,
                %(latitude)s, %(longitude)s, %(launch_attempts)s, %(launch_successes)s,
                %(status)s, %(raw_data)s
            )
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                full_name = EXCLUDED.full_name,
                locality = EXCLUDED.locality,
                region = EXCLUDED.region,
                latitude = EXCLUDED.latitude,
                longitude = EXCLUDED.longitude,
                launch_attempts = EXCLUDED.launch_attempts,
                launch_successes = EXCLUDED.launch_successes,
                status = EXCLUDED.status,
                raw_data = EXCLUDED.raw_data,
                loaded_at = CURRENT_TIMESTAMP;
        """

        for lp in launchpads:
            hook.run(
                insert_query,
                parameters={
                    "id": lp["id"],
                    "name": lp.get("name"),
                    "full_name": lp.get("full_name"),
                    "locality": lp.get("locality"),
                    "region": lp.get("region"),
                    "latitude": lp.get("latitude"),
                    "longitude": lp.get("longitude"),
                    "launch_attempts": lp.get("launch_attempts"),
                    "launch_successes": lp.get("launch_successes"),
                    "status": lp.get("status"),
                    "raw_data": json.dumps(lp),
                },
            )

        logging.info(f"Loaded {len(launchpads)} launchpads into staging_launchpads")

    load_launchpads = PythonOperator(
        task_id='load_launchpads_to_staging',
        python_callable=load_launchpads_to_staging,
    )
# ============LAUNCHES====================================         
    def load_launches_to_staging(**context):
        hook = PostgresHook(postgres_conn_id='pg_conn')
        s3 = S3Hook(aws_conn_id="minio_conn")

        try:
            json_str = s3.read_key(key=FILE_NAME["launches"], bucket_name=BUCKET_NAME)
            launches = json.loads(json_str)
        except Exception as e:
            logging.error(f"Failed to load launches: {e}")
            raise
        
        

        insert_query = """
            INSERT INTO staging_launches (
                id, name, date_utc, rocket, launchpad, success,
                payload_ids, cores, failures,  raw_data
            ) VALUES (
                %(id)s, %(name)s, %(date_utc)s, %(rocket)s, %(launchpad)s, 
                %(success)s, %(payload_ids)s, %(cores)s, %(failures)s, %(raw_data)s
            )
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                date_utc = EXCLUDED.date_utc,
                rocket = EXCLUDED.rocket,
                launchpad = EXCLUDED.launchpad,
                success = EXCLUDED.success,
                payload_ids = EXCLUDED.payload_ids,
                cores = EXCLUDED.cores,
                failures = EXCLUDED.failures,
                raw_data = EXCLUDED.raw_data,
                loaded_at = CURRENT_TIMESTAMP;
        """

        for l in launches:
            hook.run(
                insert_query,
                parameters={
                    "id": l["id"],
                    "name": l.get("name"),
                    "date_utc": l.get("date_utc"),
                    "rocket": l.get("rocket"),
                    "launchpad": l.get("launchpad"),
                    "success": l.get("success"),
                    "payload_ids": l.get("payloads"),
                    "cores": json.dumps(l.get("cores")),
                    "failures": json.dumps(l.get("failures")),
                    "raw_data": json.dumps(l),
                },
            )

        logging.info(f"Loaded {len(launches)} launches into staging_launches")

    load_launches = PythonOperator(
        task_id='load_launches_to_staging',
        python_callable=load_launches_to_staging,
    )
    
# ============CREATE====================================     
    create_dw_schema = SQLExecuteQueryOperator(
        task_id='create_dw_schema',
        conn_id='pg_conn',
        sql="""
        
        -- Dimension: rockets
        CREATE TABLE IF NOT EXISTS dim_rockets (
            rocket_key SERIAL PRIMARY KEY,
            rocket_id TEXT UNIQUE,
            name TEXT,
            type TEXT,
            active BOOLEAN,
            stages INT,
            boosters INT,
            cost_per_launch BIGINT,
            success_rate_pct INT,
            first_flight DATE,
            country TEXT,
            company TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Dimension: launchpads
        CREATE TABLE IF NOT EXISTS dim_launchpads (
            launchpad_key SERIAL PRIMARY KEY,
            launchpad_id TEXT UNIQUE,
            name TEXT,
            full_name TEXT,
            locality TEXT,
            region TEXT,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            launch_attempts INT,
            launch_successes INT,
            status TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Dimension: payloads
        CREATE TABLE IF NOT EXISTS dim_payloads (
            payload_key SERIAL PRIMARY KEY,
            payload_id TEXT UNIQUE,
            name TEXT,
            type TEXT,
            mass_kg DOUBLE PRECISION,
            mass_lbs DOUBLE PRECISION,
            customers TEXT[],
            orbit TEXT,
            rocket_id TEXT,
            launch_id TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Fact: launches
        CREATE TABLE IF NOT EXISTS fact_launches (
            fact_key SERIAL PRIMARY KEY,

            launch_id TEXT,
            
            rocket_key INT REFERENCES dim_rockets(rocket_key),
            launchpad_key INT REFERENCES dim_launchpads(launchpad_key),
            payload_key INT REFERENCES dim_payloads(payload_key),

            date_utc TIMESTAMP,
            success BOOLEAN,
            
            failures_count INT,
            cores_count INT,
            cores_success_count INT,
            cores_reused_count INT,
            payload_count INT,

            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            CONSTRAINT uq_fact_launches UNIQUE (launch_id, payload_key)
        );
        
        -- Индекс на суррогатные ключи для джойнов
        CREATE INDEX IF NOT EXISTS idx_fact_rocket_key ON fact_launches(rocket_key);
        CREATE INDEX IF NOT EXISTS idx_fact_launchpad_key ON fact_launches(launchpad_key);
        CREATE INDEX IF NOT EXISTS idx_fact_payload_key ON fact_launches(payload_key);
        
        -- Индекс на API идентификаторы для аудита/поиска
        CREATE INDEX IF NOT EXISTS idx_fact_launch_id ON fact_launches(launch_id);
        """,
    )
# =================populate_dim_rockets================================================    
    def populate_dim_rockets(**context):
        """Populate rockets dimension table from staging"""
        hook = PostgresHook(postgres_conn_id='pg_conn')
        
        sql = """
        INSERT INTO dim_rockets (
            rocket_id, name, type, active, stages, boosters,
            cost_per_launch, success_rate_pct, first_flight, country, company
        )
        SELECT
            id AS rocket_id,
            name,
            type,
            active,
            stages,
            boosters,
            cost_per_launch,
            success_rate_pct,
            first_flight,
            country,
            company
        FROM staging_rockets
        ON CONFLICT (rocket_id) DO UPDATE SET
            name = EXCLUDED.name,
            type = EXCLUDED.type,
            active = EXCLUDED.active,
            stages = EXCLUDED.stages,
            boosters = EXCLUDED.boosters,
            cost_per_launch = EXCLUDED.cost_per_launch,
            success_rate_pct = EXCLUDED.success_rate_pct,
            first_flight = EXCLUDED.first_flight,
            country = EXCLUDED.country,
            company = EXCLUDED.company,
            created_at = CURRENT_TIMESTAMP;
        """
        
        hook.run(sql)
        logging.info("Populated dim_rockets dimension table")
    
    populate_dim_rockets_task = PythonOperator(
        task_id='populate_dim_rockets',
        python_callable=populate_dim_rockets,
    )
   
# =================populate_dim_launchpads=========================== 
    def populate_dim_launchpads(**context):
        """Populate launchpads dimension table from staging"""
        hook = PostgresHook(postgres_conn_id='pg_conn')
        
        sql = """
        INSERT INTO dim_launchpads (
            launchpad_id, name, full_name, locality, region, latitude, longitude,
            launch_attempts, launch_successes, status
        )
        SELECT
            id, name, full_name, locality, region, latitude, longitude,
            launch_attempts, launch_successes, status
        FROM staging_launchpads
        ON CONFLICT (launchpad_id) DO UPDATE SET
            name = EXCLUDED.name,
            full_name = EXCLUDED.full_name,
            locality = EXCLUDED.locality,
            region = EXCLUDED.region,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            launch_attempts = EXCLUDED.launch_attempts,
            launch_successes = EXCLUDED.launch_successes,
            status = EXCLUDED.status,
            created_at = CURRENT_TIMESTAMP;
        """
        
        hook.run(sql)
        logging.info("Populated dim_rockets dimension table")
    
    populate_dim_launchpads_task = PythonOperator(
        task_id='populate_dim_launchpads',
        python_callable=populate_dim_launchpads,
    )
    
# =================populate_dim_launchpads=========================== 
    def populate_dim_payloads(**context):
        """Populate payloads dimension table from staging"""
        hook = PostgresHook(postgres_conn_id='pg_conn')
        
        sql = """
        INSERT INTO dim_payloads (
            payload_id, name, type, mass_kg, mass_lbs, customers,
            orbit, rocket_id, launch_id
        )
        SELECT
            id, name, type, mass_kg, mass_lbs, customers,
            orbit, rocket_id, launch_id
        FROM staging_payloads
        ON CONFLICT (payload_id) DO UPDATE SET
            name = EXCLUDED.name,
            type = EXCLUDED.type,
            mass_kg = EXCLUDED.mass_kg,
            mass_lbs = EXCLUDED.mass_lbs,
            customers = EXCLUDED.customers,
            orbit = EXCLUDED.orbit,
            rocket_id = EXCLUDED.rocket_id,
            launch_id = EXCLUDED.launch_id,
            created_at = CURRENT_TIMESTAMP;
        """
        
        hook.run(sql)
        logging.info("Populated dim_rockets dimension table")
    
    populate_dim_payloads_task = PythonOperator(
        task_id='populate_dim_payloads',
        python_callable=populate_dim_payloads,
    )
# =================populate_fact_launches===========================    
    def populate_fact_launches(**context):
        """Populate fact table with launches data"""
        hook = PostgresHook(postgres_conn_id='pg_conn')
        
        sql = """
        INSERT INTO fact_launches (
            launch_id,
            rocket_key,
            launchpad_key,
            payload_key,
            date_utc,
            success,
            failures_count,
            cores_count,
            cores_success_count,
            cores_reused_count,
            payload_count
        )
        SELECT
            sl.id AS launch_id,
            dr.rocket_key,
            dl.launchpad_key,
            dp.payload_key,
            sl.date_utc,
            sl.success,
            jsonb_array_length(sl.failures) AS failures_count,
            jsonb_array_length(sl.cores) AS cores_count,
            (
                SELECT COUNT(*) FROM jsonb_array_elements(sl.cores) AS c
                WHERE (c->>'landing_success')::boolean IS TRUE
            ) AS cores_success_count,
            (
                SELECT COUNT(*) FROM jsonb_array_elements(sl.cores) AS c
                WHERE (c->>'reused')::boolean IS TRUE
            ) AS cores_reused_count,
            array_length(sl.payload_ids,1) AS payload_count
        FROM staging_launches sl
        LEFT JOIN dim_rockets dr ON sl.rocket = dr.rocket_id
        LEFT JOIN dim_launchpads dl ON sl.launchpad = dl.launchpad_id
        LEFT JOIN staging_payloads sp ON sp.id = ANY(sl.payload_ids)
        LEFT JOIN dim_payloads dp ON sp.id = dp.payload_id
        ON CONFLICT ON CONSTRAINT uq_fact_launches DO UPDATE SET
            rocket_key = EXCLUDED.rocket_key,
            launchpad_key = EXCLUDED.launchpad_key,
            date_utc = EXCLUDED.date_utc,
            success = EXCLUDED.success,
            failures_count = EXCLUDED.failures_count,
            cores_count = EXCLUDED.cores_count,
            cores_success_count = EXCLUDED.cores_success_count,
            cores_reused_count = EXCLUDED.cores_reused_count,
            payload_count = EXCLUDED.payload_count,
            created_at = CURRENT_TIMESTAMP;
        """
        
        hook.run(sql)
        logging.info("Populated fact_posts fact table")
    
    populate_fact_launches_task = PythonOperator(
        task_id='populate_fact_launches',
        python_callable=populate_fact_launches,
    ) 

    create_staging >> [load_rockets, load_payloads, load_launchpads, load_launches]
    [load_rockets, load_payloads, load_launchpads, load_launches] >> create_dw_schema
    create_dw_schema >> [populate_dim_rockets_task, populate_dim_launchpads_task, populate_dim_payloads_task ]
    [populate_dim_rockets_task, populate_dim_launchpads_task, populate_dim_payloads_task ] >> populate_fact_launches_task