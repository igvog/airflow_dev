from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago

# Аргументы по умолчанию для всех задач
default_args = {
    'owner': 'bekzat',
    'start_date': days_ago(1),
    'retries': 1,
}

# Определение DAG'а
with DAG(
    dag_id='01_initialize_dwh_schema',
    default_args=default_args,
    description='Создает схемы raw, staging, marts в DWH',
    schedule_interval=None,  # Запускаем только вручную (один раз)
    catchup=False,
    tags=['infrastructure', 'setup'],
) as dag:

    # SQL запрос для создания схем
    create_schemas_sql = """
    CREATE SCHEMA IF NOT EXISTS raw;
    CREATE SCHEMA IF NOT EXISTS staging;
    CREATE SCHEMA IF NOT EXISTS marts;
    
    -- Пример таблицы для логов загрузки (метаданные)
    CREATE TABLE IF NOT EXISTS raw.upload_logs (
        filename VARCHAR(255),
        upload_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        row_count INT
    );
    """

    # Оператор, который выполняет SQL
    create_schemas_task = PostgresOperator(
        task_id='create_layers',
        postgres_conn_id='postgres_dwh', # Ссылка на коннекшн, который мы создали
        sql=create_schemas_sql,
    )

    create_schemas_task