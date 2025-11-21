from datetime import datetime as dt, timedelta
from minio.error import S3Error
from bs4 import BeautifulSoup
from minio import Minio
import requests
import logging
from logging import FileHandler, Formatter
import csv
import io
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

LOG_FILE_PATH = "/opt/airflow/logs/kaspi_project_custom.log"
LOGS_BUCKET = "logs"

logger = logging.getLogger(__name__)

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

if not logger.handlers:
    file_handler = FileHandler(LOG_FILE_PATH)
    file_handler.setFormatter(Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    ))

    logger.addHandler(file_handler)
    logger.setLevel(logging.INFO)
    logger.propagate = True

default_args = {
    'owner': 'airflow',
    'start_date': dt(2025, 11, 20),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

def _get_logical_date_str(context):
    dag_run = context.get("dag_run")

    # Airflow 2.4+ — logical_date, но на всякий случай fallback на execution_date
    if dag_run:
        logical_date = getattr(dag_run, "logical_date", None) or getattr(dag_run, "execution_date", None)
    else:
        logical_date = context.get("logical_date") or context.get("execution_date")

    if logical_date:
        return logical_date.isoformat()
    return "unknown"


def send_telegram_message(text: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("TELEGRAM_BOT_TOKEN или TELEGRAM_CHAT_ID не заданы. Сообщение не отправлено.")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
    }

    try:
        response = requests.post(url, json=payload, timeout=10)
        if response.status_code != 200:
            logger.error(f"Ошибка Telegram API: {response.status_code} - {response.text}")
        else:
            logger.info("Сообщение в Telegram успешно отправлено.")
    except Exception as e:
        logger.error(f"Ошибка при отправке сообщения в Telegram: {e}", exc_info=True)

def dag_success_alert(context):
    dag = context.get("dag")
    dag_id = dag.dag_id if dag else "unknown"

    dag_run = context.get("dag_run")
    run_id = dag_run.run_id if dag_run else context.get("run_id", "unknown")

    exec_dt = _get_logical_date_str(context)

    text = (
        f"✅ DAG <b>{dag_id}</b> успешно завершился.\n"
        f"Run ID: <code>{run_id}</code>\n"
        f"Execution date: <code>{exec_dt}</code>"
    )

    logger.info("Отправляю Telegram-уведомление об успешном завершении DAG.")
    send_telegram_message(text)

def dag_failure_alert(context):
    dag = context.get("dag")
    dag_id = dag.dag_id if dag else "unknown"

    dag_run = context.get("dag_run")
    run_id = dag_run.run_id if dag_run else context.get("run_id", "unknown")

    exec_dt = _get_logical_date_str(context)

    ti = context.get("task_instance")
    task_id = ti.task_id if ti else "unknown"
    try_number = ti.try_number if ti else "unknown"

    text = (
        f"❌ DAG <b>{dag_id}</b> упал.\n"
        f"Проблемная задача: <b>{task_id}</b> (try {try_number}).\n"
        f"Run ID: <code>{run_id}</code>\n"
        f"Execution date: <code>{exec_dt}</code>"
    )

    logger.info("Отправляю Telegram-уведомление о падении DAG.")
    send_telegram_message(text)


def get_minio_client():
    logger.debug("Creating Minio client")
    client = Minio(
        'minio:9000',
        access_key='admin',
        secret_key='adminpass',
        secure=False
    )
    return client

def scrap_to_lake():
    logger.info("=== Старт scrap_to_lake ===")
    i = 1
    rows = []
    today_date = dt.now().strftime("%d.%m.%Y")
    logger.info(f"Сегодняшняя дата: {today_date}")
    
    while True:
        url = f'https://tanba.kezekte.kz/ru/frameless/animal/list?p={i}'
        logger.info(f"Страница {i}")

        try:
            response = requests.get(url, verify=False)
        except Exception as e:
            logger.error(f"Ошибка при запросе страницы {i}: {e}", exc_info=True)
            raise

        if response.status_code != 200:
            logger.error(f"Невалидный статус-код {response.status_code} для страницы {i}. Stopping.")
            break

        soup = BeautifulSoup(response.content, 'html.parser')

        page_data_found = False

        for tr in soup.find_all('tr')[1:]:
            cells = []
            tds = tr.find_all('td')
            registration_date = None

            for idx, td in enumerate(tds):
                text = td.text.strip()
                cells.append(text)

                if idx == 6:
                    registration_date = text

            if registration_date:
                registration_date_only = registration_date.split()[0]

                if registration_date_only != today_date:
                    logger.info(
                        f"Найдена дата, отличающаяся от сегодняшней: {registration_date_only}. Stop Scrapping."
                    )
                    page_data_found = False
                    break

                rows.append(cells)
                page_data_found = True

        if not page_data_found:
            logger.info(f"На странице {i} нет данных за сегодня. Stopping.")
            break

        logger.info(f"Scraped page {i}")
        i += 1
    
    if not rows:
        logger.warning('Данных за сегодня не найдено. Нечего загружать в MinIO.')
        logger.info("=== Завершение scrap_to_lake (нет данных) ===")
        return
    
    logger.info(f"Всего строк за сегодня: {len(rows)}")

    csv_output = io.StringIO()
    writer = csv.writer(csv_output)
    writer.writerows(rows)

    client = get_minio_client()

    file_data = csv_output.getvalue()
    file_bytes = file_data.encode('utf-8')
    file_name = f'scraped_raw_data_{dt.now().strftime("%Y-%m-%d-%H-%M-%S")}.csv'

    try:
        client.put_object(
            'raw',
            file_name,
            io.BytesIO(file_bytes),
            len(file_bytes)
        )
        logger.info(f'Файл {file_name} успешно загружен в bucket "raw"')
    except S3Error as e:
        logger.error(f'Ошибка при загрузке файла в MinIO: {e}', exc_info=True)
        raise

    logger.info("=== Завершение scrap_to_lake (успех) ===")

def transform_data_from_minio(task_instance):
    logger.info("=== Старт transform_data_from_minio ===")
    client = get_minio_client()

    today_date = dt.now().strftime("%Y-%m-%d")

    bucket_name = 'raw'
    logger.info(f"Ищу файл за {today_date} в bucket '{bucket_name}'")

    try:
        objects = client.list_objects(bucket_name)
        files = [obj.object_name for obj in objects]
        logger.debug(f"Найденные файлы в bucket '{bucket_name}': {files}")
        
        file_name = next((f for f in files if today_date in f), None)

        if not file_name:
            logger.warning(f"Файл за сегодня ({today_date}) в bucket '{bucket_name}' не найден.")
            logger.info("=== Завершение transform_data_from_minio (нет файла) ===")
            return

        logger.info(f"Найден файл: {file_name}")

        data = client.get_object(bucket_name, file_name)
        file_data = data.read().decode('utf-8')
        
        datasets = list(csv.reader(io.StringIO(file_data)))
        logger.info(f"Считано строк из raw-файла: {len(datasets)}")
        
        transformed_data = []
        
        for data in datasets:
            chip_number = data[0]
            species = data[1]

            name = data[2][0].upper() + data[2][1:].lower()
            if name == '-' or name == 'Нет' or \
               name == 'Не имеется' or name == 'Без клички':
                name = None
            elif name.isdigit() and len(name) == 4:
                name = 'Number_nickname'

            gender = data[3]
            if gender.lower() == 'самец':
                gender = 'Male'
            else:
                gender = 'Female'

            breed = data[4]
            passport_number = data[5]

            registration_date = dt.strptime(data[6], '%d.%m.%Y %H:%M:%S')
            birth_date = dt.strptime(data[7], '%d.%m.%Y').date()
            
            animal_status = data[8]
            ownership_status = data[9]
            registration_type = data[10]
            unit_name = data[11]

            tagging_date = dt.strptime(data[12], '%d.%m.%Y').date()

            region_name = data[13]
            vaccination_date = data[14]
            if vaccination_date in ["Не вакцинирован", "-", "", None]:
                vaccination_date = None
            else:
                vaccination_date = dt.strptime(vaccination_date, "%d.%m.%Y").date()

            sterilization_date = data[15]
            if sterilization_date == 'Не стерилизован/кастрирован':
                sterilization_date = None
            else:
                sterilization_date = dt.strptime(data[15], '%d.%m.%Y').date()

            transformed_data.append((
                chip_number,
                species,
                name,
                gender,
                breed,
                passport_number,
                registration_date,
                birth_date,
                animal_status,
                ownership_status,
                registration_type,
                unit_name,
                tagging_date,
                region_name,
                vaccination_date,
                sterilization_date
            ))
        
        logger.info(f"Трансформировано строк: {len(transformed_data)}")

        task_instance.xcom_push(key='transformed_data', value=transformed_data)

        csv_output = io.StringIO()
        writer = csv.writer(csv_output)
        writer.writerows(transformed_data)

        cleaned_file_name = f'cleaned_data_{dt.now().strftime("%Y-%m-%d-%H-%M-%S")}.csv'

        csv_bytes = csv_output.getvalue().encode('utf-8')

        try:
            client.put_object(
                'clean',
                cleaned_file_name,
                io.BytesIO(csv_bytes),
                len(csv_bytes)
            )
            logger.info(f'Файл {cleaned_file_name} успешно загружен в bucket "clean"')
        except S3Error as e:
            logger.error(f'Ошибка при загрузке файла в MinIO (clean): {e}', exc_info=True)

    except S3Error as e:
        logger.error(f'Ошибка при чтении файла из MinIO: {e}', exc_info=True)

    logger.info("=== Завершение transform_data_from_minio ===")

create_dw_schema_sql = """
    -- Drop existing DW tables
    DROP TABLE IF EXISTS animal_dimension CASCADE;
    DROP TABLE IF EXISTS region_dimension CASCADE;
    DROP TABLE IF EXISTS unit_dimension CASCADE;
    DROP TABLE IF EXISTS animal_facts CASCADE;

    -- (Animal Dimension)
    CREATE TABLE IF NOT EXISTS animal_dimension (
        animal_id SERIAL PRIMARY KEY,  -- уникальный идентификатор животного
        species VARCHAR(100),          -- Вид животного
        name VARCHAR(100),             -- Кличка (имя животного)
        gender VARCHAR(10),            -- Пол животного
        breed VARCHAR(100),            -- Порода животного
        passport_number VARCHAR(100),  -- Номер паспорта животного
        ownership_status VARCHAR(50)   -- Статус владения животным
    );

    -- (Region Dimension)
    CREATE TABLE IF NOT EXISTS region_dimension (
        region_id SERIAL PRIMARY KEY,   -- уникальный идентификатор региона
        region_name VARCHAR(255) UNIQUE -- Регион регистрации животного
    );

    -- (Unit Dimension)
    CREATE TABLE IF NOT EXISTS unit_dimension (
        unit_id SERIAL PRIMARY KEY,     -- уникальный идентификатор подразделения
        unit_name VARCHAR(255) UNIQUE   -- Наименование подразделения
    );

    -- (Animal Facts)
    CREATE TABLE IF NOT EXISTS animal_facts (
        fact_id SERIAL PRIMARY KEY,       -- уникальный идентификатор записи факта
        animal_id INT,                    -- ID животного FK
        chip_number VARCHAR(50),          -- Номер чипа/ИНЖ
        registration_date DATE,           -- Дата регистрации животного
        birth_date DATE,                  -- Дата рождения животного
        animal_status VARCHAR(50),        -- Статус животного
        ownership_status VARCHAR(50),     -- Статус владения животным
        registration_type VARCHAR(50),    -- Тип регистрации животного
        unit_id INT,                      -- ID подразделения FK
        tagging_date DATE,                -- Дата мечения животного
        region_id INT,                    -- ID региона FK
        vaccination_date varchar(255),    -- Дата вакцинации
        sterilization_date varchar(255),  -- Дата стерилизации/кастрации
        FOREIGN KEY (animal_id) REFERENCES animal_dimension(animal_id),
        FOREIGN KEY (region_id) REFERENCES region_dimension(region_id),
        FOREIGN KEY (unit_id) REFERENCES unit_dimension(unit_id)
    );
    """

def load_data_from_minio_to_dw(task_instance):
    logger.info("=== Старт load_data_from_minio_to_dw ===")
    client = get_minio_client()

    bucket_name = 'clean'
    today_date = dt.now().strftime("%Y-%m-%d")
    logger.info(f"Ищу файл за {today_date} в bucket '{bucket_name}'")

    objects = client.list_objects(bucket_name)
    files = [obj.object_name for obj in objects]
    logger.debug(f"Найденные файлы в bucket '{bucket_name}': {files}")

    file_name = next((f for f in files if today_date in f), None)

    if not file_name:
        logger.warning(f"Файл за сегодня ({today_date}) в bucket '{bucket_name}' не найден.")
        logger.info("=== Завершение load_data_from_minio_to_dw (нет файла) ===")
        return

    logger.info(f"Найден файл: {file_name}")

    data = client.get_object(bucket_name, file_name)
    file_data = data.read().decode('utf-8')

    datasets = list(csv.reader(io.StringIO(file_data)))
    logger.info(f"Строк для загрузки в DWH: {len(datasets)}")

    hook = PostgresHook(postgres_conn_id='dwh_postgres_conn')
    conn = hook.get_conn()
    cursor = conn.cursor()

    inserted_facts = 0

    try:
        for row in datasets:
            cursor.execute("""
                INSERT INTO animal_dimension (species, name, gender, breed, passport_number, ownership_status)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING animal_id;
            """, (row[1], row[2], row[3], row[4], row[5], row[9]))
            animal_id = cursor.fetchone()[0]

            cursor.execute("""
                INSERT INTO region_dimension (region_name)
                VALUES (%s)
                ON CONFLICT (region_name) DO NOTHING
                RETURNING region_id;
            """, (row[13],))

            region_id = cursor.fetchone()
            if region_id:
                region_id = region_id[0]
            else:
                cursor.execute("SELECT region_id FROM region_dimension WHERE region_name = %s", (row[13],))
                region_id = cursor.fetchone()[0]

            cursor.execute("""
                INSERT INTO unit_dimension (unit_name)
                VALUES (%s)
                ON CONFLICT (unit_name) DO NOTHING
                RETURNING unit_id;
            """, (row[11],))

            unit_id = cursor.fetchone()
            if unit_id:
                unit_id = unit_id[0]
            else:
                cursor.execute("SELECT unit_id FROM unit_dimension WHERE unit_name = %s", (row[11],))
                unit_id = cursor.fetchone()[0]

            cursor.execute("""
                INSERT INTO animal_facts (animal_id, chip_number, registration_date, birth_date, animal_status, 
                                          ownership_status, registration_type, unit_id, tagging_date, region_id,
                                          vaccination_date, sterilization_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """, (
                animal_id, 
                row[0],  # chip_number
                row[6],  # registration_date
                row[7],  # birth_date
                row[8],  # animal_status
                row[9],  # ownership_status
                row[10], # registration_type
                unit_id,
                row[12], # tagging_date
                region_id,
                row[14], # vaccination_date
                row[15]  # sterilization_date
            ))

            inserted_facts += 1

        conn.commit()
        logger.info(f"Загружено записей в animal_facts: {inserted_facts}")
    except Exception as e:
        logger.error(f"Ошибка при загрузке данных в DWH: {e}", exc_info=True)
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

    logger.info("=== Завершение load_data_from_minio_to_dw ===")

def upload_logs_to_minio():
    logger.info("=== Старт upload_logs_to_minio ===")
    client = get_minio_client()

    object_name = f"kaspi_project_logs_{dt.now().strftime('%Y-%m-%d-%H-%M-%S')}.log"

    try:
        # читаем локальный лог-файл
        with open(LOG_FILE_PATH, "rb") as f:
            data = f.read()

        client.put_object(
            LOGS_BUCKET,
            object_name,
            io.BytesIO(data),
            len(data)
        )
        logger.info(
            f'Лог {object_name} успешно загружен в MinIO в bucket "{LOGS_BUCKET}"'
        )

    except FileNotFoundError:
        logger.warning(f"Лог-файл {LOG_FILE_PATH} не найден, нечего загружать.")
    except S3Error as e:
        logger.error(f"Ошибка при загрузке логов в MinIO: {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Неизвестная ошибка при загрузке логов в MinIO: {e}", exc_info=True)
        raise

    logger.info("=== Завершение upload_logs_to_minio ===")


dag = DAG(
    'kaspi_project',
    default_args=default_args,
    description='kaspi final project',
    schedule='@daily',
    catchup=False,
    tags=['kaspi', 'final', 'project'],
    on_success_callback=dag_success_alert,
    on_failure_callback=dag_failure_alert,
)

scrape_and_upload_task = PythonOperator(
    task_id='scrape_and_upload_lake',
    python_callable=scrap_to_lake,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_and_upload_task',
    python_callable=transform_data_from_minio,
    dag=dag,
)

create_dw_schema_task = PostgresOperator(
    task_id='create_dw_schema',
    sql=create_dw_schema_sql,
    postgres_conn_id='dwh_postgres_conn',
    autocommit=True,
    dag=dag,
)

load_dw_data_task = PythonOperator(
    task_id='load_data_from_minio_to_dw',
    python_callable=load_data_from_minio_to_dw,
    dag=dag,
)

upload_logs_task = PythonOperator(
    task_id='upload_logs_to_minio',
    python_callable=upload_logs_to_minio,
    dag=dag,
)


scrape_and_upload_task >> transform_task >> create_dw_schema_task >> load_dw_data_task >> upload_logs_task
