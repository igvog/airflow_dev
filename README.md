# eCommerce Data Warehouse ELT Pipeline

## Project Overview
This project implements a scalable **ELT (Extract-Load-Transform)** pipeline to process high-volume eCommerce user behavior logs. It transforms raw data into an analytical **Star Schema** using Apache Airflow for orchestration and PostgreSQL for data processing.

**Key Features:**
*   **Architecture:** ELT with Push-down computation (SQL-based transformations).
*   **Performance:** Optimized for high load (tested on 1M+ rows/day batch).
*   **Scalability:** Daily Partitioning logic (`@daily`) capable of handling TB-scale datasets.
*   **Idempotency:** Safe re-runs via partition replacement strategies.

---

## Prerequisites
*   Docker & Docker Compose
*   SQL Client (DBeaver, pgAdmin, or TablePlus)
*   Dataset: eCommerce behavior CSV (zipped)

---

## Step 1: Infrastructure Setup

1.  **Clone the repository** to your local machine.
2.  **Create environment file**:
    Create a file named `.env` in the root directory. Run `id -u` in your terminal to get your user ID (e.g., `501` or `1000`) and add it to the file:
    ```bash
    AIRFLOW_UID=501
    ```
3.  **Start the containers**:
    Open your terminal in the project folder and run:
    ```bash
    docker-compose up -d
    ```

---

## Step 2: Database Preparation (Raw Layer)

Before running the Airflow DAG, we must manually load the raw dataset into the Postgres database.

1.  **Unzip the dataset**: Locate your CSV file in the `dataset` directory and unzip it.
2.  **Connect to Database**: Open your SQL Client (e.g., DBeaver) and connect to:
    *   **Host:** `localhost`
    *   **Port:** `5433` (Port mapped in docker-compose)
    *   **Database:** `etl_db`
    *   **User/Pass:** `etl_user` / `etl_pass`
3.  **Clean Environment**: Delete any existing tables inside `etl_db` to ensure a clean start.
4.  **Create Raw Table**: Run the following SQL to create the source table (Note: Table name matches the DAG configuration):

    ```sql
    CREATE TABLE raw_ecommerce_events (
        event_time TIMESTAMP,
        event_type VARCHAR(50),
        product_id BIGINT,
        category_id BIGINT,
        category_code VARCHAR(500),
        brand VARCHAR(255),
        price NUMERIC(12,2),
        user_id BIGINT,
        user_session VARCHAR(100)
    );
    ```
    *(Note: Indexes will be created automatically by the DAG later).*

5.  **Import Data**:
    *   Right-click on the newly created `raw_ecommerce_events` table in your SQL Client.
    *   Select **Import Data** (or "Import from CSV").
    *   Select your unzipped CSV file and complete the import process.

---

## Step 3: Airflow Configuration

1.  **Access Airflow UI**: Open [http://localhost:8080](http://localhost:8080) in your browser.
    *   **Login:** `admin` / `admin`
2.  **Setup Connection**:
    *   Go to **Admin** -> **Connections**.
    *   Click **+** to add a new connection.
    *   **Conn Id:** `postgres_etl_target_conn` (Critical: must match exactly).
    *   **Conn Type:** `Postgres`.
    *   **Host:** `postgres-etl-target` (Service name inside Docker).
    *   **Schema:** `etl_db`.
    *   **Login:** `etl_user`.
    *   **Password:** `etl_pass`.
    *   **Port:** `5432`.
    *   *Note: You can skip the "Test" button if it is disabled in this build; just click Save.*

---

## Step 4: Deploy and Run

1.  **Deploy DAG**:
    *   Ensure your Python file (`ecommerce_final_project.py`) is located inside the `dags/` folder.
    *   Delete any unused or old DAG files from the folder to avoid confusion.
2.  **Launch**:
    *   Go to the Airflow DAGs dashboard.
    *   Find **`ecommerce_final_project`**.
    *   Toggle the **Pause/Unpause** switch to **ON**.
3.  **Monitor**:
    *   Click on the DAG name and go to the **Grid** view.
    *   Airflow will automatically trigger a Backfill for the dataset start date (`2019-10-01`).
    *   Wait for the task blocks to turn **Dark Green** (Success).

---

## Step 5: Verify Results

Once the DAG execution is successful, verify the Data Warehouse structure in your SQL Client:

1.  Check the Fact table count:
    ```sql
    SELECT count(*) FROM fact_events;
    ```
2.  Check analytical data (e.g., Sales by Brand):
    ```sql
    SELECT p.brand, COUNT(*) as sales 
    FROM fact_events f
    JOIN dim_products p ON f.product_key = p.product_key
    WHERE f.event_type = 'purchase'
    GROUP BY p.brand
    ORDER BY sales DESC;
    ```

---
**Project Status:** Production Ready 🚀