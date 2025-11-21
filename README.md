# Airflow ETL Project — Online Retail II Dataset

### This Online Retail II data set contains all the transactions occurring for a UK-based and registered, non-store online retail between 01/12/2009 and 09/12/2011.The company mainly sells unique all-occasion gift-ware. Many customers of the company are wholesalers.

## Dataset

The `online_retail_II.csv` dataset contains:

- `Invoice` — Invoice number
- `StockCode` — Product code
- `Description` — Product description
- `Quantity` — Number of items purchased
- `InvoiceDate` — Date of transaction
- `Price` — Price per item
- `CustomerID` — Customer identifier
- `Country` — Customer country


This guide explains how to set up and run the Airflow environment defined in `docker-compose.yml` for the Online Retail II ETL pipeline.

---

## Project structure

Ensure your project files are organized like this:

```
.
├── dags/
│   └── project_api_to_dwh.py
├── data/
│   └── online_retail_II.csv
├── logs/                # Airflow will create this
├── plugins/             # Empty, for future use
├── docker-compose.yml
├── .env
├── requirements.txt
└── README.md
```

---

## Step 1 — Update `.env`

Check your local user ID and replace the UID placeholder in `.env` to avoid Docker file permission issues:

```bash
id -u
```

Edit `.env` and set your UID (and any other required variables). Optionally add Telegram credentials for notifications:

```
TELEGRAM_TOKEN=your_telegram_bot_token
TELEGRAM_CHAT_ID=your_telegram_chat_id
```

Important: add `.env` to `.gitignore` to avoid committing secrets.

---

## Step 2 — Start the Docker environment

With Docker Desktop running, from the project directory run:

```bash
docker-compose up -d
```

This will:

- Pull Postgres and Airflow images
- Start two Postgres containers (one for Airflow metadata, one for ETL)
- Build the Airflow image and install dependencies from `requirements.txt`
- Start the Airflow webserver and scheduler

Note: the first run may take several minutes.

---

## Step 3 — Access the Airflow UI

Open your browser:

http://localhost:8080

---

## Step 4 — Configure Postgres connections in Airflow

In the Airflow UI: Admin → Connections → Add (+)

Fill in the form (example):

| Field               | Value                      | Notes                            |
|--------------------:|---------------------------:|----------------------------------|
| Connection Id       | `postgres_etl_target_Balga` | Must match the DAG connection id |
| Connection Type     | `Postgres`                 |                                  |
| Host                | `postgres-etl-target-Balga`| Service name in docker-compose   |
| Schema              | `etl_db`                   | Database name                    |
| Login               | `etl_Balga`                 | Database username                |
| Password            | `etl_pass`                 | Database password                |
| Port                | `5432`                     | Internal Docker network port     |

Click Test (should say "Connection successfully tested") and then Save.

If you have a second ETL DB (e.g., `postgres-etl-target-Balga`), repeat with a different Connection Id and host.

---

## Step 5 — Trigger the ETL DAG

- In the Airflow UI go to the DAGs dashboard
- Find `project_api_to_dwh`
- Click the Play (▶) button to trigger a run
- Monitor progress in Graph View or Grid View; tasks turn green when successful

---

## Step 6 — Verify data in Postgres

Use any SQL client (DBeaver, pgAdmin, TablePlus, etc.) to connect to the ETL database.

Connection details (host from host machine):

- Host: `localhost`
- Port: `5434` (for `postgres-etl-target-Balga`; check your `docker-compose.yml` for exact port)
- Database: `etl_db`
- User: `etl_Balga` 
- Password: `etl_pass`

## DAG Overview

`project_api_to_dwh` DAG includes the following tasks:

1. `fetch_data` — Read CSV data into memory
2. `create_staging_table` — Create staging table in Postgres ETL DB
3. `load_data_to_staging` — Insert data into staging table
4. `create_dw_schema` — Create dimensions and fact tables
5. `load_dw` — Load staging data into dimensional warehouse tables
6. `success_notification` — Send Telegram notification upon success


Example queries:

```sql
SELECT * FROM fact_sales LIMIT 10;
SELECT * FROM dim_customers LIMIT 10;
```

Note: inside the Docker network the Postgres containers use port `5432`; host ports are mapped (e.g., `5433`, `5434`) — check your `docker-compose.yml` for exact mappings.

---

## Step 7 — Stop the environment

To stop all containers:

```bash
docker-compose down
```

To stop and remove containers and database volumes (delete all data):

```bash
docker-compose down -v
```

---

## Environment Variables

Store sensitive values in the `.env` file:

- `TELEGRAM_TOKEN` — Your bot token
- `TELEGRAM_CHAT_ID` — Your chat ID for notifications
- `POSTGRES_PASSWORD` — Passwords for ETL databases


## Prerequisites

- Docker Desktop
- Docker Compose
- Python 3.12+ (for editing DAGs and requirements)
- SQL client (optional, for checking Postgres data)