# ETL Pipeline: SQLite to PostgreSQL Data Warehouse

KaspiLab: Data Factory - Final Project

## Overview

This project implements a complete ETL pipeline that extracts data from SQLite (Olist e-commerce dataset), loads it into PostgreSQL staging tables, and transforms it into a star schema Data Warehouse with dimensions and fact tables.

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────────────────┐
│   SQLite    │────▶│   Staging   │────▶│      Data Warehouse     │
│  (Source)   │     │   Tables    │     │  (Star Schema)          │
│             │     │             │     │                         │
│ • orders    │     │ • stage_*   │     │ • dim_date              │
│ • items     │     │             │     │ • dim_customers         │
│ • customers │     │             │     │ • dim_products          │
│ • products  │     │             │     │ • dim_sellers           │
│ • sellers   │     │             │     │ • fact_order_items      │
└─────────────┘     └─────────────┘     └─────────────────────────┘
```

## Project Structure

```
.
├── dags/
│   ├── etl_dwh_final.py          # Main DAG file
│   ├── files/
│   │   └── olist.sqlite          # Source database
│   └── logs/
│       └── etl_dwh_final.log     # ETL logs
├── logs/                          # Airflow logs
├── plugins/
├── docker-compose.yml
├── .env
├── requirements.txt
└── README.md
```

## Prerequisites

- Docker & Docker Desktop
- Git
- **Note on `olist.sqlite`**: If you plan to use the SQLite database instead of the CSV files, download the `olist.sqlite` file from [here](https://www.kaggle.com/datasets/terencicp/e-commerce-dataset-by-olist-as-an-sqlite-database) and place it in the `dags/files/` folder.


## Quick Start

### Step 1: Clone and Setup

```bash
git clone <repository-url>
cd <project-folder>
git checkout feature/etl-dwh-final-project
```

### Step 2: Configure Environment

Get your user ID:
```bash
id -u
```

Update `.env` file with your user ID to prevent permission issues.

### Step 3: Start Services

```bash
docker-compose up -d
```

Wait 2-3 minutes for initialization.

### Step 4: Access Airflow UI

Open **http://localhost:8080**

Login credentials:
- **Username:** `admin`
- **Password:** `admin`

### Step 5: Create PostgreSQL Connection

Navigate to **Admin → Connections → +**

| Field | Value |
|-------|-------|
| Connection Id | `postgres_etl_target_conn` |
| Connection Type | `Postgres` |
| Host | `postgres-etl-target` |
| Schema | `etl_db` |
| Login | `etl_user` |
| Password | `etl_pass` |
| Port | `5432` |

Click **Test** → **Save**

### Step 6: Run the DAG

1. Find `etl_dwh_final_project` DAG
2. Enable it (toggle ON)
3. Click **Play** button (▶) to trigger manually
4. Monitor progress in Graph view

## DAG Tasks Flow

```
extract_from_sqlite
        │
        ▼
create_staging_tables
        │
        ▼
┌───────┴───────┬───────────┬───────────┬───────────┐
▼               ▼           ▼           ▼           ▼
load_orders  load_items  load_cust  load_prod  load_sellers
        │               │           │           │
        └───────┬───────┴───────────┴───────────┘
                ▼
        create_dwh_schema
                │
        ┌───────┴───────┬───────────┬───────────┐
        ▼               ▼           ▼           ▼
   dim_date      dim_customers  dim_products  dim_sellers
        │               │           │           │
        └───────┬───────┴───────────┴───────────┘
                ▼
      populate_fact_order_items
```

## Data Warehouse Schema

### Dimensions
- **dim_date** - Date attributes (day, month, year, quarter, weekend flag)
- **dim_customers** - Customer information and location
- **dim_products** - Product details and categories
- **dim_sellers** - Seller information and location

### Fact Table
- **fact_order_items** - Order transactions with foreign keys to dimensions

## Verify Results

Connect to PostgreSQL:
- **Host:** `localhost`
- **Port:** `5433`
- **Database:** `etl_db`
- **User:** `etl_user`
- **Password:** `etl_pass`

Sample queries:
```sql
-- Check fact table
SELECT COUNT(*) FROM fact_order_items;

-- Check dimensions
SELECT COUNT(*) FROM dim_customers;
SELECT COUNT(*) FROM dim_products;

-- Sample analytics query
SELECT 
    dd.year,
    dd.month,
    COUNT(*) as orders,
    SUM(f.total_amount) as revenue
FROM fact_order_items f
JOIN dim_date dd ON f.order_date_key = dd.date_key
GROUP BY dd.year, dd.month
ORDER BY dd.year, dd.month;
```

## Features Implemented

- ✅ SQLite extraction with CSV fallback
- ✅ Staging layer with data validation
- ✅ Star schema DWH (4 dimensions + 1 fact)
- ✅ Rotating file logging (5MB, 3 backups)
- ✅ Error handling with try-catch
- ✅ Idempotent loads (ON CONFLICT DO NOTHING)
- ✅ Column mapping for data quality issues

## Stopping the Environment

```bash
# Stop containers
docker-compose down

# Stop and remove volumes (deletes all data)
docker-compose down -v
```

## Troubleshooting

**Connection refused error:**
- Ensure Docker containers are running: `docker ps`
- Check connection settings match exactly

**Permission denied:**
- Update `.env` with correct user ID from `id -u`

**Empty tables:**
- Verify `olist.sqlite` exists in `dags/files/`
- Check logs: `dags/logs/etl_dwh_final.log`