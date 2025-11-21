# Airflow ETL Data Warehouse Project

This project contains Apache Airflow DAG for building data warehouses from various data sources. It includes ETL pipeline that transforms data into snowflake schema data warehouse model.

## 📋 Project Overview

This project demonstrates end-to-end ETL processes using Apache Airflow, including:

* **CSV Data Processing**: Processes CSV datasets and transforms into dimensional models
* **Snowflake Schema DWH**: Implements snowflake schema for e-commerce data (Olist dataset)

## 🏗️ Project Structure

```
.
├── dags/
│   ├── olist_to_snowflake_dwh.py     # Olist CSV → Snowflake Schema DWH pipeline
│   └── data/                          # CSV data files for Olist dataset
│       ├── olist_customers_dataset.csv
│       ├── olist_geolocation_dataset.csv
│       ├── olist_order_items_dataset.csv
│       ├── olist_order_payments_dataset.csv
│       ├── olist_order_reviews_dataset.csv
│       ├── olist_orders_dataset.csv
│       ├── olist_products_dataset.csv
│       └── olist_sellers_dataset.csv
├── logs/                              # Airflow logs (auto-generated)
├── plugins/                           # Airflow plugins (for future use)
├── docker-compose.yaml               # Docker Compose configuration
├── requirements.txt                  # Python dependencies
├── .env                              # Environment variables (create this)
└── README.md                         # This file
```

## 🚀 Getting Started

### Prerequisites

* Docker Desktop installed and running
* Git (optional, for cloning the repository)

### Step 1: Set Up Environment Variables

Create a `.env` file in the project root with the following content:

```env
AIRFLOW_UID=1000
AIRFLOW__SMTP__SMTP_PASSWORD=your_email_password
```

**Important**: Replace `1000` with your local user ID. On Linux/Mac, find it with:

```bash
id -u
```

On Windows, you can use PowerShell:

```powershell
[System.Security.Principal.WindowsIdentity]::GetCurrent().User.Value
```

### Step 2: Start the Environment

With Docker Desktop running, open a terminal in the project directory and run:

```bash
docker-compose up -d
```

This will:

* Pull PostgreSQL and Airflow Docker images
* Start two PostgreSQL databases:

  * `postgres-airflow-db`: Airflow metadata database
  * `postgres-etl-target`: Target database for ETL pipelines
* Build the Airflow image with required Python packages
* Start the Airflow webserver and scheduler

> **Note**: The first launch can take a few minutes as it downloads images and builds containers.

### Step 3: Access Airflow UI

Open your web browser and navigate to:

**[http://localhost:8080](http://localhost:8080)**

Default credentials:

* **Username**: `admin`
* **Password**: `admin`

### Step 4: Configure Database Connection

Before running the DAGs, you need to configure the PostgreSQL connection in Airflow:

1. In the Airflow UI, go to **Admin → Connections**

2. Click the **+** button to add a new connection

3. Fill in the connection details:

   | Field               | Value                      |
   | ------------------- | -------------------------- |
   | **Connection Id**   | `postgres_etl_target_conn` |
   | **Connection Type** | `Postgres`                 |
   | **Host**            | `postgres-etl-target`      |
   | **Schema**          | `etl_db`                   |
   | **Login**           | `etl_user`                 |
   | **Password**        | `etl_pass`                 |
   | **Port**            | `5432`                     |

4. Click **Test** to verify the connection

5. Click **Save**

## 📊 DAGs Overview

### `olist_to_snowflake_dwh`

**Description**: Processes Brazilian E-Commerce (Olist) dataset from CSV files and transforms it into a snowflake schema data warehouse.

**Data Source**: Olist E-Commerce Dataset (CSV files in `dags/data/`)

**Schema**: Snowflake Schema

* **Fact Tables**:

  * `fact_order_items` (order items with pricing)
  * `fact_payments` (payment transactions)
  * `fact_order_reviews` (customer reviews)
  * `fact_order_delivery` (delivery tracking)
* **Dimension Tables**:

  * `dim_geolocation` (geographic data)
  * `dim_customers` (customer information)
  * `dim_sellers` (seller information)
  * `dim_products` (product details)
  * `dim_product_categories` (product categories)
  * `dim_date` (time dimension)
  * `dim_payment_types` (payment method types)

**Tasks**:

1. Create staging tables
2. Load CSV data to staging
3. Create data warehouse tables
4. Populate dimension tables
5. Populate fact tables
6. Validate data warehouse
7. Send success notification email

**Schedule**: Daily

## 🗄️ Database Access

### Connect to Target Database

You can connect to the ETL target database using any SQL client:

* **Host**: `localhost`
* **Port**: `5433`
* **Database**: `etl_db`
* **User**: `etl_user`
* **Password**: `etl_pass`

### Example Queries

**Snowflake Schema DWH**:

```sql
-- Get order statistics by category
SELECT 
    dpc.category_name_english,
    COUNT(DISTINCT foi.order_id) as order_count,
    SUM(foi.total_amount) as total_revenue
FROM fact_order_items foi
JOIN dim_products dp ON foi.product_id = dp.product_id
JOIN dim_product_categories dpc ON dp.product_category_name = dpc.product_category_name
GROUP BY dpc.category_name_english
ORDER BY total_revenue DESC;
```

## 🔧 Configuration

### Email Notifications

The project is configured to send email notifications on DAG completion. Configure SMTP settings in `docker-compose.yaml`:

```yaml
AIRFLOW__SMTP__SMTP_HOST=smtp.gmail.com
AIRFLOW__SMTP__SMTP_STARTTLS=True
AIRFLOW__SMTP__SMTP_PORT=587
AIRFLOW__SMTP__SMTP_USER=your_email@gmail.com
AIRFLOW__SMTP__SMTP_PASSWORD=${AIRFLOW__SMTP__SMTP_PASSWORD}
```

Set `AIRFLOW__SMTP__SMTP_PASSWORD` in your `.env` file.

### Dependencies

Python packages are defined in `requirements.txt`:

* `apache-airflow-providers-postgres`
* `requests`

## 🛠️ Running DAGs

### Manual Trigger

1. Go to the Airflow DAGs dashboard
2. Find your DAG (`olist_to_snowflake_dwh`)
3. Toggle the DAG ON (if it's paused)
4. Click the **Play** button (▶) to trigger a manual run
5. Click on the DAG name to view task execution in Graph or Grid view

### Scheduled Runs

DAGs are configured to run on a schedule:

* `olist_to_snowflake_dwh`: Daily

## 📝 Features

* ✅ **Staging Layer**: Raw data is first loaded into staging tables
* ✅ **Data Warehouse Models**: Snowflake schema implementation
* ✅ **Error Handling**: Try-catch blocks and proper error logging
* ✅ **Logging**: Comprehensive logging throughout the pipeline
* ✅ **Email Alerts**: Success notifications via email
* ✅ **Data Validation**: Validation tasks to ensure data quality
* ✅ **Idempotency**: DAGs can be run multiple times safely

## 🛑 Stopping the Environment

To stop all containers:

```bash
docker-compose down
```

To stop and remove volumes (deletes all data):

```bash
docker-compose down -v
```

