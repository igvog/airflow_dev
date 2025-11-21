# Airflow ETL Setup

This guide walks you through setting up and running the Airflow environment defined in the `docker-compose.yml` file.

## Project Structure

Ensure your files are arranged as follows:

```
.
├── README.md
├── api
│   ├── Dockerfile
│   ├── data
│   │   ├── olist_customers_dataset.csv
│   │   ├── olist_order_items_dataset.csv
│   │   ├── olist_orders_dataset.csv
│   │   └── olist_sellers_dataset.csv
│   ├── fastapi_post.py
│   └── requirements.txt
├── dags
│   └── api_to_dw_star_schema.py
├── docker-compose.yaml
├── plugins
│   ├── __init__.py
│   └── typeddicts.py
└── requirements.txt

```

## Step 1: SETUP Environment Variables

Create a .env file with the following:

ID=<your_airflow_id>
AIRFLOW**SMTP**SMTP_PASSWORD=<your_email_password>

In your docker-compose.yaml for Airflow, set:

AIRFLOW**SMTP**SMTP_USER: <your_email>

## Step 2: Start the Environment

With Docker Desktop running, open a terminal in the project directory and run:

```bash
docker-compose up -d
```

This will:

- Pull the Postgres and Airflow images
- Start the two Postgres databases (one for Airflow, one for the ETL)
- Build the Airflow image, installing the Python packages from `requirements.txt`
- Start the Airflow webserver and scheduler

> **Note:** The first launch can take a few minutes as it downloads images and builds.

### STEP 3 : Running the API

Clone the API folder and build the Docker image:

**docker build -t fastapi_app .**
**docker run -p 8000:8000 -d --name fastapi_app fastapi_app**

Test the API at:
**http://localhost:8000**

### STEP 4: CONNECTING POSTGRES to AIRFLOW

Connect your Postgres container to the Airflow network:

**docker network connect airflow_dev_airflow_network <name_of_postgres_container>**

## Step 3: Access Airflow

Open your web browser and go to:

**http://localhost:8081**

Log in with the default credentials (set in the `docker-compose.yml`):

- **Username:** `admin`
- **Password:** `admin`

## Step 4: Create the Postgres Connection

This is the most important step for the ETL to work. You need to tell Airflow how to connect to the `postgres-etl-target` database.

1. In the Airflow UI, go to **Admin → Connections**
2. Click the **+** button to add a new connection
3. Fill in the form with these exact values:

   | Field               | Value                      | Notes                                                              |
   | ------------------- | -------------------------- | ------------------------------------------------------------------ |
   | **Connection Id**   | `postgres_etl_target_conn` | This must match the `ETL_POSTGRES_CONN_ID` in the DAG file         |
   | **Connection Type** | `Postgres`                 |                                                                    |
   | **Host**            | `postgres-etl-target`      | This is the service name from `docker-compose.yml`                 |
   | **Schema**          | `etl_db`                   | From the `postgres-etl-target` environment variables               |
   | **Login**           | `etl_user`                 | From the `postgres-etl-target` environment variables               |
   | **Password**        | `etl_pass`                 | From the `postgres-etl-target` environment variables               |
   | **Port**            | `5432`                     | This is the port inside the Docker network, not the 5433 host port |

4. Click **Test**. It should show "Connection successfully tested."
5. Click **Save**.
6. CHANGE PG_CONN variable to the name of your connection ID

## Step 5: Run Your ETL DAG

1. Go back to the Airflow DAGs dashboard
2. Find the `api_to_dw_star_schema` DAG
3. Click the **Play** button (▶) on the right to trigger a manual run
4. You can click on the DAG name to watch the tasks run in the "Grid" or "Graph" view. If all goes well, all four tasks will turn green.

## Step 6: Verify the Data

How do you know it worked? Let's connect to the target database and check.

You can use any SQL client (like DBeaver, TablePlus, or pgAdmin) to connect to the `postgres-etl-target` database using these details:

- **Host:** `localhost`
- **Port:** `5433` (This is the host port you defined in `docker-compose.yml`)
- **Database:** `etl_db`
- **User:** `etl_user`
- **Password:** `etl_pass`

**-- Example of a connection, in my case it's a separate container with completely different connection credentials**

Once connected, run this SQL query:

```sql
SELECT * FROM dim_orders;
```

You should see something! if you do, good job!

## Stopping the Environment

To stop all the containers, run:

```bash
docker-compose down
```

To stop and remove the database volumes (deleting all your data), run:

```bash
docker-compose down -v
```
