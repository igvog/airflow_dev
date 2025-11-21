# Airflow ETL Demo Setup

This guide walks you through setting up and running the Airflow environment defined in the `docker-compose.yml` file.

## Project Structure

**Create inside project a `kaggle_cache` dir.**
There we will store the kaggle data set.

```
volumes:
    - ./kaggle_cache:/opt/airflow/kaggle_cache
```
In `docker-compose.yml` we connected the volume with that path,
where we store the dowloaded dataset by using `kagglehub` library from kaggle
locally inside `./kaggle_cache`, and `/opt/airflow/kaggle_cache` in the container.

Ensure your files are arranged as follows:

```
.
├── dags/
│   └── api_to_postgres_etl.py
├── kaggle_cache/           (Dataset dir)
├── logs/           (Airflow will create this)
├── plugins/        (Empty, for future use)
├── docker-compose.yml
├── .env
├── requirements.txt
└── README.md
```

## Step 1: Update .env File

Before you start, find your local user ID by running this in your terminal:

```bash
id -u
```

Open the `.env` file and replace `1000` with the number your terminal printed. This prevents file permission errors inside the Docker container.

```
# .env
TELEGRAM_BOT_TOKEN
TELEGRAM_CHAT_ID
```

Also get your telegram `bot token` and `chat_id` and put them in to .env file. The token you can get from `BotFather`, after creating your own bot. To get your `chat_id` you can use `@userinfobot`.

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

## Step 3: Access Airflow

Open your web browser and go to:

**http://localhost:8080**

Log in with the default credentials (set in the `docker-compose.yml`):
- **Username:** `admin`
- **Password:** `admin`

## Step 4: Create the Postgres Connection

This is the most important step for the ETL to work. You need to tell Airflow how to connect to the `postgres-etl-target` database.

1. In the Airflow UI, go to **Admin → Connections**
2. Click the **+** button to add a new connection
3. Fill in the form with these exact values:

   | Field | Value | Notes |
   |-------|-------|-------|
   | **Connection Id** | `postgres_etl_target_conn` | This must match the `ETL_POSTGRES_CONN_ID` in the DAG file |
   | **Connection Type** | `Postgres` | |
   | **Host** | `postgres-etl-target` | This is the service name from `docker-compose.yml` |
   | **Schema** | `etl_db` | From the `postgres-etl-target` environment variables |
   | **Login** | `etl_user` | From the `postgres-etl-target` environment variables |
   | **Password** | `etl_pass` | From the `postgres-etl-target` environment variables |
   | **Port** | `5432` | This is the port inside the Docker network, not the 5433 host port |

4. Click **Test**. It should show "Connection successfully tested."
5. Click **Save**.

## Step 5: Run Your ETL DAG

1. Go back to the Airflow DAGs dashboard
2. Find the `game_info_to_dwh_dag` DAG
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

Once connected, run this SQL query:

```sql
SELECT * FROM dim_game;
```

You should see the records from the API! 🎉

## Stopping the Environment

To stop all the containers, run:

```bash
docker-compose down
```

To stop and remove the database volumes (deleting all your data), run:

```bash
docker-compose down -v
```