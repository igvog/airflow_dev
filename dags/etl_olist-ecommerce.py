from __future__ import annotations
import os
import logging
from datetime import datetime, timedelta
import pandas as pd
from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator


DATA_DIR = "/opt/airflow/dags/data"
TMP_DIR = "/opt/airflow/dags/tmp"
PG_CONN_ID = "postgres_etl_target_conn"

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(seconds=5),
}

def _safe_ts(v):
    if v is None or pd.isna(v):
        return None
    try:
        return pd.to_datetime(v)
    except Exception:
        return None

# DAG
with DAG(
    dag_id="etl_olist_ecommerce",
    default_args=default_args,
    start_date=datetime(2017, 1, 1),
    schedule="@daily",
    catchup=True,
    max_active_runs=1,
    params={"load_type": Param("INCREMENTAL", enum=["INCREMENTAL", "FULL"])},
    tags=["olist"],
):

    create_dwh = PostgresOperator(
        task_id="create_dwh",
        postgres_conn_id=PG_CONN_ID,
        sql="sql/create_olist_dwh.sql",
    )

    # extract
    @task
    def extract_data():
        os.makedirs(TMP_DIR, exist_ok=True)
        files = {
            "customers": "olist_customers_dataset.csv",
            "orders": "olist_orders_dataset.csv",
            "items": "olist_order_items_dataset.csv",
            "payments": "olist_order_payments_dataset.csv",
            "products": "olist_products_dataset.csv",
            "categories": "product_category_name_translation.csv",
        }
        paths = {k: os.path.join(DATA_DIR, v) for k, v in files.items()}

        for p in paths.values():
            if not os.path.exists(p):
                raise FileNotFoundError(p)

        return paths

    @task
    def transform_dims(raw):
        customers = pd.read_csv(raw["customers"])
        dim_customer = customers[
            ["customer_id", "customer_unique_id", "customer_city", "customer_state"]
        ].drop_duplicates()

        categories = pd.read_csv(raw["categories"]).rename(
            columns={
                "product_category_name": "category_name",
                "product_category_name_english": "category_name_english",
            }
        )
        categories["category_id"] = categories.index + 1

        unknown = pd.DataFrame([{
            "category_id": 0,
            "category_name": "unknown",
            "category_name_english": "unknown"
        }])

        dim_category = pd.concat([unknown, categories], ignore_index=True)[
            ["category_id", "category_name", "category_name_english"]
        ]

        prod = pd.read_csv(raw["products"])
        merged = prod.merge(
            dim_category,
            left_on="product_category_name",
            right_on="category_name",
            how="left",
        )

        dim_product = merged[
            [
                "product_id",
                "category_id",
                "category_name",
                "category_name_english",
                "product_weight_g",
                "product_length_cm",
                "product_height_cm",
                "product_width_cm",
            ]
        ].rename(columns={"category_id": "product_category_id"})

        # Приведение типов
        dim_product["product_category_id"] = (
            pd.to_numeric(dim_product["product_category_id"], errors="coerce")
            .fillna(0)
            .astype(int)
        )

        out = {
            "dim_customer": os.path.join(TMP_DIR, "dim_customer.csv"),
            "dim_category": os.path.join(TMP_DIR, "dim_category.csv"),
            "dim_product": os.path.join(TMP_DIR, "dim_product.csv"),
        }

        dim_customer.to_csv(out["dim_customer"], index=False)
        dim_category.to_csv(out["dim_category"], index=False)
        dim_product.to_csv(out["dim_product"], index=False)

        return out

    @task
    def load_dimensions(paths):
        hook = PostgresHook(PG_CONN_ID)
        conn = hook.get_conn()
        cur = conn.cursor()
        cur.execute("SET session_replication_role = 'replica';")
        cur.execute("TRUNCATE dim_product, dim_category, dim_customer RESTART IDENTITY;")
        cur.execute("SET session_replication_role = 'origin';")

        for table, path in paths.items():
            df = pd.read_csv(path)
            cols = ",".join(df.columns)
            ph = ",".join(["%s"] * len(df.columns))
            sql = f"INSERT INTO {table} ({cols}) VALUES ({ph})"
            for row in df.itertuples(index=False, name=None):
                cur.execute(sql, row)

        conn.commit()
        cur.close()
        conn.close()
        
    @task
    def transform_facts(raw, run_date):
        run_date_dt = pd.to_datetime(run_date).date()

        orders = pd.read_csv(raw["orders"])
        orders["order_purchase_timestamp"] = pd.to_datetime(
            orders["order_purchase_timestamp"]
        ).dt.date

        fact_orders = orders[orders["order_purchase_timestamp"] == run_date_dt]

        items = pd.read_csv(raw["items"])
        fact_items = items.merge(fact_orders[["order_id"]], on="order_id")

        payments = pd.read_csv(raw["payments"])
        fact_payments = payments.merge(fact_orders[["order_id"]], on="order_id")

        out = {
            "orders": os.path.join(TMP_DIR, f"fact_orders_{run_date}.csv"),
            "items": os.path.join(TMP_DIR, f"fact_items_{run_date}.csv"),
            "payments": os.path.join(TMP_DIR, f"fact_payments_{run_date}.csv"),
        }

        fact_orders.to_csv(out["orders"], index=False)
        fact_items.to_csv(out["items"], index=False)
        fact_payments.to_csv(out["payments"], index=False)

        return out

    @task
    def load_facts(paths, run_date, load_type):
        hook = PostgresHook(PG_CONN_ID)
        conn = hook.get_conn()
        cur = conn.cursor()

        df_prod = pd.read_sql("SELECT product_id, product_sk FROM dim_product", conn)
        prod_map = dict(zip(df_prod.product_id, df_prod.product_sk))

        # fact_orders
        df_o = pd.read_csv(paths["orders"])
        for row in df_o.itertuples(index=False):
            cur.execute(
                """
                INSERT INTO fact_orders (
                    order_id, customer_sk, order_purchase_date, order_status,
                    order_approved_at, order_delivered_customer_date,
                    order_estimated_delivery_date, load_type
                )
                VALUES (%s,NULL,%s,%s,NULL,NULL,NULL,%s)
                ON CONFLICT DO NOTHING;
                """,
                (row.order_id, row.order_purchase_timestamp, row.order_status, load_type),
            )

        # fact_items
        df_i = pd.read_csv(paths["items"])
        df_i["product_sk"] = df_i["product_id"].map(prod_map).fillna(0).astype(int)

        for row in df_i.itertuples(index=False):
            cur.execute(
                """
                INSERT INTO fact_order_items (
                    order_id, product_sk, seller_id,
                    shipping_limit_date, price, freight_value, load_type
                )
                VALUES (%s,%s,%s,NULL,%s,%s,%s)
                ON CONFLICT DO NOTHING;
                """,
                (row.order_id, row.product_sk, row.seller_id, row.price, row.freight_value, load_type),
            )

        # fact_payments
        df_p = pd.read_csv(paths["payments"])
        for row in df_p.itertuples(index=False):
            cur.execute(
                """
                INSERT INTO fact_payments (
                    order_id, payment_sequential, payment_type,
                    payment_installments, payment_value, load_type
                )
                VALUES (%s,%s,%s,%s,%s,%s)
                ON CONFLICT DO NOTHING;
                """,
                (
                    row.order_id,
                    row.payment_sequential,
                    row.payment_type,
                    row.payment_installments,
                    row.payment_value,
                    load_type,
                ),
            )

        conn.commit()
        cur.close()
        conn.close()

    raw_paths = extract_data()
    dim_paths = transform_dims(raw_paths)
    fact_paths = transform_facts(raw_paths, "{{ ds }}")

    create_dwh >> load_dimensions(dim_paths) >> load_facts(
        fact_paths, "{{ ds }}", "{{ params.load_type }}"
    )
