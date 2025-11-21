# dags/currency_aggregator_etl.py
import json
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any

import requests
import xml.etree.ElementTree as ET

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.models import Variable, XCom
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from airflow.utils.email import send_email

DEFAULT_CONN_ID = "postgres_etl_target_conn"
SOURCE1 = "exchangerate_host"
SOURCE2 = "frankfurter"
SOURCE3 = "nbk_rss"

EXCHANGE_API_2 = "https://api.frankfurter.app"
NBK_RSS_URL = "https://nationalbank.kz/rss/rates_all.xml"
"3 rd src - https://currencyapi.net/api/v1/rates"


ANOMALY_DIFF_THRESHOLD_PCT = float(Variable.get("anomaly_threshold_pct", default_var=1.5))
ALERT_EMAIL = Variable.get("alert_email", default_var=None)
TG_TOKEN = Variable.get("telegram_bot_token", default_var=None)
TG_CHAT = Variable.get("telegram_chat_id", default_var=None)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": [ALERT_EMAIL] if ALERT_EMAIL else [],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

def _get_target_date(context) -> str:
    """Get target_date from dag_run.conf or fallback to execution_date.date()"""
    run_conf = context.get("dag_run").conf if context.get("dag_run") else {}
    target_date_str = run_conf.get("target_date")
    if target_date_str:
        return target_date_str
    exec_date = context.get("execution_date")
    return (exec_date.date().isoformat() if exec_date else datetime.utcnow().date().isoformat())

def check_api_availability(**context):
    """Simple check that external APIs respond (200)."""
    targets = [f"{EXCHANGE_API_2}/latest", NBK_RSS_URL]

    errors = []
    for url in targets:
        try:
            r = requests.get(url, timeout=10)
            if r.status_code != 200:
                errors.append((url, r.status_code))
        except Exception as e:
            logging.exception("API availability check failed for %s: %s", url, e)
            errors.append((url, str(e)))
    if errors:
        msg = f"API availability check failed: {errors}"
        logging.error(msg)
        raise AirflowFailException(msg)
    logging.info("All APIs reachable")

def check_postgres_connection(**context):
    """Check we can connect to Postgres."""
    try:
        hook = PostgresHook(postgres_conn_id=DEFAULT_CONN_ID)
        conn = hook.get_conn()
        cur = conn.cursor()
        cur.execute("SELECT 1;")
        cur.close()
        conn.close()
        logging.info("Postgres connection OK")
    except Exception as e:
        logging.exception("Postgres connectivity check failed: %s", e)
        raise AirflowFailException("Postgres connectivity failed")


def extract_frankfurter(**context):
    target_date = _get_target_date(context)
    url = f"{EXCHANGE_API_2}/latest"
   
    params = {"from": "USD", "to": "KZT,EUR,RUB"}
    if target_date:
       
        url = f"{EXCHANGE_API_2}/{target_date}"
        params = {"from": "USD", "to": "KZT,EUR,RUB"}
    try:
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        logging.info("frankfurter fetched keys: %s", list(data.keys()))
        context["ti"].xcom_push(key="frankfurter", value=data)
    except Exception as e:
        logging.exception("Error fetching frankfurter: %s", e)
        raise
def extract_currencyapi(**context):
    key = "0b0d40a347fff2b99071c862d4f4adcde860"
    url = "https://currencyapi.net/api/v1/rates"

    params = {
        "key": key,
        "base": "USD",
        "output": "json"
    }

    try:
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        logging.info("currencyapi fetched keys: %s", list(data.keys()))
        context["ti"].xcom_push(key="currencyapi", value=data)
    except Exception as e:
        logging.exception("Error fetching currencyapi: %s", e)
        raise

def extract_nbk_rss(**context):
    try:
        r = requests.get(NBK_RSS_URL, timeout=30)
        r.raise_for_status()
        root = ET.fromstring(r.content)
        rates = {}
        for item in root.findall(".//item"):
            title = item.findtext("title")
            if not title:
                continue
            
            parts = title.strip().split()
            try:
                cur = parts[0].replace(":", "")
                val = float(parts[-1].replace(",", "."))
                rates[cur] = val
            except Exception:
                continue
        logging.info("NBK RSS parsed currencies: %s", list(rates.keys()))
        context["ti"].xcom_push(key="nbk_rss", value={"rates": rates})
    except Exception as e:
        logging.exception("Error fetching NBK RSS: %s", e)
        context["ti"].xcom_push(key="nbk_rss", value={"rates": {}})

def merge_extracted_data(**context):
    ti = context["ti"]
    ex2 = ti.xcom_pull(task_ids="extract_frankfurter", key="frankfurter") or {}
    ex4 = ti.xcom_pull(task_ids="extract_currencyapi", key="currencyapi") or {}

    nbk = ti.xcom_pull(task_ids="extract_nbk_rss", key="nbk_rss") or {}
    merged = {"date": _get_target_date(context), "pairs": []}
    def _parse_currencyapi(d):
        pairs = []
        rates = d.get("rates", {})
        base = d.get("base", "USD")

        for to_cur, rate in rates.items():
            try:
                pairs.append({
                    "from": base,
                    "to": to_cur,
                    "rate": float(rate),
                    "source": "currencyapi"
                })
            except:
                continue

        return pairs

    def _parse_frankfurter(d):
        pairs = []
        base = d.get("base") or d.get("from") or "USD"
        rates = d.get("rates", {})
        for to_cur, rate in rates.items():
            pairs.append({"from": base, "to": to_cur, "rate": float(rate), "source": SOURCE2})
        return pairs

    merged["pairs"].extend(_parse_frankfurter(ex2) if ex2 else [])
    merged["pairs"].extend(_parse_currencyapi(ex4) if ex4 else [])


    nbk_rates = nbk.get("rates", {})
    for cur, val in nbk_rates.items():
        merged["pairs"].append({"from": cur, "to": "KZT", "rate": float(val), "source": SOURCE3})

    if not merged["pairs"]:
        raise AirflowFailException("No rate pairs after merge")

    ti.xcom_push(key="merged_rates", value=merged)
    logging.info("Merged total pairs: %s", len(merged["pairs"]))

def normalize_data(**context):
    ti = context["ti"]
    merged = ti.xcom_pull(task_ids="merge_extracted_data", key="merged_rates")
    if not merged:
        raise AirflowFailException("No merged data")
    normalized = []
    for rec in merged["pairs"]:
        cur_from = str(rec.get("from")).upper()
        cur_to = str(rec.get("to")).upper()
        if cur_from == cur_to:
            continue
        try:
            rate = float(rec.get("rate"))
        except Exception:
            continue
        normalized.append({
            "currency_from": cur_from,
            "currency_to": cur_to,
            "rate": rate,
            "source": rec.get("source"),
            "rate_date": merged.get("date")
        })
    if not normalized:
        raise AirflowFailException("Normalization resulted in empty dataset")
    ti.xcom_push(key="normalized_rates", value=normalized)
    logging.info("Normalized records: %s", len(normalized))

def schema_validate(**context):
    ti = context["ti"]
    normalized = ti.xcom_pull(task_ids="normalize_data", key="normalized_rates")
    bad = []
    for r in normalized:
        if not r.get("currency_from") or not r.get("currency_to") or r.get("rate") is None:
            bad.append(r)
    if bad:
        logging.warning("Found invalid records: %s", bad)
    clean = [r for r in normalized if r.get("currency_from") and r.get("currency_to") and r.get("rate") is not None]
    if not clean:
        raise AirflowFailException("No valid records after validation")
    ti.xcom_push(key="clean_rates", value=clean)
    logging.info("Validated records count: %s", len(clean))

def load_dim_api_source(**context):
    hook = PostgresHook(postgres_conn_id=DEFAULT_CONN_ID)
    conn = hook.get_conn()
    cur = conn.cursor()

    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS dim_api_source (
                api_id SERIAL PRIMARY KEY,
                api_name TEXT UNIQUE,
                api_url TEXT,
                api_type TEXT,
                created_at TIMESTAMP DEFAULT now()
            );
        """)
        conn.commit()

        sources = [
            ("frankfurter", "https://api.frankfurter.app", "fiat"),
            ("currencyapi", "https://currencyapi.net", "fiat"),
            ("nbk_rss", "https://nationalbank.kz/rss", "fiat")
        ]

        for name, url, typ in sources:
            cur.execute("""
                INSERT INTO dim_api_source (api_name, api_url, api_type)
                VALUES (%s, %s, %s)
                ON CONFLICT (api_name) DO NOTHING;
            """, (name, url, typ))

        conn.commit()
    except Exception as e:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()
def create_fact_crypto_rate(**context):
    hook = PostgresHook(postgres_conn_id=DEFAULT_CONN_ID)
    conn = hook.get_conn()
    cur = conn.cursor()

    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS fact_crypto_rate (
                id SERIAL PRIMARY KEY,
                coin TEXT,
                currency TEXT,
                price NUMERIC,
                market_cap NUMERIC,
                source TEXT,
                rate_date DATE,
                inserted_at TIMESTAMP DEFAULT now()
            );
        """)
        conn.commit()

        ti = context["ti"]
        crypto_data = ti.xcom_pull(task_ids="fetch_crypto_data") or []
        for record in crypto_data:
            cur.execute("""
                INSERT INTO fact_crypto_rate (coin, currency, price, market_cap, source, rate_date)
                VALUES (%s,%s,%s,%s,%s,%s)
                ON CONFLICT DO NOTHING;
            """, (
                record.get("coin"),
                record.get("currency"),
                record.get("price"),
                record.get("market_cap"),
                record.get("source"),
                record.get("rate_date"),
            ))
        conn.commit()

    except Exception as e:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

def create_fact_daily_stats(**context):
    hook = PostgresHook(postgres_conn_id=DEFAULT_CONN_ID)
    conn = hook.get_conn()
    cur = conn.cursor()

    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS fact_daily_stats (
                id SERIAL PRIMARY KEY,
                currency_from TEXT,
                currency_to TEXT,
                avg_rate NUMERIC,
                min_rate NUMERIC,
                max_rate NUMERIC,
                volatility NUMERIC,
                stat_date DATE,
                created_at TIMESTAMP DEFAULT now()
            );
        """)
        conn.commit()

        cur.execute("""
            INSERT INTO fact_daily_stats (currency_from, currency_to, avg_rate, min_rate, max_rate, volatility, stat_date)
            SELECT
                currency_from,
                currency_to,
                AVG(rate) as avg_rate,
                MIN(rate) as min_rate,
                MAX(rate) as max_rate,
                CASE WHEN MIN(rate)=0 THEN NULL ELSE (MAX(rate)-MIN(rate))/MIN(rate)*100 END as volatility,
                CURRENT_DATE
            FROM fact_exchange_rate
            WHERE rate_date = CURRENT_DATE
            GROUP BY currency_from, currency_to;
        """)
        conn.commit()

    except Exception as e:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

def create_fact_alerts_log(**context):
    hook = PostgresHook(postgres_conn_id=DEFAULT_CONN_ID)
    conn = hook.get_conn()
    cur = conn.cursor()

    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS fact_alerts_log (
                id SERIAL PRIMARY KEY,
                currency_from TEXT,
                currency_to TEXT,
                min_rate NUMERIC,
                max_rate NUMERIC,
                severity TEXT,
                alert_date DATE,
                created_at TIMESTAMP DEFAULT now()
            );
        """)
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

def load_dim_currency(**context):
    ti = context["ti"]
    clean = ti.xcom_pull(task_ids="schema_validate", key="clean_rates")
    hook = PostgresHook(postgres_conn_id=DEFAULT_CONN_ID)
    conn = hook.get_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS dim_currency (
                currency_code TEXT PRIMARY KEY,
                description TEXT,
                created_at TIMESTAMP DEFAULT now()
            );
        """)
        conn.commit()
    except Exception as e:
        conn.rollback()
        logging.exception("Failed to create dim_currency: %s", e)
        raise

    codes = {r["currency_from"] for r in clean} | {r["currency_to"] for r in clean}
    try:
        for code in codes:
            cur.execute("""
                INSERT INTO dim_currency (currency_code, description)
                VALUES (%s, %s)
                ON CONFLICT (currency_code) DO NOTHING;
            """, (code, f"{code} currency"))
        conn.commit()
        logging.info("Upserted dim_currency count: %s", len(codes))
    except Exception as e:
        conn.rollback()
        logging.exception("Error upserting dim_currency: %s", e)
        raise
    finally:
        cur.close()
        conn.close()

def load_fact_rates(**context):
    ti = context["ti"]
    clean = ti.xcom_pull(task_ids="schema_validate", key="clean_rates")
    hook = PostgresHook(postgres_conn_id=DEFAULT_CONN_ID)
    conn = hook.get_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS fact_exchange_rate (
                id SERIAL PRIMARY KEY,
                currency_from TEXT,
                currency_to TEXT,
                rate NUMERIC,
                source TEXT,
                rate_date DATE,
                inserted_at TIMESTAMP DEFAULT now()
            );
        """)
        cur.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS ux_rate_src_date ON fact_exchange_rate(currency_from, currency_to, source, rate_date);
        """)
        conn.commit()
    except Exception as e:
        conn.rollback()
        logging.exception("Failed to create fact table: %s", e)
        raise

    try:
        for r in clean:
            cur.execute("""
                INSERT INTO fact_exchange_rate (currency_from, currency_to, rate, source, rate_date)
                VALUES (%s,%s,%s,%s,%s)
                ON CONFLICT (currency_from, currency_to, source, rate_date) DO UPDATE
                  SET rate = EXCLUDED.rate, inserted_at = now();
            """, (r["currency_from"], r["currency_to"], r["rate"], r["source"], r["rate_date"]))
        conn.commit()
        logging.info("Loaded %s fact rows", len(clean))
    except Exception as e:
        conn.rollback()
        logging.exception("Error inserting fact rows: %s", e)
        raise
    finally:
        cur.close()
        conn.close()

def detect_anomalies(**context):
    ti = context["ti"]
    target_date = _get_target_date(context)
    hook = PostgresHook(postgres_conn_id=DEFAULT_CONN_ID)
    conn = hook.get_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
          SELECT currency_from, currency_to,
                 MIN(rate) as min_rate, MAX(rate) as max_rate
          FROM fact_exchange_rate
          WHERE rate_date = %s
          GROUP BY currency_from, currency_to
          HAVING (MAX(rate) - MIN(rate))/NULLIF(MIN(rate),0) * 100 > %s
        """, (target_date, ANOMALY_DIFF_THRESHOLD_PCT))
        rows = cur.fetchall()
        ti.xcom_push(key="anomaly_rows", value=rows)
        logging.info("Anomaly detection found: %s rows", len(rows))
    except Exception as e:
        logging.exception("Error during anomaly detection: %s", e)
        raise
    finally:
        cur.close()
        conn.close()

def send_alerts(**context):
    ti = context["ti"]
    rows = ti.xcom_pull(task_ids="detect_anomalies", key="anomaly_rows") or []
    target_date = _get_target_date(context)

    if not rows:
        # Тестовое уведомление
        message = f"Это тестовое уведомление для {target_date}, аномалий не найдено."
        logging.info(message)
    else:
        lines = [f"Anomalies for {target_date} (>{ANOMALY_DIFF_THRESHOLD_PCT}%):"]
        for r in rows:
            lines.append(f"{r[0]} -> {r[1]} min={r[2]} max={r[3]}")
        message = "<br>".join(lines)
        logging.warning(message)

    if ALERT_EMAIL:
        try:
            send_email(
                to=ALERT_EMAIL,
                subject=f"[Airflow] FX anomalies {target_date}",
                html_content=message
            )
            logging.info("Email alert sent to %s", ALERT_EMAIL)
        except Exception:
            logging.exception("Failed to send email alert")

    if TG_TOKEN and TG_CHAT:
        try:
            requests.post(
                f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                json={"chat_id": TG_CHAT, "text": message},
                timeout=10
            )
            logging.info("Telegram alert sent")
        except Exception:
            logging.exception("Failed to send Telegram alert")

with DAG(
    dag_id="currency_aggregator_etl",
    default_args=default_args,
    description="Daily FX rates aggregator from 3 sources with anomaly detection",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=True,
    max_active_runs=1,
    tags=["fx", "currency", "etl"],
) as dag:

    t_check_api = PythonOperator(
        task_id="check_api_availability",
        python_callable=check_api_availability,
    )

    t_check_pg = PythonOperator(
        task_id="check_postgres_connection",
        python_callable=check_postgres_connection,
    )
    t_dim_source = PythonOperator(
    task_id="load_dim_api_source",
    python_callable=load_dim_api_source,
    )

    t_fact_crypto = PythonOperator(
        task_id="create_fact_crypto_rate",
        python_callable=create_fact_crypto_rate,
    )

    t_daily_stats = PythonOperator(
        task_id="create_fact_daily_stats",
        python_callable=create_fact_daily_stats,
    )

    t_alert_log = PythonOperator(
        task_id="create_fact_alerts_log",
        python_callable=create_fact_alerts_log,
    )


    t_ex2 = PythonOperator(
        task_id="extract_frankfurter",
        python_callable=extract_frankfurter,
    )

    t_ex3 = PythonOperator(
        task_id="extract_nbk_rss",
        python_callable=extract_nbk_rss,
    )
    t_ex4 = PythonOperator(
    task_id="extract_currencyapi",
    python_callable=extract_currencyapi,
    )


    # merge
    t_merge = PythonOperator(
        task_id="merge_extracted_data",
        python_callable=merge_extracted_data,
    )

    t_norm = PythonOperator(
        task_id="normalize_data",
        python_callable=normalize_data,
    )

    t_validate = PythonOperator(
        task_id="schema_validate",
        python_callable=schema_validate,
    )

    # load
    t_dim = PythonOperator(
        task_id="load_dim_currency",
        python_callable=load_dim_currency,
    )

    t_fact = PythonOperator(
        task_id="load_fact_rates",
        python_callable=load_fact_rates,
    )


    t_anom = PythonOperator(
        task_id="detect_anomalies",
        python_callable=detect_anomalies,
    )

    t_alert = PythonOperator(
        task_id="send_alerts",
        python_callable=send_alerts,
    )
    

    #dag graph

    t_check_api >> t_ex2
    t_check_api >> t_ex3
    t_check_pg >> t_ex2
    t_check_pg >> t_ex3
    t_check_api >> t_ex4

    # Extract -> Merge -> Normalize -> Validate
    t_ex2 >> t_merge
    t_ex3 >> t_merge
    t_merge >> t_norm
    t_norm >> t_validate
    t_check_pg >> t_ex4
    t_ex4 >> t_merge

    # Validate -> Load -> Anomaly -> Alert
    t_validate >> t_dim_source
    t_validate >> t_fact_crypto
    t_validate >> t_daily_stats
    t_validate >> t_alert_log

    t_validate >> t_dim
    t_validate >> t_fact
    t_dim >> t_anom
    t_fact >> t_anom
    t_anom >> t_alert