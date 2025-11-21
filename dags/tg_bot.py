import os
import requests

from airflow.exceptions import AirflowException


BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')


def send_telegram_message(text: str) -> None:
    """Send a message to Telegram chat using Bot API."""
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": "Markdown",
    }
    try:
        resp = requests.post(url, data=payload, timeout=10)
        resp.raise_for_status()
    except Exception as e:
        raise AirflowException(f"Failed to send Telegram message: {e}")


def notify_task_failure(**context):
    ti = context["task_instance"]
    dag_id = ti.dag_id
    task_id = ti.task_id
    exec_date = context.get("ts")
    log_url = ti.log_url
    exception = context.get("exception")

    text = (
        f"❌ *Airflow Task Failed*\n"
        f"*DAG*: `{dag_id}`\n"
        f"*Task*: `{task_id}`\n"
        f"*Execution*: `{exec_date}`\n"
        f"*Exception*: `{exception}`\n"
        f"[View logs]({log_url})"
    )
    send_telegram_message(text)


def notify_dag_success(context):
    dag_run = context["dag_run"]
    dag_id = dag_run.dag_id
    run_id = dag_run.run_id
    exec_date = context.get("ts")

    text = (
        f"✅ *DAG Succeeded*\n"
        f"*DAG*: `{dag_id}`\n"
        f"*Run ID*: `{run_id}`\n"
        f"*Execution*: `{exec_date}`"
    )
    send_telegram_message(text)
