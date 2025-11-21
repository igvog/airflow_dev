import os
import requests

def telegram_alert(context):
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    task = context.get("task_instance").task_id
    dag = context.get("task_instance").dag_id
    error = context.get("exception")

    message = f"❗ Airflow ERROR\nDAG: {dag}\nTask: {task}\nError: {error}"

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    requests.post(url, data={"chat_id": chat_id, "text": message})
