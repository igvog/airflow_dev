FROM apache/airflow:3.1.3

COPY requirements.txt /tmp/

RUN pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /opt/airflow

ENTRYPOINT ["/entrypoint"]
