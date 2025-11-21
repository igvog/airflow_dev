FROM apache/airflow:2.9.2-python3.12

USER root
RUN pip install --no-cache-dir uv
USER airflow

WORKDIR /opt/airflow
COPY pyproject.toml /opt/airflow/pyproject.toml

# Сначала "компилируем" зависимости в requirements,
# потом устанавливаем их в системный python внутри контейнера
RUN uv pip compile pyproject.toml -o /tmp/req.txt \
 && uv pip install --system -r /tmp/req.txt
