FROM apache/airflow:2.10.2-python3.10

USER root

# Install Poetry
RUN pip install poetry

WORKDIR /opt/airflow

COPY pyproject.toml .

RUN poetry config virtualenvs.create false \
    && poetry install --no-root

USER airflow
