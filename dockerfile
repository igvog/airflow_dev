FROM apache/airflow:latest

USER root

RUN apt-get update && \
    apt-get -y install git && \
    apt-get clean

USER airflow

# Copy and install Python dependencies
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt