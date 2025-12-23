FROM apache/airflow:2.10.0-python3.11

USER root

RUN apt-get update && apt-get install wget

USER airflow

RUN mkdir -p ~/.postgresql && \
    wget "https://storage.yandexcloud.net/cloud-certs/CA.pem" \
         --output-document ~/.postgresql/root.crt && \
    chmod 0655 ~/.postgresql/root.crt

RUN pip install \
    psycopg2-binary \
    clickhouse-driver \
    dbt-core \
    dbt-clickhouse
RUN pip install --no-cache-dir kafka-python requests
