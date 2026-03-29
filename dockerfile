FROM apache/airflow:2.7.1

USER root

RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    git \
    && apt-get clean

USER airflow

RUN pip install --no-cache-dir \
    dbt-core==1.8.0 \
    dbt-postgres==1.8.0 \
    psycopg2-binary==2.9.10

ENV PATH="${PATH}:/home/airflow/.local/bin"