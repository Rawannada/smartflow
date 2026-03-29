FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    git \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir dbt-postgres

WORKDIR /usr/app/dbt_project

COPY . .

ENV DBT_PROFILES_DIR=.

ENTRYPOINT ["dbt"]