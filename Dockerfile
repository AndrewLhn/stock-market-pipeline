FROM apache/airflow:2.7.1-python3.10

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       build-essential \
       git \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir \
       pandas \
       boto3 \
       requests \
       duckdb \
       dbt-core \
       dbt-duckdb \
       python-dotenv