FROM apache/airflow:2.7.2
USER root

RUN apt-get update && apt-get install -y git
USER airflow

RUN pip install --no-cache-dir \
    apache-airflow-providers-snowflake \
    dbt-snowflake \
    dbt-core