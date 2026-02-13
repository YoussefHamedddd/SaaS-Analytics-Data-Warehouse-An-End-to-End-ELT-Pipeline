from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'youssef',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='saas_full_pipeline',
    default_args=default_args,
    description='A full pipeline: Create -> Load -> DBT Transformation',
    schedule_interval='@daily',
    start_date=datetime(2026, 2, 13),
    catchup=False,
    template_searchpath=['/opt/airflow/dags/sql'] # This helps Airflow find your SQL files
) as dag:

    # 1. Create Raw Tables
    # This task runs the CREATE TABLE scripts in Snowflake
    create_raw_tables = SnowflakeOperator(
        task_id='create_raw_tables',
        snowflake_conn_id='snowflake_conn',
        sql='create_raw_table.sql'
    )

    # 2. Load Raw Data
    # This task loads data from your Stage/S3 into the tables
    load_raw_data = SnowflakeOperator(
        task_id='load_raw_data',
        snowflake_conn_id='snowflake_conn',
        sql='Load_raw_tables.sql'
    )

    # 3. DBT Run
    # Running dbt inside the container using the mapped volume
    run_dbt = BashOperator(
        task_id='run_dbt_models',
        bash_command='cd /opt/airflow/my_analysis && dbt run --profiles-dir .'
    )

    # 4. DBT Test
    # Ensuring data quality after transformation
    test_dbt = BashOperator(
        task_id='test_dbt_models',
        bash_command='cd /opt/airflow/my_analysis && dbt test --profiles-dir .'
    )

    # Execution Order
    create_raw_tables >> load_raw_data >> run_dbt >> test_dbt