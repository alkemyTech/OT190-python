from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python import PythonOperator


""" DAG structure and retries. Perform ETL for Universidad Tecnologica Nacional
    TODO:
        - Transform: Python Operator (pandas)
        - Load: Python Operator (S3)
"""


def transform_data():
    pass


def load_to_s3():
    pass


# Retries OT190-39
default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(seconds=30),
}

with DAG(
    "DAG_Retries_Universidad_Tecnologica_Nacional",
    description='DAG with retries',
    default_args=default_args,
    template_searchpath='/home/juanboho/airflow/include',  # local path
    start_date=datetime(2021, 4, 22),
    schedule_interval="@hourly",
    catchup=False) as dag:

        extract = PostgresOperator(
            task_id="extract",
            postgres_conn_id="training",
            sql="SQL_Universidad_Tecnologica_Nacional.sql",
        )

        transform = PythonOperator(
            task_id="transform",
            python_callable=transform_data
        )

        load = PythonOperator(
            task_id="load",
            python_callable=load_to_s3
        )

        extract >> transform >> load
