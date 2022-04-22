from datetime import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

""" DAG structure. Perform ETL for Universidad Tres de Febrero
    TODO:
        - Extract: SQL Operator (postgres)
        - Transform: Python Operator (pandas)
        - Load: Python Operator (S3)
"""


# Python callables
def transform_data():
    pass


def load_to_s3():
    pass


with DAG(
    "DAG_Universidad_Tres_de_Febrero",
    start_date=datetime(2021, 4, 22),
    schedule_interval="@hourly",
    catchup=False) as dag:

        extract = DummyOperator(
            task_id="extract",
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
