from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python import PythonOperator


""" DAG structure, retries and logs config. Perform ETL for
    Universidad de Febrero.
    TODO:
        - Transform: Python Operator (pandas)
        - Load: Python Operator (S3)
"""

# OT190-47
logging.basicConfig(format='%(asctime)s-%(levelname)s-%(message)s', datefmt='%Y-%m-%d')
logger = logging.getLogger(__name__)


def dag_init():
    logger.info('Iniciando DAG...')


def transform_data():
    pass


def load_to_s3():
    pass


default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(seconds=30),
}

with DAG(
    "DAG_Logs_Universidad_Tres_de_Febrero",
    description='DAG with logs',
    default_args=default_args,
    template_searchpath='/home/juanboho/airflow/include',  # local path
    start_date=datetime(2021, 4, 22),
    schedule_interval="@hourly",
    catchup=False) as dag:

        task_init = PythonOperator(
            task_id="task_init",
            python_callable=dag_init
        )

        extract = PostgresOperator(
            task_id="extract",
            postgres_conn_id="db_alkemy_universidades",
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

        task_init >> extract >> transform >> load
