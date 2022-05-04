
import os
from datetime import datetime, timedelta
import logging
import pathlib
import csv

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python import PythonOperator

""" DAG structure, retries and logs config. Perform ETL for
    Universidad Tres de Febrero.
    TODO:
        - Transform: Python Operator (pandas)
        - Load: Python Operator (S3)
"""

# Logs config
logging.basicConfig(
    format='%(asctime)s-%(levelname)s-%(message)s',
    datefmt='%Y-%m-%d')

logger = logging.getLogger(__name__)

# Path sol
path_p = (pathlib.Path(__file__).parent.absolute()).parent


def dag_init():
    logger.info('Iniciando DAG...')


def extract_data(file_name_):
    """ Execute a query to a postgres database, then create and rewrite a .csv file
    """

    # Sql
    sql_file_path = f'{path_p}/include/SQL_{file_name_}.sql'
    query = open(sql_file_path, "r")
    request = query.read()

    pg_hook = PostgresHook(
        postgres_conn_id="db_alkemy_universidades",
        schema="training"
    )

    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(request)
    records = cursor.fetchall()

    # New .csv file
    csv_path = f'{path_p}/files'

    if not os.path.isdir(csv_path):
        os.makedirs(csv_path)

    csv_file_path = f'{csv_path}/{file_name_.lower()}.csv'
    headers = [i[0] for i in cursor.description]

    with open(csv_file_path, 'w', encoding='UTF8', newline='') as f:

        logger.info("Writing file...")

        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(records)

        logger.info('Writing done')

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
    "DAG_Universidad_Tres_de_Febrero",
    description='DAG SQL query',

    default_args=default_args,
    template_searchpath=f'{path_p}/airflow/include',
    start_date=datetime(2021, 4, 22),
    schedule_interval="@hourly",
    catchup=False) as dag:

        extract = PythonOperator(
            task_id="extract",
            python_callable=extract_data,
            op_args={"Universidad_Tres_de_Febrero"}
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
