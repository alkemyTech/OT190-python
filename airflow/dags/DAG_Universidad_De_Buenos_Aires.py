# Built in modules
from datetime import timedelta, datetime
# Logging
import logging
# Airflow modules
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

sql_path = "../include/SQL_Universidad_de_Buenos_Aires.sql"

logging.basicConfig(level = logging.INFO,
                    format = " %(asctime)s - %(name)s - %(message)s",
                    datefmt='%Y-%m-%d',
                    encoding= "utf-8")

logger = logging.getLogger('DAG - Universidad De Buenos Aires')


def extract():
    logger.info('Extract data')
    pass


def transform_data():
    logger.info('Transform data')
    pass


def load():
    logger.info('Load data')
    pass


default_args = {
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}


with DAG(
    "DAG_Universidad_De_Buenos_Aires",
    default_args=default_args,
    description="DAG sin procesamiento para la Universidad de Buenos Aires",
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2022, 4, 22)
) as dag:
    select_task = PostgresOperator(
        task_id="select_task",
        postgres_conn_id='db_alkemy_universidades',
        sql=sql_path
        )

    transform_data_task = PythonOperator(
        task_id="transform_data_task",
        python_callable=transform_data
        )

    load_task = PythonOperator(
        task_id="load_task",
        python_callable=load
        )

    select_task >> transform_data_task >> load_task