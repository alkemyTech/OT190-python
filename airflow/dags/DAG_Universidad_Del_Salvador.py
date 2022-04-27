'''
DAG para la Universidad Del Salvador
Sprint 2, OT190-45

1.- ETL
2.- retries
3.- logg
'''

from unittest import FunctionTestCase
from airflow import DAG
from datetime import timedelta, datetime
import logging

from airflow.operators.python import PythonOperator


logging.basicConfig(
    level = logging.INFO,
    format = " %(asctime)s - %(name)s - %(message)s",
    datefmt='%Y-%m-%d'
    )

logger = logging.getLogger('DAG - Universidad_Nacional_Del_Comahue')

def extract():
    # extract_task = PostgresOperator(
    #     task_id='PostgresOperator',
    #     sql='<path>/SQL_Universidad_Nacional_Del_Comahue.sql',
    #     postgres_conn_id='my_postgres_connection',
    #     autocommit=False
    # )
    logger.info('Extrac')
    pass

def transform():
    logger.info('Transform')
    pass

def load():
    logger.info('Load')
    pass

default_args = {
    'retries': 5,
    'retry_delay': timedelta(seconds=45)
}
with DAG(
    'DAG_Universidad_Del_Salvador_ETL_Retries_Log',
    description= 'OT190-29 DAG_Universidad_Del_Salvador, sin consultas, ni procesamiento -ETL-Retries-Log',
    schedule_interval=timedelta(hours=1),
    start_date= datetime(2024,4,21),
    default_args=default_args
) as dag:

    extract_task = PythonOperator (
        task_id='extract',
        python_callable=extract
        )

    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform
        )

    load_task = PythonOperator(
        task_id='load',
        python_callable=load
        )


    extract_task >> transform_task >> load_task
