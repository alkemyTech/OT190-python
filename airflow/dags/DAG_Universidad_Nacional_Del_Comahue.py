'''
DAG para la Universidad_Nacional_Del_Comahue, sin consultas, ni procesamiento
Sprint 2, OT190-29

1.- ETL
2.- retries
'''

from unittest import FunctionTestCase
from airflow import DAG
from datetime import timedelta, datetime

from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator


def extract():
    # extract_task = PostgresOperator(
    #     task_id='PostgresOperator',
    #     sql='<path>/SQL_Universidad_Nacional_Del_Comahue.sql',
    #     postgres_conn_id='my_postgres_connection',
    #     autocommit=False
    # )

    pass

def transform():
    pass

def load():
    pass

default_args = {
    'retries': 5,
    'retry_delay': timedelta(seconds=45)
}

with DAG(
    'DAG_Universidad_Nacional_Del_Comahue_ETL_Retries',
    description= 'OT190-29 DAG_Universidad_Nacional_Del_Comahue, sin consultas, ni procesamiento, ETL-Retries',
    schedule_interval=timedelta(hours=1),
    start_date= datetime(2024,4,21),
    default_args=default_args
) as dag:

     #ETL (Extract, Transform, Load)

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
