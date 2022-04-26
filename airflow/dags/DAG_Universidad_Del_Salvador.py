'''
DAG para la Universidad Del Salvador, sin consultas, ni procesamiento
Sprint 2, OT190-29
'''

from unittest import FunctionTestCase
from airflow import DAG
from datetime import timedelta, datetime

from airflow.operators.dummy import DummyOperator


with DAG(
    'DAG_Universidad_Del_Salvador',
    description= 'OT190-29 DAG_Universidad_Del_Salvador, sin consultas, ni procesamiento',
    schedule_interval=timedelta(hours=1),
    start_date= datetime(2024,4,21),
) as dag:

    #ETL (Extract, Transform, Load)
    
    # extract_task = PostgresOperator(
    #     task_id='PostgresOperator',
    #     sql='<path>/SQL_Universidad_Del_Salvador.sql',
    #     postgres_conn_id='my_postgres_connection',
    #     autocommit=False
    # )
    
    extract_task = DummyOperator (task_id='extract_task')
    transform_task = DummyOperator(task_id='transform_task')


    extract_task >> transform_task