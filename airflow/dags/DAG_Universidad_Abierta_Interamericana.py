'''DAG para el procesamiento de datos
Universidad Abierta Interamericana
Grupo E'''

import pendulum

from airflow import DAG
from airflow.operators.dummy import DummyOperator

with DAG(
    'DAG_Universidad_Abierta_Interamericana',
    start_date=pendulum.datetime(2022, 4, 20, tz="UTC"),
    schedule_interval='@hourly',
    catchup=False,
    tags=["Grupo_Universidades_E"],
) as dag:
    # Extraccion de datos a una base de datos postgres
    # Posibles operadores: PostgresOperator, SqlToS3Operator
    extract_task = DummyOperator(
        task_id='extract_task',
        retries=5,
        )

    # Procesamiento de datos con pandas y guardar los datos en S3
    # Posibles operadores: PythonOperator
    # S3Hook
    transform_task = DummyOperator(task_id='transform_task')

    extract_task >> transform_task
