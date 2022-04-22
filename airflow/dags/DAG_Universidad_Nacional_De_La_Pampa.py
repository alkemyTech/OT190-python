'''DAG para el procesamiento de datos
Universidad Nacional De La Pampa
Grupo E'''

import pendulum

from airflow import DAG
from airflow.operators.dummy import DummyOperator

with DAG(
    'DAG_Universidad_Nacional_De_La_Pampa',
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
    transform_task = DummyOperator(
        task_id='transform_task',
        depends_on_past=True,
    )

    extract_task >> transform_task
