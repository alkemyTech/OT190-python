'''DAG para el procesamiento de datos
Universidad Abierta Interamericana
Grupo E'''

import logging

import pendulum

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator


def logging_init():
    logging.basicConfig(
        format='%(asctime)s - %(name)s - %(message)s',
        datefmt='%Y/%m/%d',
        level=logging.DEBUG
    )
    logger = logging.getLogger(__name__)
    logger.info('Inicio de ejecución de DAG')


with DAG(
    'DAG_Universidad_Abierta_Interamericana',
    start_date=pendulum.datetime(2022, 4, 20, tz="UTC"),
    schedule_interval='@hourly',
    catchup=False,
    tags=["Grupo_Universidades_E"],
) as dag:
    # Logging de inicio de ejecución de DAG
    loginit_task = PythonOperator(
        task_id='loginit_task',
        python_callable=logging_init,
    )

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

    # Carga de datos en S3
    # Posibles operadores: PythonOperator, LocalFilesystemToS3Operator
    # S3Hook
    load_task = DummyOperator(
        task_id='load_task',
    )

    loginit_task >> extract_task >> transform_task >> load_task
