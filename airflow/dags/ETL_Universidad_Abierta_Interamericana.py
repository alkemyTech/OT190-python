import logging
import pathlib

import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator

from operators.sql_to_localcsv import SQLToLocalCsv
from helpers.universidades_transform import transform_data


parent_path = (pathlib.Path(__file__).parent.absolute()).parent


def logging_init():
    logging.basicConfig(
        format='%(asctime)s - %(name)s - %(message)s',
        datefmt='%Y/%m/%d',
        level=logging.DEBUG
    )
    logger = logging.getLogger(__name__)
    logger.info('Inicio de ejecución de DAG')


with DAG(
    "ETL_Universidad_Abierta_Interamericana",
    schedule_interval="@hourly",
    start_date=pendulum.datetime(2022, 4, 20, tz="UTC"),
    template_searchpath=f'{parent_path}/include',
    catchup=False,
    tags=["Grupo_Universidades_E"],
) as dag:
    # Logging de inicio de ejecución de DAG
    loginit_task = PythonOperator(
        task_id='loginit_task',
        python_callable=logging_init,
    )

    # Extraccion de datos a una base de datos postgres
    # y se guardan los datos localmente en formato csv
    extract_task = SQLToLocalCsv(
        task_id='extract_task',
        retries=5,
        postgres_conn_id='db_alkemy_universidades',
        sql="SQL_Universidad_Abierta_Interamericana.sql",
        csv=f'{parent_path}/files/universidad_abierta_interamericana.csv',
    )

    # Procesamiento de datos con pandas
    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=transform_data,
        op_kwargs={
            'file_from': f'{parent_path}/files/universidad_abierta_interamericana.csv',
            'file_to': f'{parent_path}/datasets/universidad_abierta_interamericana.txt',
            'dt_format': '%y/%b/%d',
            'cp_path': f'{parent_path}/assets/codigos_postales.csv',
        },
    )

    # Carga de datos en S3
    load_task = LocalFilesystemToS3Operator(
        task_id='load_task',
        filename=f'{parent_path}/datasets/universidad_abierta_interamericana.txt',
        dest_key='aws_s3_alkemy_universidades',
        dest_bucket='cohorte-abril-98a56bb4',
        replace=True,
    )

    loginit_task >> extract_task >> transform_task >> load_task