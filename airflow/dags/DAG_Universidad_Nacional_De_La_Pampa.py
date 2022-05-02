'''DAG para el procesamiento de datos
Universidad Nacional De La Pampa
Grupo E'''

import csv
import logging
import pathlib

import pendulum

from airflow import DAG
from airflow.models import BaseOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.decorators import apply_defaults

parent_path = (pathlib.Path(__file__).parent.absolute()).parent


class SQLToLocalCsv(BaseOperator):

    template_fields = ['sql']
    template_ext = ('.sql')

    @apply_defaults
    def __init__(
            self,
            name=None,
            postgres_conn_id=None,
            sql=None,
            csv=None,
            *args,
            **kwargs
    ) -> None:
        super(SQLToLocalCsv, self).__init__(*args, **kwargs)
        self.name = name
        self.postgres_conn_id = postgres_conn_id
        self.sql = sql
        self.csv = csv

    def execute(self, context):
        hook_postgres = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        with hook_postgres.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(self.sql)
                rows = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]

                with open(self.csv, 'w', newline='') as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow(columns)
                    writer.writerows(rows)


def logging_init():
    logging.basicConfig(
        format='%(asctime)s - %(name)s - %(message)s',
        datefmt='%Y/%m/%d',
        level=logging.DEBUG
    )
    logger = logging.getLogger(__name__)
    logger.info('Inicio de ejecución de DAG')


with DAG(
    'DAG_Universidad_Nacional_De_La_Pampa',
    schedule_interval='@hourly',
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
        sql="SQL_Universidad_Nacional_De_La_Pampa.sql",
        csv=f'{parent_path}/files/Universidad_Nacional_De_La_Pampa.csv',
    )

    # Procesamiento de datos con pandas
    # Posibles operadores: PythonOperator
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
