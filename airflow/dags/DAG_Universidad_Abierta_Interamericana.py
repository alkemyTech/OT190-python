'''DAG para el procesamiento de datos
Universidad Abierta Interamericana
Grupo E'''

import csv
import logging
import pathlib

import pendulum

from airflow import DAG
from airflow.models import BaseOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
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


def clean_str(x):
    string = x.replace('-', ' ')
    string = string.strip()
    string = string.lower()
    return string


def calculate_age(diff_bday):
    '''
    Calcula la edad a partie del timedelta pasado por parametro
    '''

    days = diff_bday.days
    if days < 0:
        days += 100

    return int(days / 365.2425)


def transform_data(file_from, file_to):
    '''
    Función de transformacion y normalización de datos.
    '''

    import pandas as pd

    dtf = '%y/%b/%d'

    # Codigos postales
    cp_df = pd.read_csv(f'{parent_path}/assets/codigos_postales.csv')

    cp_df.rename(
        columns={'codigo_postal': 'postal_code', 'localidad': 'location'},
        inplace=True
    )

    cp_df['location'] = cp_df['location'].str.lower()

    # Transformación de los datos
    df = pd.read_csv(file_from)

    df['university'] = df['university'].apply(clean_str)
    df['career'] = df['career'].apply(clean_str)
    df['first_name'] = df['first_name'].apply(clean_str)
    df.rename(
        columns={'first_name': 'last_name', 'last_name': 'first_name'},
        inplace=True,
    )

    df['inscription_date'] = pd.to_datetime(
        df['inscription_date'],
        format=dtf
    )

    df['age'] = pd.to_datetime(df['age'], format=dtf)
    df['age'] = df['inscription_date'] - df['age']
    df['age'] = df['age'].apply(calculate_age)

    df['gender'] = df.gender.replace({'F': 'female', 'M': 'male'})

    df['email'] = df.email.apply(lambda x: x.strip().lower())

    df.drop(['location'], axis=1, inplace=True)
    df.rename(columns={'postal_code': 'location'}, inplace=True)

    df = df.merge(cp_df, how='left', on='location')

    # Guardado de los datos procesados
    columns = [
        'university', 'career', 'inscription_date', 'first_name', 'last_name',
        'gender', 'age', 'postal_code', 'location', 'email'
    ]

    df.to_csv(file_to, columns=columns, index=False)


with DAG(
    'DAG_Universidad_Abierta_Interamericana',
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
        sql="SQL_Universidad_Abierta_Interamericana.sql",
        csv=f'{parent_path}/files/universidad_abierta_interamericana.csv',
    )

    # Procesamiento de datos con pandas
    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=transform_data,
        op_kwargs={
            'file_from': f'{parent_path}/files/universidad_abierta_interamericana.csv',
            'file_to': f'{parent_path}/datasets/universidad_abierta_interamericana.txt'
        },
    )

    # Carga de datos en S3
    load_task = LocalFilesystemToS3Operator(
        task_id='load_task',
        filename=f'{parent_path}/datasets/universidad_nacional_de_la_pampa.txt',
        dest_key='aws_s3_alkemy_universidades',
        dest_bucket='cohorte-abril-98a56bb4',
        replace=True,
    )

    loginit_task >> extract_task >> transform_task >> load_task
