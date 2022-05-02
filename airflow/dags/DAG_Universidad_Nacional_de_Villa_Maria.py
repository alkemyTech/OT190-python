import logging
import pathlib
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator

#Configuracion de logging
logger = logging.getLogger("dag_univ_nac_villamaria")
FORMAT = '%(asctime)-%(levelname)-%(message)s'
DATEFORMAT = '%Y-%m-%d'
logging.basicConfig(format=FORMAT, datefmt=DATEFORMAT, level=logging.DEBUG)

#Obtener parent path para luego configurar el template_searchpath
PARENT_PATH = (pathlib.Path(__file__).parent.absolute()).parent

#Setear conn_id (se podria obtener de varibles)
POSTGRES_CONN_ID = 'db_alkemy_universidades'

#Elegir archivo .sql de la carpeta /includes/
TEMPLATE_LOCATION = f'{PARENT_PATH}/includes/'
TEMPLATE_NAME = 'SQL_Universidad_Nacional_De_Villa_Maria.sql'

#Pasar .sql a string y sacar punto y coma final: 'being a psql command, it is not terminated by a semicolon'
QUERY = open(f'{TEMPLATE_LOCATION}/{TEMPLATE_NAME}', "r").read().replace(';', '')

#Argumentos predeterminados para configuracion
default_args = {
    'owner': 'airflow',    
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

#Query a .csv usando PostgresHook 
def pg_extract(copy_sql):
    pg_hook = PostgresHook.get_hook(POSTGRES_CONN_ID)
    logging.info('Exporting query to file')
    pg_hook.copy_expert(copy_sql, filename=f"{PARENT_PATH}/files/{pathlib.Path(TEMPLATE_NAME).stem}.csv")

#Configurar DAG
with DAG(
    dag_id="DAG_Universidad_Nacional_de_Villa_Maria",
    default_args=default_args,
    schedule_interval='@once',	
    dagrun_timeout=timedelta(minutes=60),
    description='DAG para Universidad Nacional de Villa Maria',
    start_date = datetime(2022,4,25),    
    catchup=False) as dag:

        query_database = PythonOperator(
            task_id='pg_extract_task',
            python_callable=pg_extract,
            op_kwargs={
                'copy_sql': f'COPY ({QUERY}) TO STDOUT WITH CSV HEADER'
                }
            )

        #Placeholder transformar datos
        pandas_transform = PythonOperator(
            task_id = "pandas_transform"
        )

        #Placeholder cargar datos
        load_to_s3 = DummyOperator(
            task_id = "load_to_s3"
        )

        #Flujo de ejecución
        query_database >> pandas_transform >> load_to_s3