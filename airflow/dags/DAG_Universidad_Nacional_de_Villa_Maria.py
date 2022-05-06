import logging
import pathlib
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from operators.process_univ_nac_villa_maria import transform_univ_nac_villa_maria

#Configuracion de logging
logger = logging.getLogger("dag_univ_nac_villamaria")
FORMAT = '%(asctime)-%(levelname)-%(message)s'
DATEFORMAT = '%Y-%m-%d'
logging.basicConfig(format=FORMAT, datefmt=DATEFORMAT, level=logging.DEBUG)

###############PSQL##############
#Obtener parent path para luego configurar el template_searchpath
PARENT_PATH = (pathlib.Path(__file__).parent.absolute()).parent

#Setear conn_id (se podria obtener de varibles)
POSTGRES_CONN_ID = 'db_alkemy_universidades'

#Elegir archivo .sql de la carpeta /includes/
TEMPLATE_LOCATION = f'{PARENT_PATH}/include'
TEMPLATE_NAME = 'SQL_Universidad_Nacional_De_Villa_Maria.sql'

#Preparar nombre para archivo .csv en base a nombre del archivo sql
CSV_NAME = f'{TEMPLATE_NAME.split("SQL_")[1].split(".")[0]}.csv'

#Pasar .sql a string y sacar punto y coma final: 'being a psql command, it is not terminated by a semicolon'
QUERY = open(f'{TEMPLATE_LOCATION}/{TEMPLATE_NAME}', "r").read().replace(';', '')

#Query a .csv usando PostgresHook 
def pg_extract(copy_sql):
    pg_hook = PostgresHook.get_hook(POSTGRES_CONN_ID)
    logging.info('Exporting query to file')
    pg_hook.copy_expert(copy_sql, filename=f"{PARENT_PATH}/files/{CSV_NAME}")


#DAG
#Argumentos predeterminados para configuracion
default_args = {
    'owner': 'airflow',    
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

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

        #Transformar datos
        pandas_transform = PythonOperator(
            task_id='pandas_transform',
            python_callable=transform_univ_nac_villa_maria,
        )

        #Placeholder cargar datos
        load_to_s3 = LocalFilesystemToS3Operator(
            task_id='load_to_s3',
            filename=f'{PARENT_PATH}/datasets/Universidad_De_Flores.txt',
            dest_key='aws_s3_alkemy_universidades',
            dest_bucket='cohorte-abril-98a56bb4',
            replace=True,
        )

        #Flujo de ejecución
        query_database >> pandas_transform >> load_to_s3