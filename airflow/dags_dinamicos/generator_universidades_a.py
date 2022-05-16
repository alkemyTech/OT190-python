"""
Arreglar un Dag dinamico para el grupo de universidades A
generator_universidades_a.py -- Un dag dinámico hecho para poder procesar cualquier universidad que cumpla con los requisitos

Los DAGs se generan en base a la extracción del denominado nombre base de los archivos SQL encontrados en la carpeta include, ej:
"SQL_Universidad_De_Flores" nos da un nombre base Universidad_De_Flores que se agrega a la variable lista "names_list"

El length de esa lista es las veces que se ejecuta el dag_create, una vez por archivo SQL, solo cambiando el nombre base

El nombre base le dará nombre a los archivos .csv y .txt que se generen. Es el dag_id de cada DAG

A su vez, el archivo de funciones auxiliares se debe importar dinámicamente, esto se hace con importlib.import_module,
utilizando el nombre base, y luego utilizando get_attr se obtiene la función transform, la que se pasa como python_callable
en pandas_transform. La función transform alojada en dicho archivo es distinta para cada universidad con el fin de poder
manipular los datos de cada una individualmente 
"""
import importlib
import logging
import pathlib
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from chardet import UniversalDetector

#Parent path (carpeta de airflow)
PARENT_PATH = (pathlib.Path(__file__).parent.absolute()).parent

#Setear conn_id (se podria obtener de varibles)
POSTGRES_CONN_ID = 'db_alkemy_universidades'

###DESHABILITADO; solo tengo que procesar dos archivos particulares
###Armar array con todos nombres base, obtenidos desde el nombre de cada archivo .SQL
# import os
# SQL_PATH = f'{PARENT_PATH}/include'
# def get_names_from_sql(directory):
#     base_names_list = []
#     for root, dirs, files in os.walk(directory):
#         for file in files:
#             if file.endswith(".sql"):
#                 base_names_list.append(pathlib.Path(os.path.join(root, file)).stem.split('SQL_')[1])
#     return base_names_list
# base_names_list = get_names_from_sql(SQL_PATH)
base_names_list = ["Universidad_De_Flores", 'Universidad_Nacional_De_Villa_Maria']

def create_dag(dag_id,
               sql_query,
               default_args):

    #Transformar query a .csv usando PostgresHook 
    def pg_extract(copy_sql, csv_filename):
        pg_hook = PostgresHook.get_hook(POSTGRES_CONN_ID)
        logging.info('Exporting query to file')
        pg_hook.copy_expert(copy_sql, filename=f"{PARENT_PATH}/files/{csv_filename}")

    #Importar función extract del archivo de funciones auxiliar correspondiente
    helpers = importlib.import_module(f'operators.helpers_{dag_id.lower()}')
    transform = getattr((helpers), 'transform')

    dag = DAG(dag_id,
              sql_query,
              schedule_interval=None,
              default_args=default_args)

    with dag:
        query_database = PythonOperator(
            task_id='pg_extract_task',
            python_callable=pg_extract,
            op_kwargs={
                'copy_sql': f'COPY ({sql_query}) TO STDOUT WITH CSV HEADER',
                'csv_filename' : f'{dag_id}.csv'
                }
        )

        #Transformar datos
        pandas_transform = PythonOperator(
            task_id='pandas_transform',
            python_callable=transform,
        )

        #Cargar datos
        load_to_s3 = LocalFilesystemToS3Operator(
            task_id='load_to_s3',
            filename=f'{PARENT_PATH}/datasets/{dag_id}.txt',
            dest_key='aws_s3_alkemy_universidades',
            dest_bucket='cohorte-abril-98a56bb4',
            replace=True,
        )

        #Flujo de ejecución
        query_database >> pandas_transform >> load_to_s3
    return dag


# Crear un dag por cada nombre en base_names_list (por cada archivo .sql)
for base_name in base_names_list:
    dag_id = f'{base_name}'
    sql_query = open(f'{PARENT_PATH}/include/SQL_{dag_id}.sql', "r").read().replace(';', '')

    default_args = {'owner': 'airflow',
                    'start_date': datetime(2021, 1, 1)
                    }

    schedule = '@daily'

    globals()[dag_id] = create_dag(dag_id,      
                                   sql_query,                                                                                               
                                   default_args)