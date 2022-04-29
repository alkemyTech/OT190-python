from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import logging
import pathlib
import os
import pandas as pd

# Busqueda del path donde se está ejecutando el archivo, subimos un nivel
# para situarnos en la carpeta airflow
path_p = (pathlib.Path(__file__).parent.absolute()).parent

# Configuracion logging
# Formato: %Y-%m-%d - nombre_logger - mensaje
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(message)s",
    level=logging.DEBUG,
    datefmt="%Y-%m-%d",
)
# Log con el nombre de archivo en que se encuentra
log = logging.getLogger(__name__)

# Path para descargar los archivos .csv
path_d = pathlib.Path.joinpath(path_p, "files")


def query_to_csv(sql_file, filename):
    """
    Ejecuta la query descripta en sql_file y guarda el resultado con el nombre de archivo filename
    como .csv en la carpeta files
    """
    try:
        os.stat(path_d)
    except:
        os.mkdir(path_d)

    pg_hook = PostgresHook(
        postgres_conn_id="db_alkemy_universidades", schema="training"
    )
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    university_sql = open(f"{path_p}/include/{sql_file}", "r")

    university_query = university_sql.read()

    university_df = pd.read_sql(university_query, connection)

    university_df.to_csv(f"{path_d}/{filename}")


def normalize_data():
    pass


default_args = {"owner": "airflow", "retries": 5, "retry_delay": timedelta(seconds=30)}

with DAG(
    "DAG_Universidad_de_Palermo",
    description="DAG para la Universidad de Palermo",
    default_args=default_args,
    schedule_interval="@hourly",  # Que se ejecute cada hora
    start_date=datetime(2022, 4, 22),
    template_searchpath=f"{path_p}/include",
    catchup=False
) as dag:

    # Declaro las tareas de extraer datos, transformarlos y subirlos
    extract_task = PythonOperator(
        task_id="extract_task",
        python_callable=query_to_csv,
        op_kwargs={
            "sql_file": "SQL_Universidad_de_Palermo.sql",
            "filename": "universidad_de_palermo.csv",
        },
        dag=dag
    )

    transform_task = PythonOperator(
        task_id="transform_task", python_callable=normalize_data, dag=dag
    )

    load_task = DummyOperator(task_id="load_task", dag=dag)

    # Describo el orden de ejecución en el DAG
    extract_task >> transform_task >> load_task
