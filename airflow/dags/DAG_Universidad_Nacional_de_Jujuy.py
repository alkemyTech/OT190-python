from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import sys
import logging
import pathlib

# Busqueda del path donde se está ejecutando el archivo, subimos un nivel para
# situarnos en la carpeta airflow
path_p = (pathlib.Path(__file__).parent.absolute()).parent

sys.path.append(f"/{path_p}/plugins")
from extract_universidades_c import query_to_csv
from load_universidades_c import upload_to_S3
from transform_universidades_c import normalize_data

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


default_args = {"owner": "airflow", "retries": 5, "retry_delay": timedelta(seconds=30)}

with DAG(
    "ETL_Universidad_Nacional_de_Jujuy",
    description="ETL para la Universidad Nacional de Jujuy",
    default_args=default_args,
    schedule_interval="@hourly",  # Que se ejecute cada hora
    start_date=datetime(2022, 4, 22),
    template_searchpath=f"{path_p}/include",
    catchup=False,
) as dag:

    # Declaro las tareas de extraer datos, transformarlos y subirlos
    extract_task = PythonOperator(
        task_id="extract_task",
        python_callable=query_to_csv,
        op_kwargs={
            "local_path": path_p,
            "sql_file": "SQL_Universidad_Nacional_de_Jujuy.sql",
            "filename": "universidad_nacional_de_jujuy.csv",
        },
        dag=dag,
    )

    transform_task = PythonOperator(
        task_id="transform_task",
        python_callable=normalize_data,
        dag=dag,
        op_kwargs={
            "csv_filename": "universidad_nacional_de_jujuy",
            "file_path": path_p,
        },
    )

    load_task = PythonOperator(
        task_id="load_task",
        python_callable=upload_to_S3,
        op_kwargs={
            "file_path": os.path.join(
                path_p, "datasets/universidad_nacional_de_jujuy.txt"
            ),
            "key": "universidad_nacional_de_jujuy.txt",
        },
        dag=dag,
    )

    # Describo el orden de ejecución en el DAG
    extract_task >> transform_task >> load_task
