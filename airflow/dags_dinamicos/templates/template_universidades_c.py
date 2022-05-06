'''
Genera los DAG dinámicamente de:
- Universidad de Palermo
- Universidad Nacional de Jujuy
'''
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import sys
import pathlib

# Busqueda del path donde se está ejecutando el archivo, subimos un nivel para
# situarnos en la carpeta airflow
path_p = (pathlib.Path(__file__).parent.absolute()).parent

sys.path.append(f"/{path_p}/plugins")
from extract_universidades_c import query_to_csv
from load_universidades_c import upload_to_S3
from transform_universidades_c import normalize_data

default_args = {"owner": "airflow", "retries": 5, "retry_delay": timedelta(seconds=30)}

with DAG(
    "DAG_ID_HOLDER",
    description="ETL para la UNIVERSITY_HOLDER",
    default_args=default_args,
    schedule_interval="SCHEDULE_INTERVAL_HOLDER",  # Que se ejecute cada hora
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
            "sql_file": "SQL_FILEPATH_HOLDER",
            "filename": "SQL_FILENAME_HOLDER",
        },
        dag=dag,
    )

    transform_task = PythonOperator(
        task_id="transform_task",
        python_callable=normalize_data,
        dag=dag,
        op_kwargs={
            "csv_filename": "CSV_FILENAME_HOLDER",
            "file_path": path_p,
        },
    )

    load_task = PythonOperator(
        task_id="load_task",
        python_callable=upload_to_S3,
        op_kwargs={
            "file_path": os.path.join(path_p, "LOAD_FILEPATH_HOLDER"),
            "key": "S3_KEY_HOLDER",
        },
        dag=dag,
    )

    # Describo el orden de ejecución en el DAG
    extract_task >> transform_task >> load_task