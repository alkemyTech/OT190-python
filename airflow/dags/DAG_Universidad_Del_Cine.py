from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


# dag sin consultas para universidad del cine
def extract():
    pass


def transform_data():
    pass


def load():
    pass


with DAG(
    "DAG_Universidad_Del_Cine",
    description="DAG sin procesamiento para la Universidad del Cine",
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2022, 4, 22)
) as dag:
    extract_task = PythonOperator(
        task_id="extract_task",
        python_callable=extract
        )

    transform_data_task = PythonOperator(
        task_id="transform_data_task",
        python_callable=transform_data
        )

    load_task = PythonOperator(
        task_id="load_task",
        python_callable=load
        )

    extract_task >> transform_data_task >> load_task
