from pathlib import Path

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
# From plugins: 
from python_callables.extract import extract
from python_callables.transform import transform
from python_callables.load import load

path_p = Path(__file__).resolve().parent


default_args = {
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id="ETL_Universidad_del_Cine",
    default_args=default_args,
    description="DAG sin procesamiento para la Universidad de Buenos Aires",
    schedule_interval= '@hourly',
    start_date=datetime(2022, 4, 22)
) as dag:
    extract_task = PythonOperator(
        task_id='extract_task',
        python_callable=extract,
        op_kwargs={'file':'Universidad_del_Cine'}
    )

    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=transform,
        op_kwargs={'file':f'universidad_del_cine'}
    )

    load_task = PythonOperator(
        task_id='load_task',
        python_callable=load,
        op_kwargs={
            'file_name':f'{path_p}/universidad_del_cine.txt',
            'key': 'posts.json',
            'bucket_name': 'cohorte-abril-98a56bb4',
            'replace': True
            }
    )
    
    extract_task >> transform_task >> load_task