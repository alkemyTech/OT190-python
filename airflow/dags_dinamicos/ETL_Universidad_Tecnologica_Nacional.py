from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from etl_universidades_d import DataProcessor

# from plugins import claseETL

univ = DataProcessor("Universidad_Tecnologica_Nacional")


default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(seconds=30),
}


with DAG(dag_id='ETL_Universidad_Tecnologica_Nacional',
    description='DAG ETL Universidad_Tecnologica_Nacional',
    start_date=datetime(2022,5,6),
    schedule_interval= '@daily',
    catchup=False ) as dag:

        extract = PythonOperator(
            task_id="extract",
            python_callable=univ.extract_data
        )

        transform = PythonOperator(
            task_id="transform",
            python_callable=univ.transform_data
        )

        load = PythonOperator(
            task_id="load",
            python_callable=univ.load_to_s3,
            op_kwargs={
            'bucket_name': 'cohorte-abril-98a56bb4'
            }
        )

        extract >> transform >> load