from airflow import DAG
from datetime import datetime, timedelta
from pathlib import Path
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from plugin.university_group_G import DataProcessor


#Obtain path (Current file location)
parent_path = (Path(__file__).parent.absolute()).parent

file_university = 'Universidad_J_F_Kennedy'
function_dag = DataProcessor(file_university)


#Default settings applied to all tasks
default_args = {
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}


with DAG(dag_id='ETL_Universidad_J_F_Kennedy',
    description='DAG Universidad_J_F_Kennedy',
    start_date = datetime(2022, 5, 9),  
    schedule_interval= timedelta(hours=1),
    catchup=False ) as dag:

        extract_data = PythonOperator(task_id='extract_data',
                                  python_callable= function_dag.extract_data_sql)

        process_data = PythonOperator(task_id='process_data', 
                                python_callable= function_dag.transform_data)


        load_data = LocalFilesystemToS3Operator(
            task_id='load_data',
            filename=f'{parent_path}/datasets/{file_university}',
            dest_key='aws_s3_alkemy_universidades',
            dest_bucket='cohorte-abril-98a56bb4',
            replace=True,
        )

        extract_data >> process_data >> load_data