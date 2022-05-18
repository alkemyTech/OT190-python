import os
import sys
from pathlib import Path
from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator


#Obtain path (Current file)
parent_path = (Path(__file__).parent.absolute()).parent
sys.path.append(f"{parent_path}/plugins")
from etl_universidades_gpo_b import logging_dags, extract_data_sql,transform_data


#Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

#Nombre de la universidad
name_University="Universidad_Del_Salvador"


with DAG(
     dag_id=f'DAG_{name_University}',
     description= f'Procesa la información de  la{name_University}.sql',
     default_args = default_args,
     template_searchpath=f'{parent_path}/airflow/include',
     schedule_interval= timedelta(hours=1),
     start_date = datetime(2022, 4, 24),
     catchup= False
     ) as dag:

#Initializing dags
    logging_dag = PythonOperator(
        task_id='logging_dag',
        python_callable=logging_dags,
        op_kwargs={'name_University': name_University}
        )

    #Get the  data from a db in Postgres
    extract_data = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data_sql,
        op_kwargs={'name_University': name_University},
        do_xcom_push=True
        )


    #Process data with pandas
    process_data= PythonOperator(
        task_id='process_data',
        python_callable=transform_data,
        op_kwargs={'name_University': name_University}
        )



    #Load data to S3
    # [START howto_transfer_local_to_s3]
    load_data= LocalFilesystemToS3Operator(
       task_id='load_data',
       filename=f'{parent_path}/datasets/{name_University}.txt',
       aws_conn_id='aws_s3_alkemy_universidades',
       dest_key=f'{name_University}.txt',
       dest_bucket='cohorte-abril-98a56bb4',
       replace=True,
       )

    # [END howto_transfer_local_to_s3]


    #Task order
    logging_dag >> extract_data >> process_data