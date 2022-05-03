#Implementar SQL Operator para la Universidad J.F Kennedy

import os
import logging
import csv
from pathlib import Path
from airflow import DAG   
from datetime import timedelta, datetime 
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


logging.basicConfig(
    format='%(asctime)s - %(name)s - %(message)s',
    datefmt='%Y-%m-%d',
    level=logging.DEBUG,
    )
logger = logging.getLogger(__name__)

def logging_dags():
    logger.info('Initializing DAG')


#Obtain path (Current file location)
parent_path = (Path(__file__).parent.absolute()).parent

def extract_data_sql():
    file_university = 'Universidad_J_F_Kennedy'
    csv_file = f"{parent_path}/files/{file_university}.csv"

    #Open sql request
    with open(f"{parent_path}/include/SQL-{file_university}.sql", 'r', encoding='utf-8') as f:
        request = f.read()
        f.close()
    
    #Connect with the database
    pg_hook = PostgresHook(postgres_conn_id='db_alkemy_universidades', schema= 'training')
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(request)
    sources = cursor.fetchall()

    #Create folder FILES
    os.makedirs(f"{parent_path}/files", exist_ok=True)

    #Create csv file
    with open(csv_file, mode="a") as file:
        writer = csv.writer(file, delimiter=",")
        writer.writerow(['university', 'career', 'inscription_date', 'last_name', 'gender', 'age', 'postal_code', 'email'])
        for source in sources:
            writer.writerow([source[0], source[1], source[2], source[3], source[4], source[5], source[6], source[7]])



#Default settings applied to all tasks
default_args = {
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(   
    'DAG_Universidad_J_F_Kennedy',   
    description= 'Procesa la información de la Universidad J.F Kennedy',
    default_args = default_args,
    template_searchpath=f'{parent_path}/airflow/include',
    schedule_interval= timedelta(hours=1),  
    start_date = datetime(2022, 4, 24)  
) as dag:
    #Taks to be executed within the DAG

    #Initializing dags
    logging_dag = PythonOperator(task_id='logging_dag', python_callable=logging_dags)

    #Extract data SQL query to Postgres database
    #It is proposed to use PostgresOperator

    extract_data = PythonOperator(task_id='extract_data',
                                  python_callable=extract_data_sql)

    #Process data with pandas
    #It is proposed to use PythonOperator
    process_data= DummyOperator(task_id='process_data')


    #Load data to S3
    #It is proposed to use PythonOperator
    load_data= DummyOperator(task_id='load_data')

    #Task order
    logging_dag >> extract_data >> process_data >> load_data