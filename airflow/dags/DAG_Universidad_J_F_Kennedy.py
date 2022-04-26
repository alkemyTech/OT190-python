#Configuración de DAG para procese la Universidad J.F Kennedy

from airflow import DAG   
from datetime import timedelta, datetime 
from airflow.operators.dummy import DummyOperator


with DAG(   
    'DAG_Universidad_J_F_Kennedy',   
    description= 'Procesa la información de la Universidad J.F Kennedy',
    schedule_interval= timedelta(hours=1),  
    start_date = datetime(2022, 4, 24)  
) as dag:
    #Taks to be executed within the DAG

    #Extract data SQL query to Postgres database
    #It is proposed to use PostgresOperator

    extract_data = DummyOperator(task_id='extract_data')


    #Process data with pandas
    #It is proposed to use PythonOperator
    process_data= DummyOperator(task_id='process_data')


    #Load data to S3
    #It is proposed to use PythonOperator
    load_data= DummyOperator(task_id='load_data')

    #Task order
    extract_data >> process_data >> load_data