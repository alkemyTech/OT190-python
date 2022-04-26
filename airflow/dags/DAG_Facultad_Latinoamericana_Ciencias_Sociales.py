#Configuración de los retries para la Facultad Latinoamericana de Ciencias Sociales

from airflow import DAG   
from datetime import timedelta, datetime 
from airflow.operators.dummy import DummyOperator


#Default settings applied to all tasks
default_args = {
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(   
    'DAG_Facultad_Latinoamericana_Ciencias_Sociales',   
    description= 'Procesa la información de la Facultad Latinoamericana de Ciencias Sociales',
    default_args = default_args,
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

    load_data= DummyOperator(task_id='load_data')

    #Task order
    extract_data >> process_data >> load_data