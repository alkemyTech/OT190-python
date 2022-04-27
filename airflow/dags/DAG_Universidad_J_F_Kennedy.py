#Configurar los logs para la Universidad J.F Kennedy
import logging
from airflow import DAG   
from datetime import timedelta, datetime 
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(message)s',
    datefmt='%Y-%m-%d',
    level=logging.DEBUG,
    )
logger = logging.getLogger(__name__)

def logging_dags():
    logger.info('Initializing DAG')


#Default settings applied to all tasks
default_args = {
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(   
    'DAG_Universidad_J_F_Kennedy',   
    description= 'Procesa la información de la Universidad J.F Kennedy',
    default_args = default_args,
    schedule_interval= timedelta(hours=1),  
    start_date = datetime(2022, 4, 24)  
) as dag:
    #Taks to be executed within the DAG

    #Initializing dags
    logging_dag = PythonOperator(task_id='logging_dag', python_callable=logging_dags)

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
    logging_dag >> extract_data >> process_data >> load_data