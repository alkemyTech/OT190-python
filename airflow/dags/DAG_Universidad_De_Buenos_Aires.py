# Built in modules
from datetime import timedelta, datetime
# Logging
import logging
# Airflow Modules
from airflow import DAG
from airflow.operators.python import PythonOperator


logging.basicConfig(level = logging.INFO,
                    format = " %(asctime)s - %(name)s - %(message)s",
                    datefmt='%Y-%m-%d',
                    encoding= "utf-8")

logger = logging.getLogger('DAG - Universidad Del Cine')

# dag sin consultas para universidad del cine
def extract():
    logger.info('Extract data')
    pass


def transform_data():
    logger.info('Extract data')
    pass


def load():
    logger.info('Extract data')
    pass


default_args = {
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}


with DAG(
    "DAG_Universidad_Del_Cine",
    default_args=default_args,
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
