from datetime import datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
import logging

"""
Configuración del DAG sin consultas ni procesamiento
para la Universidad Nacional de Jujuy
"""

# Configuracion logging
# Formato: %Y-%m-%d - nombre_logger - mensaje
def logging_config():
    logging.basicConfig(format='%(asctime)s - %(name)s - %(message)s',
                        level=logging.DEBUG,
                        datefmt='%Y-%m-%d')
    log = logging.getLogger(__name__)
    log.info('Inicializando DAG')


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    "DAG_Universidad_Nacional_de_Jujuy",
    description="DAG para la Universidad Nacional de Jujuy",
    schedule_interval="@hourly",  # Que se ejecute cada hora
    start_date=datetime(2022, 4, 22)
) as dag:
    logging_task= PythonOperator(task_id= "logging", python_callable=logging_config)
    
    # Solo declaro las tareas de extraer datos, transformarlos y subirlos
    extract_task = DummyOperator(task_id="extract_task", dag=dag)

    transform_task = DummyOperator(task_id="transform_task", dag=dag)

    load_task = DummyOperator(task_id="load_task", dag=dag)

    # Describo el orden de ejecución en el DAG
    logging_task >> extract_task >> transform_task >> load_task
