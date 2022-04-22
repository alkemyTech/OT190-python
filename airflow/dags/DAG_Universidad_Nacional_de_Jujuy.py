from datetime import datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator

'''
Configuración del DAG sin consultas ni procesamiento para la Universidad Nacional de Jujuy
'''

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(
    'DAG_Universidad_Nacional_de_Jujuy',
    description='DAG sin consultas ni procesamiento para la Universidad Nacional de Jujuy',
    schedule_interval='@hourly', #Que se ejecute cada hora
    start_date=datetime.now() 
) as dag:
    # Solo declaro las tareas de extraer datos, transformarlos y subirlos
    extract_task = DummyOperator( 
        task_id = 'extract_task', 
        dag = dag)

    transform_task = DummyOperator(
        task_id = 'transform_task',
        dag = dag)
    
    load_task = DummyOperator(
        task_id = 'load_task',
        dag = dag)

    # Describo el orden de ejecución en el DAG
    extract_task >> transform_task >> load_task
