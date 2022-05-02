# Built in modules
from datetime import timedelta, datetime
from pathlib import Path
# Logging
import logging
# Airflow modules
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


sql_folder = Path(__file__).resolve().parent.parent
sql_path = f'{sql_folder}/include/'

logging.basicConfig(level = logging.INFO,
                    format = " %(asctime)s - %(name)s - %(message)s",
                    datefmt='%Y-%m-%d',
                    encoding= "utf-8")

logger = logging.getLogger('DAG - Universidad del Cine')


def extract():
<<<<<<< HEAD
=======
    """
    Funcion responsable de descargar datos y almacenarlos
    dentro de la carpeta files
    """
>>>>>>> 7cf399a0589b2c99e0f32bb84788f46f5fdc9519
    file = 'Universidad_del_Cine'
    # Leyendo script.sql
    logger.info(f'Leyendo SQL_{file}.sql')
    with open(f'{sql_path}/SQL_{file}.sql', 'r', encoding='utf-8') as f:
        query = f.read()
        f.close()

    pg_hook = PostgresHook(
        postgres_conn_id='db_alkemy_universidades'
        )

    logger.info(f'Ejecutando consulta SQL_{file}.sql')
    pandas_df = pg_hook.get_pandas_df(query)

    # Guardando archivo en la carpeta files
    logger.info(f'Guardando datos en {file}.csv')
    
    csv_path= f'{sql_folder}/files/{file}.csv'
    pandas_df.to_csv(csv_path, sep=',', index=False)

    logger.info('Extracción Finalizada con éxito')


def transform_data():
    """
    Funcion responsable de leer los datos,
    procesarlos y guardarlos en la carpeta dataframe
    """
    file = "Universidad_del_Cine"
    logger.info('Transformando datos')
    logger.info('Datos guardados')
    pass


def load():
    logger.info('Load data')
    pass


default_args = {
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}


with DAG(
    "DAG_Universidad_del_Cine",
    default_args=default_args,
    description="DAG sin procesamiento para la Universidad del Cine",
    template_searchpath=sql_path,
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