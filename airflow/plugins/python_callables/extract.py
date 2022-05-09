import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pathlib import Path

logging.basicConfig(level = logging.INFO,
                    format = " %(asctime)s - %(name)s - %(message)s",
                    datefmt='%Y-%m-%d',
                    encoding= "utf-8")

logger = logging.getLogger('')


sql_folder = Path(__file__).resolve().parent.parent.parent
sql_path = f'{sql_folder}/include/'

def extract(file):
    """
    Funcion responsable de descargar datos y almacenarlos
    dentro de la carpeta files
    """
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
    