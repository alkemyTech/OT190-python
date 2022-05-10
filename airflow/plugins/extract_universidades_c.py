"""
Archivo de extracción para las universidades:
- Universidad de Palermo
- Universidad Nacional de Jujuy
"""
import logging
import os
from airflow.hooks.postgres_hook import PostgresHook

# Configuracion logging
# Formato: %Y-%m-%d - nombre_logger - mensaje
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(message)s",
    level=logging.DEBUG,
    datefmt="%Y-%m-%d",
)
# Log con el nombre de archivo en que se encuentra
log = logging.getLogger(__name__)


def query_to_csv(local_path, sql_file, filename):
    """
    Ejecuta la query descripta en sql_file y guarda el resultado con el nombre de archivo filename
    como .csv en la carpeta files
    """
    files_path = os.path.join(local_path, "files")
    try:
        os.stat(files_path)
    except:
        log.debug("Creando directorio files")
        os.mkdir(files_path)

    log.debug("Conectando con la base de datos")

    pg_hook = PostgresHook(
        postgres_conn_id="db_alkemy_universidades", schema="training"
    )

    try:
        university_sql = open(f"{local_path}/include/{sql_file}", "r").read()
    except Exception as e:
        log.error(f"Error: {e}")

    log.debug("Ejecutando consulta sql y obteniendo resultado")
    pandas_df = pg_hook.get_pandas_df(sql=university_sql)

    log.debug(f"Guardando resultado como {filename}")
    pandas_df.to_csv(f"{files_path}/{filename}")
