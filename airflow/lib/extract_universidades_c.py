"""
Archivo de extracción para las universidades:
- Universidad de Palermo
- Universidad Nacional de Jujuy
Lee los parámetros de la función a partir del .yml
"""

import pathlib
import logging
import os
from airflow.hooks.postgres_hook import PostgresHook

# Busqueda del path donde se está ejecutando el archivo, subimos un nivel
# para situarnos en la carpeta airflow
path_p = (pathlib.Path(__file__).parent.absolute()).parent

# Configuracion logging
# Formato: %Y-%m-%d - nombre_logger - mensaje
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(message)s",
    level=logging.DEBUG,
    datefmt="%Y-%m-%d",
)
# Log con el nombre de archivo en que se encuentra
log = logging.getLogger(__name__)

# Path para descargar los archivos .csv
path_d = pathlib.Path.joinpath(path_p, "files")


def query_to_csv(sql_file, filename):
    """
    Ejecuta la query descripta en sql_file y guarda el resultado con el nombre de archivo filename
    como .csv en la carpeta files
    """
    try:
        os.stat(path_d)
    except:
        log.debug("Creando directorio files")
        os.mkdir(path_d)

    log.debug("Conectando con la base de datos")

    pg_hook = PostgresHook(
        postgres_conn_id="db_alkemy_universidades", schema="training"
    )

    try:
        university_sql = open(f"{path_p}/include/{sql_file}", "r").read()
    except Exception as e:
        log.error(f"Error: {e}")

    log.debug("Ejecutando consulta sql y obteniendo resultado")
    pandas_df = pg_hook.get_pandas_df(sql=university_sql)

    log.debug(f"Guardando resultado como {filename}")
    pandas_df.to_csv(f"{path_d}/{filename}")
