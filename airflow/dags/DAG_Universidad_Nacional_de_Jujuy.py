from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
import os
import logging
import pathlib
import pandas as pd
import numpy as np

# Busqueda del path donde se está ejecutando el archivo, subimos un nivel para
# situarnos en la carpeta airflow
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

    log.info("Conectando con la base de datos")

    pg_hook = PostgresHook(
        postgres_conn_id="db_alkemy_universidades", schema="training"
    )

    try:
        university_sql = open(f"{path_p}/include/{sql_file}", "r").read()
    except Exception as e:
        log.error(f"Error: {e}")

    log.info("Ejecutando consulta sql y obteniendo resultado")
    pandas_df = pg_hook.get_pandas_df(sql=university_sql)

    log.info(f"Guardando resultado como {filename}")
    pandas_df.to_csv(f"{path_d}/{filename}")


def normalize_strings(columna):
    """
    Dada una columna, normaliza los strings a
    minúsculas, sin espacios extras, ni guiones
    """
    try:
        columna = columna.apply(lambda x: str(x).strip())
        columna = columna.apply(lambda x: str(x).replace("-", " "))
        columna = columna.apply(lambda x: str(x).replace("_", " "))
        columna = columna.apply(lambda x: x.lower())
    except:
        log.error("Error al querer normalizar los caracteres")
    return columna


def normalize_data(csv_filename):
    """
    Normaliza los datos del .csv pasado por parámetro y los guarda en un .txt
    """

    log.info("Leyendo datos del csv")
    df_univ = pd.read_csv(f"{path_d}/{csv_filename}.csv")

    log.info("Normalizando datos")
    # university: str minúsculas, sin espacios extras, ni guiones
    df_univ["university"] = normalize_strings(df_univ["university"])

    # career: str minúsculas, sin espacios extras, ni guiones
    df_univ["career"] = normalize_strings(df_univ["career"])

    # inscription_date: str %Y-%m-%d format
    old_date = pd.to_datetime(df_univ["inscription_date"])
    df_univ["inscription_date"] = pd.to_datetime(old_date, "%Y-%m-%d")

    # first_name: str minúscula y sin espacios, ni guiones
    # last_name: str minúscula y sin espacios, ni guiones
    df_univ["names"] = normalize_strings(df_univ["nombre"])
    try:
        df_univ["first_name"] = df_univ["names"].apply(lambda x: str(x).split(" ")[0])
        df_univ["last_name"] = df_univ["names"].apply(lambda x: str(x).split(" ")[1])
    except:
        df_univ["first_name"] = "NULL"
        df_univ["last_name"] = df_univ["names"]

    # gender: str choice(male, female)
    df_univ["sexo"] = normalize_strings(df_univ["sexo"])
    dict_gender = {"f": "female", "m": "male"}
    df_univ["gender"] = df_univ["sexo"].map(dict_gender)

    # age: int
    df_univ["birth_date"] = pd.to_datetime(df_univ["birth_date"], format="%Y-%m-%d")
    df_univ["age"] = (
        (df_univ["inscription_date"] - df_univ["birth_date"]) / np.timedelta64(1, "Y")
    ).astype(int)

    # location: str minúscula sin espacios extras, ni guiones
    df_univ["location"] = normalize_strings(df_univ["location"])

    # postal_code: str
    df_cp = pd.read_csv(f"{path_p}/assets/codigos_postales.csv")
    df_cp["localidad"] = df_cp["localidad"].apply(lambda x: x.lower())
    dict_cp = dict(zip(df_cp["localidad"], df_cp["codigo_postal"]))
    df_univ["postal_code"] = df_univ["location"].apply(lambda x: dict_cp[x])

    # email: str minúsculas, sin espacios extras, ni guiones
    df_univ["email"] = normalize_strings(df_univ["email"])

    # Guardando información necesaria en un .txt
    df_univ = df_univ[
        [
            "university",
            "career",
            "inscription_date",
            "first_name",
            "last_name",
            "gender",
            "age",
            "postal_code",
            "location",
            "email",
        ]
    ]
    log.info(f"Guardando archivo transformado en: datasets/{csv_filename}.txt")
    df_univ.to_csv(f"{path_p}/datasets/{csv_filename}.txt", sep="\t")


def upload_to_S3(filename, key, bucketname):
    """
    Sube el archivo a S3
    """
    log.info(f"Intentando subir archivo {filename} a S3")
    try:
        hook = S3Hook("s3_conn")
        hook.load_file(filename=filename, key=key, bucket_name=bucketname, replace=True)
    except Exception as e:
        log.error(f"No se pudo subir el archivo a S3: {e}")
    log.info(f"Archivo subido a S3")


default_args = {"owner": "airflow", "retries": 5, "retry_delay": timedelta(seconds=30)}

with DAG(
    "DAG_Universidad_Nacional_de_Jujuy",
    description="DAG para la Universidad Nacional de Jujuy",
    default_args=default_args,
    schedule_interval="@hourly",  # Que se ejecute cada hora
    start_date=datetime(2022, 4, 22),
    template_searchpath=f"{path_p}/include",
    catchup=False,
) as dag:

    # Declaro las tareas de extraer datos, transformarlos y subirlos
    extract_task = PythonOperator(
        task_id="extract_task",
        python_callable=query_to_csv,
        op_kwargs={
            "sql_file": "SQL_Universidad_Nacional_de_Jujuy.sql",
            "filename": "universidad_nacional_de_jujuy.csv",
        },
        dag=dag,
    )

    transform_task = PythonOperator(
        task_id="transform_task",
        python_callable=normalize_data,
        dag=dag,
        op_kwargs={"csv_filename": "universidad_nacional_de_jujuy"},
    )

    load_task = PythonOperator(
        task_id="load_task",
        python_callable=upload_to_S3,
        op_kwargs={
            "filename": os.path.join(
                path_p, "datasets/universidad_nacional_de_jujuy.txt"
            ),
            "key": "universidad_nacional_de_jujuy.txt",
            "bucketname": "cohorte-abril-98a56bb4",
        },
        dag=dag,
    )

    # Describo el orden de ejecución en el DAG
    extract_task >> transform_task >> load_task
