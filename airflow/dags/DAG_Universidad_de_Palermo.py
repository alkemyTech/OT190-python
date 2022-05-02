from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import logging
import pathlib
import os
import pandas as pd

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
    df_univ["university"] = normalize_strings(df_univ["universidad"])

    # career: str minúsculas, sin espacios extras, ni guiones
    df_univ["career"] = normalize_strings(df_univ["careers"])

    # inscription_date: str %Y-%m-%d format
    old_date = pd.to_datetime(df_univ["fecha_de_inscripcion"])
    df_univ["inscription_date"] = pd.to_datetime(old_date, "%Y-%m-%d")

    # first_name: str minúscula y sin espacios, ni guiones
    # last_name: str minúscula y sin espacios, ni guiones
    df_univ["names"] = normalize_strings(df_univ["names"])
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
    today = datetime.now()
    df_univ["age"] = df_univ["birth_dates"].apply(
        lambda x: (100 + (int(str(today.year)[2:4]) - int(x[7:9])))
    )

    # postal_code: str
    df_univ["postal_code"] = df_univ["codigo_postal"].astype(str)

    # location: str minúscula sin espacios extras, ni guiones
    df_cp = pd.read_csv(f"{path_p}/assets/codigos_postales.csv")
    df_cp["localidad"] = df_cp["localidad"].apply(lambda x: x.lower())
    dict_cp = dict(zip(df_cp["codigo_postal"], df_cp["localidad"]))
    df_univ["location"] = df_univ["postal_code"].apply(lambda x: dict_cp[int(x)])

    # email: str minúsculas, sin espacios extras, ni guiones
    df_univ["email"] = normalize_strings(df_univ["correos_electronicos"])

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
    return df_univ


default_args = {"owner": "airflow", "retries": 5, "retry_delay": timedelta(seconds=30)}

with DAG(
    "DAG_Universidad_de_Palermo",
    description="DAG para la Universidad de Palermo",
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
            "sql_file": "SQL_Universidad_de_Palermo.sql",
            "filename": "universidad_de_palermo.csv",
        },
        dag=dag,
    )

    transform_task = PythonOperator(
        task_id="transform_task",
        python_callable=normalize_data,
        dag=dag,
        op_kwargs={"csv_filename": "universidad_de_palermo"},
    )

    load_task = DummyOperator(task_id="load_task", dag=dag)

    # Describo el orden de ejecución en el DAG
    extract_task >> transform_task >> load_task
