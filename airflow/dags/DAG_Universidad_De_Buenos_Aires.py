# Built in modules
from datetime import timedelta, datetime
from pathlib import Path
# Logging
import logging
# Pandas
import pandas as pd
# Airflow modules
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.S3_hook import S3Hook


sql_folder = Path(__file__).resolve().parent.parent
sql_path = f'{sql_folder}/include/'

#Functions 
root_folder = Path(__file__).resolve().parent.parent


def transform_data_module(file:str):
    """
    Transforma Todas las columnas a lower con la funcion lowercase_df
    definida dentro de esta funcion.
    """
    def lowercase_df(df):
        """
        Transforma todas las columnas del dataframe (df), menos 'birth_date'
        y 'postal_code' a lower case
        """
        for column in df:
            if column != "birth_date" and column != "inscription_date" and column != "postal_code":
                df[f'{column}'] = df[f'{column}'].str.lower()
                df[f'{column}'] = df[f'{column}'].apply(lambda x: x.replace('-', ' '))
    
    files_path = f'{root_folder}/files'
    df = pd.read_csv(f'{files_path}/{file}.csv')
    
    lowercase_df(df)

    df['gender'] = df.gender.replace({'f': 'female', 'm': 'male'})

    return df


def calculate_age(df, file:str):
    """
    Crea una columna llamada 'age', realiza la diferencia entre las columnas 'inscription_date'
    y 'birth_date' para obtener la edad en días. Con la edad en días realiza una transformacion
    en años con la funcion calculate
    """
    def calculate(diff_days):
        """
        Obtiene diferencia de días, determina si son negativos o positivos.
        Si son negativos, aumenta 100 años y returna la division entre días y años (edad).
        Si son positivos solo retorna la division
        """
        days = diff_days.days
        if days < 0:
            days += int(100 * 365.2425)

        return int(days / 365.2425)
    if file == 'universidad_del_cine':
        df['inscription_date'] = pd.to_datetime(df.inscription_date, format='%d-%m-%Y')
        df['birth_date'] = pd.to_datetime(df.birth_date, format='%d-%m-%Y')
        df['age'] = df['inscription_date'] - df['birth_date']

    elif file == 'universidad_de_buenos_aires':
        df['inscription_date'] = pd.to_datetime(df.inscription_date)
        df['birth_date'] = pd.to_datetime(df.birth_date, format='%y-%b-%d')
        df['age'] = df['inscription_date'] - df['birth_date']

    df['age'] = df['age'].apply(calculate)
    return df


def postal_code_or_location(df, postal_code_or_location:str):
    """
    Recibe un dataframe y un string, el string define si se va a
    agregar location, o postal_code, solo acepta esos dos valores.

    Con un dataframe complementario llamado 'codigos_postales', define
    location a traves de la columna 'codigo_postal' o postal_code
    a través de la columna 'localidad'
    """
        # Leyendo Dataframe complementario
    complementary = pd.read_csv(f'{root_folder}/assets/codigos_postales.csv')

    if postal_code_or_location == "location":
        # Obteniendo localidad con codigo_postal
        complementary["localidad"] = complementary["localidad"].apply(lambda x: x.lower())
        dict_cp = dict(zip(complementary["codigo_postal"], complementary["localidad"]))
        df["location"] = df["postal_code"].apply(lambda x: dict_cp[x])

    elif postal_code_or_location == "codigo_postal":
        # Obteniendo codigo postal con localidad
        complementary["localidad"] = complementary["localidad"].apply(lambda x: x.lower())
        dict_cp = dict(zip(complementary["localidad"], complementary["codigo_postal"]))
        df["postal_code"] = df["location"].apply(lambda x: dict_cp[x])

    return df


def save_df_text(df, file_name:str):
    """
    Guarda el dataframe en un archivo con el mismo nombre
    del string pasado en {file_name}.txt en la carpeta dataset
    """
    with open(f'{root_folder}/datasets/{file_name}.txt', 'w+') as f:
        f.write(df.to_string())
        f.close()



logging.basicConfig(level = logging.INFO,
                    format = " %(asctime)s - %(name)s - %(message)s",
                    datefmt='%Y-%m-%d',
                    encoding= "utf-8")

logger = logging.getLogger('DAG - Universidad De Buenos Aires')


def extract():
    """
    Funcion responsable de descargar datos y almacenarlos
    dentro de la carpeta files
    """
    file = 'Universidad_de_Buenos_Aires'
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
    logger.info(f'Guardando datos en {file.lower()}.csv')
    
    csv_path= f'{sql_folder}/files/{file.lower()}.csv'
    pandas_df.to_csv(csv_path, sep=',', index=False)

    logger.info('Extracción Finalizada con éxito')


def transform_data():
    """
    Funcion responsable de leer los datos crudos,
    procesarlos y guardarlos en la carpeta dataframe
    """
    file = "universidad_de_buenos_aires"
    logger.info('Transformando datos')
    df = transform_data_module(file)
    logger.info('Calculando edad')
    df = calculate_age(df, file)
    logger.info('Creando columna de localidad')
    df = postal_code_or_location(df, 'location')
    logger.info(f'Guardando archivo en {file}.txt')
    df = save_df_text(df, file)
    logger.info('Datos guardados')


def load_to_s3(file_name:str, key: str, bucket_name: str):
    """
    Sube un archivo a s3
    """
    logger.info(f'Intentando Subir archivo {file_name}')
    hook = S3Hook('aws_s3_alkemy_universidades')
    logger.info('Subiendo archivo')
    hook.load_file(
        filename=file_name,
        key=key,
        bucket_name=bucket_name,
        replace=True
    )
    logger.info('Archivo subido con exito')


default_args = {
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}


with DAG(
    "DAG_Universidad_de_Buenos_Aires",
    default_args=default_args,
    description="DAG sin procesamiento para la Universidad de Buenos Aires",
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
        python_callable=load_to_s3,
        op_kwargs={
            'file_name': f'{root_folder}/datasets/universidad_de_buenos_aires.txt',
            'key': 'posts.json',
            'bucket_name': 'cohorte-abril-98a56bb4'
        }
    )

    extract_task >> transform_data_task >> load_task