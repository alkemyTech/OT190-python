'''
DAG Implementación de SQL Operator para el grupo de universidades B, Universidad_Nacional_Del_Comahue
Sprint 3, OT190-61


DO:
1.- Configurar un Python Operators, para que extraiga información de la base de datos utilizando el .sql disponible en el repositorio base de las siguientes universidades:
Universidad Del Salvador
Dejar la información en un archivo .csv dentro de la carpeta files.
2.- Procesador los datos

3.- Datos Finales:
university: str minúsculas, sin espacios extras, ni guiones
career: str minúsculas, sin espacios extras, ni guiones
inscription_date: str %Y-%m-%d format
first_name: str minúscula y sin espacios, ni guiones
last_name: str minúscula y sin espacios, ni guiones
gender: str choice(male, female)
age: int
postal_code: str
location: str minúscula sin espacios extras, ni guiones
email: str minúsculas, sin espacios extras, ni guiones
Aclaraciones:
Para calcular codigo postal o locación se va a utilizar el .csv que se encuentra en el repo.
La edad se debe calcular en todos los casos

'''

import os
import logging
import csv
from pathlib import Path
from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd
from dateutil.relativedelta import relativedelta
from dateutil import parser
import numpy as np
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(message)s',
    datefmt='%Y-%m-%d',
    level=logging.DEBUG,
    )
logger = logging.getLogger(__name__)

def logging_dags(**kwargs):
    name_University = kwargs['name_University']
    logger.info('Iniciando DAG ' + name_University)


#Obtain path (Current file np)
parent_path = (Path(__file__).parent.absolute()).parent

def extract_data_sql(**kwargs):
    file_university = kwargs['name_University']
    csv_file = f"{parent_path}/files/{file_university}.csv"



    #Read a SQL query from a sql file
    with open(f"{parent_path}/include/SQL_{file_university}.sql", 'r', encoding='utf-8') as f:
        SQL_Universidad = f.read() #Save the sql query
        f.close()
    #print(SQL_Universidad)

    #Conecction with the database
    pg_hook = PostgresHook(  postgres_conn_id="db_alkemy_universidades", schema="training"     )

    # Create a folder if don't exist
    os.makedirs(f"{parent_path}/files", exist_ok=True)

    # save as CVS file
    pandas_df = pg_hook.get_pandas_df(sql=SQL_Universidad) #Capture value from a query and with panda sent to a pandas dataFrame (panda_df)
    pandas_df.to_csv(f"{csv_file}") # Whit panda data framen sent directly to a cvs file
    logger.info(f"Se creo archivo {file_university} CVS")


def fechas(x):
    try:
        return datetime.strptime(str(x[:7]+str(int(x[7:])+2000 if (int(x[7:]) > 1 and int(x[7:]) <23 )  else int(x[7:])+1900)),'%d-%b-%Y')
    except:
        return datetime.strptime(x,'%Y-%m-%d')


def transform_data(**kwargs):


    file_university = kwargs['name_University']
    #Read the CVS as dataFrame
    csv_file = f'{parent_path}/files/{file_university}.csv'
    columns=['university', 'career','first_name','location','email']

    age=[]

    print(csv_file)
    df = pd.read_csv(f'{csv_file}')

    #Transform data in lowercase and changue score by spaces
    for column in df:
        if column in columns:
            try:
                df[f'{column}'] = df[f'{column}'].str.lower()
                df[f'{column}'] = df[f'{column}'].apply(lambda x: x.replace('_', ' '))
            except:
                pass

    #Transform column gender if m = male if f=female
    df['gender'] = df.gender.replace({'F': 'female', 'M': 'male'})

    #Convierte el year de 2 digitos a 4
    df['age'] = df['age'].apply(fechas)
    df['inscription_date'] = df['inscription_date'].apply(fechas)

    #Obtiene la edad al momento de la inscripcion
    for i in range(df.shape[0]):
        age.append(int(relativedelta(df.loc[i]['inscription_date'], df.loc[i]['age']).years))
    df['age'] = age

    #Obtiene el CP
    CP = pd.read_csv(f'{parent_path}/assets/codigos_postales.csv')
    CP['localidad'] = CP['localidad'].str.lower()
    if (str(df['location'].loc[0]).isdigit() ):
        key,val='codigo_postal','localidad'
    else:
        key,val='localidad','codigo_postal'

    dict_cp = dict(zip(CP[key], CP[val]))
    df['location'] = df['location'].apply(lambda x: dict_cp[x])

    # Create a folder if don't exist
    os.makedirs(f"{parent_path}/datasets", exist_ok=True)

    df.to_csv(f"{parent_path}/datasets/{file_university}.txt", sep="\t")



'''
university: str minúsculas, sin espacios extras, ni guiones
career: str minúsculas, sin espacios extras, ni guiones
inscription_date: str %Y-%m-%d format
first_name: str minúscula y sin espacios, ni guiones
last_name: str minúscula y sin espacios, ni guiones
    gender: str choice(male, female)
    age: int
    postal_code: str
location: str minúscula sin espacios extras, ni guiones
email: str minúsculas, sin espacios extras, ni guiones'''


#Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

#Nombre de la universidad
name_University= 'Universidad_Nacional_Del_Comahue'


with DAG(
    dag_id=f'DAG_{name_University}',
    description= f'Procesa la información de  la{name_University}.sql',
    default_args = default_args,
    template_searchpath=f'{parent_path}/airflow/include',
    schedule_interval= timedelta(hours=1),
    start_date = datetime(2022, 4, 24),
    
) as dag:
    #Taks to be executed within the DAG

    #Initializing dags
    logging_dag = PythonOperator(
        task_id='logging_dag',
        python_callable=logging_dags,
        op_kwargs={'name_University': name_University}
        )

    #Get the  data from a db in Postgres
    extract_data = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data_sql,
        op_kwargs={'name_University': name_University},
        do_xcom_push=True
        )


    #Process data with pandas
    process_data= PythonOperator(
        task_id='process_data',
        python_callable=transform_data,
        op_kwargs={'name_University': name_University}
        )



    #Load data to S3
    # [START howto_transfer_local_to_s3]
    load_data= LocalFilesystemToS3Operator(
       task_id='load_data',
       filename=f'{parent_path}/datasets/{name_University}.txt',
       dest_key='aws_s3_alkemy_universidades',
       dest_bucket='cohorte-abril-98a56bb4',
       replace=True,
       )

    # [END howto_transfer_local_to_s3]


    #Task order
    logging_dag >> extract_data >> process_data
