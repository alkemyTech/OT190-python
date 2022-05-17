from airflow.hooks.postgres_hook import PostgresHook
from datetime import timedelta, datetime
import pandas as pd
from dateutil.relativedelta import relativedelta
from dateutil import parser
import numpy as np
import logging
import csv
from pathlib import Path
import os

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(message)s',
    datefmt='%Y-%m-%d',
    level=logging.DEBUG,
    )
logger = logging.getLogger(__name__)

parent_path = (Path(__file__).parent.absolute()).parent


def logging_dags(**kwargs):
    name_University = kwargs['name_University']
    logger.info('Iniciando DAG ' + name_University)

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

