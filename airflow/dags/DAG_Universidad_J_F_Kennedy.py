#Crear una función Python con Pandas para la Universidad J.F Kennedy

import os
import logging
import csv
from pathlib import Path
from airflow import DAG   
import pandas as pd
from dateutil.relativedelta import relativedelta
from dateutil import parser
from datetime import timedelta, datetime 
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


logging.basicConfig(
    format='%(asctime)s - %(name)s - %(message)s',
    datefmt='%Y-%m-%d',
    level=logging.DEBUG,
    )
logger = logging.getLogger(__name__)

def logging_dags():
    logger.info('Initializing DAG')


#Obtain path (Current file location)
parent_path = (Path(__file__).parent.absolute()).parent
file_university = 'Universidad_J_F_Kennedy'

def extract_data_sql(file_university):   
    csv_file = f"{parent_path}/files/{file_university}.csv"
    #Open sql request
    with open(f"{parent_path}/include/SQL-{file_university}.sql", 'r', encoding='utf-8') as f:
        request = f.read()
        f.close()
    
    #Connect with the database
    pg_hook = PostgresHook(postgres_conn_id='db_alkemy_universidades', schema= 'training')
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(request)
    sources = cursor.fetchall()

    #Create folder FILES
    os.makedirs(f"{parent_path}/files", exist_ok=True)

    #Create csv file
    with open(csv_file, mode="w") as file:
        writer = csv.writer(file, delimiter=",")
        writer.writerow(['university', 'career', 'inscription_date', 'last_name', 'gender', 'age', 'postal_code', 'email'])
        for source in sources:
            writer.writerow([source[0], source[1], source[2], source[3], source[4], source[5], source[6], source[7]])

def transform_data(file_university):

    csv_file = f"{parent_path}/files/{file_university}.csv"
    txt_file = f"{parent_path}/datasets/{file_university}.txt"

    months = {'Jan':'01', 'Feb':'02', 'Mar':'03', 'Apr':'04','May':'05','Jun':'06','Jul':'07','Aug':'08','Sep':'09','Oct':'10','Nov':'11','Dec':'12'}
    abreviations = {'mr.': '','dr.': '','mrs.': '','ms.': '','md': '','dds': '','jr.': '','dvm': '','phd': ''}
    dates = []
    birth = []
    #Process data obtained from SQL query
    pandas_data = pd.read_csv(csv_file, index_col=False)
    pandas_data['university'] = (pandas_data['university'].str.replace("-", " ")).str.lower()
    pandas_data['career'] = (pandas_data['career'].str.replace("-", " ")).str.lower()
    pandas_data['last_name'] = (pandas_data['last_name'].str.replace("-", " ")).str.lower()
    pandas_data['email'] = (pandas_data['email'].str.replace("-", " ")).str.lower()
    pandas_data['postal_code'] = pandas_data['postal_code'].apply(lambda x: str(x)).str.replace("-", " ")
    
    for i in range(pandas_data.shape[0]):
        dates.append(parser.parse(pandas_data.loc[i]['inscription_date']).date())
    
    pandas_data['inscription_date'] = pd.to_datetime(dates, format='%Y-%m-%d')
    
    # Delete abreviations
    for abreviation, blank in abreviations.items():
        pandas_data['last_name'] = pandas_data['last_name'].apply(lambda x: x.replace(abreviation, blank))
    
    #Columns are now without trailing and leading spaces
    pandas_data.columns = pandas_data.columns.str.strip()
    
    pandas_data["first_name"] = pandas_data["last_name"].str.split(" ", expand=True)[0]
    pandas_data["last_name"] = pandas_data["last_name"].str.split(" ", expand=True)[1]
    pandas_data['gender'] = (pandas_data['gender'].str.replace("m", "male")).str.replace("f","female")

    #Calculate age
    for i in range(pandas_data.shape[0]):
        year = int(pandas_data.loc[i]['age'][:2])+1900 if int(pandas_data.loc[i]['age'][:2]) > 10 else int(pandas_data.loc[i]['age'][:2])+2000
        birth.append(str(year)+"-"+months[pandas_data.loc[i]['age'][3:6]]+"-"+pandas_data.loc[i]['age'][7:])

    pandas_data['age'] = birth
    pandas_data['age'] = pd.to_datetime(pandas_data['age'], format='%Y/%m/%d')
    pandas_data['age'] = pandas_data['age'].apply(lambda x: relativedelta(datetime.now(),x).years)

    #Generate column location
    postal_location = pd.read_csv(f"{parent_path}/assets/codigos_postales.csv")
    postal_location.columns = ['postal_code', 'location']
    postal_location['postal_code'] = postal_location['postal_code'].apply(lambda x: str(x))
    pandas_data =  pd.merge(left=pandas_data,right=postal_location, how='left', left_on='postal_code', right_on='postal_code')
    pandas_data['location'] = (pandas_data['location'].str.replace("-", " ")).str.lower()
   
    #Create folder FILES
    os.makedirs(f"{parent_path}/datasets", exist_ok=True)

    #Order Columns
    pandas_data = pandas_data[['university', 'career', 'inscription_date','first_name', 'last_name', 'gender', 'age', 'postal_code', 'location', 'email']]
    
    #Create txt file
    pandas_data.to_csv(txt_file, index=None, sep=' ', mode='a')


#Default settings applied to all tasks
default_args = {
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(   
    'DAG_Universidad_J_F_Kennedy',   
    description= 'Procesa la información de la Universidad J.F Kennedy',
    default_args = default_args,
    template_searchpath=f'{parent_path}/airflow/include',
    schedule_interval= timedelta(hours=1),  
    start_date = datetime(2022, 4, 24)  
) as dag:
    #Taks to be executed within the DAG

    #Initializing dags
    logging_dag = PythonOperator(task_id='logging_dag', python_callable=logging_dags)

    #Extract data SQL query to Postgres database
    extract_data = PythonOperator(task_id='extract_data',
                                  python_callable=extract_data_sql,
                                  op_kwargs={"file_university": file_university})

    #Process data with pandas
    process_data = PythonOperator(task_id='process_data', 
                                python_callable=transform_data,
                                op_kwargs={"file_university": file_university}
                                )


    #Load data to S3
    #It is proposed to use PythonOperator
    load_data= DummyOperator(task_id='load_data')

    #Task order
    logging_dag >> extract_data >> process_data >> load_data