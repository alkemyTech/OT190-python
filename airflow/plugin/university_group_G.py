import os
import logging
import csv
import pathlib
from helpers.transform_data_university import transform_datas
from airflow.hooks.postgres_hook import PostgresHook

class DataProcessor():
    
    def __init__(self, file_university):
        self.file_university = file_university
        
        #Obtain path (Current file location)
        self.parent_path = (pathlib.Path(__file__).parent.absolute()).parent
        
        logging.basicConfig(
        format='%(asctime)s - %(name)s - %(message)s',
        datefmt='%Y-%m-%d',
        level=logging.DEBUG,)
        logger = logging.getLogger(__name__)

        logger.info('Initializing DAG')

   
    def extract_data_sql(self):

        csv_file = f"{self.parent_path}/files/{self.file_university}.csv"

        #Open sql request
        with open(f"{self.parent_path}/include/SQL-{self.file_university}.sql", 'r', encoding='utf-8') as f:
            request = f.read()
            f.close()
        
        #Connect with the database
        pg_hook = PostgresHook(postgres_conn_id='db_alkemy_universidades', schema= 'training')
        connection = pg_hook.get_conn()
        cursor = connection.cursor()
        cursor.execute(request)
        sources = cursor.fetchall()

        #Create folder FILES
        os.makedirs(f"{self.parent_path}/files", exist_ok=True)
        columns = [i[0] for i in cursor.description]

        #Create csv file
        with open(csv_file, mode="w") as file:
            writer = csv.writer(file, delimiter=",")
            writer.writerow(columns)
            for source in sources:
                writer.writerow([source[0], source[1], source[2], source[3], source[4], source[5], source[6], source[7]])




    def transform_data(self):
        csv_file = f"{self.parent_path}/files/{self.file_university}.csv"
        txt_file = f"{self.parent_path}/datasets/{self.file_university}.txt"
        assets= f'{self.parent_path}/assets/codigos_postales.csv'

        #Create folder FILES
        os.makedirs(f"{self.parent_path}/datasets", exist_ok=True)
        
        transform_datas(self.file_university, csv_file, txt_file, assets)

    