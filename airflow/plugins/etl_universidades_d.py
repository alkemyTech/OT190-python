import os
import logging
import csv
import pathlib
from helpers.data_transformer import DataTransformer
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook

class DataProcessor():
    
    def __init__(self, file_name_):
        self.file_name = file_name_
        
        # cd parent path
        self.path_p = (pathlib.Path(__file__).parent.absolute()).parent
        
        # logs config
        logging.basicConfig(
            format='%(asctime)s-%(levelname)s-%(message)s',
            datefmt='%Y-%m-%d')

        self.logger = logging.getLogger(__name__)
    
    """ Performs ETL process for two universities (group D).
        Extract from a Postgres db, transform with pandas and load
        to AWS S3.

        Note: file_name_ (str) as "Name_of_University" assumes convention for:

            include/
                SQL_Name_of_University.sql  # extraction query.
            files/
                name_of_university.csv  # query records.
            dataset/
                name_of_university.txt  # pandas transformation, also data to S3.
    """
   

    def extract_data(self):
        """ Execute a query to a postgres database, then create and rewrite a .csv file
        """

        # Sql
        sql_file_path = f'{self.path_p}/include/SQL_{self.file_name}.sql'
        query = open(sql_file_path, "r")
        request = query.read()

        pg_hook = PostgresHook(
            postgres_conn_id="db_alkemy_universidades",
            schema="training"
        )

        connection = pg_hook.get_conn()
        cursor = connection.cursor()
        cursor.execute(request)
        records = cursor.fetchall()

        # New .csv file
        csv_path = f'{self.path_p}/files'

        if not os.path.isdir(csv_path):
            os.makedirs(csv_path)

        csv_file_path = f'{csv_path}/{self.file_name.lower()}.csv'
        headers = [i[0] for i in cursor.description]

        with open(csv_file_path, 'w', encoding='UTF8', newline='') as f:

            self.logger.info("Writing file...")

            writer = csv.writer(f)
            writer.writerow(headers)
            writer.writerows(records)

            self.logger.info('Writing done')


    def transform_data(self):
        data_trans = DataTransformer(f'{self.path_p}/files/{self.file_name}.csv')
    
        txt_path = f'{self.path_p}/dataset'

        if not os.path.isdir(txt_path):
            os.makedirs(txt_path)
        
        txt_file_path = f'{txt_path}/{self.file_name.lower()}.txt'
        asset_path = f'{self.path_p}/assets/codigos_postales.csv'
        data_trans.transformFile(asset_path, txt_file_path)


    def load_to_s3(self, bucket_name):
        
        s3_name = f'{self.file_name.lower()}.txt'
        file_path = f'{self.path_p}/dataset/{s3_name}' # refactor...

        hook = S3Hook('aws_alkemy_universidades')
        hook.load_file(
            filename=file_path, # wich file
            key=s3_name, # wich file name in s3
            bucket_name=bucket_name,
            replace=True
            )
    

    def tester(self):
        self.logger.info("funciona")
        self.logger.info(f'path: {self.path_p}')
        self.logger.info(f'name: {self.file_name}')
    

    def test_trans(self):
        self.logger.info("testing dataTransformer")
        d_trans = DataTransformer(f'{self.path_p}/files/{self.file_name}.csv')
        d_trans.tester()


