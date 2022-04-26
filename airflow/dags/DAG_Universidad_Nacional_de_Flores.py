import airflow
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy import DummyOperator

default_args = {
    'owner': 'airflow',    
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id = "DAG_Universidad_Nacional_de_Flores",
    default_args=default_args,
    schedule_interval='@once',	
    dagrun_timeout=timedelta(minutes=60),
    description='ETL para Universidad Nacional de Flores',
    start_date = datetime(2022,4,25),
    template_searchpath= '/home/wotw/airflow/includes/',
    catchup=False) as dag:

        query_database = PostgresOperator(
            task_id = "query_database",
            sql = 'SQL_Universidad_De_Flores.sql',
            postgres_conn_id = "psql_rds_alkemy_universidades"            
        )

        pandas_transform = DummyOperator(
            task_id = "pandas_transform"
        )

        load_to_s3 = DummyOperator(
            task_id = "load_to_s3"
        )

        query_database >> pandas_transform >> load_to_s3