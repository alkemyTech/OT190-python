import logging

from airflow.hooks.S3_hook import S3Hook

logging.basicConfig(level = logging.INFO,
                    format = " %(asctime)s - %(name)s - %(message)s",
                    datefmt='%Y-%m-%d',
                    encoding= "utf-8")

logger = logging.getLogger('')


def load(file_name:str, key: str, bucket_name: str):
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