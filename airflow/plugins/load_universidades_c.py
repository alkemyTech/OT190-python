from airflow.hooks.S3_hook import S3Hook
import logging

# Configuracion logging
# Formato: %Y-%m-%d - nombre_logger - mensaje
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(message)s",
    level=logging.DEBUG,
    datefmt="%Y-%m-%d",
)
# Log con el nombre de archivo en que se encuentra
log = logging.getLogger(__name__)


def upload_to_S3(file_path, key):
    """
    Sube el archivo a S3
    """
    log.info(f"Intentando subir archivo {key} a S3")
    try:
        hook = S3Hook("aws_s3_alkemy_universidades")
        hook.load_file(
            filename=file_path,
            key=key,
            bucket_name="cohorte-abril-98a56bb4",
            replace=True,
        )
    except Exception as e:
        log.error(f"No se pudo subir el archivo a S3: {e}")
    log.info(f"Archivo subido a S3")
