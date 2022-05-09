import logging
from python_callables.data_with_pandas.data_with_pandas import transform_data_module, calculate_age, postal_code_or_location, save_df_text

logging.basicConfig(level = logging.INFO,
                    format = " %(asctime)s - %(name)s - %(message)s",
                    datefmt='%Y-%m-%d',
                    encoding= "utf-8")

logger = logging.getLogger('')

def transform(file):
    """
    Funcion responsable de leer los datos crudos,
    procesarlos y guardarlos en la carpeta dataframe
    """
    logger.info('Transformando datos')
    df = transform_data_module(file)
    logger.info('Calculando edad')
    df = calculate_age(df, file)
    logger.info('Creando columna de localidad')
    df = postal_code_or_location(df, 'location')
    logger.info(f'Guardando archivo en {file}.txt')
    df = save_df_text(df, file)
    logger.info('Datos guardados')