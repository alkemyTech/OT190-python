"""
Archivo de transformación para las universidades:
- Universidad de Palermo
- Universidad Nacional de Jujuy
"""
import logging
import pandas as pd
from datetime import datetime
import numpy as np

# Configuracion logging
# Formato: %Y-%m-%d - nombre_logger - mensaje
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(message)s",
    level=logging.DEBUG,
    datefmt="%Y-%m-%d",
)
# Log con el nombre de archivo en que se encuentra
log = logging.getLogger(__name__)


def normalize_strings(columna):
    """
    Dada una columna, normaliza los strings a
    minúsculas, sin espacios extras, ni guiones
    """
    try:
        columna = columna.apply(lambda x: str(x).strip())
        columna = columna.apply(lambda x: str(x).replace("-", " "))
        columna = columna.apply(lambda x: str(x).replace("_", " "))
        columna = columna.apply(lambda x: x.lower())
    except:
        log.error("Error al querer normalizar los caracteres")
    return columna


def normalize_data(file_path, csv_filename):
    """
    Normaliza los datos del .csv pasado por parámetro y los guarda en un .txt
    """
    log.info("Leyendo datos del csv")
    df_univ = pd.read_csv(f"{file_path}/files/{csv_filename}.csv")

    es_univ_palermo = csv_filename == "universidad_de_palermo"
    log.info("Normalizando datos")

    # university: str minúsculas, sin espacios extras, ni guiones
    if es_univ_palermo:
        df_univ["university"] = normalize_strings(df_univ["universidad"])
    else:
        df_univ["university"] = normalize_strings(df_univ["university"])

    # career: str minúsculas, sin espacios extras, ni guiones
    if es_univ_palermo:
        df_univ["career"] = normalize_strings(df_univ["careers"])
    else:
        df_univ["career"] = normalize_strings(df_univ["career"])

    # inscription_date: str %Y-%m-%d format
    if es_univ_palermo:
        old_date = pd.to_datetime(df_univ["fecha_de_inscripcion"])
    else:
        old_date = pd.to_datetime(df_univ["inscription_date"])
    df_univ["inscription_date"] = pd.to_datetime(old_date, "%Y-%m-%d")

    # first_name: str minúscula y sin espacios, ni guiones
    # last_name: str minúscula y sin espacios, ni guiones
    if es_univ_palermo:
        df_univ["names"] = normalize_strings(df_univ["names"])
    else:
        df_univ["names"] = normalize_strings(df_univ["nombre"])
    try:
        df_univ["first_name"] = df_univ["names"].apply(lambda x: str(x).split(" ")[0])
        df_univ["last_name"] = df_univ["names"].apply(lambda x: str(x).split(" ")[1])
    except:
        df_univ["first_name"] = "NULL"
        df_univ["last_name"] = df_univ["names"]

    # gender: str choice(male, female)
    df_univ["sexo"] = normalize_strings(df_univ["sexo"])
    dict_gender = {"f": "female", "m": "male"}
    df_univ["gender"] = df_univ["sexo"].map(dict_gender)

    # age: int
    if es_univ_palermo:
        df_univ["birth_dates"] = df_univ["birth_dates"].apply(
            lambda x: datetime.strptime(x, "%d/%b/%y").strftime("%Y-%m-%d")
        )
        df_univ["birth_dates"] = pd.to_datetime(df_univ["birth_dates"])
        df_univ["birth_dates"] = df_univ["birth_dates"].apply(
            lambda x: x.replace(year=x.year - 100) if x.year > 2005 else x
        )
        df_univ["age"] = (
            (df_univ["inscription_date"] - df_univ["birth_dates"])
            / np.timedelta64(1, "Y")
        ).astype(int)
    else:
        df_univ["birth_date"] = pd.to_datetime(df_univ["birth_date"], format="%Y-%m-%d")
        df_univ["age"] = (
            (df_univ["inscription_date"] - df_univ["birth_date"])
            / np.timedelta64(1, "Y")
        ).astype(int)

    if es_univ_palermo:
        # postal_code: str
        df_univ["postal_code"] = df_univ["codigo_postal"].astype(str)

        # location: str minúscula sin espacios extras, ni guiones
        df_cp = pd.read_csv(f"{file_path}/assets/codigos_postales.csv")
        df_cp["localidad"] = df_cp["localidad"].apply(lambda x: x.lower())
        dict_cp = dict(zip(df_cp["codigo_postal"], df_cp["localidad"]))
        df_univ["location"] = df_univ["postal_code"].apply(lambda x: dict_cp[int(x)])
    else:
        # location: str minúscula sin espacios extras, ni guiones
        df_univ["location"] = normalize_strings(df_univ["location"])

        # postal_code: str
        df_cp = pd.read_csv(f"{file_path}/assets/codigos_postales.csv")
        df_cp["localidad"] = df_cp["localidad"].apply(lambda x: x.lower())
        dict_cp = dict(zip(df_cp["localidad"], df_cp["codigo_postal"]))
        df_univ["postal_code"] = df_univ["location"].apply(lambda x: dict_cp[x])

    # email: str minúsculas, sin espacios extras, ni guiones
    if es_univ_palermo:
        df_univ["email"] = normalize_strings(df_univ["correos_electronicos"])
    else:
        df_univ["email"] = normalize_strings(df_univ["email"])

    # Guardando información necesaria en un .txt
    df_univ = df_univ[
        [
            "university",
            "career",
            "inscription_date",
            "first_name",
            "last_name",
            "gender",
            "age",
            "postal_code",
            "location",
            "email",
        ]
    ]
    log.info(f"Guardando archivo transformado en: datasets/{csv_filename}.txt")
    df_univ.to_csv(f"{file_path}/datasets/{csv_filename}.txt", sep="\t")
