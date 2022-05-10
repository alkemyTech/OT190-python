# Built-in
from pathlib import Path
# Pandas
import pandas as pd
# Airflow modules


root_folder = Path(__file__).resolve().parent.parent.parent
print(root_folder)


def transform_data_module(file:str):
    """
    Transforma Todas las columnas a lower con la funcion lowercase_df
    definida dentro de esta funcion.
    """
    def lowercase_df(df):
        """
        Transforma todas las columnas del dataframe (df), menos 'birth_date'
        y 'postal_code' a lower case
        """
        for column in df:
            if column != "birth_date" and column != "inscription_date" and column != "postal_code":
                df[f'{column}'] = df[f'{column}'].str.lower()
                df[f'{column}'] = df[f'{column}'].apply(lambda x: x.replace('-', ' '))
    
    files_path = f'{root_folder}/files'
    df = pd.read_csv(f'{files_path}/{file}.csv')
    
    lowercase_df(df)

    df['gender'] = df.gender.replace({'f': 'female', 'm': 'male'})

    return df


def calculate_age(df, file:str):
    """
    Crea una columna llamada 'age', realiza la diferencia entre las columnas 'inscription_date'
    y 'birth_date' para obtener la edad en días. Con la edad en días realiza una transformacion
    en años con la funcion calculate
    """
    def calculate(diff_days):
        """
        Obtiene diferencia de días, determina si son negativos o positivos.
        Si son negativos, aumenta 100 años y returna la division entre días y años (edad).
        Si son positivos solo retorna la division
        """
        days = diff_days.days
        if days < 0:
            days += int(100 * 365.2425)

        return int(days / 365.2425)
    if file == 'universidad_del_cine':
        df['inscription_date'] = pd.to_datetime(df.inscription_date, format='%d-%m-%Y')
        df['birth_date'] = pd.to_datetime(df.birth_date, format='%d-%m-%Y')
        df['age'] = df['inscription_date'] - df['birth_date']

    elif file == 'universidad_de_buenos_aires':
        df['inscription_date'] = pd.to_datetime(df.inscription_date)
        df['birth_date'] = pd.to_datetime(df.birth_date, format='%y-%b-%d')
        df['age'] = df['inscription_date'] - df['birth_date']

    df['age'] = df['age'].apply(calculate)
    return df


def postal_code_or_location(df, postal_code_or_location:str):
    """
    Recibe un dataframe y un string, el string define si se va a
    agregar location, o postal_code, solo acepta esos dos valores.

    Con un dataframe complementario llamado 'codigos_postales', define
    location a traves de la columna 'codigo_postal' o postal_code
    a través de la columna 'localidad'
    """
        # Leyendo Dataframe complementario
    complementary = pd.read_csv(f'{root_folder}/assets/codigos_postales.csv')

    if postal_code_or_location == "location":
        # Obteniendo localidad con codigo_postal
        complementary["localidad"] = complementary["localidad"].apply(lambda x: x.lower())
        dict_cp = dict(zip(complementary["codigo_postal"], complementary["localidad"]))
        df["location"] = df["postal_code"].apply(lambda x: dict_cp[x])

    elif postal_code_or_location == "codigo_postal":
        # Obteniendo codigo postal con localidad
        complementary["localidad"] = complementary["localidad"].apply(lambda x: x.lower())
        dict_cp = dict(zip(complementary["localidad"], complementary["codigo_postal"]))
        df["postal_code"] = df["location"].apply(lambda x: dict_cp[x])

    return df


def save_df_text(df, file_name:str):
    """
    Guarda el dataframe en un archivo con el mismo nombre
    del string pasado en {file_name}.txt en la carpeta dataset
    """
    with open(f'{root_folder}/datasets/{file_name}.txt', 'w+') as f:
        f.write(df.to_string())
        f.close()