import pandas as pd
from datetime import datetime
import dateutil
import pathlib

#university: str minúsculas, sin espacios extras, ni guiones
def tr_university(value):
    value = str(value)
    value = value.lower()
    value = value.strip() 
    value = value.strip('-')
    return value

# career: str minúsculas, sin espacios extras, ni guiones
def tr_career(value):
    value = str(value)
    value = value.lower()
    value = value.strip()
    value = value.strip('-')
    return value

# inscription_date: str %Y-%m-%d format
def tr_inscription_date(value):
    value = str(value)
    return value

# last_name: str minúscula y sin espacios, ni guiones
# POR CONVENCIÓN contiene todo el contenido de first_name
def tr_last_name(value):
    df_univ['last_name'] = df_univ['first_name']
    value = str(value)
    value = value.lower()
    value = value.replace(' ', '')
    value = value.strip('-')
    return value

# first_name: str minúscula y sin espacios, ni guiones 
# POR CONVENCIÓN queda vacío
def tr_first_name(value):
    value = None
    return value
    df_univ['first_name'] = None

# gender: str choice(male, female)
def tr_gender(value):
    value = str(value)
    value = value.lower()
    if value == 'f':
        return 'female'
    if value == 'm':
        return 'male'
    else:
        return None

# age: int
def tr_age(value):
    datefmt = '%Y-%m-%d'
    value = datetime.strptime(value, datefmt)
    value = dateutil.relativedelta.relativedelta(datetime.now(), value)
    value = value.years 
    return value

# postal_code: str
def tr_postal_code(value):
    value = str(value)
    return value

# location: str minúscula sin espacios extras, ni guiones
# SE UTILIZA EL CSV AUXILIAR 'codigos_postales.csv'
def tr_location(value):
    try:
        value = int(value)
    except:
        return None
    df_aux = df_pc[df_pc.eq(value).any(1)]
    if not df_aux.empty:
        return df_aux['localidad'].values[0]
    else:
        return None

# email: str minúsculas, sin espacios extras, ni guiones
def tr_email(value):
    value = str(value)
    value = value.lower()
    value = value.strip()
    value = value.strip('-')
    return value

def transform_univ_de_flores():
    global PARENT_PATH
    global df_univ
    global df_pc
    global df_univ_pc

    #Obtener parent path para luego configurar el template_searchpath
    PARENT_PATH = (pathlib.Path(__file__).parent.absolute()).parent.parent
    df_univ = pd.read_csv(f'{PARENT_PATH}/files/Universidad_De_Flores.csv')
    df_pc = pd.read_csv(f'{PARENT_PATH}/assets/codigos_postales.csv')
    df_univ_pc = df_univ['postal_code']

    df_univ['university'] = df_univ['university'].apply(tr_university)
    df_univ['career'] = df_univ['career'].apply(tr_university)
    df_univ['inscription_date'] = df_univ['inscription_date'].apply(tr_inscription_date)
    df_univ['last_name'] = df_univ['last_name'].apply(tr_last_name)
    df_univ['first_name'] = df_univ['first_name'].apply(tr_first_name)
    df_univ['gender'] = df_univ['gender'].apply(tr_gender)
    df_univ['age'] = df_univ['age'].apply(tr_age)
    df_univ['postal_code'] = df_univ['postal_code'].apply(tr_postal_code)
    df_univ['location'] = df_univ_pc.apply(tr_location)
    df_univ['email'] = df_univ['email'].apply(tr_email)

    df_univ.to_csv(f'{PARENT_PATH}/datasets/Universidad_De_Flores.txt',index=False)

try:
    transform_univ_de_flores()
except Exception as e:
    print(e)
    pass