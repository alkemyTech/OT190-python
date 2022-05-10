import pandas as pd
from datetime import datetime
import dateutil
import pathlib

#university: str minúsculas, sin espacios extras, ni guiones
def tr_university(value):
    value = str(value)
    value = value.lower()
    value = value.strip() 
    value = value.replace('_', ' ')
    return value

# career: str minúsculas, sin espacios extras, ni guiones
def tr_career(value):
    value = str(value)
    value = value.lower()
    value = value.strip()
    value = value.replace('_', ' ')
    return value

# inscription_date: str %Y-%m-%d format
def tr_inscription_date(value):
    value = str(value)
    return value

# last_name: str minúscula y sin espacios, ni guiones
# POR CONVENCIÓN contiene todo el contenido de first_name
def tr_last_name(value):
    value = str(value)
    value = value.lower()
    value = value.replace('_', ' ')
    value = value.replace(' ', '')
    return value

# first_name: str minúscula y sin espacios, ni guiones 
# POR CONVENCIÓN queda vacío
def tr_first_name(value):
    value = None
    return value

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
    datefmt = '%d-%b-%y'
    value = datetime.strptime(value, datefmt)
    if value.year >= datetime.now().year:
        value = value - dateutil.relativedelta.relativedelta(years=100)
    if value.year + 18 > datetime.now().year:
        value = value - dateutil.relativedelta.relativedelta(years=100)
    value = dateutil.relativedelta.relativedelta(datetime.now(), value)
    value = abs(value.years)
    return value

# postal_code: str
def tr_postal_code(value):
    value = value.replace('_', ' ')
    try:
        value = str(value)
    except:
        return None
    df_aux = df_pc[df_pc.eq(value).any(1)]
    if not df_aux.empty:
        return str(df_aux['codigo_postal'].values[0])
    else:
        return None

# location: str minúscula sin espacios extras, ni guiones
def tr_location(value):
    value = value.lower()
    value = value.strip() 
    value = value.replace('_', ' ')
    return value

# email: str minúsculas, sin espacios extras, ni guiones
def tr_email(value):
    value = str(value)
    value = value.lower()
    value = value.strip() 
    value = value.replace('_', ' ')
    return value

def transform():
    global PARENT_PATH
    global df_univ
    global df_pc
    global df_univ_pc

    #Obtener parent path para luego configurar el template_searchpath
    PARENT_PATH = (pathlib.Path(__file__).parent.absolute()).parent.parent
    df_univ = pd.read_csv(f'{PARENT_PATH}/files/Universidad_Nacional_De_Villa_Maria.csv')
    df_pc = pd.read_csv(f'{PARENT_PATH}/assets/codigos_postales.csv')
    df_univ_location = df_univ['location']


    df_univ['university'] = df_univ['university'].apply(tr_university)
    df_univ['career'] = df_univ['career'].apply(tr_university)
    df_univ['inscription_date'] = df_univ['inscription_date'].apply(tr_inscription_date)
    df_univ['last_name'] = df_univ['first_name']
    df_univ['last_name'] = df_univ['last_name'].apply(tr_last_name)
    df_univ['first_name'] = df_univ['first_name'].apply(tr_first_name)
    df_univ['gender'] = df_univ['gender'].apply(tr_gender)
    df_univ['age'] = df_univ['age'].apply(tr_age)
    df_univ['location'] = df_univ['location'].apply(tr_location)
    df_univ['postal_code'] = df_univ_location.apply(tr_postal_code)
    df_univ['email'] = df_univ['email'].apply(tr_email)

    df_univ.to_csv(f'{PARENT_PATH}/datasets/Universidad_De_Villa_Maria.txt',index=False)

try:
    transform()
except Exception as e:
    print(e)
    pass