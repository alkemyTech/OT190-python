import pandas as pd
from dateutil import parser
from dateutil.relativedelta import relativedelta
from datetime import datetime

def transform_datas(file_university, csv_file, txt_file, assets):

    months = {'Jan':'01', 'Feb':'02', 'Mar':'03', 'Apr':'04','May':'05','Jun':'06','Jul':'07','Aug':'08','Sep':'09','Oct':'10','Nov':'11','Dec':'12'}
    abreviations = {'mr.': '','dr.': '','mrs.': '','ms.': '','md': '','dds': '','jr.': '','dvm': '','phd': ''}
    dates = []
    birth = []
    #Process data obtained from SQL query
    pandas_data = pd.read_csv(csv_file, index_col=False)
    pandas_data['university'] = (pandas_data['university'].str.replace("-", " ")).str.lower()
    pandas_data['career'] = (pandas_data['career'].str.replace("-", " ")).str.lower()
    pandas_data['last_name'] = (pandas_data['last_name'].str.replace("-", " ")).str.lower()
    pandas_data['email'] = (pandas_data['email'].str.replace("-", " ")).str.lower()
    

    #Generate column location
    postal_location = pd.read_csv(assets)
    postal_location.columns = ['postal_code', 'location']
    

    if file_university == 'Facultad_Latinoamericana_Ciencias_Sociales':
        pandas_data['location'] = (pandas_data['location'].str.replace("-", " ")).str.lower()
        postal_location['location'] = (postal_location['location'].str.replace("-", " ")).str.lower()
        postal_location['postal_code'] = postal_location['postal_code'].apply(lambda x: str(x))
        pandas_data =  pd.merge(left=pandas_data,right=postal_location, how='left', left_on='location', right_on='location')
        for i in range(pandas_data.shape[0]):
            birth.append(parser.parse(pandas_data.loc[i]['age']).date())
        pandas_data['age'] = pd.to_datetime(birth, format='%Y-%m-%d')
        pandas_data['age'] = pandas_data['age'].apply(lambda x: relativedelta(datetime.now(),x).years)
    
    elif file_university == 'Universidad_J_F_Kennedy':
        pandas_data['postal_code'] = pandas_data['postal_code'].apply(lambda x: str(x)).str.replace("-", " ")
        postal_location['postal_code'] = postal_location['postal_code'].apply(lambda x: str(x))
        pandas_data =  pd.merge(left=pandas_data,right=postal_location, how='left', left_on='postal_code', right_on='postal_code')
        pandas_data['location'] = (pandas_data['location'].str.replace("-", " ")).str.lower()

        #Calculate age
        for i in range(pandas_data.shape[0]):
            year = int(pandas_data.loc[i]['age'][:2])+1900 if int(pandas_data.loc[i]['age'][:2]) > 10 else int(pandas_data.loc[i]['age'][:2])+2000
            birth.append(str(year)+"-"+months[pandas_data.loc[i]['age'][3:6]]+"-"+pandas_data.loc[i]['age'][7:])

        pandas_data['age'] = birth
        pandas_data['age'] = pd.to_datetime(pandas_data['age'], format='%Y/%m/%d')
        pandas_data['age'] = pandas_data['age'].apply(lambda x: relativedelta(datetime.now(),x).years)

    for i in range(pandas_data.shape[0]):
        dates.append(parser.parse(pandas_data.loc[i]['inscription_date']).date())
    pandas_data['inscription_date'] = pd.to_datetime(dates, format='%Y-%m-%d')
    
    # Delete abreviations
    for abreviation, blank in abreviations.items():
        pandas_data['last_name'] = pandas_data['last_name'].apply(lambda x: x.replace(abreviation, blank))
    
    #Columns are now without trailing and leading spaces
    pandas_data.columns = pandas_data.columns.str.strip()
    
    pandas_data["first_name"] = pandas_data["last_name"].str.split(" ", expand=True)[0]
    pandas_data["last_name"] = pandas_data["last_name"].str.split(" ", expand=True)[1]
    pandas_data['gender'] = (pandas_data['gender'].str.replace("m", "male")).str.replace("f","female")

    #Order Columns
    pandas_data = pandas_data[['university', 'career', 'inscription_date','first_name', 'last_name', 'gender', 'age', 'postal_code', 'location', 'email']]
    
    #Create txt file
    pandas_data.to_csv(txt_file, index=None, sep=' ', mode='a')