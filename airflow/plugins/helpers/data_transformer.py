""" lib/data_transforme.py """

from asyncio.log import logger
import pandas as pd

class DataTransformer():
    """ Transforma los datos """

    def __init__(self, file_path_):
        self.file_path = file_path_

    def readFile(self):
        self.df = pd.read_csv(self.file_path)

    def normalizeFile(self, input_file):
        self.fix_cp(input_file)
        self.age_fix()
        self.name_fix()
        self.gender_fix()
        self.date_fix('inscription_date')
        self.str_fix()

    def exportFile(self, destination_path):
        # Reorder cols
        self.df = self.df[[
            'university',
            'career',
            'inscription_date',
            'first_name',
            'last_name',
            'gender',
            'age',
            'postal_code',
            'location',
            'email'
        ]]
        self.df.to_csv(destination_path, index=False)

    def transformFile(self, input_path, destination_path):
        # logs...
        self.readFile()
        self.normalizeFile(input_path)
        self.exportFile(destination_path)

    # ---------- Cleanning functions ----------
    def fix_cp(self, asset_path):

        """ fix postal_code, location issue.
            df : dataset to fix.
            asset_path: location of asset "./assets/codigos_postales.csv"
        """

        # asset csv to df
        cp_df = pd.read_csv(asset_path)

        # column fix on asset
        cp_df = cp_df.rename(columns={"codigo_postal": "postal_code", "localidad": "location"})
        cp_df['location'] = cp_df['location'].str.lower()

        # merge
        if 'postal_code' in self.df.columns:
            print('aca')
            self.df = self.df.merge(cp_df, on="postal_code")
        
        elif 'location' in self.df.columns:
            self.df['location'] = self.df['location'].str.lower() # since merge on str type
            self.df = self.df.merge(cp_df.drop_duplicates('location'), on="location")


    def str_fix(self):
        """ Fix strings to lowercase, remove dash and extra spaces """
        
        for col in self.df.columns:
            if col != 'age':
                self.df[col] = self.df[col].astype(str).str.replace('_', ' ')
                self.df[col] = self.df[col].astype(str).str.lower()
                self.df[col] = self.df[col].astype(str).str.strip()
    

    def age_fix(self):
        """ Transform birth_date data in age data. Use quick solution for Y2K issue """
        bd_year = pd.to_datetime(self.df['birth_date']).dt.year
        bd_year_fix = bd_year.apply(lambda y : y - 100 if y > 2006 else y)  # Youngest: 2006
        insc_year = pd.to_datetime(self.df['inscription_date']).dt.year

        self.df['age'] = insc_year - bd_year_fix.astype(int)
        self.df = self.df.drop(['birth_date'], axis=1)


    def name_fix(self):
        """ name fix """
        self.df['last_name'] = self.df['name']
        self.df = self.df.rename(columns={"name":"first_name"})
        self.df['first_name'] = None


    def gender_fix(self):
        """ 'gender' col in to choice: male/female"""

        self.df['gender'] = self.df['gender'].replace({'m':'male', 'f':'female'})


    def date_fix(self, col):
        """ date format for col in df """
        self.df[col] = pd.to_datetime(self.df[col])
        self.df[col] = pd.to_datetime(self.df[col], format='%Y-%m-%d')


    def tester(self):
        logger.info("Test method exe")