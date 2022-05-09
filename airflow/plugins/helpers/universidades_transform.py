def calculate_age(diff_bday):
    '''
    Calcula la edad a partie del timedelta pasado por parametro
    '''

    days = diff_bday.days
    years = int(days / 365.2425)
    if years < 0:
        years += 100

    return years


def transform_data(file_from, file_to, dt_format, cp_path):
    '''
    Función de transformacion y normalización de datos.
    '''

    import pandas as pd

    columns = [
        'university', 'career', 'inscription_date', 'first_name', 'last_name',
        'gender', 'age', 'postal_code', 'location', 'email'
    ]

    # Transformación de los datos
    data = pd.read_csv(file_from)
    data.drop(['location'], axis=1, inplace=True)

    for col_name in columns:
        '''
        Tranforma la columna segun el formato correspondiente
        '''

        if col_name == 'location':
            continue

        if col_name in ['inscription_date', 'age']:
            data[col_name] = pd.to_datetime(data[col_name], format=dt_format)
            continue

        if col_name == 'gender':
            format_gender = {'F': 'female', 'M': 'male'}
            data[col_name] = data[col_name].replace(format_gender)
            continue

        if col_name == 'email':
            data[col_name] = data[col_name].apply(lambda x: x.strip().lower())
            continue

        if data[col_name].dtype == 'object':
            if col_name in ['university', 'career', 'first_name', 'last_name',
                            'postal_code']:
                data[col_name] = data[col_name].apply(
                                    lambda x: x.replace('-', ' ')
                                )

            data[col_name] = data[col_name].apply(lambda x: x.strip().lower())

    data['age'] = (data['inscription_date'] - data['age']).apply(calculate_age)

    if len(data[data.last_name.notna()]) == 0:
        data.rename(
            columns={'first_name': 'last_name', 'last_name': 'first_name'},
            inplace=True
        )

    join_cp_on = 'postal_code'
    if not data.postal_code.dtype == 'int':
        data.rename(columns={'postal_code': 'location'}, inplace=True)
        join_cp_on = 'location'

    '''
    Crea un DataFrame normalizado con nombre de localidad y código postal
    '''
    cp_df = pd.read_csv(cp_path)

    cp_df.rename(
        columns={'codigo_postal': 'postal_code', 'localidad': 'location'},
        inplace=True
    )

    cp_df['location'] = cp_df['location'].str.lower()

    data = data.merge(cp_df, how='left', on=join_cp_on)

    # Guardado de los datos procesados
    data.to_csv(file_to, columns=columns, index=False)
