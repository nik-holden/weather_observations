import pandas as pd
import pyodbc
from sqlalchemy import create_engine, event
from urllib.parse import quote_plus
from dateutil import tz
from datetime import datetime as dt
import math

from config import db_credentials
from common_functions import set_current_flag


def write_to_db(dataframe):
    db_username, db_password = db_credentials()

    server = 'nz-personal-nh.database.windows.net'
    database = 'general-data-collection'
    username = db_username
    password = db_password
    conn = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password
    quoted = quote_plus(conn)
    engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted)) #, fast_executemany=True)



    df = dataframe

    df.columns = df.columns.str.replace('.', '_')

    df.rename(columns={'metric_precipRate': 'metric_precipitationRate',
                    'metric_precipTotal': 'metric_precipitationTotal',
                    'metric_dewpt': 'metric_dewPoint',
                    'metric_elev': 'metric_elevation'}, inplace=True)

    df = add_columns_to_dataframe(df)
    
    df.drop('observation_time_corrected', axis=1, inplace=True)
    
    # insert
    table_name = 'raw_observations'
    df.to_sql(table_name, engine, index=False, if_exists='replace', schema='staging')

    conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = conn.cursor()

    table_columns = cursor.columns(schema='weather', table='raw_observations')

    columns = [row.column_name for row in table_columns if row.column_name not in 'id']

    columns_str = ', \n'.join(columns)

    sql_stmt = f"""
    INSERT INTO weather.raw_observations(
        {columns_str})
        SELECT {columns_str}
        FROM staging.raw_observations"""

    #cursor.execute(sql_stmt)

    #conn.commit()

    sql_stmt = """UPDATE weather.raw_observations SET 
            current_date_flag = CASE WHEN format(obsTimeLocal, 'yyyyMMdd') = format(SYSDATETIMEOFFSET() AT TIME ZONE 'New Zealand Standard Time', 'yyyMMdd') THEN 1 ELSE 0 END
            ,current_month_flag = CASE WHEN format(obsTimeLocal, 'yyyyMM') = format(SYSDATETIMEOFFSET() AT TIME ZONE 'New Zealand Standard Time', 'yyyMM') THEN 1 ELSE 0 END
            """

    #cursor.execute(sql_stmt)
    
    #conn.commit()

def add_columns_to_dataframe(df):

    df['observation_time_corrected'] = df['obsTimeLocal'].apply(apply_utc_offset)
    df['observation_date_key'] = df['observation_time_corrected'].apply(date_key_convert)
    df['observation_year'] = df['observation_time_corrected'].apply(year_convert)
    df['observation_month'] = df['observation_time_corrected'].apply(month_convert)
    df['observation_day'] = df['observation_time_corrected'].apply(day_convert)
    df['observation_hour'] = df['observation_time_corrected'].apply(hour_convert)
    df['observation_10M_reporting_period'] = df['observation_time_corrected'].apply(M10_rep_per_convert)
    df['observation_05M_reporting_period'] = df['observation_time_corrected'].apply(M05_rep_per_convert)
    df['current_date_flag'] = 0
    df['current_month_flag'] = 0
    df['observation_date'] = df['observation_time_corrected'].apply(observation_date)

    return df


def apply_utc_offset(date_to_convert):
    # Apply offset to utc time to get local time corrected for DST.  The raw local time is 30 minutes ahead of what it should be
    date_to_convert = dt.strptime(date_to_convert, '%Y-%m-%dT%H:%M:%SZ')
    return date_to_convert# + date_to_convert.astimezone(tz.gettz('Pacific/Auckland')).utcoffset()


def year_convert(date_to_convert):
    return date_to_convert.strftime('%Y')

def month_convert(date_to_convert):
    return date_to_convert.strftime('%m')

def day_convert(date_to_convert):
    return date_to_convert.strftime('%d')

def date_key_convert(date_to_convert):
    return date_to_convert.strftime('%Y%m%d')

def observation_date(date_to_convert):
    return date_to_convert.strftime('%Y-%m-%d')

def hour_convert(date_to_convert):
    return date_to_convert.strftime('%H')

def M10_rep_per_convert(date_to_convert):
    ymdh = date_to_convert.strftime('%Y-%m-%d %H')
    time = int(date_to_convert.strftime('%M'))
    time = str(math.floor(time / 10) * 10).zfill(2)
    return f'{ymdh} {time}'

def M05_rep_per_convert(date_to_convert):
    ymdh = date_to_convert.strftime('%Y-%m-%d %H')
    time = int(date_to_convert.strftime('%M'))
    time = str(math.floor(time / 5) * 5).zfill(2)
    return f'{ymdh} {time}'