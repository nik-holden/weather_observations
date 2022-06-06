
import sys
sys.path.append('/TIME_pws_data_gather')

import TIME_pws_data_gather.config as config
import pyodbc
from sqlalchemy import create_engine, event
from urllib.parse import quote_plus

server = config.SERVER
database = config.DATABASE
azure_sql_driver = config.AZURE_SQL_DRIVER

db_username, db_password = config.db_credentials()

def set_current_flag(schema, table_name, date_column):
    sql_stmt = f"""UPDATE {schema}.{table_name} SET 
            current_date_flag = CASE WHEN format({date_column}, 'yyyyMMdd') = format(SYSDATETIMEOFFSET() AT TIME ZONE 'New Zealand Standard Time', 'yyyMMdd') THEN 1 ELSE 0 END
            ,current_month_flag = CASE WHEN format({date_column}, 'yyyyMM') = format(SYSDATETIMEOFFSET() AT TIME ZONE 'New Zealand Standard Time', 'yyyMM') THEN 1 ELSE 0 END
            
            """
    return sql_stmt

def azure_sql_pandas_connection():

    server = server
    database = database 
    username = db_username
    password = db_password
    conn = f"""DRIVER={{{azure_sql_driver}}};SERVER='{server}';DATABASE='{database}';UID='{username}';PWD='{password}'"""
    quoted = quote_plus(conn)
    engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))

    return conn, engine

def azure_sql_odbc_connection():
    
    server = server
    database = database
    username = db_username
    password = db_password
    
    conn = pyodbc.connect('DRIVER=' + azure_sql_driver + ';SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    return conn