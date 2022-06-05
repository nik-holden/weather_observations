import sys
sys.path.append('/TIME_pws_data_gather')

from sqlalchemy import create_engine, event
from urllib.parse import quote_plus
from TIME_pws_data_gather.common_functions import azure_sql_odbc_connection


def write_raw_data_to_db(df, schema, table_name):
    """db_username, db_password = db_credentials()

    server = 'nz-personal-nh.database.windows.net'
    database = 'general-data-collection'
    username = db_username
    password = db_password
    conn = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password"""
    conn = azure_sql_odbc_connection
    quoted = quote_plus(conn)
    engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))

    # insert Pandas df into staging DB table

    df.to_sql(table_name, engine, index=False, if_exists='replace', schema=schema)

    #conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    conn = cf.azure_sql_odbc_connection
    cursor = conn.cursor()

    table_columns = cursor.columns(schema='weather', table='raw_observations')

    columns = [row.column_name for row in table_columns if row.column_name not in 'id']

    columns_str = ', \n'.join(columns)

    sql_stmt = f"""
    INSERT INTO weather.raw_observations(
        {columns_str})
        SELECT {columns_str}
        FROM staging.raw_observations"""

    cursor.execute(sql_stmt)

    conn.commit()

    sql_stmt = """UPDATE weather.raw_observations SET 
            current_date_flag = CASE WHEN format(obsTimeLocal, 'yyyyMMdd') = format(SYSDATETIMEOFFSET() AT TIME ZONE 'New Zealand Standard Time', 'yyyMMdd') THEN 1 ELSE 0 END
            ,current_month_flag = CASE WHEN format(obsTimeLocal, 'yyyyMM') = format(SYSDATETIMEOFFSET() AT TIME ZONE 'New Zealand Standard Time', 'yyyMM') THEN 1 ELSE 0 END
            """

    cursor.execute(sql_stmt)
    
    conn.commit()