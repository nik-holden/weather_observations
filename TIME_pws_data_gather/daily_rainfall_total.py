import pyodbc

from .config import db_credentials
from .common_functions import set_current_flag

def daily_rainfall_total():
    
    db_username, db_password = db_credentials()

    server = 'nz-personal-nh.database.windows.net'
    database = 'general-data-collection'
    driver = 'ODBC Driver 17 for SQL Server'
    username = db_username
    password = db_password
    
    conn = pyodbc.connect('DRIVER=' + driver + ';SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = conn.cursor()

    sql_stmt = f"""
WITH cte_src AS (
    SELECT observation_date, stationID
    ,max([metric_precipitationTotal]) AS metric_precipitationDailyTotal
    ,min(metric_temp) AS metric_min_temp
    ,max(metric_temp) AS metric_max_temp
    ,[current_date_flag]
    ,[current_month_flag]
    ,observation_date_key
    FROM weather.raw_observations
    GROUP BY observation_date, stationID, [current_date_flag], [current_month_flag], observation_date_key
    )

    MERGE [weather].[daily_weather_metrics] AS tgt
    USING cte_src AS src
    ON src.stationID = tgt.stationID AND src.observation_date = tgt.observation_date
    WHEN MATCHED THEN
    UPDATE SET [metric_precipitationDailyTotal] = src.[metric_precipitationDailyTotal]
	, metric_min_temp = src.metric_min_temp
	, metric_max_temp = src.metric_max_temp
    WHEN NOT MATCHED THEN
    INSERT ([observation_date]
	, [stationID]
	, [metric_precipitationDailyTotal]
	, metric_min_temp, metric_max_temp
	, [current_date_flag]
	, [current_month_flag]
	, observation_date_key)
    VALUES (src.observation_date
	, src.stationID
	, src.[metric_precipitationDailyTotal]
	, src.[current_date_flag]
	, src.[current_month_flag]
	, src.metric_min_temp
	, src.metric_max_temp
	, src.observation_date_key
	);

    """
    cursor.execute(sql_stmt)
    
    conn.commit()

    sql_stmt = """UPDATE weather.total_daily_rainfall SET 
    current_date_flag = CASE WHEN observation_date = CAST(GETUTCDATE() AT TIME ZONE 'New Zealand Standard Time' AS DATE) THEN 1 ELSE 0 END
    ,current_month_flag = CASE WHEN format(observation_date, 'yyyyMM') = format(CAST(GETUTCDATE() AT TIME ZONE 'New Zealand Standard Time' AS DATE), 'yyyyMM') THEN 1 ELSE 0 END
    """

    cursor.execute(sql_stmt)

    conn.commit()
