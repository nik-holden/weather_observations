def set_current_flag(schema, table_name, date_column):
    sql_stmt = f"""UPDATE {schema}.{table_name} SET 
            current_date_flag = CASE WHEN format({date_column}, 'yyyyMMdd') = format(SYSDATETIMEOFFSET() AT TIME ZONE 'New Zealand Standard Time', 'yyyMMdd') THEN 1 ELSE 0 END
            ,current_month_flag = CASE WHEN format({date_column}, 'yyyyMM') = format(SYSDATETIMEOFFSET() AT TIME ZONE 'New Zealand Standard Time', 'yyyMM') THEN 1 ELSE 0 END
            
            """
    return sql_stmt