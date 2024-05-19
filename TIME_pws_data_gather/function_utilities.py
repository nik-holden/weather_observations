import sys
sys.path.append('/TIME_pws_data_gather')

from datetime import datetime as dt
from datetime import timedelta as td
import math
from dateutil import tz

def add_columns_to_dataframe(df):

    df['observation_time_corrected'] = df['obsTimeLocal']#.apply(convert_str_to_timestamp)
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
    date_to_convert = dt.strptime(date_to_convert, '%Y-%m-%d %H:%M:%S')
    return date_to_convert + date_to_convert.astimezone(tz.gettz('Pacific/Auckland')).utcoffset()

def convert_str_to_timestamp(date_to_convert):
    # Apply offset to utc time to get local time corrected for DST.  The raw local time is 30 minutes ahead of what it should be
    date_to_convert = dt.strptime(date_to_convert, '%Y-%m-%d %H:%M:%S')
    date_to_convert = date_to_convert - td(minutes=30)
    return date_to_convert

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