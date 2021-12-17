import pandas as pd
import math

from datetime import datetime as dt
from datetime import timedelta as td
from dateutil import tz

from io import StringIO

consolidated_file_name = 'weather_stations_obsertavtions.csv'
container = 'webdata'
storage_account = 'weatherobservationdata'

def read_blob_csv(file_name, blob_service_client):

    file = f'{file_name}'

    print(file)

    blob_client_instance = blob_service_client.get_blob_client(container=container, blob=file)

    weather_observation_csv_data = blob_client_instance.download_blob()

    df = pd.read_csv(StringIO(weather_observation_csv_data.content_as_text()))

    copy_raw_file(file, blob_service_client)

    return df

def copy_raw_file(file, blob_service_client):
    src_file = f'{file}'
    dest_file = f'processed/{file}'



    source_blob = blob_service_client.get_blob_client(container=container, blob=file)
    dest_blob = blob_service_client.get_blob_client(container=container, blob=dest_file)


    properties = dest_blob.start_copy_from_url(source_blob.url)
    print(f'{file} has been copied')
    copy_props = properties.copy

    source_blob.delete_blob()
    print(f'{file} has been deleted\n')

def copy_consolidated_file_to_pulic_location(file, blob_service_client, public_location):

    src_file = f'consolidated/{file}'
    dest_file = f'{file}'
  
    source_blob = blob_service_client.get_blob_client(container=container, blob=src_file)

    dest_blob = blob_service_client.get_blob_client(container=public_location, blob=dest_file)

    properties = dest_blob.start_copy_from_url(source_blob.url)
    print(f'{file} has been copied to public location')
    copy_props = properties.copy

def add_columns_to_dataframe(df):

    df['observation_time_corrected'] = df['obsTimeUtc'].apply(apply_utc_offset)
    df['observaton_date_key'] = df['observation_time_corrected'].apply(date_key_convert)
    df['observaton_year'] = df['observation_time_corrected'].apply(year_convert)
    df['observaton_month'] = df['observation_time_corrected'].apply(month_convert)
    df['observaton_day'] = df['observation_time_corrected'].apply(day_convert)
    df['observaton_hour'] = df['observation_time_corrected'].apply(hour_convert)
    df['observaton_10M_reporting_period'] = df['observation_time_corrected'].apply(M10_rep_per_convert)
    df['observaton_05M_reporting_period'] = df['observation_time_corrected'].apply(M05_rep_per_convert)
    df['current_date'] = df['observation_time_corrected'].apply(current_date)
    df['current_month'] = df['observation_time_corrected'].apply(current_month)

    return df


def apply_utc_offset(date_to_convert):
    # Apply offset to utc time to get local time corrected for DST.  The raw local time is 30 minutes ahead of what it should be
    date_to_convert = dt.strptime(date_to_convert, '%Y-%m-%dT%H:%M:%SZ') 
    return date_to_convert + date_to_convert.astimezone(tz.gettz('Pacific/Auckland')).utcoffset()


def year_convert(date_to_convert):
    return date_to_convert.strftime('%Y')

def month_convert(date_to_convert):
    return date_to_convert.strftime('%m')

def day_convert(date_to_convert):
    return date_to_convert.strftime('%d')

def date_key_convert(date_to_convert):
    return date_to_convert.strftime('%Y%m%d')

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


def current_date(date_to_convert):
    day_format = "%Y%m%d"
    date_to_convert = date_to_convert.strftime(day_format)
    current_date = (dt.utcnow() + dt.utcnow().astimezone(tz.gettz('Pacific/Auckland')).utcoffset()).strftime(day_format)
    #dt.date(dt.now()).strftime("%Y%m%d")
    if date_to_convert == current_date:
        current_date_marker = 1
    else:
        current_date_marker = 0

    return current_date_marker

def current_month(date_to_convert):
    month_format = "%Y%m"
    date_to_convert = date_to_convert.strftime(month_format)
    current_month = (dt.utcnow() + dt.utcnow().astimezone(tz.gettz('Pacific/Auckland')).utcoffset()).strftime(month_format)
    #dt.date(dt.now()).strftime("%Y%m")
    if date_to_convert == current_month:
        current_month_marker = 1
    else:
        current_month_marker = 0

    return current_month_marker




def consolidate(blob_service_client):

    container_client = blob_service_client.get_container_client(container)
    

    file_list = container_client.list_blobs('raw/')


    try:
        all_df = pd.concat([read_blob_csv(file_name.name, blob_service_client) for file_name in file_list], axis=0)
        print('raw file has been read')
        raw_files = True

    except:
        raw_files = False
        print('No raw data files')



    try:
        existing_df = read_blob_csv(f'consolidated/{consolidated_file_name}', blob_service_client)
        print('consolidated file has been read')
        consolidated_file = True

    except:
        consolidated_file = False
        print('There is no concolidated data file, one has been created in the consolidation folder')



    if consolidated_file == False:
        df = all_df

    else:
        frames = [all_df, existing_df]

        df = pd.concat(frames)

    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
    
    add_columns_to_dataframe(df)

    output = df.to_csv(mode='w', index=False)

    # Create a blob client using the local file name as the name for the blob
    blob_client = blob_service_client.get_blob_client(container=container,
                                                      blob=f'consolidated/{consolidated_file_name}')

    blob_client.upload_blob(str.encode(output), overwrite=True)

    copy_consolidated_file_to_pulic_location(file=consolidated_file_name, blob_service_client=blob_service_client, public_location='public-weatherobservations')

    print('Process completed successfully')
