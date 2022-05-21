import requests
import pandas
import datetime
from datetime import datetime as dt
from .write_to_db import write_to_db

# PSW
base_url = 'https://api.weather.com/v2/pws/observations'
period = 'current'
format = 'json'
units = 'm'
apiKey = '2af8653072354d19b8653072358d194f'

epoch = datetime.datetime.utcfromtimestamp(0)
file_time = (datetime.datetime.now() - epoch).total_seconds() * 1000.0
# blob storage

container = 'webdata/raw'
storage_account = 'weatherobservationdata'
file_name = f'pws_observations_{file_time}.csv'

personal_weather_station = {
    'stations': [{'StationId': 'IAUKHIGH2', 'Owner': 'P Holden'}, {'StationId': 'INEWPL81', 'Owner': 'N Holden'},
                 {'StationId': 'IUPPER72', 'Owner': 'P Whiting'}, {'StationId': 'IKATIKAT9', 'Owner': 'Purple Hen Country Lodge'}, 
                 {'StationId': 'ICLYDE9', 'Owner': 'New Crops - CLyde'}, {'StationId': 'IWGNLYAL3', 'Owner': 'MARANUI - Lyall Bay'},
                 {'StationId': 'IKATIK3', 'Owner': 'Katikati - Town'}, {'StationId': 'IALEXA39', 'Owner': 'Alexandra - Town'}]}


def get_blob_file_name():
    epoch = datetime.datetime.utcfromtimestamp(0)
    file_time = dt.now().strftime("%Y-%m-%d_%H-%M-%S")

    file_name = f'pws_observations_{file_time}.csv'
    return file_name


def station_url(base_url, period, stationId, format, units, apiKey):
    url = f'{base_url}/{period}?stationId={stationId}&format={format}&units={units}&apiKey={apiKey}'

    return url


def get_weather_station_observations(url):
    response = requests.get(url)
    status = response.status_code

    json_payload = response.json()

    weather_observation = json_payload['observations'][0]

    return weather_observation


def json_to_pandas_dataframe(weather_observation_data):
    data = pandas.json_normalize(weather_observation_data)

    data = data.loc[:, ~data.columns.str.contains('^Unnamed')]
    
    return data


def weather_obs(blob_service_client):
    #blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    station_observation_list = []
    for weather_station_dict in personal_weather_station.get('stations'):
        stationId = weather_station_dict.get('StationId')
        url = station_url(base_url,
                          period,
                          stationId,
                          format,
                          units,
                          apiKey)
        #  A try/except block has been added due to a weather station going no longer being reachable and causing the job to fail
        try:
            station_observation_list.append(get_weather_station_observations(url))
        
        except:
            pass # add an error message in here


    data = json_to_pandas_dataframe(station_observation_list)

    output = data.to_csv(mode='w', index=False)

    # Create a blob client using the local file name as the name for the blob
    blob_client = blob_service_client.get_blob_client(container=container, blob=get_blob_file_name())

    blob_client.upload_blob(str.encode(output))

    write_to_db(data)