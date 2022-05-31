import datetime
import logging
import azure.functions as func
from azure.storage.blob import BlobServiceClient

from .wunderground_data import *
from .consolidate_weater_observation_csv_files import *
from .daily_rainfall_total import *


def main(mytimer: func.TimerRequest) -> None: 
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()
    
    connection_string = 'DefaultEndpointsProtocol=https;AccountName=weatherobservationdata;AccountKey=BqBTfTgRLh9df2dTAgjlNsBM6PlMO5pt/5H+dT0TB2gceX7ZXbxMbgvK6jqMl1bWIv+9sYzGgtWnU7Paz4GdAg==;EndpointSuffix=core.windows.net' #parser.parse_args().connection_string

    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    weather_obs(blob_service_client)
    daily_rainfall_total()
    consolidate(blob_service_client)
    



    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
