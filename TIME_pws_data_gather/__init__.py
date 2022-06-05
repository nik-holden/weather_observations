import sys
sys.path.append('/')

import logging
import azure.functions as func
import .config

from .wunderground_data import *
from .consolidate_weater_observation_csv_files import *
from .daily_rainfall_total import *


def main(mytimer: func.TimerRequest) -> None: 
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    blob_service_client = config.blob_ser_client()

    weather_obs(blob_service_client)
    daily_rainfall_total()
    consolidate(blob_service_client)
    



    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
