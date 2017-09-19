import logging
from datetime import datetime

import requests

from common import db_session
from configuration import API_KEY
from .models import TrainActivity

BASE_V2_URL = 'http://realtime.mbta.com/developer/api/v2'

logger = logging.getLogger(__name__)

def format_mbta_request_url(api_key: str):
    return '{}/predictionsbyroutes?api_key={}&routes=Green-b&format=json'.format(BASE_V2_URL, api_key)


def get_and_insert_current_predictions_by_routes() -> (int, int):
    """Queries the MBTA and upserts an observation row for each datapoint
        Returns: (number of new rows inserted, number of rows upserted)
    """
    list_of_train_activities = get_current_predictions_by_routes()

    with db_session(autoflush=False, echo=True) as session:
        with session.no_autoflush:
            for activity in list_of_train_activities:
                session.merge(activity)
        new_records_count = len(session.new)

    updated_records_count = len(list_of_train_activities) - new_records_count
    return new_records_count, updated_records_count


def get_current_predictions_by_routes(api_key=API_KEY) -> [TrainActivity]:
    """Queries the MBTA api and returns a list with an activity data point for each vehicle"""
    # r = requests.get('http://realtime.mbta.com/developer/api/v2/predictionsbyroute?api_key=wX9NwuHnZU2ToO7GmGR9uw&route=Green-B&direction=1&format=json')
    request_url = format_mbta_request_url(api_key)
    r = requests.get(request_url)
    json = r.json()['mode'][0]['route'][0]

    # top level data for each datapoint
    route_id = json.get('route_id')
    route_name = json.get('route_name')
    eastbound = json.get('direction')[1]
    direction_id = eastbound.get('direction_id')
    direction_name = eastbound.get('direction_name')

    trips = eastbound.get('trip')

    list_of_train_activities: [TrainActivity] = []
    for trip in trips:
        trip_data = trip.get('vehicle')
        trip_data['route_id'] = route_id
        trip_data['route_name'] = route_name
        trip_data['direction_id'] = direction_id
        trip_data['direction_name'] = direction_name
        trip_data.update({'trip_id': trip.get('trip_id')})
        trip_data.update({'trip_name': trip.get('trip_name')})
        trip_data.update({'trip_headsign': trip.get('trip_headsign')})
        trip_data.update({'timestamp': datetime.fromtimestamp(int(trip_data.get('vehicle_timestamp')))})
        list_of_train_activities.append(TrainActivity(**trip_data))

    return list_of_train_activities


def get_observations_since(high_water_timestamp=0) -> ([TrainActivity], int):
    with db_session() as session:
        observations = session.query(TrainActivity).filter(TrainActivity.vehicle_timestamp > high_water_timestamp)
        all_obs = observations.all()

        if not all_obs:
            return None, None
        new_high_water_mark = max([obs.vehicle_timestamp for obs in observations])
        session.expunge_all()
    return all_obs, new_high_water_mark

