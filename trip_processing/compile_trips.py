from itertools import groupby

import logging

from common import db_session
from observations.models import TrainActivity
from observations.observations import get_observations_since
from trip_processing.tables import Trip

logger = logging.getLogger(__name__)

def process_trips_from_observations():
    observations: [TrainActivity]
    observations, _ = get_observations_since()
    grouped_by_trip_id = groupby(sorted(observations, key=lambda x: x.trip_id), key=lambda x: x.trip_id)
    new_trip_ids = []
    existing_trip_ids = []
    with db_session() as session:
        for trip_id, group in grouped_by_trip_id:
            existing_trip = session.query(Trip).get(trip_id)
            if existing_trip:
                print('\n\nfound existing trip id {}'.format(trip_id))
                existing_trip_ids.append(existing_trip.trip_id)
                if existing_trip.trip_end:
                    print('found trip that ended and started again {}'.format(existing_trip.trip_id))
                #TODO set an updated_at field if the date of the last observation is after the current update_at field
            else:
                print('\n\nfound new trip id {}'.format(trip_id))
                new_trip_ids.append(trip_id)
                first_obs = sorted(group, key=lambda x: x.vehicle_timestamp)[0]
                group_data = {}
                group_data['trip_id'] = trip_id
                group_data['vehicle_id'] = first_obs.vehicle_id
                group_data['direction_name'] = first_obs.direction_name
                group_data['route_name'] = first_obs.route_name
                group_data['route_id'] = first_obs.route_id
                group_data['trip_name'] = first_obs.trip_name
                group_data['trip_headsign'] = first_obs.trip_headsign
                group_data['trip_start'] = first_obs.timestamp
                trip = Trip(**group_data)
                session.add(trip)
        num_trips_marked_as_complete = mark_completed_trips_from_obs(session, new_trip_ids + existing_trip_ids)
        print('Marked {} trips as complete'.format(num_trips_marked_as_complete))
    return new_trip_ids, existing_trip_ids

def mark_completed_trips_from_obs(session, trip_ids_from_observations):
    """Any trip ids which don't have values in the most recent set of observations can be set as completed"""
    print('checking {} observations for completed trips'.format(len(trip_ids_from_observations)))
    uncompleted = Trip.uncompleted(session)
    trips_with_no_observations = [trip for trip in uncompleted if trip.trip_id not in trip_ids_from_observations]
    for trip in trips_with_no_observations:
        trip.trip_end = TrainActivity.most_recent_for_trip_id(trip.trip_id).timestamp

    return len(trips_with_no_observations)

# def check_for_completed_trips():
#     with db_session() as session:
#         uncompleted_trips =

#TODO go through all uncompleted trips, and if their update_at date is longer ago than something, set them as completed

