from typing import Optional

from LatLon import get_delta_miles, Miles
from observations.models import TrainActivity
from trip_processing.tables import Trip
from utils.functional import pairwise


def set_trip_as_completed(session, trip_id) -> Optional[int]:
    #Todo add check for completed trip
    trip = Trip.by_id(session, trip_id)
    full_trip_observations = sorted(TrainActivity.for_trip_id(session, trip_id), key=lambda obs: obs.timestamp)
    if not full_trip_observations:
        return None
    trip.set_distance(calculate_trip_distance(full_trip_observations))
    trip.trip_end = full_trip_observations[-1].timestamp
    if not trip.trip_start:
        trip.trip_start = full_trip_observations[0].timestamp

def calculate_trip_distance(full_trip_observations: [TrainActivity]) -> Miles:
    total_distance: Miles = 0.0
    for obs1, obs2 in pairwise(full_trip_observations):
        total_distance += distance_between_activities(obs1, obs2)
    return total_distance

def distance_between_activities(obs1: TrainActivity, obs2: TrainActivity) -> Miles:
    return get_delta_miles(
        obs1.vehicle_lat,
        obs1.vehicle_lon,
        obs2.vehicle_lat,
        obs2.vehicle_lon)