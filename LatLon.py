from math import sin, cos, atan2, sqrt, radians
from typing import NewType

from gpxpy import geo

KM_PER_M = .001
M_PER_KM = 1000

MILE_PER_KM = 0.621371
KM_PER_MILE = 1.60934

Miles = NewType('Miles', float)
Meters = NewType('Meters', float)

def meters_to_miles(meters: Meters) -> Miles:
    return meters * KM_PER_M *  MILE_PER_KM

def miles_to_meters(miles) -> Meters:
    return miles / KM_PER_M / MILE_PER_KM

def distance_meters_from_lat_long(lat1, lon1, lat2, lon2):
    # approximate radius of earth in km
    R = 6373.0

    lat1_rad, lon1_rad, lat2_rad, lon2_rad = radians(lat1), radians(lon1), radians(lat2), radians(lon2)
    # lon1_rad = radians(lon1)
    # lat2_rad = radians(lat2)
    # lon2_rad = radians(lon2)

    dlon_rad = lon2_rad - lon1_rad
    dlat_rad = lat2_rad - lat1_rad

    a = sin(dlat_rad / 2) ** 2 + cos(lat1_rad) * cos(lat2_rad) * sin(dlon_rad / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    distance = R * c

    return distance

def haversine_distance_meters(lat1: float, lon1: float, lat2: float, lon2: float) -> Meters:
    return geo.haversine_distance(lat1, lon1, lat2, lon2)

def get_distance_deltas_miles_from_trip(trip):
    distance_delta = [0]
    trip[0]['delta_miles'] = 0
    for first, second in zip(trip, trip[1:]):
        distance_delta.append(get_delta_miles(
                                    first['vehicle_lat'],
                                    first['vehicle_lon'],
                                    second['vehicle_lat'],
                                    second['vehicle_lon']))
    return distance_delta

def get_delta_miles(lat1: float, lon1: float, lat2: float, lon2: float) -> Miles:
    return meters_to_miles(haversine_distance_meters(lat1, lon1, lat2, lon2))