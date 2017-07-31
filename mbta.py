#!/usr/bin/env python2
import requests
import logging
import datetime
from LatLon import LatLon

MILE_PER_KM = 0.621371
KM_PER_MILE = 1.60934

from common import get_cursor
from configuration import SCHEMA_NAME

logger = logging.getLogger(__name__)


def get_all_trip_ids():
    with get_cursor(SCHEMA_NAME) as cursor:
        sql ="""
        SELECT DISTINCT on(trip_id) trip_id, timestamp FROM testtable
        WHERE char_length(trip_id) < 7
        """
        cursor.execute(sql)
        for result in cursor.fetchall():
            yield result['trip_id']

def get_trip_by_id(trip_id):
    with get_cursor(SCHEMA_NAME) as cursor:
        sql = """
        SELECT * FROM testtable
        WHERE trip_id=%(trip_id)s
        """
        cursor.execute(sql, dict(trip_id=trip_id))
        return [dict(row) for row in cursor.fetchall()]

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

def get_delta_miles(lat1, lon1, lat2, lon2):
    return LatLon(lat1, lon1).distance(LatLon(lat2, lon2)) * MILE_PER_KM

# def get_completed_trips_for_today():
#     sql = """
#     SELECT * FROM testtable
#     WHERE


if __name__ == '__main__':
    get_current_data()