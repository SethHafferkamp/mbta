#!/usr/bin/env python2
from typing import List

import logging
import datetime

from trip_processing.tables import Trip



from common import get_cursor
from configuration import SCHEMA_NAME

logger = logging.getLogger(__name__)

def get_trip_ids_between(session, start_datetime: datetime, end_datetime: datetime) -> List[int]:
    return session.query(Trip).filter(Trip.trip_start > start_datetime).filter(Trip.trip_start < end_datetime).with_entities(Trip.trip_id).all()

def get_trips_between(session, start_datetime: datetime, end_datetime: datetime):
    trip_ids = get_trip_ids_between(session, start_datetime, end_datetime)



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



# def get_completed_trips_for_today():
#     sql = """
#     SELECT * FROM testtable
#     WHERE


if __name__ == '__main__':
    get_current_data()