###
###
#THIS FILE IS UNUSED NOW. I switched from explicit DDL sql to sqlalchemy Table definitions in tables.py
###
###

import json
from datetime import datetime

from common import get_cursor
from configuration import SCHEMA_NAME


def do_nothing():
    with get_cursor() as cursor:
        cursor.execute('set search_path to testschema')

def drop_activities_table():
    with get_cursor(SCHEMA_NAME) as cursor:
        cursor.execute("""
            DROP TABLE testtable;
            """
        )

def create_activities_table():
    with get_cursor(SCHEMA_NAME) as cursor:
        create_table_schema = """
            CREATE TABLE testtable (
            id                      serial PRIMARY KEY,
            vehicle_lon             double precision NOT NULL,
            vehicle_id              integer NOT NULL,
            direction_name          varchar(20) NOT NULL,
            vehicle_timestamp       integer NOT NULL,
            route_name              text NOT NULL,
            vehicle_bearing         integer NOT NULL,
            route_id                varchar(20) NOT NULL,
            trip_name               text NOT NULL,
            trip_headsign           text NOT NULL,
            vehicle_lat             double precision NOT NULL,
            trip_id                 varchar(40),
            timestamp               timestamp with time zone NOT NULL,
            _created                timestamp with time zone NOT NULL
        )
        """
        print (create_table_schema)
        cursor.execute(create_table_schema)

def create_unique_index():
    with get_cursor('testschema') as cursor:
        cursor.execute("""
            CREATE UNIQUE INDEX trip_id_timestamp ON testtable (trip_id, timestamp);
        """)


args = {
    "vehicle_lon": -71.06322,
    "vehicle_id": 3812,
    "direction_name": "Eastbound",
    "vehicle_timestamp": 1462415973,
    "route_name": "Green Line B",
    "vehicle_bearing": 30,
    "route_id": "Green-B",
    "trip_name": "B train from Boston College to Park Street",
    "trip_headsign": "Park Street",
    "vehicle_lat": 42.35554,
    "trip_id": "3812_1462415400_1",
}

bad_args = {
    "vehicle_lon": -71.06322,
    "vehicle_id": 'BAD ARG',
    "direction_name": "Eastbound",
    "vehicle_timestamp": 1462415973,
    "route_name": "Green Line B",
    "vehicle_bearing": 30,
    "route_id": "Green-B",
    "trip_name": "B train from Boston College to Park Street",
    "trip_headsign": "Park Street",
    "vehicle_lat": 42.35554,
    "trip_id": "3812_1462415400_1",
}

# {
#     "_id": {
#         "$oid": "572ab2f7fb8a351317c0299c"
#     },
#     "vehicle_lon": -71.06322,
#     "vehicle_id": 3812,
#     "direction_name": "Eastbound",
#     "vehicle_timestamp": 1462415973,
#     "route_name": "Green Line B",
#     "vehicle_bearing": 30,
#     "route_id": "Green-B",
#     "trip_name": "B train from Boston College to Park Street",
#     "trip_headsign": "Park Street",
#     "vehicle_lat": 42.35554,
#     "trip_id": "3812_1462415400_1",
#     "timestamp": {
#         "$date": "2016-05-05T02:41:59.569Z"
#     },
#     "__v": 0
# }
