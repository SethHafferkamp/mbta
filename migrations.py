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

#IntegrityError duplicate key value violates unique constraint "route_id_timestamp"
def insert_activity(**kwargs):
    with get_cursor(SCHEMA_NAME) as cursor:
        kwargs['timestamp'] = datetime.fromtimestamp(kwargs.get('vehicle_timestamp'))
        kwargs['_created'] = datetime.now()
        # print('inserting: ' + json.dumps(kwargs))
        INSERT_ACTIVITY_SQL = """
            INSERT INTO testtable (
            vehicle_lon,
            vehicle_id,
            direction_name,
            vehicle_timestamp,
            route_name,
            vehicle_bearing,
            route_id,
            trip_name,
            trip_headsign,
            vehicle_lat,
            trip_id,
            timestamp,
            _created
            ) VALUES (
              %(vehicle_lon)s,
              %(vehicle_id)s,
              %(direction_name)s,
              %(vehicle_timestamp)s,
              %(route_name)s,
              %(vehicle_bearing)s,
              %(route_id)s,
              %(trip_name)s,
              %(trip_headsign)s,
              %(vehicle_lat)s,
              %(trip_id)s,
              %(timestamp)s,
              %(_created)s
            );"""
        try:
            cursor.execute(INSERT_ACTIVITY_SQL, kwargs)
        except Exception as e:
            print(e.message)
            return False
        return True

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
