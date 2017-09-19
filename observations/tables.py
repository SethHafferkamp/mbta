from datetime import datetime

from sqlalchemy import Table, MetaData, Column, INTEGER, FLOAT, TEXT, TIMESTAMP, text, PrimaryKeyConstraint
from sqlalchemy.orm import mapper

from configuration import SCHEMA_NAME
from .models import TrainActivity
from configuration import metadata


train_activity = Table('train_activity', metadata,
                       Column('vehicle_lon', FLOAT, nullable=False),
                       Column('vehicle_lat', FLOAT, nullable=False),
                       Column('vehicle_id', INTEGER, nullable=False),
                       Column('direction_name', TEXT, nullable=False),
                       Column('vehicle_timestamp', INTEGER, nullable=False),
                       Column('route_name', TEXT, nullable=False),
                       Column('vehicle_bearing', INTEGER, nullable=False),
                       Column('route_id', TEXT, nullable=False),
                       Column('trip_name', TEXT, nullable=False),
                       Column('trip_headsign', TEXT, nullable=False),
                       Column('trip_id', TEXT, nullable=False),
                       Column('timestamp', TIMESTAMP, nullable=False),
                       Column('_created', TIMESTAMP, server_default=text('now()')),
                       PrimaryKeyConstraint('trip_id', 'timestamp', name='train_activity_pk'),
                       schema=SCHEMA_NAME
                       )


mapper(TrainActivity, train_activity)




#IntegrityError duplicate key value violates unique constraint "route_id_timestamp"
def insert_activity(**kwargs):
    from common import get_cursor
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
