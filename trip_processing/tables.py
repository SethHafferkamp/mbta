from sqlalchemy import Table, Column, INTEGER, TEXT, TIMESTAMP, FLOAT, PrimaryKeyConstraint, text
from sqlalchemy.ext.declarative import declarative_base

from LatLon import Miles
from common import db_session
from configuration import metadata, SCHEMA_NAME

Base = declarative_base(metadata=metadata)

class Trip(Base):
    __tablename__ = 'trip'
    __table_args__ = (
        {"schema": SCHEMA_NAME},
        # PrimaryKeyConstraint('trip_id', name='trip_pk')
    )
    trip_id = Column('trip_id', TEXT, nullable=False, primary_key=True)
    vehicle_id = Column('vehicle_id', INTEGER, nullable=False)
    direction_name = Column('direction_name', TEXT, nullable=False)
    route_name = Column('route_name', TEXT, nullable=False)
    route_id = Column('route_id', TEXT, nullable=False)
    trip_name = Column('trip_name', TEXT, nullable=False)
    trip_headsign = Column('trip_headsign', TEXT, nullable=False)
    trip_start = Column('trip_start', TIMESTAMP, nullable=False)
    trip_end = Column('trip_end', TIMESTAMP, nullable=True)
    distance = Column('distance', FLOAT, nullable=True)
    _created = Column('_created', TIMESTAMP, server_default=text('now()'))

    def set_distance(self, miles: Miles):
        self.distance = miles

    @classmethod
    def all(cls, session):
        return session.query(cls).all()

    @classmethod
    def by_id(cls, session, trip_id):
        return session.query(cls).get(trip_id)

    @classmethod
    def uncompleted(cls, session):
        return session.query(Trip).filter(Trip.trip_end is None)
#
#
# train_datapoint = Table('train_datapoint', metadata,
#                         Column('trip_id', TEXT, nullable=False),
#                         Column('vehicle_id', INTEGER, nullable=False),
#                         Column('vehicle_lon', FLOAT, nullable=False),
#                         Column('vehicle_lat', FLOAT, nullable=False),
#                         Column('vehicle_bearing', INTEGER, nullable=False),
#                         Column('timestamp', TIMESTAMP, nullable=False),
#                         Column('_created', TIMESTAMP, server_default=text('now()')),
#                         schema=SCHEMA_NAME
#                         )
#
