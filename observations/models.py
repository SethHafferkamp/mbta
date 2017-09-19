from sqlalchemy import desc


class TrainActivity():
    def __init__(self, *, vehicle_lon, vehicle_lat, vehicle_id, vehicle_timestamp, route_name, vehicle_bearing, route_id, trip_name, trip_headsign, trip_id, direction_name, direction_id, vehicle_label, timestamp):
        self.vehicle_lon = vehicle_lon
        self.vehicle_lat = vehicle_lat
        self.vehicle_id = vehicle_id
        self.vehicle_timestamp = vehicle_timestamp
        self.route_name = route_name
        self.vehicle_bearing = vehicle_bearing
        self.route_id = route_id
        self.trip_name = trip_name
        self.trip_headsign = trip_headsign
        self.trip_id = trip_id
        self.direction_name = direction_name
        self.direction_id = direction_id
        self.vehicle_label = vehicle_label
        self.timestamp = timestamp

    @classmethod
    def for_trip_id(cls, session, trip_id):
        return session.query(TrainActivity).filter(TrainActivity.trip_id == trip_id).all()

    @classmethod
    def most_recent_for_trip_id(cls, session, trip_id):
        return session.query(TrainActivity).filter(TrainActivity.trip_id == trip_id).order_by(desc(TrainActivity.vehicle_timestamp)).limit(1)
