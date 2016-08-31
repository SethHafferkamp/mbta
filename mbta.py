#!/usr/bin/env python2
import requests
import datetime

from common import get_cursor
from migrations import insert_activity


def get_current_data():
    # r = requests.get('http://realtime.mbta.com/developer/api/v2/predictionsbyroute?api_key=wX9NwuHnZU2ToO7GmGR9uw&route=Green-B&direction=1&format=json')
    r = requests.get('http://realtime.mbta.com/developer/api/v2/predictionsbyroutes?api_key=wX9NwuHnZU2ToO7GmGR9uw&routes=Green-b&format=json')
    json = r.json()['mode'][0]['route'][0]
    # return json
    route_id = json.get('route_id')
    route_name = json.get('route_name')

    eastbound = json.get('direction')[1]

    direction_id = eastbound.get('direction_id')
    direction_name = eastbound.get('direction_name')

    trips = eastbound.get('trip')

    # trip_data['datetime'] = datetime.datetime.fromtimestamp(float(trip_data.get('vehicle_timestamp')))


    data = []
    for trip in trips:
        trip_data = trip.get('vehicle')
        trip_data['route_id'] = route_id
        trip_data['route_name'] = route_name
        trip_data['direction_id'] = direction_id
        trip_data['direction_name'] = direction_name
        trip_data.update({'trip_id': trip.get('trip_id')})
        trip_data.update({'trip_name': trip.get('trip_name')})
        trip_data.update({'trip_headsign': trip.get('trip_headsign')})
        trip_data.update({'vehicle_timestamp': float(trip_data.get('vehicle_timestamp'))})
        data.append(trip_data)

# 	data = [trip.get('vehicle').update({'trip_id': trip.get('trip_id')}) for trip in trips]

    rows_inserted = 0
    duplicates = 0
    for d in data:
        if insert_activity(**d):
            rows_inserted +=1
        else:
            duplicates +=1
    print (str(rows_inserted) + ' rows inserted, ' + str(duplicates) + ' duplicates found')

        # a = requests.post('http://localhost:8000/api/v1/Trip', data=d);
    # return a
    # return data

if __name__ == '__main__':
    get_current_data()