import requests
import json
from pprint import pprint
import csv
import time
import psycopg2
from collections import defaultdict, OrderedDict
import geojson
import datetime
import utils
import shapely.geometry as SG
from shapely.geometry import LineString

# APP_ID="4abd99df"
# APP_KEY="0f76ba70a21836b0991d192dceae511b"
APP_ID = "860e7675"
APP_KEY = "1d36a20279e6ac727ddfdcaeba2e97ea"

FIELDS = [
        "stationName",
        "destinationName",
        "expectedArrival",
        "naptanId",
        "vehicleId",
        "destinationNaptanId",
        "direction",
        "timestamp",
        "timeToStation",
        "currentLocation",
        "id",
        "bearing",
        "towards",
        "lineId",
        "lineName"
]

def request(path, **kwargs):
    r = requests.get("https://api.tfl.gov.uk{path}?app_id={app_id}&app_key={app_key}".format(path=path, app_id=APP_ID, app_key=APP_KEY))
    return r.json()

def lookup_station(station_id, station_locs={}):
    if station_id not in stations_locs.keys():
        r = request("/StopPoint/{}".format(station_id))
        station_locs[station_id] = (r['lat'], r['lon'])
    return station_locs[station_id]

def lookup_line_path(line_id, direction):
    r = request("/Line/{}/Route/Sequence/{}".format(line_id, direction))
    assert len(r['lineStrings']) == 1
    assert len(json.loads(r['lineStrings'][0])) == 1
    return LineString(json.loads(r['lineStrings'][0])[0])

def lookup_line(line_id):
    stops_direction = {}
    for direction in ['inbound', 'outbound']:
        r = request("/Line/{}/Route/Sequence/{}".format(line_id, direction))
        stop_points =  r['stopPointSequences'][0]
        stops = OrderedDict()
        for stop in stop_points['stopPoint']:
            stops[stop['id']] = (stop['name'], stop['lat'], stop['lon'])
        stops_direction[direction] = stops
    return stops_direction

def fetch_arrival_predictions(line):
    bus_arrivals = {}
    r = request("/line/{0}/arrivals".format(line))
    for arrival in r:
        bus_id = arrival['vehicleId']
        if bus_id in bus_arrivals.keys():
            bus_arrivals[bus_id].append(arrival)
        else:
            bus_arrivals[bus_id] = [arrival]
    return {bus_id: min(arrivals, key=lambda k: k['timeToStation']) for bus_id, arrivals in bus_arrivals.items()}

def previous_stop(bus_arrival, line_info):
    """Returns the previous stop to the current stop for the line."""
    next_stop = bus_arrival['naptanId']
    line_direction = line_info[bus_arrival['direction']]
    next_index = list(line_direction.keys()).index(next_stop)
    return list(line_direction.values())[next_index-1]

def current_stop(bus_arrival, line_info):
    current_stop = bus_arrival['naptanId']
    line_direction = line_info[bus_arrival['direction']]
    try:
        return line_direction[current_stop]
    except KeyError as e:
        print(e, line_direction)
        return None

def current_stops(bus_arrivals, line_info, current):
    for bus_id, bus_arrival in bus_arrivals.items():
            current[bus_id] = current_stop(bus_arrival, line_info)
    return current

def prepare_geojson(bus_id, pos):
    bus_feature = geojson.Feature(id=bus_id, geometry=pos, properties={'registration':bus_id})
    return bus_feature

def get_position(current, next_pos, proportion):
    segment = LineString(((current[2], current[1]), (next_pos[2], next_pos[1])))
    return segment.interpolate(distance=proportion, normalized=True)

def time_to_station(bus_arrivals, time_to_dest):
    """Returns total time estimated to next stop for each bus ID."""
    for bus_id, bus_arrival in bus_arrivals.items():
        new_prediction = bus_arrival['timeToStation']
        if (bus_id not in time_to_dest.keys()) or (time_to_dest[bus_id] <= new_prediction):
            time_to_dest[bus_id] = new_prediction
    return time_to_dest

def estimate_to_station(bus_arrivals, next_stop, line, eta, interpol_dt):
    for bus_id, bus_arrival in bus_arrivals.items():
        new_prediction = bus_arrival['timeToStation']
        #print(bus_id, next_stop[bus_id], current_stop(bus_arrival, line_info))
        if bus_id in eta.keys():
            if eta[bus_id] > new_prediction:
                eta[bus_id] = new_prediction
            elif (eta[bus_id] < new_prediction and
            next_stop[bus_id] != current_stop(bus_arrival, line_info)):
                eta[bus_id] = new_prediction
            elif eta[bus_id] > interpol_dt:
                eta[bus_id] = eta[bus_id] - interpol_dt
            else:
                eta[bus_id] = 0
        else:
            eta[bus_id] = new_prediction
    return eta


def prepare_event(line_info, bus_arrivals, time_to_dest, eta, path):
    bus_place = {}
    collection = []
    for bus_id, bus_arrival in bus_arrivals.items():
        try:
            previous = previous_stop(bus_arrival, line_info)
            current = current_stop(bus_arrival, line_info)
            proportion = eta[bus_id] / time_to_dest[bus_id]
            pos = get_position(current, previous, proportion)
            pos = path.interpolate(path.project(pos, normalized=True),normalized=True)#attempt to get it on the line
            collection.append(prepare_geojson(bus_id, pos))
            #print(bus_id, eta[bus_id], bus_arrival['timeToStation'], time_to_dest[bus_id])
        except Exception as e:
            print(e)

    e = {'name' : "Buses",
        'description' : "buses",
        'icon' : 'bus',
        'attribution' : 'TfL',
        'viewType' : 'MOST_RECENT',
        'events' : [{'timestamp' : 0,
                    'featureCollection' : geojson.FeatureCollection(collection)}]}
    utils.post_events('buses', e, 'http://localhost:8080/api')

def post_line_test(line):
    API_ROOT='http://localhost:8080/api'
    test = {'name' : 'busroute',
        'description' : 'buses are noob',
        'data' : geojson.FeatureCollection([geojson.Feature(id='test123', geometry=line)])
        }
    pprint(test)
    r = requests.put("{}/import/geojson".format(API_ROOT), json=test)
    return r

if __name__ == "__main__":
    LINE_ID = '88'
    line_info = lookup_line(LINE_ID)
    path = lookup_line_path(LINE_ID, 'inbound')
    time_to_dest = {}#tracks total time to next dest
    bus_arrivals = fetch_arrival_predictions(LINE_ID)
    eta = {}#tracks estimated time to next dest
    going_towards = {}
    going_towards = current_stops(bus_arrivals, line_info, going_towards)
    interpol_dt = 2
    api_dt = 6
    while True:
        if api_dt == 6:
            bus_arrivals = fetch_arrival_predictions(LINE_ID)
            time_to_dest = time_to_station(bus_arrivals, time_to_dest)
            api_dt = 0
        eta = estimate_to_station(bus_arrivals, going_towards, line_info, eta, interpol_dt)
        going_towards = current_stops(bus_arrivals, line_info, going_towards)
        prepare_event(line_info, bus_arrivals, time_to_dest, eta, path)
        api_dt += interpol_dt
        time.sleep(interpol_dt)
