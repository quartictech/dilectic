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
    for bus in r:
        if bus['vehicleId'] in bus_arrivals.keys():
            bus_arrivals[bus['vehicleId']].append(bus)
        else:
            bus_arrivals[bus['vehicleId']] = [bus]
    return {bus: sorted(arrivals, key=lambda k: k['timeToStation']) for bus, arrivals in bus_arrivals.items()}
    # json.dump(r, open("{0}.json".format(line), "w"), indent=1)

def previous_stop(bus, line_info):
    next_stop = bus['naptanId']
    line_direction = line_info[bus['direction']]
    next_index = list(line_direction.keys()).index(next_stop)
    return list(line_direction.values())[next_index-1]

def current_stop(bus, line_info):
    current_stop = bus['naptanId']
    line_direction = line_info[bus['direction']]
    return line_direction[current_stop]

def prepare_geojson(bus, pos):
    bus_feature = geojson.Feature(id=bus, geometry=pos,
        properties={'registration':bus})
    return bus_feature

def get_position(current, next_pos, proportion):
    segment = LineString(((current[2], current[1]), (next_pos[2], next_pos[1])))
    return segment.interpolate(distance=proportion, normalized=True)

def time_to_station(line, time_to_dest):
    buses = fetch_arrival_predictions(line)
    for bus, bus_deets in buses.items():
        if bus in time_to_dest.keys():
            if time_to_dest[bus] <= bus_deets[0]['timeToStation']:
                time_to_dest[bus] = bus_deets[0]['timeToStation']
            else:
                continue
        else:
            time_to_dest[bus] = bus_deets[0]['timeToStation']
    return time_to_dest

def estimate_to_station(line, eta, dt):
    buses = fetch_arrival_predictions(line)
    for bus, bus_deets in buses.items():
        if bus in eta.keys():
            if eta[bus] > bus_deets[0]['timeToStation']:
                eta[bus] = bus_deets[0]['timeToStation']
            else:
                if eta[bus] > dt:
                    eta[bus] = eta[bus] - dt
                else:
                    eta[bus] = 0
        else:
            eta[bus] = bus_deets[0]['timeToStation']
    return eta


def prepare_event(line, time_to_dest, eta, path):
    buses = fetch_arrival_predictions(line)
    bus_place = {}
    line_info = lookup_line(line)
    collection = []
    for bus, bus_stops in buses.items():
        try:
            previous = previous_stop(bus_stops[0], line_info)
            current = current_stop(bus_stops[0], line_info)
            proportion = eta[bus]/time_to_dest[bus]
            pos = get_position(current, previous, proportion)
            # pos = path.interpolate(path.project(pos, normalized=True))#attempt to get it on the line
            collection.append(prepare_geojson(bus, pos))
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

if __name__ == "__main__":
    LINE_ID = '88'

    path = lookup_line_path(LINE_ID, 'inbound')
    time_to_dest = {}#tracks total time to next dest
    eta = {}#tracks estimated time to next dest
    dt=5
    while True:
        time_to_dest = time_to_station(LINE_ID, time_to_dest)
        eta = estimate_to_station(LINE_ID, eta, dt)
        prepare_event(LINE_ID, time_to_dest, eta, path)
        time.sleep(dt)
