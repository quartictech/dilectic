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

def estimate_to_station(bus_arrivals, next_stop, line_info, eta, interpol_dt):
    for bus_id, bus_arrival in bus_arrivals.items():
        new_prediction = bus_arrival['timeToStation']
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

def prepare_geojson(line_id, bus_id, pos):
    bus_feature = geojson.Feature(id=bus_id, geometry=pos, properties={
        'vehicle id': bus_id,
        'route': line_id
    })
    return bus_feature

def prepare_event(line_ids, line_info, bus_arrivals, time_to_dest, eta, path):
    collection = []

    for line_id in line_ids:
        for bus_id, bus_arrival in bus_arrivals[line_id].items():
            try:
                previous = previous_stop(bus_arrival, line_info[line_id])
                current = current_stop(bus_arrival, line_info[line_id])
                proportion = eta[line_id][bus_id] / time_to_dest[line_id][bus_id]

                pos = get_position(current, previous, proportion)
                pos = path[line_id].interpolate(path[line_id].project(pos, normalized=True),normalized=True)#attempt to get it on the line
                collection.append(prepare_geojson(line_id, bus_id, pos))
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
    API_DT = 10
    ANIMATION_DT = 3
    LINE_IDS = ['88', '15', '9']

    line_info = {}
    path = {}
    bus_arrivals = {}
    going_towards = {}
    eta = {}
    time_to_dest = {}
    for line_id in LINE_IDS:
        line_info[line_id] = lookup_line(line_id)
        path[line_id] = lookup_line_path(line_id, 'inbound')
        bus_arrivals[line_id] = fetch_arrival_predictions(line_id)
        going_towards[line_id] = current_stops(bus_arrivals[line_id], line_info[line_id], {})
        eta[line_id] = {}#tracks estimated time to next dest
        time_to_dest[line_id] = {} #tracks total time to next dest

    t = 0
    while True:
        if t >= API_DT:
            t -= API_DT
            for line_id in LINE_IDS:
                bus_arrivals[line_id] = fetch_arrival_predictions(line_id)
                time_to_dest[line_id] = time_to_station(bus_arrivals[line_id], time_to_dest[line_id])

        for line_id in LINE_IDS:
            eta[line_id] = estimate_to_station(bus_arrivals[line_id], going_towards[line_id], line_info[line_id], eta[line_id], ANIMATION_DT)
            going_towards[line_id] = current_stops(bus_arrivals[line_id], line_info[line_id], going_towards[line_id])

        prepare_event(LINE_IDS, line_info, bus_arrivals, time_to_dest, eta, path)

        t += ANIMATION_DT
        time.sleep(ANIMATION_DT)