import requests
import time
import datetime
import calendar
import configparser
from pprint import pprint
import geojson
import sys

config = configparser.ConfigParser()
config.read('tfl.conf')

app_id = config['DEFAULT']['AppID']
app_key = config['DEFAULT']['AppKey']

api_root = 'https://api.tfl.gov.uk'

s = requests.Session()

lines = ['bakerloo', 'central', 'circle', 'district', 'hammersmith-city',
            'metropolitan', 'jubilee', 'northern', 'piccadilly', 'victoria',
            'waterloo-city']
lines_data = s.get(api_root + '/Line/' + ','.join(lines), params = {'app_id' : app_id, 'app_key' : app_key})

def get_stop_name(naptan):
    name = s.get(api_root + '/StopPoint/' + naptan)
    return name.json()['commonName']


def get_stop_locations():
    stations  = {}
    for l in lines:
        stop_points = s.get(api_root + '/Line/' + l + '/StopPoints').json()
        for stop in stop_points:
            stations[stop['stationNaptan']]  = geojson.Point((stop['lon'], stop['lat']))
    return stations

def get_stop_trains():
    loc_train = {}
    resp = s.get(api_root + '/Line/' + ','.join(lines) + '/Arrivals')
    arrivals = resp.json()
    for a in arrivals:
        if 'naptanId' in a.keys():
            current_loc = a['naptanId']
            if current_loc in loc_train.keys():
                loc_train[current_loc].add(a['vehicleId'])
            else:
                loc_train[current_loc] = set([a['vehicleId']])
    return loc_train

def make_feature_collection(stations):
    features = []
    t = calendar.timegm(time.gmtime())
    for i, pos, ntrains in stations:
        get_stop_name(i)
        features.append(geojson.Feature(id=i, geometry=pos, properties={'timestamp':t, 'ntrains':ntrains, 'name':get_stop_name(i)}))
    collection = geojson.FeatureCollection(features)
    return collection

def post_to(feature_collection):
    r = requests.post('http://localhost:8080/api/layer/live/1234', json=feature_collection)
    print(r)
    return


if __name__ == "__main__":

    stations = get_stop_locations()
    assert len(stations.keys()) == 270 #check that we have all the stations
    loc_train = {}#for every station, which train
    new_loc_train = {}
    while True:
        new_loc_train = get_stop_trains()
        update_stations = []
        for station, trains in new_loc_train.items():
            if station in loc_train.keys():
                new_trains = (trains - loc_train[station])
                if len(new_trains) != 0:#the station sees trains arrive
                    update_stations.append((station, stations[station], len(new_trains)))
        updates = make_feature_collection(update_stations)
        pprint(updates)
        post_to(updates)
        loc_train = new_loc_train.copy()
        time.sleep(9)
