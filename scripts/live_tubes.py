import requests
import time
import datetime
import calendar
import configparser
from pprint import pprint
import geojson
import psycopg2
import sys

conn = psycopg2.connect("dbname=postgres user=postgres")
cur = conn.cursor()
cur.execute("SELECT * FROM naptan;")
config = configparser.ConfigParser()
config.read('tfl.conf')

def get_naptan_locs(db_cursor, naptan):
    query = db_cursor.mogfiry("SELECT * FROM naptan WHERE stopareacode IN %s;",
                        naptan)
    pass

app_id = config['DEFAULT']['AppID']
app_key = config['DEFAULT']['AppKey']

api_root = 'https://api.tfl.gov.uk'

s = requests.Session()

lines = ['bakerloo', 'central', 'circle', 'district', 'hammersmith-city',
            'metropolitan', 'jubilee', 'northern', 'piccadilly', 'victoria',
            'waterloo-city']
lines_data = s.get(api_root + '/Line/' + ','.join(lines), params = {'app_id' : app_id, 'app_key' : app_key})

def get_stop_locations():
    stations  = {}
    for l in lines:
        stop_points = s.get(api_root + '/Line/' + l + '/StopPoints').json()
        for stop in stop_points:
            stations[stop['stationNaptan']]  = geojson.Point((stop['lat'], stop['lon']))
    return stations

def update_locs(loc_train):
    resp = s.get(api_root + '/Line/' + ','.join(lines) + '/Arrivals')
    arrivals = resp.json()
    for a in arrivals:
        if 'naptanId' in a.keys():
            current_loc = a['naptanId']
            loc_train[current_loc] = a['vehicleId']
    return loc_train

def make_feature_collection(stations):
    features = []
    t = calendar.timegm(time.gmtime())
    for i, pos in stations:
        features.append(geojson.Feature(id=i, geometry=pos, properties={'timestamp':t}))
    collection = geojson.FeatureCollection(features)
    return collection


if __name__ == "__main__":

    stations = get_stop_locations()
    assert len(stations.keys()) == 270 #check that we have all the stations
    loc_train = {}#for every station, which train
    new_loc_train = {}
    while True:
        new_loc_train = update_locs(new_loc_train)
        update_stations = []
        for k, v in new_loc_train.items():
            if k in loc_train.keys() and v != loc_train[k]:
                update_stations.append((k, stations[k]))
        updates = make_feature_collection(update_stations)
        pprint(updates)
        loc_train = new_loc_train.copy()
        time.sleep(9)
