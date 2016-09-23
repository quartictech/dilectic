import requests
import time
import configparser
import pprint

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

def update_locs(loc_train):
    resp = s.get(api_root + '/Line/' + ','.join(lines) + '/Arrivals')
    arrivals = resp.json()
    for a in arrivals:
        if 'naptanId' in a.keys():
            current_loc = a['naptanId']
            loc_train[current_loc] = a['vehicleId']
    return loc_train

loc_train = {}#for every station, which train
new_loc_train = {}
while True:
    new_loc_train = update_locs(new_loc_train)
    for k, v in new_loc_train.items():
        if k in loc_train.keys() and v != loc_train[k]:
            print(new_loc_train[k])
    loc_train = new_loc_train.copy()
    time.sleep(9)
