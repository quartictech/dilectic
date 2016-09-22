import requests
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
# pprint.pprint(lines_data.json())

resp = s.get(api_root + '/Line/' + ','.join(lines) + '/Arrivals')
arrivals = resp.json()
destination_loc_ids = []
current_loc_ids = []
loc_train = {}#for every station, which train
for a in arrivals:
    if 'destinationNaptanId' in a.keys():
        destination = a['destinationNaptanId']
        destination_loc_ids.append(destination)
    if 'naptanId' in a.keys():
        current_loc = a['naptanId']
        current_loc_ids.append(current_loc)
        pprint.pprint(a)
        loc_train[current_loc] = a['vehicleId']
print(loc_train)
