import json
import requests
import geojson
import pprint
from utils import post_events

API_ROOT = 'http://localhost:8080/api'


def prepare_geojson(d):
    loc = d.get('geography')#already geojson
    geom = d.get('geometry')
    props = {'timestamp' : d.get('currentUpdateDateTime', None),
            'location' : d['location'],
            'severity' : d['severity'],
            'category' : d['category'],
            'subcategory' : d['subCategory']}
    if 'comments' in d.keys():
        props['comments'] = d.get('comments')
    if 'currentUpdate' in d.keys():
        props['currentUpdate'] = d.get('currentUpdate')
    feat1 = geojson.Feature(id=d['id'], geometry=loc, properties=props)
    feat2 = geojson.Feature(id=d['id'], geometry=geom, properties=props)
    return geojson.FeatureCollection([feat2])

def prepare_events(disruptions):
    e = {'name' : 'Road Disruptions',
        'description' : 'TfL Road Disruptions',
        'viewType' : 'LOCATION_AND_TRACK',
        }
    events = []
    for d in disruptions:
        event = {
            'timestamp' : d.get('currentUpdateDateTime', None),
            'featureCollection' : prepare_geojson(d)
        }
        events.append(event)
    e['events'] = events
    return e



if __name__=="__main__":
    r = requests.get('https://api-neon.tfl.gov.uk/Road/All/Disruption/')
    disruptions = r.json()

    events = prepare_events(disruptions)
    pprint.pprint(events)
    post_events('7777', events, API_ROOT)
