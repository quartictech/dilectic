import requests
import json
import geojson
import pprint


def prepare_geojson(d):
    loc = d.get('geography')#already geojson
    geom = d.get('geometry')
    event = {'timestamp' : d['currentUpdateDateTime'],
            'severity' : d['severity'],
            'category' : d['category'],
            'subcategory' : d['subCategory'],
            'comments' : d.get('comments'),
            'currentUpdate' : d.get('currentUpdate')}
    feat1 = geojson.Feature(id=d['id'], geometry=loc, properties=event)
    feat2 = geojson.Feature(id=d['id'], geometry=geom, properties=event)
    return geojson.FeatureCollection([feat1, feat2])

def prepare_events(disruptions):
    e = {'name' : 'Road Disruptions',
        'description' : 'TfL Road Disruptions'}
    events = []
    for d in disruptions:
        events.append(prepare_geojson(d))
    e['events'] = events
    return e



if __name__=="__main__":
    r = requests.get('https://api-neon.tfl.gov.uk/Road/All/Disruption/')
    disruptions = r.json()

    events = prepare_events(disruptions)
    pprint.pprint(events)
