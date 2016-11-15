import csv
import os.path
import sys
import json
import requests

from dilectic.utils import *
from dilectic.actions import *

@task
def disruptions(cfg):
    def prepare_features(d):
        loc = d.get('geography')#already geojson
        geom = d.get('geometry')
        props = {
            'timestamp' : d.get('currentUpdateDateTime', None),
            'location' : d['location'],
            'severity' : d['severity'],
            'category' : d['category'],
            'subcategory' : d['subCategory']
        }
        if 'comments' in d.keys():
            props['comments'] = d.get('comments')
        if 'currentUpdate' in d.keys():
            props['currentUpdate'] = d.get('currentUpdate')
        # feat1 = geojson.Feature(id=d['id'], geometry=loc, properties=props)
        feat2 = geojson.Feature(id=d['id'], geometry=geom, properties=props)
        return [feat2]

    def prepare_geojson(disruptions):
        features = []
        for d in disruptions:
            features = features + prepare_features(d)
        return geojson.FeatureCollection(features)

    def generate_geojson(out_path):
        r = requests.get('https://api-neon.tfl.gov.uk/Road/All/Disruption/')
        output = prepare_geojson(r.json())
        json.dump(output, open(out_path, "w"), indent=1)

    dest = os.path.join(cfg.derived_dir, "disruptions.geojson")
    return {
        "actions": [lambda: generate_geojson(dest)],
        "targets": [dest]
    }
