import csv
import os.path
import sys
import json
from datetime import datetime
from collections import defaultdict
from pprint import pprint
from pyproj import Proj, transform

from dilectic.utils import *
from dilectic.actions import *

bngs = Proj(init='epsg:27700')
wgs84 = Proj(init='epsg:4326')

@task
def traffic_counts_geojson(cfg):
    def make(path, out_path):
        features = []
        counts = defaultdict(lambda: defaultdict(list))
        details = {}
        with open(path) as infile:
            rdr = csv.DictReader(infile)
            for row in rdr:
                timestamp = int(datetime(int(row["AADFYear"]), 1, 1).strftime("%s")) * 1000
                for key, value in row.items():
                    if key.startswith("Fd"):
                        counts[row["CP"]][key].append({"timestamp": timestamp, "value": value})
                details[row["CP"]] = {"road": row["Road"], "eastings": row["S Ref E"], "northings": row["S Ref N"]}

        for cp, detail in details.items():
            lat_long = transform(bngs, wgs84, float(detail["eastings"]), float(detail["northings"]))
            feature = {
                "type": "Feature",
                "id": row["CP"],
                "geometry": {
                    "type": "Point",
                    "coordinates": lat_long,
                },
                "properties": {
                    "Road": detail["road"],
                    "name": "{0} ({1})".format(detail["road"], cp),
                }
            }
            for key in counts[cp].keys():
                feature["properties"][key] = {
                    "type": "timeseries",
                    "series": sorted(counts[cp][key], key=lambda x: x["timestamp"])
                    }
            features.append(feature)

        output = {
            "type": "FeatureCollection",
            "features": features
        }
        json.dump(output, open(out_path, "w"))

    source = os.path.join(cfg.derived_dir, "gb-road-traffic-counts/AADF-data-major-roads.csv")
    dest = os.path.join(cfg.derived_dir, "gb-road-traffic-counts/AADF-data-major-roads.geojson")
    return {
        "actions": [lambda: make(source, dest)],
        "targets": [dest],
        "file_dep": [source]
    }
