import csv
import os.path
import sys
import json
from datetime import datetime
from collections import defaultdict
from pprint import pprint
from pyproj import Proj, transform

bngs = Proj(init='epsg:27700')
wgs84 = Proj(init='epsg:4326')

def traffic_counts_geojson(path, out_path):
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
        "name": "Road Traffic Counts",
        "description": "AADR Road Traffic Counts",
        "data": {
            "type": "FeatureCollection",
            "features": features
        }
    }
    json.dump(output, open(out_path, "w"))

if __name__ == "__main__":
    traffic_counts_geojson(sys.argv[1], sys.argv[2])
