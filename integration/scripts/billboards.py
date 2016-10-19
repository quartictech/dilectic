import csv
import os.path
import sys
import json

STREETVIEW_API="http://maps.googleapis.com/maps/api/streetview"
STREETVIEW_API_KEY="AIzaSyDtJsTGvARpNJBAC4C1eAARnk2djfvepzo"

def signkick_geojson(path, out_path):
    features = []
    with open(path) as infile:
        rdr = csv.DictReader(infile)
        for row in rdr:
            feature = {
                "type": "Feature",
                "id":  row["identifier"],
                "geometry": {
                    "type": "Point",
                    "coordinates": [row["lon"], row["lat"]]
                },
                "properties": {
                    "Name": row["title"],
                    "Is Digital": row["isdigital"],
                    "Price": row["postingcyclepricefrom"],
                    "street_view": "{0}?size=640x640&location={1},{2}&sensor=false&key={3}".format(STREETVIEW_API,
                        row["lat"], row["lon"], STREETVIEW_API_KEY)
                }
            }
            if row["description"] != "-":
                feature["properties"]["Description"] = row["description"]
            features.append(feature)
            print(row)

    output = {
        "name": "Outdoor Advertising Stock London",
        "description": "Digital billboards, panels and signs in London",
        "data": {
            "type": "FeatureCollection",
            "features": features
        }
    }
    json.dump(output, open(out_path, "w"), indent=1)
if __name__ == "__main__":
    signkick_geojson(sys.argv[1], sys.argv[2])
