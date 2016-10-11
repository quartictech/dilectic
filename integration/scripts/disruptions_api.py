import requests
import json


def prepare_geojson(disruption):
    coord = disruption['geography']['coordinates']
    print(coord)


if __name__=="__main__":
    r = requests.get('https://api-neon.tfl.gov.uk/Road/All/Disruption/')
    disruptions = r.json()

    for d in disruptions:
        print(d.keys())
        prepare_geojson(d)
        break
