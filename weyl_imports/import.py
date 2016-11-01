import requests
import sys
import yaml

from pprint import pprint

CATALOG_API_ROOT = "http://localhost:8090/api"

POSTGRES_URL = "jdbc:postgresql://localhost/postgres"
POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "dilectic"

if __name__ == "__main__":
    for config_file in sys.argv[1:]:
        print "Processing " + config_file + " ..."

        with open(config_file, "r") as stream:
            partial_config = yaml.load(stream)

        full_config = {
            "metadata": {
                "name": partial_config["name"],
                "description": partial_config["description"],
                "attribution": partial_config.get("attribution", "<< Unknown >>"),
            },
            "locator": {
                "type": "postgres",
                "url": POSTGRES_URL,
                "user": POSTGRES_USER,
                "password": POSTGRES_PASSWORD,
                "query": partial_config["query"]
            }
        }
        if "icon" in partial_config:
            full_config["metadata"]["icon"] = partial_config["icon"]

        r = requests.put(CATALOG_API_ROOT + "/datasets", json=full_config)


# TODO: still need to handle static geojson imports:

# curl -XPUT -H Content-Type:application/json $API_ROOT/import/geojson -d @../data/derived/signkick.json
# curl -XPUT -H Content-Type:application/json $API_ROOT/import/geojson -d @../data/derived/gb-road-traffic-counts/AADF-data-major-roads.json
# curl -XPUT -H Content-Type:application/json $API_ROOT/import/geojson -d @../data/derived/zoopla.json
#
# source $DIR/../integration/env/bin/activate
# python $DIR/../integration/scripts/live/disruptions_api.py
