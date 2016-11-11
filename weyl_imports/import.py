import argparse
import requests
import sys
import yaml

from pprint import pprint

CATALOG_API_ROOT = "http://localhost:8090/api"

POSTGRES_HOST = "localhost"
NGINX_HOST = "localhost"
NGINX_PORT = "80"
POSTGRES_PORT = "5432"
POSTGRES_DB = "postgres"
POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "dilectic"

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Register datasets with Catalogue.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("config_file", nargs="*", help="YAML config files")
    parser.add_argument("-c", "--catalogue_api_root", help="Catalogue API root URL", default=CATALOG_API_ROOT)
    parser.add_argument("-p", "--postgres_host", help="Postgres host", default=POSTGRES_HOST)
    parser.add_argument("-n", "--nginx_host", help="Nginx host", default=NGINX_HOST)
    parser.add_argument("-g", "--nginx_port", help="Nginx port", default=NGINX_PORT)
    parser.add_argument("-o", "--postgres_port", help="Postgres port", default=POSTGRES_PORT)
    parser.add_argument("-l", "--log-only", help="Just log requests", action="store_true")
    args = parser.parse_args()

    for config_file in args.config_file:
        print("Processing " + config_file + " ...")

        with open(config_file, "r", encoding='utf-8') as stream:
            partial_config = yaml.load(stream)

        full_config = {
            "metadata": {
                "name": partial_config["name"],
                "description": partial_config["description"],
                "attribution": partial_config.get("attribution", "<< Unknown >>"),
            }
        }
        if "icon" in partial_config:
            full_config["metadata"]["icon"] = partial_config["icon"]

        if partial_config["type"] == "postgres":
            full_config["locator"] = {
                "type": "postgres",
                "url": "jdbc:postgresql://{host}:{port}/{db}".format(host=args.postgres_host, port=args.postgres_port, db=POSTGRES_DB),
                "user": POSTGRES_USER,
                "password": POSTGRES_PASSWORD,
                "query": partial_config["query"]
            }

        if partial_config["type"] == "websocket":
            full_config["locator"] = {
                "type": "websocket",
                "url": "ws://{host}:{port}{context_path}".format(host=args.nginx_host, port=args.nginx_port, context_path=partial_config["context_path"])
            }

        if partial_config["type"] == "geojson":
            full_config["locator"] = {
                "type": "geojson",
                "url": "http://{host}:{port}{path}".format(host=args.nginx_host, port=args.nginx_port, path=partial_config["path"])
            }

        if args.log_only:
            pprint(full_config)
        else:
            r = requests.put(args.catalogue_api_root + "/datasets", json=full_config)


# TODO: still need to handle static geojson imports:

# curl -XPUT -H Content-Type:application/json $API_ROOT/import/geojson -d @../data/derived/signkick.json
# curl -XPUT -H Content-Type:application/json $API_ROOT/import/geojson -d @../data/derived/gb-road-traffic-counts/AADF-data-major-roads.json
# curl -XPUT -H Content-Type:application/json $API_ROOT/import/geojson -d @../data/derived/zoopla.json
#
# source $DIR/../integration/env/bin/activate
# python $DIR/../integration/scripts/live/disruptions_api.py
