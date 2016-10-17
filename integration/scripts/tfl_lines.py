import requests
import json
from pprint import pprint
import csv
import time
import psycopg2
from collections import defaultdict

APP_ID="4abd99df"
APP_KEY="0f76ba70a21836b0991d192dceae511b"

FIELDS = [
        "stationName",
        "destinationName",
        "expectedArrival",
        "naptanId",
        "vehicleId",
        "destinationNaptanId",
        "direction",
        "timestamp",
        "timeToStation",
        "currentLocation",
        "id",
        "bearing",
        "towards",
        "lineId",
        "lineName"
]

def request(path, **kwargs):
    r = requests.get("https://api.tfl.gov.uk{path}?app_id={app_id}&app_key={app_key}".format(path=path, app_id=APP_ID, app_key=APP_KEY))
    return r.json()

def fetch_lines(mode):
    r = request("/line/mode/{0}".format(mode))
    for line in r:
        yield line["name"]

def fetch_arrival_predictions(line):
    r = request("/line/{0}/arrivals".format(line))
    for bus in r:
        yield bus
    json.dump(r, open("{0}.json".format(line), "w"), indent=1)

def create_table(curs):
    curs.execute("""
        CREATE TABLE IF NOT EXISTS tfl_arrivals(
            lineId VARCHAR,
            vehicleId VARCHAR,
            lastTimestamp TIMESTAMP,
            lastExpectedArrival TIMESTAMP,
            direction VARCHAR,
            naptanId VARCHAR)
            """)

if __name__ == "__main__":
    conn_str = "host=localhost dbname=postgres user=postgres password=dilectic"
    conn = psycopg2.connect(conn_str)
    curs = conn.cursor()
    create_table(curs)
    lines = fetch_lines("bus")
    old_predictions = {}
    line_id = "38"
    while True:
        print("Fetching")
        predictions = defaultdict(lambda: defaultdict(list))
        arrivals = []
        for prediction in fetch_arrival_predictions(line_id):
            vehicle_id = prediction["vehicleId"]
            naptan_id = prediction["naptanId"]
            predictions[vehicle_id][naptan_id] = {
                "direction": prediction["direction"],
                "expectedArrival": prediction["expectedArrival"],
                "timestamp": prediction["timestamp"],
                "stationName": prediction["stationName"]
            }
        for vehicle_id in old_predictions.keys():
            if not vehicle_id in predictions:
                station_name = list(old_predictions[vehicle_id].keys())[0]
                arrival = old_predictions[vehicle_id][naptan_id]
                arrivals.append({
                    "lineId": line_id,
                    "vehicleId": vehicle_id,
                    "naptanId": naptan_id,
                    "stationName": arrival["stationName"],
                    "direction": arrival["direction"],
                    "lastExpectedArrival": arrival["expectedArrival"],
                    "lastTimestamp": arrival["timestamp"]
                })
            else:
                for naptan_id in old_predictions[vehicle_id]:
                    if not naptan_id in predictions[vehicle_id]:
                        arrival = old_predictions[vehicle_id][naptan_id]
                        arrivals.append({
                        "lineId": line_id,
                        "vehicleId": vehicle_id,
                        "naptanId": naptan_id,
                        "stationName": arrival["stationName"],
                        "direction": arrival["direction"],
                        "lastExpectedArrival": arrival["expectedArrival"],
                        "lastTimestamp": arrival["timestamp"]
                        })

        for arrival in arrivals:
            pprint(arrival)
        old_predictions = predictions
            # curs.execute("INSERT INTO tfl_arrivals(lineId, vehicleId, towards, direction, naptanId, ts, expectedArrival ) VALUES(%s, %s, %s, %s, %s, %s, %s)",
            #     (bus["lineId"], bus["vehicleId"], bus["towards"], bus["direction"], bus["naptanId"], bus["timestamp"], bus["expectedArrival"]))
        conn.commit()
        time.sleep(2)
