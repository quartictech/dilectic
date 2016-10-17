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

def fetch_arrivals(line):
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
    stations = fetch_stations("38")
    old_predictions = {}
    while True:
        print("Fetching")
        predictions = defaultdict(lambda: defaultdict(list))
        arrivals = []
        for prediction in fetch_arrivals("38"):
            vehicle_id = prediction["vehicleId"]
            station_name = prediction["stationName"]
            predictions[vehicle_id][station_name] = (prediction["direction"], prediction["expectedArrival"], prediction["timestamp"])
        for vehicle_id in old_predictions.keys():
            if not vehicle_id in predictions:
                station_name = next(old_predictions[vehicle_id].keys())
                arrival = old_predictions[vehicle_id][naptan_id]
                arrivals.append({
                    "lineId": "38",
                    "vehicleId": vehicle_id,
                    "stationName": station_name,
                    "direction": arrival[0],
                    "lastExpectedArrival": arrival[1],
                    "lastTimestamp": arrival[2]
                })
            else:
                for station_name in old_predictions[vehicle_id]:
                    if not station_name in predictions[vehicle_id]:
                        arrival = old_predictions[vehicle_id][station_name]
                        arrivals.append({
                        "lineId": "38",
                        "vehicleId": vehicle_id,
                        "stationName": station_name,
                        "direction": arrival[0],
                        "lastExpectedArrival": arrival[1],
                        "lastTimestamp": arrival[2]
                        })

        for arrival in arrivals:
            print(stations[arrival["direction"]].keys())
            station = stations[arrival["direction"]][arrival["stationName"]]
            print(station)
        old_predictions = predictions
            # curs.execute("INSERT INTO tfl_arrivals(lineId, vehicleId, towards, direction, naptanId, ts, expectedArrival ) VALUES(%s, %s, %s, %s, %s, %s, %s)",
            #     (bus["lineId"], bus["vehicleId"], bus["towards"], bus["direction"], bus["naptanId"], bus["timestamp"], bus["expectedArrival"]))
        conn.commit()
        time.sleep(2)
