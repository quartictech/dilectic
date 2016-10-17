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

def lookup_station(station_id, station_locs={}):
    if station_id not in stations_locs.keys():
        r = request("/StopPoint/{}".format(station_id))
        station_locs[station_id] = (r['lat'], r['lon'])
    return station_locs[station_id]

def lookup_line(line_id):
    stops_direction = {}
    for direction in ['inbound', 'outbound']:
        r = request("/Line/{}/Route/Sequence/{}".format(line_id, direction))
        stop_points =  r['stopPointSequences'][0]
        stops = []
        for stop in stop_points['stopPoint']:
            stops.append((stop['id'], stop['name'], stop['lat'], stop['lon']))
        stops_direction[direction] = stops
    return stops_direction

def fetch_arrival_predictions(line):
    bus_arrivals = {}
    r = request("/line/{0}/arrivals".format(line))
    for bus in r:
        if bus['vehicleId'] in bus_arrivals.keys():
            bus_arrivals[bus['vehicleId']].append(bus)
        else:
            bus_arrivals[bus['vehicleId']] = [bus]
    return {bus: sorted(arrivals, key=lambda k: k['timeToStation']) for bus, arrivals in bus_arrivals.items()}
    # json.dump(r, open("{0}.json".format(line), "w"), indent=1)

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
    test = fetch_arrival_predictions('88')
    pprint(test)
    # conn_str = "host=localhost dbname=postgres user=postgres password=dilectic"
    # conn = psycopg2.connect(conn_str)
    # curs = conn.cursor()
    # create_table(curs)
    # old_predictions = {}
    # line_id = "88"
    # while True:
    #     print("Fetching")
    #     predictions = defaultdict(lambda: defaultdict(list))
    #     arrivals = []
    #     for prediction in fetch_arrival_predictions(line_id):
    #         vehicle_id = prediction["vehicleId"]
    #         naptan_id = prediction["naptanId"]
    #         predictions[vehicle_id][naptan_id] = {
    #             "direction": prediction["direction"],
    #             "expectedArrival": prediction["expectedArrival"],
    #             "timestamp": prediction["timestamp"],
    #             "stationName": prediction["stationName"]
    #         }
    #     for vehicle_id in old_predictions.keys():
    #         if not vehicle_id in predictions:
    #             station_name = list(old_predictions[vehicle_id].keys())[0]
    #             arrival = old_predictions[vehicle_id][naptan_id]
    #             arrivals.append({
    #                 "lineId": line_id,
    #                 "vehicleId": vehicle_id,
    #                 "naptanId": naptan_id,
    #                 "stationName": arrival["stationName"],
    #                 "direction": arrival["direction"],
    #                 "lastExpectedArrival": arrival["expectedArrival"],
    #                 "lastTimestamp": arrival["timestamp"]
    #             })
    #         else:
    #             for naptan_id in old_predictions[vehicle_id]:
    #                 if not naptan_id in predictions[vehicle_id]:
    #                     arrival = old_predictions[vehicle_id][naptan_id]
    #                     arrivals.append({
    #                     "lineId": line_id,
    #                     "vehicleId": vehicle_id,
    #                     "naptanId": naptan_id,
    #                     "stationName": arrival["stationName"],
    #                     "direction": arrival["direction"],
    #                     "lastExpectedArrival": arrival["expectedArrival"],
    #                     "lastTimestamp": arrival["timestamp"]
    #                     })
    #
    #     for arrival in arrivals:
    #         pprint(arrival)
    #     old_predictions = predictions
    #         # curs.execute("INSERT INTO tfl_arrivals(lineId, vehicleId, towards, direction, naptanId, ts, expectedArrival ) VALUES(%s, %s, %s, %s, %s, %s, %s)",
    #         #     (bus["lineId"], bus["vehicleId"], bus["towards"], bus["direction"], bus["naptanId"], bus["timestamp"], bus["expectedArrival"]))
    #     conn.commit()
    #     time.sleep(2)
