from datetime import datetime
import requests
import sys

def post_events(layer, data, API_ROOT='http://localhost:8080/api'):
    r = requests.post("{}/layer/live/{}".format(API_ROOT, layer), json=data)
    print(r.text)
