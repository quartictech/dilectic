from datetime import datetime
import requests
import sys

def parse_date(s, fmt='%d/%m/%Y'):
    if not s:
        return None
    try:
        return datetime.strptime(s, fmt)
    except:
        raise ValueError("Can't parse {} using {}".format(s, fmt))

def post_events(layer, data, API_ROOT='http://localhost:8080/api'):
    r = requests.post("{}/layer/live/{}".format(API_ROOT, layer), json=data)
    if (r.status_code < 200 or r.status_code > 299):
        print(r.text)
