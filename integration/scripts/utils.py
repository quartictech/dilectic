from datetime import datetime

def parse_date(s, fmt='%d/%m/%Y'):
    if not s:
        return None
    try:
        return datetime.strptime(s, fmt)
    except:
        raise ValueError("Can't parse {} using {}".format(s, fmt))

def post_shit(layer, data, API_ROOT):
    r = requests.post("{}/layer/live/{}".format(API_ROOT, layer), json=data)
    print(r.text)
