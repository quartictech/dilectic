#
#THIS FEED APPEARS NOT TO HAVE POLYGONS...ONLY ROADS
#
import requests
import xml.etree.ElementTree as ET
from utils import parse_date
import configparser

def read_feed(request):
    ns = {'tflns' : "http://www.tfl.gov.uk/tims/1.0"}
    root = ET.fromstring(request.text)
    for child in root:
        if child.tag == '{http://www.tfl.gov.uk/tims/1.0}Disruptions':
            newroot = child
    collect_events(newroot.findall('tflns:Disruption', ns))

def clean_tag(tag):
    return tag.replace('{http://www.tfl.gov.uk/tims/1.0}','')

def collect_events(disruptions):
    e = {'name' : 'Disruptions',
    'description' : 'TfL Traffic Disruptions'}
    events = []
    for d in disruptions:
        props = {}
        for child in d:
            props[child.tag.replace('{http://www.tfl.gov.uk/tims/1.0}','')] = child.text
        print(props)
        if clean_tag(child.tag) == 'CauseArea':
            #needs finishing
            break

def setup_feed():
    config = configparser.ConfigParser()
    config.read('tfl.conf')
    s = requests.Session()
    auth = {'app_id':config['DEFAULT']['AppID'], 'app_key':config['DEFAULT']['AppKey']}
    r = requests.get('https://data.tfl.gov.uk/tfl/syndication/feeds/tims_feed.xml',
        data=auth)
    return r

def prepare_events():
    e = {'name':'dirupt',
    'desc':'xyz'}
    events = [{'timestamp',
                'featureCollection'}]
if __name__ == "__main__":
    r = setup_feed()
    read_feed(r)
