import requests
import xml.etree.ElementTree as ElementTree

r = requests.get('https://s3-eu-west-1.amazonaws.com/jamcams.tfl.gov.uk/jamcams-camera-list.xml')
xml = r.text

def fill_jamcams():
    root = ElementTree.fromstring(xml)
    cams = root[1].findall('camera')
    for c in cams:
        v = []
        for t in c.itertext():
            v.append(t)
        yield v
