import requests
import xml.etree.ElementTree as ElementTree
from utils import parse_date


def fill_jamcams(data_dir):
    # TODO: we should switch to https://api.tfl.gov.uk/Place/Type/jamcam
    r = requests.get('https://s3-eu-west-1.amazonaws.com/jamcams.tfl.gov.uk/jamcams-camera-list.xml')
    xml = r.text
    root = ElementTree.fromstring(xml)
    cams = root[1].findall('camera')
    for c in cams:
        v = []
        for t in c.itertext():
            v.append(t)
        values = (v[0], v[1], v[2], 'https://s3-eu-west-1.amazonaws.com/jamcams.tfl.gov.uk/{}'.format(v[3].replace("jpg", "mp4")),
                parse_date(v[4], fmt='%Y-%m-%dT%H:%M:%SZ'), v[7], v[8], v[10])
        yield values
