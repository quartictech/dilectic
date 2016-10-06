import requests
from requests_oauthlib import OAuth1
import json
import pprint
import configparser
import os
import sys
import argparse
from metrology import Metrology
import geojson
from shapely.geometry import Polygon


def setup_stream():
    config = configparser.ConfigParser()
    config.read('twitter.conf')

    # query = {'locations' : config['query']['locations'],
    #        'track' : config['query']['track']}

    query = {'track' : config['query']['track']}

    s = requests.Session()
    auth = OAuth1(
        config['api']['appkey'], config['api']['appsecret'],
        config['api']['useroauthtoken'], config['api']['useroauthtokensecret'])

    r = requests.post('https://stream.twitter.com/1.1/statuses/filter.json',
        data=query,
        stream=True,
        auth=auth)

    return r

def get_centre(coords):
    p = Polygon(coords[0])
    return (p.centroid.coords.xy[0][0], p.centroid.coords.xy[1][0])

def get_tweet_properties(tweet):
    t = json.loads(tweet)
    country = None
    full_place_name = None
    if t['place']:
        country = t['place']['country']
        full_place_name = t['place']['full_name']
    text = t['text']
    user = t['user']['screen_name']
    place = t['place']
    time = t['timestamp_ms']
    t_id = t['id']
    # props = {'timestamp':time, 'user':user, 'message':text, 'country':country,
                # 'place_name':full_place_name, 'id':t_id, 'source':user}
    props = {'source':user, 'message':text}
    return props

def prepare_geojson(tweet):
    t = json.loads(tweet)
    coords = t['coordinates']
    t_id=t['id']
    loc=None
    if coords:
        loc = geojson.Point((coords['coordinates'][0], coords['coordinates'][1]))
    elif t['place']:
        loc = geojson.Point(get_centre(t['place']['bounding_box']['coordinates']))
    if loc:
        tweet_feature = geojson.Feature(id=t_id, geometry=loc, properties=None)
    else:
        return None
    return geojson.FeatureCollection([tweet_feature])

def post_shit(tweet, API_ROOT):
    r = requests.post("{}/layer/live/{}".format(API_ROOT, "Twitter"), json=tweet)
    print(r.text)

def prepare_event(tweet):
    e = {'name' : 'Twitter',
    'description' : 'Events from Twitter',
    'viewType' : 'LOCATION_AND_TRACK'}
    geojson = prepare_geojson(tweet)
    events = [{'timestamp' : json.loads(tweet)['timestamp_ms'],
                'featureCollection':prepare_geojson(tweet),
                'feedEvent':get_tweet_properties(tweet)}]
    e['events'] = events
    pprint.pprint(e)
    post_shit(e, "http://localhost:8080/api")

def read_stream(request, outfile=None):
    if outfile:
        with open(outfile, 'a') as f:
            while True:#to avoid the stream hanging up
                try:
                    for line in request.iter_lines():
                        meter = Metrology.meter('tweets')
                        if meter.count % 10 == 0:
                            print("Wrote", meter.count, "tweets. Rate=", meter.one_minute_rate * 60 * 60 * 24,"/day")
                        if os.path.getsize(outfile)/10**9 < 500:
                            try:
                                decoded = line.decode('utf-8')
                                f.write(decoded + "\n")
                                meter.mark()
                            except Exception as e:
                                print(e)
                                print(line)
                        else:
                            sys.exit()
                except requests.exceptions.StreamConsumedError as e:
                    print(e)
    else:
        while True:#to avoid the stream hanging up
            for line in request.iter_lines():
                try:
                    decoded = line.decode('utf-8')
                    prepare_event(decoded)
                    # import sys
                    # sys.exit()
                except Exception as e:
                    print(e)
                    print(line)

def get_tweets(outfile=None):
    r = setup_stream()
    read_stream(r, outfile)

if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument("-f", "--file", help="Specify file location", nargs='?',
                    action="store")
  args = parser.parse_args()
  outfile = args.file
  get_tweets(outfile)
#dict_keys(['place', 'source', 'contributors', 'retweeted', 'lang', 'retweet_count', 'text', 'id', 'possibly_sensitive', 'id_str', 'filter_level', 'favorited', 'coordinates', 'truncated', 'in_reply_to_user_id_str', 'is_quote_status', 'in_reply_to_user_id', 'in_reply_to_status_id', 'timestamp_ms', 'entities', 'created_at', 'user', 'in_reply_to_status_id_str', 'favorite_count', 'geo', 'in_reply_to_screen_name'])