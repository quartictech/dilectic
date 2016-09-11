import requests
from requests_oauthlib import OAuth1
import json
import pprint
import configparser
import os
import sys
import argparse
from metrology import Metrology

def setup_stream():
    config = configparser.ConfigParser()
    config.read('twitter.conf')

    query = {'locations' : config['query']['locations'],
           'track' : config['query']['track']}

    s = requests.Session()
    auth = OAuth1(
        config['api']['appkey'], config['api']['appsecret'],
        config['api']['useroauthtoken'], config['api']['useroauthtokensecret'])

    r = requests.post('https://stream.twitter.com/1.1/statuses/filter.json',
        data=query,
        stream=True,
        auth=auth)

    return r

def read_stream(request, outfile=None):
    if outfile:
        with open(outfile, 'a') as f:
            while True:#to avoid the stream hanging up
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
    else:
        try:
            decoded = line.decode('utf-8')
            pprint.pprint(json.loads(decoded))
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
