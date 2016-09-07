import requests
from requests_oauthlib import OAuth1
import json
import configparser
import os
import sys
import argparse
from metrology import Metrology

if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument("-f", "--file", help="Specify file location", nargs='?',
                    type=argparse.FileType('a'),
                    action="store")
  args = parser.parse_args()
  outfile = args.file
  print(outfile)

  config = configparser.ConfigParser()
  config.read('twitter.conf')
  
  query = {'locations' : config['query']['locations'],
           'track' : config['query']['track']}

  s = requests.Session()
  auth = OAuth1(
    config['api']['appkey'], config['api']['appsecret'],
    config['api']['useroauthtoken'], config['api']['useroauthtokensecret'])

  count = 0
  meter = Metrology.meter('tweets')
  while True:
    r = requests.post('https://stream.twitter.com/1.1/statuses/filter.json',
      data=query,
      stream=True,
      auth=auth)
    with outfile as file:
      for line in r.iter_lines():
        if meter.count % 10 == 0:
            print("Wrote", meter.count, "tweets. Rate=", meter.one_minute_rate * 60 * 60 * 24,"/day")
        if os.path.getsize(outfile.name)/10**9 < 500:
          try:
            decoded = line.decode('utf-8')
            file.write(decoded + "\n")
            meter.mark()
            #print(json.loads(decoded))
            # tweet = json.loads(line.decode('utf-8'))
          except Exception as e:
            print(e)
            print(line)
        else:
          sys.exit()


#dict_keys(['place', 'source', 'contributors', 'retweeted', 'lang', 'retweet_count', 'text', 'id', 'possibly_sensitive', 'id_str', 'filter_level', 'favorited', 'coordinates', 'truncated', 'in_reply_to_user_id_str', 'is_quote_status', 'in_reply_to_user_id', 'in_reply_to_status_id', 'timestamp_ms', 'entities', 'created_at', 'user', 'in_reply_to_status_id_str', 'favorite_count', 'geo', 'in_reply_to_screen_name'])

