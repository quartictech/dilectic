import requests
from requests_oauthlib import OAuth1
import json
import configparser
import os
import sys

config = configparser.ConfigParser()
config.read('twitter.conf')

data = {'track' : 'tube london'}

s = requests.Session()
auth = OAuth1(config['DEFAULT']['appkey'], config['DEFAULT']['appsecret'],
config['DEFAULT']['useroauthtoken'], config['DEFAULT']['useroauthtokensecret'])
r = requests.post('https://stream.twitter.com/1.1/statuses/filter.json',
    data=data,
    stream=True,
    auth=auth
    )

with open('/Volumes/Storage/quartic/tube_tweets.json', 'a') as file:
    for line in r.iter_lines():
        if os.path.getsize('/Volumes/Storage/quartic/tube_tweets.json')/10**9 < 500:
            try:
                decoded = line.decode('utf-8')
                file.write(decoded)
                print(json.loads(decoded))
                # tweet = json.loads(line.decode('utf-8'))
            except Exception as e:
                print(e)
                print(line)
        else:
            sys.exit()


#dict_keys(['place', 'source', 'contributors', 'retweeted', 'lang', 'retweet_count', 'text', 'id', 'possibly_sensitive', 'id_str', 'filter_level', 'favorited', 'coordinates', 'truncated', 'in_reply_to_user_id_str', 'is_quote_status', 'in_reply_to_user_id', 'in_reply_to_status_id', 'timestamp_ms', 'entities', 'created_at', 'user', 'in_reply_to_status_id_str', 'favorite_count', 'geo', 'in_reply_to_screen_name'])
