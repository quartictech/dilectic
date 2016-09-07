import requests
from requests_oauthlib import OAuth1
import json
import configparser
import os
import sys
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("-f", "--file", help="Specify file location", nargs='?',
                    type=argparse.FileType('a'),
                    action="store")
args = parser.parse_args()
outfile = args.file



config = configparser.ConfigParser()
config.read('twitter.conf')

data = {'track' : 'tube london'}

s = requests.Session()
auth = OAuth1(config['DEFAULT']['appkey'], config['DEFAULT']['appsecret'],
config['DEFAULT']['useroauthtoken'], config['DEFAULT']['useroauthtokensecret'])

def write_to_file(f):
    with open(outfile) as file:
        for line in r.iter_lines():
            if os.path.getsize(outfile)/10**9 < 500:
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

def get_tweets():
    r = requests.post('https://stream.twitter.com/1.1/statuses/filter.json',
    data=data,
    stream=True,
    auth=auth
    )
    for line in r.iter_lines():
            try:
                decoded = line.decode('utf-8')
                print(json.loads(decoded))
                # tweet = json.loads(line.decode('utf-8'))
            except Exception as e:
                print(e)
                print(line)

if __name__ == "__main__":
    if outfile:
        write_to_file(outfile)
    else:
        get_tweets()


#dict_keys(['place', 'source', 'contributors', 'retweeted', 'lang', 'retweet_count', 'text', 'id', 'possibly_sensitive', 'id_str', 'filter_level', 'favorited', 'coordinates', 'truncated', 'in_reply_to_user_id_str', 'is_quote_status', 'in_reply_to_user_id', 'in_reply_to_status_id', 'timestamp_ms', 'entities', 'created_at', 'user', 'in_reply_to_status_id_str', 'favorite_count', 'geo', 'in_reply_to_screen_name'])
