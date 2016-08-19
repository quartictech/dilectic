import requests
from requests_oauthlib import OAuth1
import json
import configparser

config = configparser.ConfigParser()
config.read('twitter.conf')

for key in config['DEFAULT']:
    print(key)

s = requests.Session()
auth = OAuth1(config['DEFAULT']['appkey'], config['DEFAULT']['appsecret'],
config['DEFAULT']['useroauthtoken'], config['DEFAULT']['useroauthtokensecret'])
r = requests.post('https://stream.twitter.com/1.1/statuses/filter.json',
    data="track=London",
    stream=True,
    auth=auth
    )

for line in r.iter_lines():
    print(line)
