import requests
from requests_oauthlib import OAuth1
import json
import configparser

config = configparser.ConfigParser()
config.read('twitter.conf')

data = {'track' : 'tube'}

s = requests.Session()
auth = OAuth1(config['DEFAULT']['appkey'], config['DEFAULT']['appsecret'],
config['DEFAULT']['useroauthtoken'], config['DEFAULT']['useroauthtokensecret'])
r = requests.post('https://stream.twitter.com/1.1/statuses/filter.json',
    data=data,
    stream=True,
    auth=auth
    )

for line in r.iter_lines():
    try:
        tweet = json.loads(line.decode('utf-8'))
        print(tweet)

    except:
        print(line)
