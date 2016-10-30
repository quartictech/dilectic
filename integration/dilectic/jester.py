import requests

class Jester(object):
    def __init__(self, api_root):
        self.api_root = api_root
        r = requests.get("{0}/ping".format(api_root))
        assert r.status_code == 200

    def dataset(self, name, description, attribution, source, icon=None):
        data = {
            "metadata": {
                "name": name,
                "description": description,
                "attribution": attribution,
                "icon": icon
            },
            "source": source
        }

        r = requests.put("{0}/datasets".format(self.api_root), json=data)

class PostgresDatasource(object):
    def __init__(self, url, user, password):
        self.url = url
        self.user = user
        self.password = password

    def query(self, query):
        return {
            "type": "Postgres",
            "user": self.user,
            "password": self.password,
            "url": self.url,
            "query": query
        }
