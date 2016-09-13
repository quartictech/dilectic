import yaml
import os.path

class Catalog(object):
    def __init__(self):
        pass


class Context(object):
    def __init__(self, base_dir, config):
        self.data_dir = os.path.join(base_dir, os.path.expanduser(config['data_dir']))

    def resolve_path(self, rel_path):
        return os.path.join(self.data_dir, rel_path)

def configure(config_fname):
    with open(config_fname) as f:
        try:
            config = yaml.load(f)
            return Context(os.path.dirname(config_fname), config)
        except yaml.YAMLError as exc:
            print(exc)
