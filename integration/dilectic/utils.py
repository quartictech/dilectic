import yaml
import psycopg2
import functools
import os.path
from datetime import datetime

class Config:
    def __init__(self, path):
        with open(path) as f:
            config = yaml.load(f)
            self.config = config
            print(config)
            self.data_dir = config['data_dir']
            self.raw_dir = os.path.join(self.data_dir, "raw")
            self.derived_dir = os.path.join(self.data_dir, "derived")
            self._db_conn = None

    def db(self):
        if not self._db_conn:
            self._db_conn = self._postgres_connect()
        return self._db_conn

    def _postgres_connect(self):
        conn_str = "host={db[host]} dbname={db[dbname]} user={db[user]} password={db[password]}".format(db=self.config['db'])
        return psycopg2.connect(conn_str)

def task(task_f):
    def wrapper(cfg):
        def inner():
            f = functools.partial(task_f, cfg)
            return f()
        inner.create_doit_tasks = inner
        return inner
    wrapper.dilectic_task = True
    return wrapper

def parse_date(s, fmt='%d/%m/%Y'):
    if not s:
        return None
    try:
        return datetime.strptime(s, fmt)
    except:
        raise ValueError("Can't parse {} using {}".format(s, fmt))

from doit.cmd_base import TaskLoader
import inspect
from pprint import pprint
from doit import loader

class IntegrationTaskLoader(TaskLoader):
    def __init__(self, config, *modules):
        super(IntegrationTaskLoader, self).__init__()
        self._modules = modules
        self._config = config

    def apply_config(self, mod_dict):
        output = {}
        for name, member in inspect.getmembers(mod_dict):
            if hasattr(member, 'dilectic_task'):
                output[name] = member(self._config)
            else:
                output[name] = member
        return output

    def load_tasks(self, cmd, params, args):
        task_list = []
        for module in self._modules:
            members = self.apply_config(module)
            task_list.extend(loader.load_tasks(members, self.cmd_names, cmd.execute_tasks))
        doit_config = loader.load_doit_config(members)
        return task_list, doit_config
