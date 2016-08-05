import psycopg2
import argparse
import subprocess
import os
import yaml

class DBMake(object):
    def __init__(self):
        self.tasks = []
        parser = argparse.ArgumentParser(description="dbmake")
        parser.add_argument('--recreate', dest='recreate', action='store_true')
        parser.set_defaults(feature=False)
        self.args = parser.parse_args()
        self.config = {}
        self.data_dir = None

    def _drop(self, type):
        if type == "table":
            return "DROP TABLE {name} CASCADE"
        elif type == "materialized view":
            return "DROP MATERIALIZED VIEW {name} CASCADE"
        elif type == "index":
            return "DROP INDEX {name}"
        else:
            raise ValueError("Unrecognised type: " + + type)

    def index(self, name, create=None):
        return self._create('index', name, create, None, None)

    def table(self, name, create=None, sql_file=None, fill=None):
        return self._create('table', name, create, sql_file, fill)

    def materialized_view(self, name, create=None):
        return self._create('materialized view', name, create, None, None)

    def _create(self, type, name, create, sql_file, fill):
        if create is None and sql_file is None:
            raise ValueError("create or sql_file must be set")

        def _action(conn):
            print("Making", name)
            curs = conn.cursor()
            create_table = False

            if self.args.recreate:
                print("Dropping:", name)
                curs.execute(self._drop(type).format(name=name))
                create_table = True
            else:
                sql = "SELECT to_regclass('{name}')".format(name=name)
                curs.execute(sql)
                create_table = curs.fetchone()[0] is None
            if create_table:
                if create:
                    curs.execute(create)
                else:
                    env = os.environ.copy()
                    env['PGPASSWORD'] = self.config['db']['password']
                    command = "psql -h {db[host]} -U {db[user]} {db[dbname]} < {fname}".format(fname=os.path.join(self.data_dir, sql_file), db=self.config['db'])
                    subprocess.call(command, shell=True, env=env)
                print("Creating:", name)
                if fill is not None:
                    fill(conn, self.data_dir)
            else:
                print("Skipping:", name)
            conn.commit()

        self.tasks.append(_action)

    def run(self, config, **kwargs):
        with open(config) as f:
            try:
                self.config = yaml.load(f)
                print(self.config)
            except yaml.YAMLError as exc:
                print(exc)
        conn_str = "host={db[host]} dbname={db[dbname]} user={db[user]} password={db[password]}".format(db=self.config['db'])
        conn = psycopg2.connect(conn_str)
        self.data_dir = os.path.join(os.path.dirname(config), self.config['data_dir'])

        for task in self.tasks:
            task(conn)
