import psycopg2
import argparse
import subprocess
import os

class DBMake(object):
    def __init__(self):
        self.tasks = []
        parser = argparse.ArgumentParser(description="dbmake")
        parser.add_argument('--recreate', dest='recreate', action='store_true')
        parser.set_defaults(feature=False)
        self.args = parser.parse_args()

    def _drop(self, type):
        if type == "table":
            return "DROP TABLE {name}"
        elif type == "materialized view":
            return "DROP MATERIALIZED VIEW {name}"
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
                    env['PGPASSWORD'] = self.db_args['password']
                    command = "psql -h {host} -U {user} {dbname} < {fname}".format(fname=sql_file, **self.db_args)
                    subprocess.call(command, shell=True, env=env)
                print("Creating:", name)
                if fill is not None:
                    fill(conn)
            else:
                print("Skipping:", name)
            conn.commit()

        self.tasks.append(_action)

    def run(self, **kwargs):
        conn_str = "host={host} dbname={dbname} user={user} password={password}".format(**kwargs)
        conn = psycopg2.connect(conn_str)
        self.db_args = kwargs

        for task in self.tasks:
            task(conn)
