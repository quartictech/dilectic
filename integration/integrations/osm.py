import os.path

from dilectic.utils import *
from dilectic.actions import *

@task
def osm(cfg):
    def object_exists(name):
        with cfg.db().cursor() as curs:
            sql = "SELECT to_regclass('{name}')".format(name=name)
            curs.execute(sql)
            return curs.fetchone()[0] is not None
    def uptodate():
        return (
            object_exists("planet_osm_polygon") and
            object_exists("planet_osm_line") and
            object_exists("planet_osm_point") and
            object_exists("planet_osm_roads")
        )

    def column_exists(table, column):
        with cfg.db().cursor() as curs:
            curs.execute("""SELECT column_name FROM information_schema.columns
                WHERE table_name='{table}' and column_name='{name}'""".format(
                    name=column,
                    table=table
                ))
            return curs.fetchone() is not None

    def rename_column(table, column, new_name):
        with cfg.db().cursor() as curs:
            if column_exists(table, column):
                curs.execute("ALTER TABLE {table} RENAME {column} TO {new_name}".format(
                table=table,
                column=column,
                new_name=new_name
                ))
                cfg.db().commit()

    def rename_column_action(table):
        return {
            "name": "hack {0}".format(table),
            "actions": [lambda: rename_column(table, "way", "geom")],
            "uptodate": [lambda: column_exists(table, "geom")]
        }


    input_file = os.path.join(cfg.raw_dir, "greater-london-latest.osm.pbf")
    yield {
        "name": "osm_to_pgsql",
        "actions": ["PGPASSWORD={db[password]} osm2pgsql {source} -d {db[dbname]} -U {db[user]} -H {db[host]}".format(
                    source=input_file,
                    db=cfg.config["db"])],
        "uptodate": [uptodate],
        "file_dep": [input_file]
    }

    yield rename_column_action("planet_osm_line")
    yield rename_column_action("planet_osm_point")
    yield rename_column_action("planet_osm_polygon")
    yield rename_column_action("planet_osm_roads")
