import csv
import os.path
from datetime import datetime

from dilectic.utils import *
from dilectic.actions import *

@task
def uk_postcodes_table(cfg):
    def fill_postcodes_table():
        f = open(os.path.join(cfg.derived_dir, "ukpostcodes.csv"))
        rdr = csv.reader(f)
        next(rdr)
        for row in rdr:
            yield (row[1], row[2], row[3])

    return db_create(cfg, '_uk_postcodes',
        create="""CREATE TABLE IF NOT EXISTS _uk_postcodes (
      	postcode VARCHAR,
            latitude DOUBLE PRECISION,
    	longitude DOUBLE PRECISION);
        """,
        fill=fill_postcodes_table)

@task
def uk_postcodes_geocoded(cfg):
    return db_create(cfg, 'uk_postcodes',
        create = """ CREATE MATERIALIZED VIEW uk_postcodes AS
            SELECT
                p.*,
                ST_SetSRID(ST_MakePoint(p.longitude, p.latitude), 4326) as geom
            FROM
                _uk_postcodes p
                """,
    task_dep=["uk_postcodes_table"])
