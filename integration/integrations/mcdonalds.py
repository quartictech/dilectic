import csv
import os.path
from datetime import datetime
import json
from collections import defaultdict
import logging

from dilectic.utils import *
from dilectic.actions import *


@task
def mcdonalds(cfg):
    def fill_mcdonalds_table():
        path = os.path.join(cfg.raw_dir, 'mcdonalds.csv')
        with open(path) as f:
            rdr = csv.reader(f)
            next(rdr)
            for line in rdr:
                if len(line) < 7:
                    continue
                values = (line[0], line[1], line[2], line[3], line[4], line[5], line[6])

                yield values
    return db_create(cfg.db(), "mcdonalds",
    create="""CREATE TABLE IF NOT EXISTS mcdonalds (
        Name VARCHAR,
        Street VARCHAR,
        Town VARCHAR,
        PostCode VARCHAR,
        PhoneNumber VARCHAR,
        Latitude DOUBLE PRECISION,
        Longitude DOUBLE PRECISION
    )""",
    fill=fill_mcdonalds_table)


@task
def mcdonalds_geocoded(cfg):
    return db_create(cfg.db(), 'mcdonalds_geocoded',
    create = """ CREATE MATERIALIZED VIEW mcdonalds_geocoded AS
        SELECT
            m.*,
            ST_SetSRID(ST_MakePoint(m.Longitude, m.Latitude), 4326) as geom
        FROM
            mcdonalds m
            """,
        task_dep=["mcdonalds"])
