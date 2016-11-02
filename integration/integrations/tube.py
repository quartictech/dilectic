import os.path
from datetime import datetime
import json
from collections import defaultdict
import logging

from dilectic.utils import *
from dilectic.actions import *

@task
def tube(cfg):
    def fill_tube():
        with open(os.path.join(cfg.raw_dir, "tfl_lines.json")) as f:
            data = json.load(f)
            for row in data["features"]:
                props = row["properties"]
                lines = props["lines"]
                unclosed_lines = [line for line in lines if not "closed" in line]
                if len(unclosed_lines) > 0:
                    yield (props["id"], unclosed_lines[0]["name"], unclosed_lines[0]["colour"], row["geometry"])
    return db_create(cfg, '_tube',
    create="""CREATE TABLE IF NOT EXISTS _tube (
        id VARCHAR,
        name VARCHAR,
        color VARCHAR,
        geojson VARCHAR
        )
    """,
    fill=fill_tube)

@task
def tube_geocoded(cfg):
    return db_create(cfg, 'tube',
    create = """ CREATE MATERIALIZED VIEW tube AS
        SELECT
            t.id,
            t.name,
            ST_SetSRID(ST_GeomFromGeoJson(t.geojson), 4326) as geom
        FROM
            _tube t
            """,
    task_dep=["tube"])
