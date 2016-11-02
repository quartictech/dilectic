import requests
from dilectic.utils import parse_date, task
from dilectic.actions import *
import logging

@task
def jamcams_create(cfg):
    def fill_jamcams():
        r = requests.get('https://api.tfl.gov.uk/Place/Type/jamcam')
        for cam in r.json():
            props = cam.copy()
            for v in cam["additionalProperties"]:
                props[v["key"]] = v["value"]
            yield (props["commonName"], props["view"], props["imageUrl"], props["videoUrl"], props["lat"], props["lon"])

    return db_create(cfg,
        'jamcams',
        create="""
        CREATE TABLE IF NOT EXISTS jamcams (
        Location VARCHAR,
        View VARCHAR,
        ImageFile VARCHAR,
        VideoFile VARCHAR,
        Lat DOUBLE PRECISION,
        Lon DOUBLE PRECISION
        )""",
        fill=fill_jamcams)

@task
def jamcams_geocoded(cfg):
    return db_create(cfg, 'jamcams_geocoded',
    create = """ CREATE MATERIALIZED VIEW jamcams_geocoded AS
        SELECT
            m.*,
            ST_SetSRID(ST_MakePoint(m.Lon, m.Lat), 4326) as geom
        FROM
            jamcams m
            """,
        task_dep=["jamcams_create"])
