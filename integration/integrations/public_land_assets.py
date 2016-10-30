import csv
import os.path
from collections import defaultdict
import glob

from dilectic.utils import *
from dilectic.actions import *

@task
def public_land_assets(cfg):
    def process_csv(f):
        owner = f.split('/')[-1].strip('_assets.csv')
        with open(f, encoding='windows-1252') as csvfile:
            rdr = csv.reader(csvfile)
            next(rdr) #jump the headers
            for line in rdr:
                for index,element in enumerate(line):
                    if element.strip() == '':
                        line[index] = None

                line.insert(0, owner)
                yield line

    def fill_land_and_assets_table():
        path = os.path.join(cfg.derived_dir, 'gla-land-assets/*.csv')
        for f in glob.glob(path):
            yield from process_csv(f)
    return db_create(cfg.db(), 'public_land_assets',
    create="""CREATE TABLE IF NOT EXISTS public_land_assets (
        Owner VARCHAR,
        Borough VARCHAR,
        UniqueAssetId VARCHAR,
        HoldingName VARCHAR,
        Address VARCHAR,
        PostCode VARCHAR,
        SubUnit VARCHAR,
        UPRN VARCHAR,
        Description VARCHAR,
        Occupied VARCHAR,
        LandBuilding VARCHAR,
        AssetCategory VARCHAR,
        Tenure VARCHAR,
        SiteAreaHectares REAL,
        BuildingAreaGIASqm REAL,
        BuildingAreaNIAsqm REAL,
        Easting INT,
        Northing INT)""",
        fill=fill_land_and_assets_table)

@task
def public_land_assets_geocoded(cfg):
    return db_create(cfg.db(), 'public_land_assets_geocoded',
    create = """ CREATE MATERIALIZED VIEW public_land_assets_geocoded AS
        SELECT
            pla.*,
            ST_SetSRID(ST_MakePoint(pla.Easting, pla.Northing), 27700) as geom
        FROM
            public_land_assets pla
            """,
    task_dep= ["public_land_assets"])
