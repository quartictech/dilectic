import csv
import os.path
import glob
from datetime import datetime

from dilectic.utils import *
from dilectic.actions import *

@task
def crime_table(cfg):
    def fill_crime_table():
        paths = glob.glob(os.path.join(cfg.derived_dir, 'crime_data/*/*'))
        assert len(paths) > 0
        counter = 0
        for files in paths:
            with open(files) as f:
                rdr = csv.reader(f)
                next(rdr)
                for line in rdr:
                    # if counter%10 != 0: #do this if testing otherwise huge
                    #     counter += 1
                    #     continue
                    # counter += 1
                    if len(line) == 10:
                        values = (line[0], datetime.strptime(line[1], '%Y-%M'),
                                line[2], line[3], line[4], line[5],
                                line[6], line[7], line[8], None, line[9], None)
                    elif len(line) == 12:
                        values = (line[0], datetime.strptime(line[1], '%Y-%M'),
                                    line[2], line[3], line[4], line[5],
                                    line[6], line[7], line[8], line[9], line[10], line[11])
                    else:
                        print('Odd line length {}'.format(len(line)))
                        print(line)

                    yield values
    return db_create(cfg, 'crime',
    create="""CREATE TABLE IF NOT EXISTS crime (
        CrimeId VARCHAR,
        MonthYear DATE,
        ReportedBy VARCHAR,
        FallsWithin VARCHAR,
        Longitude DOUBLE PRECISION,
        Latitude DOUBLE PRECISION,
        Location VARCHAR,
        LsoaCode VARCHAR,
        LsoaName VARCHAR,
        CrimeType VARCHAR,
        LastOutcomeCat VARCHAR,
        Context VARCHAR
    )""",
    fill=fill_crime_table)

@task
def crime_geocoded(cfg):
    return db_create(cfg, 'crime_geocoded',
    create = """ CREATE MATERIALIZED VIEW crime_geocoded AS
        SELECT
            m.*,
            ST_SetSRID(ST_MakePoint(m.Longitude, m.Latitude), 4326) as geom
        FROM
            crime m
            """,
    task_dep=["crime_table"])
