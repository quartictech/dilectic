import sys
import zipfile
import codecs
import csv
import os.path
import glob
from collections import defaultdict
import logging

from dilectic.utils import *
from dilectic.actions import *

def process_companies_csv(f):
    rdr = csv.reader(f)
    next(rdr) # Skip header
    count = 0
    for line in rdr:
        dissolution_date = parse_date(line[13])
        incorporation_date = parse_date(line[14])

        values = (line[0], line[1], line[10], line[11], line[12],
        dissolution_date, incorporation_date, line[26], line[27],
        line[28], line[9])

        yield values

def extract_companies_zip(fname):
    zip = zipfile.ZipFile(fname)
    names = list(zip.namelist())
    print("%s : %d files" % (fname, len(names)))
    for name in names:
        f = codecs.iterdecode(zip.open(name), 'utf-8')
        yield from process_companies_csv(f)

@task
def companies_house_register(cfg):
    def fill_companies_table():
        path = os.path.join(cfg.raw_dir, "companies-house-register/*.zip")
        for f in glob.glob(path):
            yield from extract_companies_zip(f)

    return db_create(cfg.db(), 'companies',
    create="""CREATE TABLE IF NOT EXISTS companies (
        CompanyName VARCHAR,
        CompanyNumber VARCHAR UNIQUE,
        CompanyCategory VARCHAR,
        CompanyStatus VARCHAR,
        CountryOfOrigin VARCHAR,
	    DissolutionDate DATE,
        IncorporationDate DATE,
        SICText1 VARCHAR,
        SICText2 VARCHAR,
        SICText3 VARCHAR,
        Postcode VARCHAR)
    """,
    fill=fill_companies_table)

@task
def companies_house_register_geocoded(cfg):
    return db_create(cfg.db(), 'companies_geocoded',
    create = """ CREATE MATERIALIZED VIEW companies_geocoded AS
        SELECT
            c.*,
            ST_Transform(p.geom, 900913) as geom
        FROM
            companies c
        LEFT JOIN uk_postcodes p ON p.postcode = c.postcode """,
    task_dep = ["companies_house_register", "uk_postcodes_geocoded"])
