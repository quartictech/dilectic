from zipfile import ZipFile, BadZipFile
import os.path
import csv
import codecs
import subprocess
import re

from dilectic.utils import *
from dilectic.actions import *

def fix_zip_file(path):
    #zip file they provide is fucked so we fix it.
    z = ZipFile(path)
    if z.testzip() != None:
        subprocess.call(["zip", "-FF", path, "--out", path + '.fix'])
        path = path + '.fix'
    return path

def process_prices_zip(path):
    path = fix_zip_file(path)
    with ZipFile(path) as z:
        for f in z.namelist():
            print('Processing {}'.format(f))
            f = codecs.iterdecode(z.open(f), 'utf-7', errors='ignore')
            rdr = csv.reader(f)
            count = 0
            next(rdr)
            for row in rdr:
                if len(row) == 29:
                    try:
                        #format 1:
                        #['861696', '{5C1CC576-24F6-453B-9984-3898CC3AA528}',
                        #'155000', '11/01/1995 00:00',
                        #'1', '1',
                        #'1995', '1995/1',
                        #'N20 9AQ', 'F',
                        #'Y', 'L',
                        #'GREENLEAF COURT, 17',
                        #13 below:
                        # 'FLAT 1', 'OAKLEIGH PARK NORTH',
                        #'LONDON', 'LONDON',
                        #'BARNET', 'GREATER LONDON',
                        #'A', 'N20 9AQ',
                        #'Outer', 'E09000003',
                        #'Barnet', 'E05000058',
                        #'Oakleigh', 'E02000031',
                        #'E01000273', 'E00001314']
                        fmt = '%d/%m/%Y %H:%M'
                        values = (
                        row[2], parse_date(row[3], fmt), re.sub('\\s+', ' ', row[8]), row[9], row[10],
                        row[11], row[12], row[13], row[14], row[15],
                        row[16], row[17], row[18], row[19])
                    except UnicodeDecodeError as e:
                        print(e)
                        print(row)

                elif len(row) == 30:
                    try:#retarded change in the input data format..
                        #format 2: ['{0FF377DF-DC87-4D54-90DC-5E200B155704}', '215000',
                        #'01-Jan-13', 'HA0 4EE',
                        #'HA04EE', 'F',
                        #'N', 'L',
                        #' ', ' ', 'WEMBLEY', 'BRENT', 'GREATER LONDON',
                        #'A', '2013', '1', '1', '2',
                        #'00AEGJ', 'E00002335', 'E01000474', 'E02000118',
                        #'Outer', '2013/1', 'HA04', 'HA0',
                        #'E05000085', 'E05000085', 'E09000005', 'Brent']
                        fmt = '%d-%b-%y'
                        values = (
                        row[1], utils.parse_date(row[2], fmt), re.sub('\\s+', ' ', row[3]), row[5], row[6],
                        row[7], "", "", row[8], row[9],#addr is 2 lines shorter - insert blank
                        row[10], row[11], row[12], row[13])
                    except Exception as e:
                        print(e)

                yield values


@task
def london_price_houses(cfg):
    def fill_london_house_prices():
        data = 'London-price-paid-house-price-data-since-1995-CSV.zip'
        path = os.path.join(cfg.raw_dir, data)
        yield from process_prices_zip(path)
    return db_create(cfg.db(), 'london_price_houses',
    create="""CREATE TABLE IF NOT EXISTS london_price_houses (
        Price INT,
        DateProcessed DATE,
        Postcode VARCHAR,
        PropertyType VARCHAR,
        WhetherNew VARCHAR,
        Tenure VARCHAR,
        Addr1 VARCHAR,
        Addr2 VARCHAR,
        Addr3 VARCHAR,
        Addr4 VARCHAR,
        Town VARCHAR,
        LocalAuthority VARCHAR,
        County VARCHAR,
        RecordStatus VARCHAR)
    """,
    fill=fill_london_house_prices)

@task
def london_price_houses_geocoded(cfg):
    db_create(cfg.db(), 'london_price_houses_geocoded',
    create = """ CREATE MATERIALIZED VIEW london_price_houses_geocoded AS
        SELECT
            lph.*,
            ST_Transform(p.geom, 900913) as geom
        FROM
            london_price_houses lph
        LEFT JOIN uk_postcodes p ON lph.postcode = p.postcode """,
    task_dep = ["london_price_houses"])
