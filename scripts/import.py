from dbmake import DBMake
import glob
import csv
import sys
from datetime import datetime
import zipfile
import codecs

def parse_date(s):
    if not s:
        return None
    try:
        return datetime.strptime(s, '%d/%m/%Y')
    except:
        raise ValueError("Can't parse " + s)

def process_companies_csv(conn, f):
    rdr = csv.reader(f)
    next(rdr) # Skip header
    count = 0
    curs = conn.cursor()
    for line in rdr:
        dissolution_date = parse_date(line[13])
        incorporation_date = parse_date(line[14])

        sql = "INSERT INTO companies VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        values = (line[0], line[1], line[10], line[11], line[12],
        dissolution_date, incorporation_date, line[26], line[27],
        line[28], line[9])

        curs.execute(sql, values)
        count += 1
        if count % 10000 == 0:
            print(count)
            conn.commit()

def extract_companies_zip(fname, conn):
    zip = zipfile.ZipFile(fname)
    names = list(zip.namelist())
    print("%s : %d files" % (fname, len(names)))
    for name in names:
        f = codecs.iterdecode(zip.open(name), 'utf-8')
        process_companies_csv(conn, f)

def fill_companies_table(conn):
    for f in glob.glob("../data/register/zip/*.zip"):
        extract_companies_zip(f, conn)

db = DBMake()
db.table('companies',
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

db.table('postcode_districts', sql_file='../data/postcode_districts.sql')


db.materialized_view('postcode_districts_clean',
    create="""CREATE MATERIALIZED VIEW postcode_districts_clean AS
        SELECT
            g.name,
            ST_SetSRID(ST_Transform(g.geom, 900913), 900913) as geom,
            row_number() over() AS gid
        FROM
            (SELECT name, (ST_Dump(ST_MakeValid(geom))).geom FROM postcode_districts) AS g
        WHERE ST_GeometryType(g.geom) = 'ST_MultiPolygon'
            OR ST_GeometryType(g.geom) = 'ST_Polygon'"""
)

db.materialized_view('companies_by_postcode_district',
    create="""CREATE MATERIALIZED VIEW companies_by_postcode_district AS
        SELECT
            substring(Postcode from 0 for GREATEST(0, char_length(Postcode) - 3)) as postcode_district,
            count(*)
        FROM
            companies c
        GROUP BY postcode_district"""
    )

db.materialized_view('postcode_district_company_count',
    create="""CREATE MATERIALIZED VIEW postcode_district_company_count AS
        SELECT
            c.postcode_district AS name,
            c.count AS count,
            geom
        FROM postcode_districts_clean d
        LEFT JOIN companies_by_postcode_district c
        ON c.postcode_district = d.name""")

db.index('postcode_district_company_count_index',
    create="CREATE INDEX postcode_district_company_count_index ON postcode_district_company_count USING GIST(geom)")

db.run('host=localhost dbname=postgres user=postgres password=dilectic')
