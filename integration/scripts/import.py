#!/usr/bin/env python3
from dbmake import DBMake
from companies_register import fill_companies_table
from uk_postcodes import fill_postcodes_table
from gva_gdhi import gva_gdhi_to_pg
from london_price_houses import fill_london_house_prices
from mcdonalds import fill_mcdonalds_table
from gla_land_and_assets import fill_land_and_assets_table
from borough_profiles import fill_borough_profiles
from tube import fill_tube
from naptan import fill_naptan
from crime import fill_crime_table
from jamcams import fill_jamcams
from net_migration_borough import fill_migration_borough_table
from ni_num_registrations import fill_ni_borough_table, get_table_columns
import os.path
import sys

db = DBMake()
config_file = os.path.join(os.path.dirname(__file__), '../config.yml')

# db.table('jamcams',
#     create="""CREATE TABLE IF NOT EXISTS jamcams (
#         Corridor VARCHAR,
#         Location VARCHAR,
#         View VARCHAR,
#         File VARCHAR,
#         Date DATE,
#         Lat DOUBLE PRECISION,
#         Lon DOUBLE PRECISION,
#         PostCode VARCHAR
#         )""",
#         fill=fill_jamcams)

db.table('nino_registration_boroughs',
    create="""CREATE TABLE IF NOT EXISTS nino_registration_boroughs ({0})""".format(get_table_columns(config_file)),
    fill=fill_ni_borough_table,
    direct=True)

# db.table('migration_boroughs',
#     create="""CREATE TABLE IF NOT EXISTS migration_boroughs (
#         BoroughCode VARCHAR,
#         Borough VARCHAR,
#         NatChange JSON,
#         InternalNet JSON,
#         InternationalNet JSON,
#         OtherChange JSON
#
#     )""",
#     fill=fill_migration_borough_table,
#     direct=True)



# db.materialized_view('jamcams_geocoded',
#     create = """ CREATE MATERIALIZED VIEW jamcams_geocoded AS
#         SELECT
#             m.*,
#             ST_SetSRID(ST_MakePoint(m.Lon, m.Lat), 4326) as geom
#         FROM
#             jamcams m
#             """)

db.table('_tube',
    create="""CREATE TABLE IF NOT EXISTS _tube (
        id VARCHAR,
        name VARCHAR,
        color VARCHAR,
        geojson VARCHAR
        )
    """,
    fill=fill_tube)

db.materialized_view('tube',
    create = """ CREATE MATERIALIZED VIEW tube AS
        SELECT
            t.id,
            t.name,
            ST_SetSRID(ST_GeomFromGeoJson(t.geojson), 4326) as geom
        FROM
            _tube t
            """)

db.table('_uk_postcodes',
    create="""CREATE TABLE IF NOT EXISTS _uk_postcodes (
  	postcode VARCHAR,
        latitude DOUBLE PRECISION,
	longitude DOUBLE PRECISION);
    """,
    fill=fill_postcodes_table)

db.materialized_view('uk_postcodes',
    create = """ CREATE MATERIALIZED VIEW uk_postcodes AS
        SELECT
            p.*,
            ST_SetSRID(ST_MakePoint(p.longitude, p.latitude), 4326) as geom
        FROM
            _uk_postcodes p
            """)


db.table('london_gva_gdhi',
    create="""CREATE TABLE IF NOT EXISTS london_gva_gdhi (
        UKI VARCHAR,
        Year INT,
        AreaName VARCHAR,
        GVA INT,
        GVAPerHead INT,
        PerHeadIndices INT,
        GrossDisposableHouse INT,
        GDHIPerHead INT)
    """,
    fill=gva_gdhi_to_pg)

db.table('public_land_assets',
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

db.table('london_price_houses',
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

db.materialized_view('london_price_houses_geocoded',
    create = """ CREATE MATERIALIZED VIEW london_price_houses_geocoded AS
        SELECT
            lph.*,
            ST_Transform(p.geom, 900913) as geom
        FROM
            london_price_houses lph
        LEFT JOIN uk_postcodes p ON lph.postcode = p.postcode """)

db.table('mcdonalds',
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

# db.table('naptan',
#    create="""CREATE TABLE IF NOT EXISTS naptan (
#        StopAreaCode VARCHAR,
#        Name VARCHAR,
#        NameLang VARCHAR,
#        AdministrativeAreaCode VARCHAR,
#        StopAreaType VARCHAR,
#        GridType VARCHAR,
#        Easting INT,
#        Northing INT,
#        CreationDateTime DATE,
#        ModificationDateTime DATE,
#        RevisionNumber INT,
#        Modification VARCHAR,
#        Status VARCHAR
#    )""",
#    fill=fill_naptan)

db.table('crime',
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

db.table('postcode_districts', sql_file='derived/postcode_districts.sql')
db.table('lsoa_2001_ew_bfe_v2', sql_file='derived/lsoa_2001_ew_bfe_v2.sql')
db.table('lsoa_2011_ew_bfe_v2', sql_file='derived/lsoa_2011_ew_bfe_v2.sql')
db.table('lsoa_2011_london_gen_mhw', sql_file='derived/lsoa_2011_london_gen_mhw.sql')

db.table('London_Borough_Excluding_MHW', sql_file='derived/London_Borough_Excluding_MHW.sql')
db.table('local_authority_green_belt_boundaries_2014_15', sql_file='derived/local_authority_green_belt_boundaries_2014_15.sql')

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

db.materialized_view('lsoa_2001_ew_bfe_v2_clean',
    create="""CREATE MATERIALIZED VIEW lsoa_2001_ew_bfe_v2_clean AS
        SELECT
            g.code,
            g.name,
            ST_SetSRID(ST_Transform(g.geom, 900913), 900913) as geom,
            row_number() over() AS gid
        FROM
            (SELECT lsoa01cd as code, lsoa01nm as name, (ST_Dump(ST_MakeValid(geom))).geom FROM lsoa_2001_ew_bfe_v2) AS g
        WHERE ST_GeometryType(g.geom) = 'ST_MultiPolygon'
            OR ST_GeometryType(g.geom) = 'ST_Polygon'"""
)

db.materialized_view('lsoa_2011_ew_bfe_v2_clean',
    create="""CREATE MATERIALIZED VIEW lsoa_2011_ew_bfe_v2_clean AS
        SELECT
            g.code,
            g.name,
            ST_SetSRID(ST_Transform(g.geom, 900913), 900913) as geom,
            row_number() over() AS gid
        FROM
            (SELECT lsoa11cd as code, lsoa11nm as name, (ST_Dump(ST_MakeValid(geom))).geom FROM lsoa_2011_ew_bfe_v2) AS g
        WHERE ST_GeometryType(g.geom) = 'ST_MultiPolygon'
            OR ST_GeometryType(g.geom) = 'ST_Polygon'"""
)

db.materialized_view('companies_geocoded',
    create = """ CREATE MATERIALIZED VIEW companies_geocoded AS
        SELECT
            c.*,
            ST_Transform(p.geom, 900913) as geom
        FROM
            companies c
        LEFT JOIN uk_postcodes p ON p.postcode = c.postcode """)

db.materialized_view('public_land_assets_geocoded',
    create = """ CREATE MATERIALIZED VIEW public_land_assets_geocoded AS
        SELECT
            pla.*,
            ST_SetSRID(ST_MakePoint(pla.Easting, pla.Northing), 27700) as geom
        FROM
            public_land_assets pla
            """)


db.materialized_view('mcdonalds_geocoded',
    create = """ CREATE MATERIALIZED VIEW mcdonalds_geocoded AS
        SELECT
            m.*,
            ST_SetSRID(ST_MakePoint(m.Longitude, m.Latitude), 4326) as geom
        FROM
            mcdonalds m
            """)

db.materialized_view('crime_geocoded',
    create = """ CREATE MATERIALIZED VIEW crime_geocoded AS
        SELECT
            m.*,
            ST_SetSRID(ST_MakePoint(m.Longitude, m.Latitude), 4326) as geom
        FROM
            crime m
            """)

db.run(config=config_file)
