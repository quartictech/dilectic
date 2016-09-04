#!/usr/bin/env python
from dbmake import DBMake
from companies_register import fill_companies_table
from uk_postcodes import fill_postcodes_table
from gva_gdhi import gva_gdhi_to_pg
from london_price_houses import fill_london_house_prices
from mcdonalds import fill_mcdonalds_table
from gla_land_and_assets import fill_land_and_assets_table
import os.path

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

db.table('uk_postcodes',
    create="""CREATE TABLE IF NOT EXISTS uk_postcodes (
        id SERIAL,
        postcode VARCHAR);
        SELECT AddGeometryColumn ('public','uk_postcodes','geom',4326,'POINT',2);
    """,
    fill=fill_postcodes_table)

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
        Eastling INT,
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

db.table('mcdonalds',
    create="""CREATE TABLE IF NOT EXISTS mcdonalds (
        Name VARCHAR,
        Street VARCHAR,
        Town VARCHAR,
        PostCode VARCHAR,
        PhoneNumber VARCHAR
    );
    SELECT AddGeometryColumn ('public', 'mcdonalds', 'geom', 4326, 'POINT', 2);""",
    fill=fill_mcdonalds_table)

db.table('postcode_districts', sql_file='../data/postcode_districts.sql')

db.table('lsoa_2001_ew_bfe_v2', sql_file='lsoa/data/lsoa_2001_ew_bfe_v2.sql')
db.table('lsoa_2011_ew_bfe_v2', sql_file='lsoa/data/lsoa_2011_ew_bfe_v2.sql')
db.table('lsoa_2011_london_gen_mhw', sql_file='statistical-gis-boundaries-london/ESRI/lsoa_2011_london_gen_mhw.sql')

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

db.materialized_view('companies_by_postcode_district',
    create="""CREATE MATERIALIZED VIEW companies_by_postcode_district AS
        SELECT
            substring(Postcode from 0 for GREATEST(0, char_length(Postcode) - 3)) as postcode_district,
            count(*)
        FROM
            companies c
        GROUP BY postcode_district"""
    )

db.materialized_view('companies_geocoded',
    create = """ CREATE MATERIALIZED VIEW companies_geocoded AS
        SELECT
            c.*,
            ST_Transform(p.geom, 900913) as geom
        FROM
            companies c
        LEFT JOIN uk_postcodes p ON p.postcode = c.postcode """)

db.index('companies_geocoded_index', create="CREATE INDEX companies_geocoded_index ON companies_geocoded USING GIST(geom)")
db.index('lsoa_2001_ew_bfe_v2_clean_index', create="CREATE INDEX lsoa_2001_ew_bfe_v2_clean_index ON lsoa_2001_ew_bfe_v2_clean USING GIST(geom)")
db.index('lsoa_2011_ew_bfe_v2_clean_index', create="CREATE INDEX lsoa_2011_ew_bfe_v2_clean_index ON lsoa_2011_ew_bfe_v2_clean USING GIST(geom)")

db.materialized_view('companies_by_lsoa',
    create="""CREATE MATERIALIZED VIEW companies_by_lsoa AS
    with g AS (SELECT
            l.code,
            count(c.geom) as count
        FROM lsoa_2011_ew_bfe_v2_clean l
        LEFT JOIN companies_geocoded c ON ST_contains(l.geom, c.geom)
        GROUP BY l.code)
        SELECT g.code, ll.name, g.count, ll.geom FROM g LEFT JOIN lsoa_2011_ew_bfe_v2_clean ll ON ll.code = g.code
        """)

db.materialized_view('companies_by_lsoa_london',
    create="""CREATE MATERIALIZED VIEW companies_by_lsoa_london AS
        select c.* from companies_by_lsoa c inner join lsoa_2011_london_gen_mhw on lsoa11cd = code
        """
    )

db.index('companies_by_lsoa_london_index',
    create="CREATE INDEX companies_by_lsoa_london_index ON companies_by_lsoa_london USING GIST(geom)")

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

config_file = os.path.join(os.path.dirname(__file__), '../config.yml')
db.run(config=config_file)
