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
import os.path

db = DBMake()

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
            t.color as "_color",
            ST_SetSRID(ST_GeomFromGeoJson(t.geojson), 4326) as geom
        FROM
            _tube t
            """)

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

db.table('london_borough_profiles',
    """ CREATE TABLE IF NOT EXISTS london_borough_profiles (
--        Code VARCHAR,
        AreaName VARCHAR,
        "Inner Outer London" VARCHAR,
        "GLA Population Estimate (2015)" DOUBLE PRECISION,
        "GLA Household Estimate (2015)" DOUBLE PRECISION,
        "Inland Area Hectares" DOUBLE PRECISION,
        "Population Density Per Hectare 2015" DOUBLE PRECISION,
        "Average Age 2015" DOUBLE PRECISION,
        "Proportion Of Population Aged 0 to 15 (2015)" DOUBLE PRECISION,
        "Proportion Of Population Of Working Age (2015)" DOUBLE PRECISION,
        "Proportion Of Population Aged 65 And Over (2015)" DOUBLE PRECISION,
        "Net Internal Migration (2014)" DOUBLE PRECISION,
        "Net International Migration (2014)" DOUBLE PRECISION,
        "Net Natural Change (2014)" DOUBLE PRECISION,
        "Percentage Of Resident Population Born Abroad (2014)" DOUBLE PRECISION,
        "Largest Migrant Population By Country Of Birth (2011)" VARCHAR,
        "Percentage Of Largest Migrant Population (2011)" DOUBLE PRECISION,
--        Second largest migrant population by country of birth (2011),
--        % of second largest migrant population (2011),
--        Third largest migrant population by country of birth (2011),
--        % of third largest migrant population (2011),
--        % of population from BAME groups (2013),
--        % people aged 3+ whose main language is not English (2011 Census),
--        "Overseas nationals entering the UK (NINo), (2014/15)",
--        "New migrant (NINo) rates, (2014/15)",
--        Largest migrant population arrived during 2014/15,
--        Second largest migrant population arrived during 2014/15,
--        Third largest migrant population arrived during 2014/15,
--        EmploymentRatePercentage2014 DOUBLE PRECISION,
--        Male employment rate (2014),
--        Female employment rate (2014),
--        UnemploymentRate2014 DOUBLE PRECISION,
--        Youth Unemployment (claimant) rate 18-24 (Dec-14),
--        Proportion of 16-18 year olds who are NEET (%) (2014),
--        Proportion of the working-age population who claim out-of-work benefits (%) (May-2014),
--        % working-age with a disability (2014),
--        Proportion of working age people with no qualifications (%) 2014,
--        Proportion of working age with degree or equivalent and above (%) 2014,
        "Gross Annual Pay, (2014)" DOUBLE PRECISION,
--        Gross Annual Pay - Male (2014),
--        Gross Annual Pay - Female (2014),
        "Modelled Household median income estimates 2012/13" DOUBLE PRECISION,
--        % adults that volunteered in past 12 months (2010/11 to 2012/13),
--        Number of jobs by workplace (2013),
--        % of employment that is in public sector (2013),
        "Jobs Density, 2013" DOUBLE PRECISION,
--        "Number of active businesses, 2013",
--        Two-year business survival rates (started in 2011),
        "Crime rates per thousand population 2014/15" DOUBLE PRECISION,
--        Fires per thousand population (2014),
--        Ambulance incidents per hundred population (2014),
--        "Median House Price, 2014",
--        "Average Band D Council Tax charge (�), 2015/16",
--        New Homes (net) 2013/14,
--        "Homes Owned outright, (2014) %",
--        "Being bought with mortgage or loan, (2014) %",
--        "Rented from Local Authority or Housing Association, (2014) %",
--        "Rented from Private landlord, (2014) %",
--        "% of area that is Greenspace, 2005",
--        Total carbon emissions (2013),
--        "Household Waste Recycling Rate, 2013/14",
--        "Number of cars, (2011 Census)",
--        "Number of cars per household, (2011 Census)",
--        "% of adults who cycle at least once per month, 2013/14",
--        "Average Public Transport Accessibility score, 2014",
--        "Achievement of 5 or more A*- C grades at GCSE or equivalent including English and Maths, 2013/14",
--        Rates of Children Looked After (2014),
--        % of pupils whose first language is not English (2014),
--        % children living in out-of-work households (2014),
--        "Male life expectancy, (2011-13)",
--        "Female life expectancy, (2011-13)",
--        Teenage conception rate (2013),
--        Life satisfaction score 2011-14 (out of 10),
--        Worthwhileness score 2011-14 (out of 10),
         "Happiness Score (2011-14) Out Of 10" DOUBLE PRECISION,
--        Anxiety score 2011-14 (out of 10),
--        Childhood Obesity Prevalance (%) 2013/14,
--        People aged 17+ with diabetes (%),
--        Mortality rate from causes considered preventable,
        "Political control in council" VARCHAR,
        "Proportion of seats won by Conservatives in 2014 election" DOUBLE PRECISION,
        "Proportion of seats won by Labour in 2014 election" DOUBLE PRECISION,
        "Proportion of seats won by Lib Dems in 2014 election" DOUBLE PRECISION,
        "Turnout At 2014 Local Elections" DOUBLE PRECISION)""",
        fill=fill_borough_profiles)


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

db.table('naptan',
    create="""CREATE TABLE IF NOT EXISTS naptan (
        StopAreaCode VARCHAR,
        Name VARCHAR,
        NameLang VARCHAR,
        AdministrativeAreaCode VARCHAR,
        StopAreaType VARCHAR,
        GridType VARCHAR,
        Easting INT,
        Northing INT,
        CreationDateTime DATE,
        ModificationDateTime DATE,
        RevisionNumber INT,
        Modification VARCHAR,
        Status VARCHAR
    )""",
    fill=fill_naptan)

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

db.table('postcode_districts', sql_file='../data/postcode_districts.sql')
db.table('lsoa_2001_ew_bfe_v2', sql_file='lsoa/data/lsoa_2001_ew_bfe_v2.sql')
db.table('lsoa_2011_ew_bfe_v2', sql_file='lsoa/data/lsoa_2011_ew_bfe_v2.sql')
db.table('lsoa_2011_london_gen_mhw', sql_file='statistical-gis-boundaries-london/ESRI/lsoa_2011_london_gen_mhw.sql')

db.table('London_Borough_Excluding_MHW', sql_file='statistical-gis-boundaries-london/ESRI/London_Borough_Excluding_MHW.sql')

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

db.index('companies_geocoded_index', create="CREATE INDEX companies_geocoded_index ON companies_geocoded USING GIST(geom)")
db.index('lsoa_2001_ew_bfe_v2_clean_index', create="CREATE INDEX lsoa_2001_ew_bfe_v2_clean_index ON lsoa_2001_ew_bfe_v2_clean USING GIST(geom)")
db.index('lsoa_2011_ew_bfe_v2_clean_index', create="CREATE INDEX lsoa_2011_ew_bfe_v2_clean_index ON lsoa_2011_ew_bfe_v2_clean USING GIST(geom)")

config_file = os.path.join(os.path.dirname(__file__), '../config.yml')
db.run(config=config_file)
