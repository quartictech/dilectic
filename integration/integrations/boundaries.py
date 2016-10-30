from dilectic.utils import *
from dilectic.actions import *

@task
def postcode_districts_table(cfg):
    return db_create(cfg.db(), 'postcode_districts', sql_file=os.path.join(cfg.derived_dir, 'postcode_districts.sql'), db_config=cfg.config["db"])

@task
def lsoa_2001_table(cfg):
    return db_create(cfg.db(), 'lsoa_2001_ew_bfe_v2', sql_file=os.path.join(cfg.derived_dir,'lsoa_2001_ew_bfe_v2.sql'), db_config=cfg.config["db"])

@task
def lsoa_2011_table(cfg):
    return db_create(cfg.db(), 'lsoa_2011_ew_bfe_v2', sql_file=os.path.join(cfg.derived_dir,'lsoa_2011_ew_bfe_v2.sql'), db_config=cfg.config["db"])

@task
def lsoa_2011_london_gen_mhw_table(cfg):
    return db_create(cfg.db(), 'lsoa_2011_london_gen_mhw', sql_file=os.path.join(cfg.derived_dir,'lsoa_2011_london_gen_mhw.sql'), db_config=cfg.config["db"])

@task
def london_borough_excluding_mhw_table(cfg):
    return db_create(cfg.db(), 'london_borough_excluding_mhw', sql_file=os.path.join(cfg.derived_dir, 'london_borough_excluding_mhw.sql'), db_config=cfg.config["db"])

@task
def local_authority_green_belt_boundaries_2014_15(cfg):
    return db_create(cfg.db(), 'local_authority_green_belt_boundaries_2014_15', sql_file=os.path.join(cfg.derived_dir, 'local_authority_green_belt_boundaries_2014_15.sql'), db_config=cfg.config["db"])

@task
def postcode_districts_clean(cfg):
    return db_create(cfg.db(), 'postcode_districts_clean',
    create="""CREATE MATERIALIZED VIEW postcode_districts_clean AS
        SELECT
            g.name,
            ST_SetSRID(ST_Transform(g.geom, 900913), 900913) as geom,
            row_number() over() AS gid
        FROM
            (SELECT name, (ST_Dump(ST_MakeValid(geom))).geom FROM postcode_districts) AS g
        WHERE ST_GeometryType(g.geom) = 'ST_MultiPolygon'
            OR ST_GeometryType(g.geom) = 'ST_Polygon'""",
    task_dep=["postcode_districts_table"]
    )

@task
def lsoa_2001_clean(cfg):
    return db_create(cfg.db(), 'lsoa_2001_ew_bfe_v2_clean',
    create="""CREATE MATERIALIZED VIEW lsoa_2001_ew_bfe_v2_clean AS
        SELECT
            g.code,
            g.name,
            ST_SetSRID(ST_Transform(g.geom, 900913), 900913) as geom,
            row_number() over() AS gid
        FROM
            (SELECT lsoa01cd as code, lsoa01nm as name, (ST_Dump(ST_MakeValid(geom))).geom FROM lsoa_2001_ew_bfe_v2) AS g
        WHERE ST_GeometryType(g.geom) = 'ST_MultiPolygon'
            OR ST_GeometryType(g.geom) = 'ST_Polygon'""",
    task_dep=["lsoa_2001_table"]
    )

@task
def lsoa_2011_clean(cfg):
    return db_create(cfg.db(), 'lsoa_2011_ew_bfe_v2_clean',
    create="""CREATE MATERIALIZED VIEW lsoa_2011_ew_bfe_v2_clean AS
        SELECT
            g.code,
            g.name,
            ST_SetSRID(ST_Transform(g.geom, 900913), 900913) as geom,
            row_number() over() AS gid
        FROM
            (SELECT lsoa11cd as code, lsoa11nm as name, (ST_Dump(ST_MakeValid(geom))).geom FROM lsoa_2011_ew_bfe_v2) AS g
        WHERE ST_GeometryType(g.geom) = 'ST_MultiPolygon'
            OR ST_GeometryType(g.geom) = 'ST_Polygon'""",
    task_dep=["lsoa_2011_table"]
    )
