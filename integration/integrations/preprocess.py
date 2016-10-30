import os.path
from dilectic.utils import *
from dilectic.actions import *
import logging

@task
def task_migration_xls(cfg):
    """Net Migration By Borough XLS"""
    source = os.path.join(cfg.raw_dir, "net-migration-natural-change-region-borough.xls")
    dest = os.path.join(cfg.derived_dir, "net-migration-natural-change-region-borough.csv")
    return xls2csv(source, dest, 3)

@task
def task_gla_land_assets(cfg):
    sources = [
        "GLA_assets.xlsx",
        "LFB_assets.xlsx",
        "LLDC_assets.xlsx",
        "MPS_assets.xlsx",
        "TFL_assets.xlsx"
    ]
    yield mkdir_p(os.path.join(cfg.derived_dir, "gla-land-assets"), name="mkdir_p")
    for s in sources:
        source = os.path.join(cfg.raw_dir, "gla-land-assets", s)
        dest = os.path.join(cfg.derived_dir, "gla-land-assets", s.replace(".xlsx", ".csv"))
        yield xlsx2csv(source, dest, name={"{0}_to_csv".format(s)})

@task
def task_green_belt(cfg):
    import os
    table_name = "local_authority_green_belt_boundaries_2014_15"
    yield unzip(
        name = "unzip",
        source = os.path.join(cfg.raw_dir, "Local_Authority_green_belt_boundaries_2014-15.zip"),
        dest = cfg.derived_dir,
        targets = [os.path.join(cfg.derived_dir, "Local_Authority_green_belt_boundaries_2014-15.shp")]
    )
    yield shp_to_sql(
        source = os.path.join(cfg.derived_dir, "Local_Authority_green_belt_boundaries_2014-15.shp"),
        dest = os.path.join(cfg.derived_dir, table_name + ".sql"),
        srid = 4326,
        name = "shp_to_sql"
    )

@task
def task_statistical_gis_boundaries_london(cfg):
    files = ["LSOA_2011_London_gen_MHW.shp", "London_Borough_Excluding_MHW.shp"]

    yield unzip(
        name = "unzip",
        source = os.path.join(cfg.raw_dir, "statistical-gis-boundaries-london.zip"),
        dest = cfg.derived_dir,
        targets = [os.path.join(cfg.derived_dir, "statistical-gis-boundaries-london/ESRI", f) for f in files]
    )

    for f in files:
        table_name = f.replace(".shp", "").lower()
        yield shp_to_sql(
            source = os.path.join(cfg.derived_dir, "statistical-gis-boundaries-london/ESRI", f),
            dest = os.path.join(cfg.derived_dir, table_name + ".sql"),
            srid = 27700,
            name = "shp_to_sql: " + f
        )

@task
def task_postcode_boundaries(cfg):
    shape_files = {
        "Areas.shp": "postcode_areas",
        "Districts.shp": "postcode_districts",
        "Sectors.shp": "postcode_sectors"
    }

    yield unzip(
        name = "unzip",
        source = os.path.join(cfg.raw_dir, "UK-postcode-boundaries-Jan-2015.zip"),
        dest = cfg.derived_dir,
        targets = [os.path.join(cfg.derived_dir, "Distribution", k) for k in shape_files.keys()]
    )

    for shape_file, table_name in shape_files.items():
        yield shp_to_sql(
            source = os.path.join(cfg.derived_dir, "Distribution", shape_file),
            dest = os.path.join(cfg.derived_dir, "{0}.sql".format(table_name)),
            srid = 27700,
            name = shape_file
        )

@task
def task_statistical_gis_boundaries(cfg):
    yield unzip(
        source = os.path.join(cfg.raw_dir, "LSOAs.zip"),
        dest = cfg.derived_dir,
        files = [
            "data/Lower_layer_super_output_areas_(E+W)_2001_Boundaries_(Full_Extent)_V2.zip",
            "data/Lower_layer_super_output_areas_(E+W)_2011_Boundaries_(Full_Extent)_V2.zip"
            ],
        name="unzip"
    )

    yield unzip(
        name = "unzip_lsoa 2011",
        source = os.path.join(cfg.derived_dir, "data/Lower_layer_super_output_areas_(E+W)_2011_Boundaries_(Full_Extent)_V2.zip"),
        dest = cfg.derived_dir,
        files = []
    )

    yield shp_to_sql(
        source = os.path.join(cfg.derived_dir, "LSOA_2011_EW_BFE_V2.shp"),
        dest = os.path.join(cfg.derived_dir, "lsoa_2011_ew_bfe_v2.sql"),
        srid = 27700,
        name = "shp2sql 2011"
        )

    yield unzip(
        name = "unzip_lsoa 2001",
        source = os.path.join(cfg.derived_dir, "data/Lower_layer_super_output_areas_(E+W)_2001_Boundaries_(Full_Extent)_V2.zip"),
        dest = cfg.derived_dir,
        files = []
    )

    yield shp_to_sql(
        source = os.path.join(cfg.derived_dir, "LSOA_2001_EW_BFE_V2.shp"),
        dest = os.path.join(cfg.derived_dir, "lsoa_2001_ew_bfe_v2.sql"),
        srid = 27700,
        name = "shp2sql 2001"
        )

@task
def task_crime_data(cfg):
    return unzip(
        source = os.path.join(cfg.raw_dir, "crime_data.zip"),
        dest = cfg.derived_dir,
        targets = [os.path.join(cfg.derived_dir, "crime_data")]
    )

@task
def task_uk_postcodes(cfg):
    return unzip(
        source = os.path.join(cfg.raw_dir, "ukpostcodes.zip"),
        dest = cfg.derived_dir,
        targets = [os.path.join(cfg.derived_dir, "ukpostcodes.csv")]
    )

@task
def task_gb_road_traffic_counts(cfg):
    dest = os.path.join(cfg.derived_dir, "gb-road-traffic-counts")
    yield mkdir_p(dest, name="mkdir_p")
    yield unzip(
        name="unzip",
        source = os.path.join(cfg.raw_dir, "gb-road-traffic-counts.zip"),
        dest = dest,
        targets = [os.path.join(dest, "data", "AADF-data-major-roads.zip")]
    )
    yield unzip(
        name="AADF-data-major-roads",
        source = os.path.join(dest, "data", "AADF-data-major-roads.zip"),
        dest = dest,
        targets = [os.path.join(dest, "AADF-data-major-roads.csv")]
    )
