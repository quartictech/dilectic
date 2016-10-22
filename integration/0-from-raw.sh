#!/bin/sh
shp_to_sql() {
    shp2pgsql -s $3 $1 $2 > $OUTPUT/$2.sql
}

xls2csv $INPUT/net-migration-natural-change-region-borough.xls | awk 'BEGIN { RS = ""; FS=""} { print $3 }' > $OUTPUT/net-migration-natural-change-region-borough.csv
mkdir -p $OUTPUT/gla-land-assets
xlsx2csv $INPUT/gla-land-assets/GLA_assets.xlsx $OUTPUT/gla-land-assets/GLA_assets.csv
xlsx2csv $INPUT/gla-land-assets/LFB_assets.xlsx $OUTPUT/gla-land-assets/LFB_assets.csv
xlsx2csv $INPUT/gla-land-assets/LLDC_assets.xlsx $OUTPUT/gla-land-assets/LLDC_assets.csv
xlsx2csv $INPUT/gla-land-assets/MPS_assets.xlsx $OUTPUT/gla-land-assets/MPS_assets.csv
xlsx2csv $INPUT/gla-land-assets/TFL_assets.xlsx $OUTPUT/gla-land-assets/TFL_assets.csv

unzip -o $INPUT/Local_Authority_green_belt_boundaries_2014-15.zip
shp_to_sql Local_Authority_green_belt_boundaries_2014-15.shp local_authority_green_belt_boundaries_2014_15 4326

unzip -o $INPUT/statistical-gis-boundaries-london.zip
shp_to_sql statistical-gis-boundaries-london/ESRI/LSOA_2011_London_gen_MHW.shp lsoa_2011_london_gen_mhw 27700
shp_to_sql statistical-gis-boundaries-london/ESRI/London_Borough_Excluding_MHW.shp London_Borough_Excluding_MHW 27700

unzip -o $INPUT/LSOAs.zip "data/Lower_layer_super_output_areas_(E+W)_2001_Boundaries_(Full_Extent)_V2.zip"
unzip -o "data/Lower_layer_super_output_areas_(E+W)_2001_Boundaries_(Full_Extent)_V2.zip"
shp_to_sql LSOA_2001_EW_BFE_V2.shp lsoa_2001_ew_bfe_v2 27700

unzip -o $INPUT/LSOAs.zip "data/Lower_layer_super_output_areas_(E+W)_2011_Boundaries_(Full_Extent)_V2.zip"
unzip -o "data/Lower_layer_super_output_areas_(E+W)_2011_Boundaries_(Full_Extent)_V2.zip"
shp_to_sql LSOA_2011_EW_BFE_V2.shp lsoa_2011_ew_bfe_v2 27700

unzip -o $INPUT/UK-postcode-boundaries-Jan-2015.zip
shp_to_sql Distribution/Areas.shp postcode_areas 27700
shp_to_sql Distribution/Districts postcode_districts 27700
shp_to_sql Distribution/Sectors.shp postcode_sectors 27700

unzip -o $INPUT/crime_data.zip -d $OUTPUT
unzip -o $INPUT/ukpostcodes.zip -d $OUTPUT

mkdir -p $OUTPUT/gb-road-traffic-counts
unzip -o $INPUT/gb-road-traffic-counts.zip -d $OUTPUT/gb-road-traffic-counts
unzip -o $OUTPUT/gb-road-traffic-counts/data/AADF-data-major-roads.zip -d $OUTPUT/gb-road-traffic-counts
