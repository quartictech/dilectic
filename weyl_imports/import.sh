#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
echo $DIR

CONTEXT_PATH=""
if [ "$#" -eq 1  ]; then
    CONTEXT_PATH=${1}
fi

API_ROOT=http://localhost:8080${CONTEXT_PATH}/api
IMPORT_API=$API_ROOT/import/postgres

source $DIR/../integration/env/bin/activate
python $DIR/../integration/scripts/live/disruptions_api.py

curl -XPUT -H Content-Type:application/json $IMPORT_API -d '{
	"name": "UK Postcodes",
	"description": "All postcode centroids in the UK",
	"query": "SELECT * from uk_postcodes"
}'

curl -XPUT -H Content-Type:application/json $IMPORT_API -d '{
	"name": "Companies",
 	"description": "All LTD companies in the UK",
 	"query": "SELECT companyname,geom from companies_geocoded limit 1000000"
}'

curl -XPUT -H Content-Type:application/json $IMPORT_API -d '{
 	"name": "Postcode Districts",
 	"description": "Postcode districts in the UK",
 	"query": "SELECT * from postcode_districts"
}'

curl -XPUT -H Content-Type:application/json $IMPORT_API -d '{
 	"name": "London Boroughs",
 	"description": "London Borough Boundaries",
 	"query": "SELECT lb.name, lb.geom, lbp.*, mb.* from london_borough_excluding_mhw lb left join london_borough_profiles lbp on lb.name = lbp.AreaName left join migration_boroughs mb ON lb.name=mb.borough"
}'


curl -XPUT -H Content-Type:application/json $IMPORT_API -d '{
	"name": "London House Sales",
	"description": "London House Sales and Prices",
	"query": "SELECT * from london_price_houses_geocoded"
}'

curl -XPUT -H Content-Type:application/json $IMPORT_API -d '{
	"name": "Public Land Assets",
	"description": "GLA Public Land Assets",
	"query": "SELECT * from public_land_assets_geocoded"
}'

curl -XPUT -H Content-Type:application/json $IMPORT_API -d '{
	"name": "McDonalds™",
	"description": "McDonalds™ Locations",
	"query": "SELECT * from mcdonalds_geocoded"
}'

curl -XPUT -H Content-Type:application/json $IMPORT_API -d '{
	"name": "London LSOAs",
	"description": "London Lower Super Output Areas",
	"query": "select * from lsoa_2011_london_gen_mhw"
}'

curl -XPUT -H Content-Type:application/json $IMPORT_API -d @json_imports/roads.json
curl -XPUT -H Content-Type:application/json $IMPORT_API -d @json_imports/parking.json
curl -XPUT -H Content-Type:application/json $IMPORT_API -d @json_imports/nightlife.json
curl -XPUT -H Content-Type:application/json $IMPORT_API -d @json_imports/greenspace.json
curl -XPUT -H Content-Type:application/json $IMPORT_API -d @json_imports/buildings.json
curl -XPUT -H Content-Type:application/json $IMPORT_API -d @json_imports/rail.json
curl -XPUT -H Content-Type:application/json $IMPORT_API -d @json_imports/tube_query.json
curl -XPUT -H Content-Type:application/json $IMPORT_API -d @json_imports/residential.json
curl -XPUT -H Content-Type:application/json $IMPORT_API -d @json_imports/residential_land_use.json
curl -XPUT -H Content-Type:application/json $IMPORT_API -d @json_imports/commercial.json

curl -XPUT -H Content-Type:application/json $IMPORT_API -d '{
	"name": "Jamcams",
	"description": "TFL traffic camera feeds",
	"query": "select * from jamcams_geocoded",
  "icon": "camera"
}'

curl -XPUT -H Content-Type:application/json $IMPORT_API -d '{
	"name": "London Crime",
	"description": "London crime events for MET, BTP and City Police",
	"query": "select * from crime_geocoded where crimetype is not null limit 1000000"
}'

curl -XPUT -H Content-Type:application/json $IMPORT_API -d '{
	"name": "Green Belts 2014-2015",
	"description": "Local Authority Green Belt Boundaries 2014-2015",
	"query": "SELECT gb_name as name, area_ha, perim_km, la_name, year, ST_Force_2d(geom) as geom from local_authority_green_belt_boundaries_2014_15"
}'

curl -XPUT -H Content-Type:application/json $IMPORT_API -d '{
        "name": "London Boroughs (NI Number Applications)",
        "description": "London Borough Boundaries with NI Number Applications By Country",
        "query": "SELECT lb.name, lb.geom, lbp.*, mb.*, n.* from london_borough_excluding_mhw lb left join london_borough_profiles lbp on lb.name = lbp.AreaName left join migration_boroughs mb ON lb.name=mb.borough left join nino_registration_boroughs n on n.borough = lbp.AreaName"
}'

curl -XPUT -H Content-Type:application/json $API_ROOT/import/geojson -d @../data/derived/signkick.json

curl -XPUT -H Content-Type:application/json $API_ROOT/import/geojson -d @../data/derived/gb-road-traffic-counts/AADF-data-major-roads.json

