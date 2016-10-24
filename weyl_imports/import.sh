#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
echo $DIR

CONTEXT_PATH=""
if [ "$#" -eq 1  ]; then
    CONTEXT_PATH=${1}
fi

API_ROOT=http://localhost:8080${CONTEXT_PATH}/api

curl -XPUT -H Content-Type:application/json $API_ROOT/import/postgres -d @json_imports/postcodes.json
curl -XPUT -H Content-Type:application/json $API_ROOT/import/postgres -d @json_imports/companies.json
curl -XPUT -H Content-Type:application/json $API_ROOT/import/postgres -d @json_imports/postcode_districts.json
curl -XPUT -H Content-Type:application/json $API_ROOT/import/postgres -d @json_imports/boroughs.json
curl -XPUT -H Content-Type:application/json $API_ROOT/import/postgres -d @json_imports/boroughs_nino.json
curl -XPUT -H Content-Type:application/json $API_ROOT/import/postgres -d @json_imports/house_sales.json
curl -XPUT -H Content-Type:application/json $API_ROOT/import/postgres -d @json_imports/public_land_assets.json
curl -XPUT -H Content-Type:application/json $API_ROOT/import/postgres -d @json_imports/mcdonalds.json
curl -XPUT -H Content-Type:application/json $API_ROOT/import/postgres -d @json_imports/lsoas.json
curl -XPUT -H Content-Type:application/json $API_ROOT/import/postgres -d @json_imports/roads.json
curl -XPUT -H Content-Type:application/json $API_ROOT/import/postgres -d @json_imports/parking.json
curl -XPUT -H Content-Type:application/json $API_ROOT/import/postgres -d @json_imports/nightlife.json
curl -XPUT -H Content-Type:application/json $API_ROOT/import/postgres -d @json_imports/greenspace.json
curl -XPUT -H Content-Type:application/json $API_ROOT/import/postgres -d @json_imports/buildings.json
curl -XPUT -H Content-Type:application/json $API_ROOT/import/postgres -d @json_imports/rail.json
curl -XPUT -H Content-Type:application/json $API_ROOT/import/postgres -d @json_imports/tube_query.json
curl -XPUT -H Content-Type:application/json $API_ROOT/import/postgres -d @json_imports/residential.json
curl -XPUT -H Content-Type:application/json $API_ROOT/import/postgres -d @json_imports/residential_land_use.json
curl -XPUT -H Content-Type:application/json $API_ROOT/import/postgres -d @json_imports/commercial.json
curl -XPUT -H Content-Type:application/json $API_ROOT/import/postgres -d @json_imports/jamcams.json
curl -XPUT -H Content-Type:application/json $API_ROOT/import/postgres -d @json_imports/crime.json
curl -XPUT -H Content-Type:application/json $API_ROOT/import/postgres -d @json_imports/green_belt.json


curl -XPUT -H Content-Type:application/json $API_ROOT/import/geojson -d @../data/derived/signkick.json
curl -XPUT -H Content-Type:application/json $API_ROOT/import/geojson -d @../data/derived/gb-road-traffic-counts/AADF-data-major-roads.json
curl -XPUT -H Content-Type:application/json $API_ROOT/import/geojson -d @../data/derived/zoopla.json

source $DIR/../integration/env/bin/activate
python $DIR/../integration/scripts/live/disruptions_api.py
