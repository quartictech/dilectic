#!/bin/sh
set -eu

pip3 install -r ${INTEGRATION}/requirements.txt

python3 ${INTEGRATION}/scripts/billboards.py ${INPUT}/signkick.csv ${OUTPUT}/signkick.json
python3 ${INTEGRATION}/scripts/gb_road_traffic_counts.py ${OUTPUT}/gb-road-traffic-counts/AADF-data-major-roads.csv ${OUTPUT}/gb-road-traffic-counts/AADF-data-major-roads.json
