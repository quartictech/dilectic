#!/bin/sh
pip3 install -r /home/integration/requirements.txt

python3 /home/integration/scripts/billboards.py /home/data/raw/signkick.csv /home/data/derived/signkick.json
python3 /home/integration/scripts/gb_road_traffic_counts.py /home/data/derived/gb-road-traffic-counts/AADF-data-major-roads.csv /home/data/derived/gb-road-traffic-counts/AADF-data-major-roads.json
