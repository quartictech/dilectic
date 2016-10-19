#!/bin/sh
pip3 install -r /home/integration/requirements.txt

python3 /home/integration/scripts/billboards.py /home/data/raw/signkick.csv /home/data/derived/signkick.json
