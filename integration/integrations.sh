#!/bin/sh
pip3 install -r /integration/requirements.txt
python3 /integration/run.py
python3 /integration/test.py
