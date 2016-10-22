#!/bin/sh
set -eu

pip3 install -r ${INTEGRATION}/requirements.txt

python3 ${INTEGRATION}/scripts/import.py
