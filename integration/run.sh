#!/bin/bash
set -eu
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

ROOT_DIR=${1}

source $DIR/config.sh

docker_run() {
  echo "Running $1 in docker"
  docker run --rm --net="host" \
    -v ${ROOT_DIR}/integration:/integration \
    -v ${ROOT_DIR}/data/raw:/raw \
    -v ${ROOT_DIR}/data/derived:/derived \
    -e "INPUT=/raw" \
    -e "OUTPUT=/derived" \
    -e "INTEGRATION=/integration" \
    -w /work \
    eu.gcr.io/quartic-oliver/data-worker:0.2.0-8-g0450536 \
    /integration/$1
}

docker_run 0-from-raw.sh
docker_run 1-dump-osm-to-sql.sh
docker_run 2-import-postgres.sh
docker_run 3-import-geojson.sh
