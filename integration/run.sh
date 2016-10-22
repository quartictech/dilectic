#!/bin/bash
set -eu
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $DIR/config.sh

ROOT_DIR=${1}

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
    ${DOCKER_IMAGE} \
    /integration/$1
}

docker_run 0-from-raw.sh
docker_run 1-dump-osm-to-sql.sh
docker_run 2-import-postgres.sh
docker_run 3-import-geojson.sh
