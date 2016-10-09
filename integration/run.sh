#!/bin/sh
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $DIR/config.sh

docker_run() {
  echo "Running $1 in docker"
  docker run --rm --net="host" \
    -v $DIR:/integration \
    -v $DERIVED_DIR:/derived \
    -v $RAW_DIR:/raw \
    -e "INPUT=/raw" \
    -e "OUTPUT=/derived" \
    -w /work \
    quartic/data-worker \
    /integration/$1
}

docker_run 0-from-raw.sh
docker_run 1-dump-osm-to-sql.sh
docker_run 2-import-postgres.sh
