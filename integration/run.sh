#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $DIR/config.sh

docker_run() {
 echo "Running $1 in docker"
 docker run --net="host" -v $DIR:/home/integration -v $DERIVED_DIR:/home/data/derived -v $RAW_DIR:/home/data/raw data-integration /home/integration/$1
}

#docker_run 0-from-raw.sh
#docker_run 1-dump-osm-to-sql.sh
docker_run 2-import-postgres.sh
