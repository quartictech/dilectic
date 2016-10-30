#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $DIR/config.sh

docker_run() {
 echo "Running $1 in docker"
 docker run -w /home/integration --net="host" -v $DIR:/home/integration -v $DERIVED_DIR:/home/data/derived -v $RAW_DIR:/home/data/raw data-integration $1
}

docker_run "python3 /home/integration/dodo.py"
