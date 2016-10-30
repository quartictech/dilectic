#!/bin/bash
set -eu
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $DIR/config.sh

ROOT_DIR=${1}

docker_run() {
  echo "Running $1 in docker"
  docker run --rm --net="host" \
    -v ${ROOT_DIR}/integration:/integration \
    -v ${ROOT_DIR}/data/raw:/data/raw \
    -v ${ROOT_DIR}/data/derived:/data/derived \
    -e "INPUT=/data/raw" \
    -e "OUTPUT=/data/derived" \
    -e "INTEGRATION=/integration" \
    -w /work \
    ${DOCKER_IMAGE} \
    $1
}

docker_run "python3 /integration/run.py"
