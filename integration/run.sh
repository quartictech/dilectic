#!/bin/bash
set -eu
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $DIR/config.sh
ROOT_DIR=$DIR/..

docker_run() {
  echo "Running $1 in docker"
  docker run --rm --net="host" \
    -v ${ROOT_DIR}/integration:/integration \
    -v ${ROOT_DIR}/data/raw:/data/raw \
    -v ${ROOT_DIR}/data/derived:/data/derived \
    -v ${ROOT_DIR}/data/final:/data/final \
    -w /work \
    ${DOCKER_IMAGE} \
    $1
}

docker_run "/integration/integrations.sh"
