#!/bin/bash
set -eu
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $DIR/config.sh

# From https://circleci.com/docs/docker/#caching-docker-layers
if [[ -e ~/docker/image.tar ]]; then docker load -i ~/docker/image.tar; fi
docker run ${DOCKER_IMAGE} echo "Nothing to do"
mkdir -p ~/docker; docker save ${DOCKER_IMAGE} > ~/docker/image.tar
