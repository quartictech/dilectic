#!/bin/sh

docker_run() {
 echo "Running $1 in docker"
 sudo docker run -v $PWD/derived/:/home/data/out  -v $PWD/raw/:/home/data/raw -v $(realpath $1):/home/script.sh data-integration sh /home/script.sh
}

docker_run integrations.sh
