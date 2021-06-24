#!/bin/bash
set -ex
mvn package shade:shade
export DOCKER_CLI_EXPERIMENTAL=enabled
docker buildx build -t eclipfs/metaserver --platform=linux/amd64 . --push
docker buildx build -t eclipfs/metaserver-db --platform=linux/amd64 database --push
