#!/bin/bash
export DOCKER_CLI_EXPERIMENTAL=enabled
docker buildx build -t eclipfs/chunkserver --platform=linux/arm,linux/arm64,linux/amd64 . --push
