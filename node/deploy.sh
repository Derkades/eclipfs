#!/bin/bash
set -ex
docker build -t eclipfs/chunkserver .
docker push eclipfs/chunkserver
