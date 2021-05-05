#!/bin/bash
set -ex
mvn package shade:shade
docker build -t eclipfs/metaserver .
docker push eclipfs/metaserver
docker build -t eclipfs/metaserver-db database
docker push eclipfs/metaserver-db
