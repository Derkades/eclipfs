#!/bin/bash
set -ex
docker build -t derkades/eclipsefs-node .
docker push derkades/eclipsefs-node
