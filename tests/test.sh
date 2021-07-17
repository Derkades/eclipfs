#!/bin/bash
set -e

# if [[ $EUID -ne 0 ]]; then
#    echo "This script must be run as root"
#    exit 1
# fi

echo 'Stopping containers'
docker-compose rm -sf

echo 'Building metaserver'
cd ../metaserver
mvn package shade:shade
docker build -t eclipfs/metaserver .
echo 'Building database'
docker build -t eclipfs/metaserver-db database
cd ..
echo 'Building mount'
docker build -t eclipfs/mount mount
echo 'Building chunkserver'
docker build -t eclipfs/chunkserver chunkserver
cd tests

echo 'Starting database'
docker-compose up -d db
docker-compose logs -f db &
sleep 4
kill %1
# fail test if container is not running
docker ps | grep tests_db_1

echo 'Starting metaserver'
docker-compose up -d metaserver
docker-compose logs -f metaserver &
sleep 3
kill %1
docker ps | grep tests_metaserver_1

echo 'Creating node entries in database'
docker-compose exec db psql -U eclipfs -d eclipfs -c "INSERT INTO node (token, location, name) VALUES ('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'loc1', 'node1');"
docker-compose exec db psql -U eclipfs -d eclipfs -c "INSERT INTO node (token, location, name) VALUES ('bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb', 'loc2', 'node2');"
docker-compose exec db psql -U eclipfs -d eclipfs -c "SELECT * FROM node;"

echo 'Creating user entry in database'
# unhashed password is "password"
docker-compose exec db psql -U eclipfs -d eclipfs -c "INSERT INTO \"user\" (username, password, write_access) VALUES ('testuser', '"'$2y$04$AM6u27QsMCU5QkXIiRQqh.xMuteycYZ44g2/D4XpCpIVLfGwvLG1u'"', 't');"
docker-compose exec db psql -U eclipfs -d eclipfs -c "SELECT * FROM \"user\";"

echo 'Starting nodes'
docker-compose up -d chunk1 chunk2
docker-compose logs -f chunk1 chunk2 &
sleep 2
kill %1
docker ps | grep tests_chunk1_1
docker ps | grep tests_chunk2_1

echo 'Starting mount'
set +e
sudo umount ./mount
set -e
docker-compose up -d mount
docker-compose logs -f mount &
sleep 3
kill %1
docker ps | grep tests_mount_1

mount | grep "$(pwd)/mount"

docker-compose logs -f mount &
echo 'Creating test file'
echo "this is a test file" > ./mount/textfile
echo 'waiting some time before stopping mount'
sleep 5
kill %1

docker ps | grep tests_mount_1

echo 'Stopping mount'
docker-compose rm -sf mount
sudo umount ./mount

echo 'Starting mount to check file'
docker-compose up -d mount
docker-compose logs -f mount &
sleep 3
kill %1
docker ps | grep tests_mount_1

docker-compose logs -f mount &
grep "this is a test file" ./mount/textfile
kill %1

echo 'Stopping mount'
docker-compose rm -sf mount
sudo umount ./mount

jobs

echo 'Stopping containers'
docker-compose rm -sf
