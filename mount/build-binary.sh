#!/bin/sh
set -ex
pyinstaller --onefile --name eclipfs src/mount.py
# https://github.com/JonathonReinhart/staticx/
staticx dist/eclipfs dist/eclipfs-static
