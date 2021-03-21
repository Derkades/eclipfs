import os
from os import environ as env

METASERVER = env['METASERVER']
USERNAME = env['USERNAME']
PASSWORD = env['PASSWORD']

PREFERRED_LOCATION = env['LOCATION']

MOUNT_UID = os.getuid()
MOUNT_GID = os.getgid()
MODE_DIR = 0o777
MODE_FILE = 0o666

REQUEST_TIMEOUT = 15

# Do not change this
CHUNKSIZE = 1_000_000
ROOT_INODE = 1
