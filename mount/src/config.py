import os
from os import environ as env

METASERVER = env['METASERVER']
USERNAME = env['USERNAME']
PASSWORD = env['PASSWORD']

PREFERRED_LOCATION = env['LOCATION']

# Owner for files, set to a user ID or os.getuid() for the user running the program
MOUNT_UID = os.getuid()
MOUNT_GID = os.getgid()

# Permission mode for directories and files
MODE_DIR = 0o777
MODE_FILE = 0o666

REQUEST_TIMEOUT = 15
REQUEST_RETRY_COUNT = 120
REQUEST_BACKOFF_FACTOR = 0.1
REQUEST_BACKOFF_MAX = 1

MAX_WRITE_BUFFER_SIZE = 5

# Max time to keep entries in read cache. Note that the read cache doesn't have
# a maximum size. For example, if you transfer files at 20MB/s with READ_CACHE_TIME=30
# it will use ~600MB of system memory. Python may use way more, but this
# is memory that will be garbage collected when the system needs it.
READ_CACHE_TIME = 30

# Will improve write throughput from ~5MB/s to ~20MB/s but can affect system stability when the filesystem does
# not respond. Only enable if "writing" to the filesystem is the bottleneck instead of uploading over the network.
ENABLE_WRITEBACK_CACHE = False

# Do not change this for an existing filesystem
# CHUNKSIZE = 1_000_000

# Do not change, ever.
ROOT_INODE = 1
