import os
import sys
from pathlib import Path

DIR_ENTRIES = 100
WRITE = False
WRITE_SIZE = 1000

if WRITE:
    WRITE_BYTES = b'a' * WRITE_SIZE

def create_dirs(base):
    for i in range(DIR_ENTRIES):
        d = Path(base, str(i))
        d.mkdir()
        for j in range(DIR_ENTRIES):
            d2 = Path(d, str(j))
            d2.mkdir()
            for k in range(DIR_ENTRIES):
                f = Path(d2, str(k))
                f.touch()
                if WRITE:
                    with f.open('wb') as open_file:
                        open_file.write(WRITE_BYTES)
                print(f)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print('Usage:', sys.argv[0], '<dir>')
        exit(1)

    base = Path(sys.argv[1])
    if not base.is_dir():
        print('Not a directory:', base)
        exit(1)

    create_dirs(base)
