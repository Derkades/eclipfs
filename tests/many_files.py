import os
import sys
from pathlib import Path

def create_dirs(base):
    for i in range(200):
        d = Path(base, str(i))
        d.mkdir()
        for j in range(200):
            d2 = Path(d, str(j))
            d2.mkdir()
            for k in range(200):
                f = Path(d2, str(k))
                f.touch()
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
