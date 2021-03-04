import api
import sys

if len(sys.argv) == 1:
    r = api.get('directoryListRoot')
elif len(sys.argv) == 2:
    params = {'path' : sys.argv[1]}
    r = api.get('directoryInfo', params=params)
else:
    print('Invalid usage')
    exit(1)

(success, response) = r

if success:
    if len(sys.argv) == 1:
        directories = response['directories']
    else:
        directories = response['directory']['subdirectories']

    if directories:
        for directory in directories:
            print(directory)
    else:
        print('No directories')
else:
    print(f'Error: {response}')
