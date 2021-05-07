from flask import Flask, Response, request, abort, jsonify
from time import sleep
import schedule
import dsnapi
from os import environ as env
from pathlib import Path
from requests.exceptions import RequestException
import requests
import random
import shutil
import threading
import hashlib


app = Flask(__name__)


fs_lock = threading.Lock()


def verify_request_auth(typ):
    if 'node_token' not in request.args:
        abort(401, 'Missing node_token')

    if len(env['TOKEN']) != 32:
        raise Exception('Node token should be 32 characters long')

    if typ == 'read':
        token = env['TOKEN'][:16]
    elif typ == 'write':
        token = env['TOKEN']
    else:
        raise Exception('invalid type argument')

    if not request.args['node_token'] == token:
        abort(403, 'Invalid node_token')


def get_chunk_path(chunk_id, mkdirs=False) -> Path:
    base = env['DATA_DIR']
    dir_1 = str(chunk_id // 1_000_000_000)
    dir_2 = str(chunk_id % 1_000_000_000 // 1_000_000)
    dir_3 = str(chunk_id % 1_000_000 // 1_000)
    file_4 = str(chunk_id % 1_000) + '.efs'

    dirs = Path(base, dir_1, dir_2, dir_3)
    if mkdirs:
        dirs.mkdir(parents=True, exist_ok=True)
    return Path(dirs, file_4)


def get_temp_path(temp_id) -> Path:
    # prevent directory traversal by ensuring temp_id is int
    return Path('/tmp/eclipfs-chunkserver-' + str(int(temp_id)) + '.efs')


def read_chunk(chunk_id):
    """
    Read chunk data from local disk

    Parameters:
        inode: File id / inode
        index: Chunk index
    Returns:
        Chunk data, or None if the chunk does not exist locally
    """
    path = get_chunk_path(chunk_id)
    if not path.exists():
        print('WARNING: path', path, 'not found!')
        return None
    with path.open('rb') as file:
        data = file.read()
    return data


@app.route('/ping', methods=['GET'])
def ping():
    verify_request_auth('write')
    return Response(response='pong', content_type="text/plain")


@app.route('/upload', methods=['POST'])
def upload():
    verify_request_auth('write')

    if 'id' not in request.args:
        abort(400, 'Missing id parameter')

    if request.content_type != 'application/octet-stream':
        abort(400, 'Request content type must be application/octet-stream')

    if request.data == b'':
        abort(400, 'Request body is empty')

    temp_id = int(request.args['id'])

    data = request.data

    if len(data) > 1000000:
        abort(404, 'Data too large')

    fs_lock.acquire()

    temp_path = get_temp_path(temp_id)
    print('writing to', temp_path)
    with temp_path.open('wb') as file:
        file.write(data)

    fs_lock.release()
    return Response('ok', content_type='text/plain')


@app.route('/finalize', methods=['POST'])
def finalize():
    if not request.json:
        abort(400, 'request body not valid json')

    if 'temp_id' not in request.json:
        abort(400, 'Missing temp_id parameter')
    if 'chunk_id' not in request.json:
        abort(400, 'Missing chunk parameter')

    temp_id = int(request.json['temp_id'])
    chunk_id = int(request.json['chunk_id'])

    temp_path = get_temp_path(temp_id)

    fs_lock.acquire()
    if not temp_path.exists():
        fs_lock.release()
        print('temp file does not exist', temp_path)
        abort(404, 'temp file does not exist')

    new_path = get_chunk_path(chunk_id, mkdirs=True)

    shutil.move(temp_path, new_path)
    fs_lock.release()
    return Response('ok', content_type='text/plain')


@app.route('/download', methods=['GET'])
def download():
    verify_request_auth('read')

    if 'chunk' not in request.args:
        abort(400, "Missing 'chunk' parameter")

    # inode = int(request.args['file'])
    # index = int(request.args['index'])
    chunk_id = int(request.args['chunk'])

    fs_lock.acquire()
    data = read_chunk(chunk_id)
    fs_lock.release()
    if data is not None:
        return Response(data, content_type='application/octet-stream')
    else:
        return abort(404, 'Chunk not found. Is the token valid and of the correct length?')


@app.route('/replicate', methods=['POST'])
def replicate():
    verify_request_auth('write')

    if 'chunk' not in request.args:
        abort(400, "Missing 'chunk' parameter")

    if 'checksum' not in request.args:
        abort(400, "Missing 'checksum' parameter")

    if 'address' not in request.args:
        abort(400, 'Missing address')

    chunk_id = int(request.args['chunk'])
    checksum = request.args['checksum']
    address = request.args['address']

    try:
        r = requests.post(address, headers={'Content-Type': 'application/octet-stream'})
        if r.status_code == 200:
            data = r.content
            if hashlib.md5(data).hexdigest() != checksum:
                abort(500, 'Checksum mismatch', checksum, 'data len', len(data))

            fs_lock.acquire()
            path = get_chunk_path(chunk_id, mkdirs=True)

            print('replication - writing to', path)
            with open(path, 'wb') as file:
                file.write(data)

            fs_lock.release()

            return Response('ok', content_type='text/plain')
        else:
            abort(500, r.text)
    except RequestException as e:
        abort(500, (address, e))


def announce():
    try:
        (success, response, error_message) = dsnapi.announce()
        if not success:
            print('Unable to contact metaserver:', response, error_message)
    except RequestException as e:
        print('Unable to contact metaserver:', e)


def random_subdir(base: Path):
    subdirs = list(base.iterdir())
    if len(subdirs) == 0:
        return None
    return Path(base, random.choice(subdirs))


def garbage_collect():
    fs_lock.acquire()
    base = Path(env['DATA_DIR'])

    base2 = random_subdir(base)
    if base2 is None:
        print('skip garbage collection, not storing any data')
        fs_lock.release()
        return

    base3 = random_subdir(base2)
    if base3 is None:
        print('No directories in', base2, '- delete')
        base2.rmdir()
        fs_lock.release()
        return

    base4 = random_subdir(base3)
    if base4 is None:
        print('No files in', base3, '- delete')
        base3.rmdir()
        fs_lock.release()
        return

    print('Garbage collecting dir', base4)

    base_int = int(base2.name + '000000000') + int(base3.name + '000000') + int(base4.name + '000')
    chunks = [base_int + int(path.name[:-4]) for path in base4.iterdir()]

    if len(chunks) == 0:
        print('No files in', base4, '- delete')
        base4.rmdir()
        fs_lock.release()
        return

    print('Making request to metaserver for', len(chunks), 'chunks')

    (success, response, error_message) = dsnapi.get('checkGarbage', data={'chunks': chunks})
    if not success:
        print('garbage collection error', response, error_message)
        fs_lock.release()
        return


    garbage = response['garbage']
    for chunk_id in garbage:
        to_delete = Path(base4, str(chunk_id - base_int) + '.efs')
        print('delete', to_delete)
        to_delete.unlink()

    fs_lock.release()


def timers():
    announce()
    schedule.every(7).to(9).seconds.do(announce)
    schedule.every(60).to(120).seconds.do(garbage_collect)
    while True:
        schedule.run_pending()
        sleep(1)


t = threading.Thread(target=timers, args=[])
t.daemon = True # required to exit nicely on SIGINT
t.start()
