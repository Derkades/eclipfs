from time import sleep
from os import environ as env
from pathlib import Path
import random
import shutil
import threading
import hashlib
import logging
from uuid import uuid4

import requests
from requests.exceptions import RequestException
from flask import Flask, Response, request, abort

import schedule
import dsnapi


app = Flask(__name__)
fs_lock = threading.Lock()
temp_uuid = uuid4().hex


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
    return Path('/tmp/eclipfs-chunkserver-' + temp_uuid + '-' + str(int(temp_id)) + '.efs')


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
        log.warning('Path %s not found!', path)
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
    log.debug('Writing to temporary file %s', temp_path)
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
        log.warning('Temp file does not exist: %s', temp_path)
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

    log.debug('replication request: chunk_id=%s checksum=%s address=%s', chunk_id, checksum, address)

    try:
        r = requests.get(address, headers={'Content-Type': 'application/octet-stream'})
        if r.status_code == 200:
            data = r.content
            if hashlib.md5(data).hexdigest() != checksum:
                abort(500, 'Checksum mismatch', checksum, 'data len', len(data))

            fs_lock.acquire()
            path = get_chunk_path(chunk_id, mkdirs=True)

            log.info('Replication: writing to %s', path)
            with open(path, 'wb') as file:
                file.write(data)

            fs_lock.release()

            return Response('ok', content_type='text/plain')
        else:
            log.warning('Failed to make replication request to %s: %s', address, r.text)
            abort(500, r.text)
    except RequestException as e:
        log.warning('Failed to make replication request to %s: %s', address, e)
        abort(500, (address, e))


def announce():
    try:
        (success, response, error_message) = dsnapi.announce()
        if not success:
            log.warning('Unable to contact metaserver: %s %s', response, error_message)
    except (requests.ConnectionError, RequestException) as e:
        log.warning('Unable to contact metaserver: %s', e)


def random_subdir(base: Path):
    subdirs = list(base.iterdir())
    if len(subdirs) == 0:
        return None
    return Path(base, random.choice(subdirs))


def garbage_collect():
    try:
        log.info('Starting garbage collection')
        fs_lock.acquire()
        base = Path(env['DATA_DIR'])

        base2 = random_subdir(base)
        if base2 is None:
            log.info('Skip garbage collection, not storing any data')
            fs_lock.release()
            return

        base3 = random_subdir(base2)
        if base3 is None:
            log.info('No directories in %s, delete.', base2)
            base2.rmdir()
            fs_lock.release()
            return

        base4 = random_subdir(base3)
        if base4 is None:
            log.info('No directories in %s, delete.', base3)
            base3.rmdir()
            fs_lock.release()
            return

        log.debug('Garbage collecting dir %s', base4)

        base_int = int(base2.name + '000000000') + int(base3.name + '000000') + int(base4.name + '000')
        chunks = [base_int + int(path.name[:-4]) for path in base4.iterdir()]

        if len(chunks) == 0:
            log.info('No files in %s, delete.', base4)
            base4.rmdir()
            fs_lock.release()
            return

        log.debug('Garbage collection - Making request to metaserver for %s chunks', len(chunks))

        (success, response, error_message) = dsnapi.get('checkGarbage', data={'chunks': chunks})
        if not success:
            log.error('Garbage collection error %s %s', response, error_message)
            fs_lock.release()
            return

        garbage = response['garbage']
        log.info('Garbage collection - deleting %s files', len(garbage))
        for chunk_id in garbage:
            to_delete = Path(base4, str(chunk_id - base_int) + '.efs')
            log.debug('deleting file %s', to_delete)
            to_delete.unlink()

        fs_lock.release()
    except (requests.ConnectionError, RequestException) as e:
        fs_lock.release()
        log.warning('Unable to contact metaserver for garbage collection: %s', e)


def timers():
    announce()
    schedule.every(13).to(16).seconds.do(announce)
    schedule.every(60).to(120).seconds.do(garbage_collect)
    while True:
        schedule.run_pending()
        sleep(1)


def init_logging():
    formatter = logging.Formatter('%(asctime)s.%(msecs)03d %(threadName)s: '
                                '[%(name)s] %(message)s', datefmt="%Y-%m-%d %H:%M:%S")
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    if 'DEBUG' in env:
        handler.setLevel(logging.DEBUG)
        root_logger.setLevel(logging.DEBUG)
    else:
        handler.setLevel(logging.INFO)
        root_logger.setLevel(logging.INFO)
    root_logger.addHandler(handler)

    global log
    log = root_logger


init_logging()

t = threading.Thread(target=timers, args=[])
t.daemon = True  # required to exit nicely on SIGINT
t.start()
